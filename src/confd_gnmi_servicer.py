import logging
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from enum import Enum

import grpc

import gnmi_pb2
from confd_gnmi_common import PORT, get_timestamp_ns
from gnmi_pb2_grpc import gNMIServicer, add_gNMIServicer_to_server

log = logging.getLogger('confd_gnmi_servicer')
class AdapterType(Enum):
    DEMO = 0
    API = 1
    NETCONF = 2

class ConfDgNMIServicer(gNMIServicer):

    # parameterized constructor
    def __init__(self, adapter_type):
        self.adapter_type = adapter_type
        assert isinstance(self.adapter_type, AdapterType)

    @staticmethod
    def extract_user_metadata(context):
        log.debug("=>")
        log.debug("metadata=%s", context.invocation_metadata())
        metadict = dict(context.invocation_metadata())
        username = metadict["username"]
        password = metadict["password"]
        log.debug("<= username=%s password=:-)", username)
        return username, password

    def get_and_connect_adapter(self, username, password):
        log.debug("==> self.adapter_type=%s username=%s password=:-)",
                  self.adapter_type, username)
        adapter = None
        if self.adapter_type == AdapterType.DEMO:
            from confd_gnmi_demo_adapter import GnmiDemoServerAdapter
            adapter = GnmiDemoServerAdapter.get_adapter()
        elif self.adapter_type == AdapterType.API:
            from confd_gnmi_api_adapter import GnmiConfDApiServerAdapter
            adapter = GnmiConfDApiServerAdapter.get_adapter()
            adapter.authenticate(username=username, password=password)
        log.debug("<== adapter=%s", adapter)
        return adapter

    @staticmethod
    def get_val_encoding(val):
        log.debug("==> val=%s", val)
        encoding = None
        if val.HasField("json_val"):
            encoding = gnmi_pb2.Encoding.JSON
        elif val.HasField("json_ietf_val"):
            encoding = gnmi_pb2.Encoding.JSON_IETF
        elif val.HasField("ascii_val"):
            encoding = gnmi_pb2.Encoding.ASCII
        elif val.HasField("bytes_val"):
            encoding = gnmi_pb2.Encoding.BYTES
        else:
            if any(val.HasField(a) for a in
                   ["string_val", "int_val", "uint_val", "bool_val",
                    "bytes_val", "float_val", "leaflist_val"]):
                encoding = gnmi_pb2.Encoding.PROTO
        log.debug("<== encoding=%s", encoding)
        return encoding

    @staticmethod
    def _ensure_encoding_supported(encoding, adapter, context):
        log.debug("==> encoding=%s", encoding)
        if encoding not in adapter.encodings():
            text = f'gNMI: unsupported encoding: {encoding}'
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(text)
            raise NotImplementedError(text)
        log.debug("==>")

    def verify_updates_encoding_supported(self, updates, adapter, context):
        log.debug("==> updates=%s", updates)
        for u in updates:
            encoding = self.get_val_encoding(u.val)
            self._ensure_encoding_supported(encoding, adapter, context)
        log.debug("==>")

    def verify_encoding_supported(self, encoding, adapter, context):
        log.debug("==> encoding=%s", encoding)
        if not encoding:
            encoding = gnmi_pb2.Encoding.JSON
        self._ensure_encoding_supported(encoding, adapter, context)
        log.debug("==>")

    def get_connected_adapter(self, context):
        """
        Get adapter and connect it to ConfD if needed
        Currently we always create new instance, later on
        a pool of adapters can be maintained.
        :param context:
        :return:
        """
        log.debug("==>")
        (username, password) = self.extract_user_metadata(context)
        adapter = self.get_and_connect_adapter(username=username,
                                               password=password)
        log.debug("<== adapter=%s", adapter)
        return adapter

    def Capabilities(self, request, context):
        """Capabilities allows the client to retrieve the set of capabilities
        that is supported by the target. This allows the target to validate the
        service version that is implemented and retrieve the set of models that
        the target supports. The models can then be specified in subsequent RPCs
        to restrict the set of data that is utilized.
        Reference: gNMI Specification Section 3.2
        """
        log.info("==> request=%s context=%s", request, context)

        adapter = self.get_connected_adapter(context)

        supported_models = [
            gnmi_pb2.ModelData(
                name=cap.name,
                organization=cap.organization,
                version=cap.version
            ) for cap in adapter.capabilities()
        ]

        supported_encodings = adapter.encodings()

        response = gnmi_pb2.CapabilityResponse(
            supported_models=supported_models,
            supported_encodings=supported_encodings,
            gNMI_version="proto3",
            extension=[])
        # context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        # context.set_details('Method not implemented!')
        # raise NotImplementedError('Method not implemented!')
        log.info("<== response=%s", response)
        return response

    def Get(self, request, context):
        """
        Retrieve a snapshot of data from the target. A Get RPC requests that
        the target snapshots a subset of the data tree as specified by the paths
        included in the message and serializes this to be returned to the
        client using the specified encoding.
        Reference: gNMI Specification Section 3.3
        """
        log.info("==> request=%s context=%s", request, context)
        adapter = self.get_connected_adapter(context)
        self.verify_encoding_supported(request.encoding, adapter, context)
        notifications = adapter.get(request.prefix, request.path,
                                    request.type, request.use_models)
        response = gnmi_pb2.GetResponse(notification=notifications)

        log.info("<== response=%s", response)
        return response

    def Set(self, request, context):
        """Set allows the client to modify the state of data on the target. The
        paths to modified along with the new values that the client wishes
        to set the value to.
        Reference: gNMI Specification Section 3.4
        """
        log.info("==> request=%s context=%s", request, context)
        adapter = self.get_connected_adapter(context)
        self.verify_updates_encoding_supported(request.update, adapter, context)
        # TODO for now we do not process replace list
        with adapter.update_transaction(request.prefix) as trans:
            ops = trans.delete(request.delete) + trans.update(request.update)

        # Note: UpdateResult timestamp is deprecated, setting to -1
        results = [gnmi_pb2.UpdateResult(timestamp=-1, path=path, op=op)
                   for path, op in ops]

        response = gnmi_pb2.SetResponse(prefix=request.prefix,
                                        response=results, timestamp=get_timestamp_ns())

        log.info("<== response=%s", response)
        return response

    @staticmethod
    def _read_sub_request(request_iterator, handler, stop_on_end=False):
        log.debug("==> stop_on_end=%s", stop_on_end)
        try:
            for req in request_iterator:
                # TODO check req mode is POLL
                log.debug("req=%s", req)
                if hasattr(req, "poll"):
                    handler.poll()
                else:
                    # TODO exception, not expected other type of request
                    assert False
        except grpc.RpcError as e:
            # check if this is end of Poll sending
            if handler.is_poll():
                log.exception(e)
        log.debug("Request loop ended.")
        if stop_on_end:
            log.debug("stopping handler")
            handler.stop()
        log.debug("<== _read_sub_request finished")

    def Subscribe(self, request_iterator, context):
        """Subscribe allows a client to request the target to send it values
        of particular paths within the data tree. These values may be streamed
        at a particular cadence (STREAM), sent one off on a long-lived channel
        (POLL), or sent as a one-off retrieval (ONCE).
        Reference: gNMI Specification Section 3.5
        """
        log.info(
            "==> request_iterator=%s context=%s", request_iterator, context)

        def subscribe_rpc_done():
            log.info("==>")
            if not handler.is_once():
                handler.stop()
            log.info("<==")

        request = next(request_iterator)
        adapter = self.get_connected_adapter(context)
        self.verify_encoding_supported(request.subscribe.encoding, adapter,
                                       context)
        context.add_callback(subscribe_rpc_done)
        # first request, should contain subscription list (`subscribe`)
        assert hasattr(request, "subscribe")
        handler = adapter.get_subscription_handler(request.subscribe)

        thr = None
        if not handler.is_once():
            thr = threading.Thread(target=ConfDgNMIServicer._read_sub_request,
                                   args=(request_iterator, handler,
                                         handler.is_poll()))
            thr.start()
        # `yield from` can be used, but to allow altering (e.g. path conversion)
        # response later on we use `for`
        for response in handler.read():
            log.debug("response received, calling yield")
            yield response

        if thr is not None:
            thr.join()
        log.info("<==")

    @staticmethod
    def serve(port=PORT, adapter_type=AdapterType.DEMO, insecure=False,
              key_file=None, crt_file=None):
        log.info("==> port=%s adapter_type=%s", port, adapter_type)

        server = grpc.server(ThreadPoolExecutor(max_workers=10))
        add_gNMIServicer_to_server(ConfDgNMIServicer(adapter_type), server)
        if insecure:
            server.add_insecure_port("[::]:{}".format(port))
        else:
            assert key_file is not None and crt_file is not None
            with open(key_file, "rb") as k, open(crt_file, "rb") as c:
                key = k.read()
                crt = c.read()
            server.add_secure_port("[::]:{}".format(port),
                                   grpc.ssl_server_credentials([(key, crt)]))
        server.start()
        log.info("<== server=%s", server)
        return server
