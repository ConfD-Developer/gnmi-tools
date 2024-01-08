import itertools
import json
import subprocess
import threading
import xml.etree.cElementTree as ET
from time import sleep

import grpc
import pytest

import gnmi_pb2
from confd_gnmi_client import ConfDgNMIClient
from confd_gnmi_common import make_gnmi_path, datatype_str_to_int, \
    make_formatted_path, get_timestamp_ns
from confd_gnmi_demo_adapter import GnmiDemoServerAdapter
from confd_gnmi_servicer import AdapterType, ConfDgNMIServicer
from utils.utils import log, nodeid_to_path


@pytest.mark.grpc
@pytest.mark.usefixtures("fix_method")
class GrpcBase:
    NS_IETF_INTERFACES = GnmiDemoServerAdapter.NS_INTERFACES
    NS_OC_INTERFACES = "openconfig-interfaces:"

    PREFIX_MAP = {
        NS_IETF_INTERFACES: "if:",
        NS_OC_INTERFACES: "oc-if:"
    }

    @pytest.fixture
    def fix_method(self, request):
        log.debug("==> fixture method setup request={}".format(request))
        # set_logging_level(logging.DEBUG)
        nodeid_path = nodeid_to_path(request.node.nodeid)
        log.debug("request.fixturenames=%s", request.fixturenames)
        self.set_adapter_type()
        self.server = ConfDgNMIServicer.serve(adapter_type=self.adapter_type, insecure=True)
        self.client = ConfDgNMIClient(insecure=True)
        log.debug("<== fixture method setup")
        yield
        log.debug("==> fixture method teardown (nodeid %s)" % nodeid_path)
        self.client.close()
        self.server.stop(0)
        self.server.wait_for_termination()
        log.debug("<== fixture method teardown")

    @pytest.fixture(autouse=True)
    def _setup(self):
        self.leaves = ["name", "type", "enabled"]
        self.leaf_paths_str = [f"interface[name={{}}if_{{}}]/{leaf}" for leaf in self.leaves]
        self.list_paths_str = ["interface[name={}if_{}]", "interface", "ietf-interfaces:interfaces{}" ]
        self.leaf_paths_str_for_gnmi_tools = [
            ("/top/empty-leaf", [None]),
            ("/top/down/str-leaf", "test3"),
            ("/top/down/int-leaf", 44),
            ("/top/pres", {}),
            ("/top-pres/empty-leaf", [None]),
            ("/top-pres/down/str-leaf", "test4"),
            ("/top-pres/down/int-leaf", 10),
            ("/top-pres/pres", {}),
            ("/double-key-list[type=t1][name=n1]/admin-state", "In-Service"),
            ("/double-key-list[name=1010/0/AD-2-RX]/admin-state", "In-Service",
             {"check_path": False}),
            ("/double-key-list[name=1010/0/AD-2-RX][type=opticalTransport]/admin-state", "In-Service"),
            ("/double-key-list[name=\"1010/0/[AD 4-11]-1-RX\"][type=opticalTransport]/admin-state", "In-Service"),
            ("/double-key-list[name=\"ab[cd\"][type=opticalTransport]/admin-state", "In-Service")
        ]

    @staticmethod
    def mk_gnmi_if_path(path_str, if_state_str="", if_id=None):
        if if_id is not None and if_state_str is not None:
            path_str = path_str.format(if_state_str, if_id)
        return make_gnmi_path(path_str)

    def _do_reset_cfg(self):
        raise NotImplementedError

    @pytest.fixture
    def reset_cfg(self):
        yield from self._do_reset_cfg()


class AdapterTests(GrpcBase):
    def test_capabilities(self, request):
        log.info("testing capabilities")
        capabilities = self.client.get_capabilities()

        def capability_supported(cap):
            supported = False
            for s in capabilities.supported_models:
                if s.name == cap['name'] and s.organization == cap['organization']:
                    log.debug("capability cap=%s found in s=%s", cap, s)
                    supported = True
                    break
            return supported

        # check if selected capabilities are supported
        for cap in GnmiDemoServerAdapter.capability_list:
            assert capability_supported(cap)

        # mandatory according to standard
        assert gnmi_pb2.Encoding.JSON in capabilities.supported_encodings
        # supported by this codebase as well
        assert gnmi_pb2.Encoding.JSON_IETF in capabilities.supported_encodings

    @staticmethod
    def assert_update(update, path_val):
        """
        Asserts that the update matches the expected path and value.

        Args:
            update (Update): The update object to be checked.
            path_val (tuple): A tuple containing the expected path and value and optional options map

        Raises:
            AssertionError: If the update does not match the expected path and value.
        """
        # Check if the path should be validated
        check_path = True

        # Check if the options attribute map has the "check_path" key
        if len(path_val) >= 3 and "check_path" in path_val[2]:
            check_path = path_val[2]["check_path"]

        # Validate the path if required
        if check_path:
            assert update.path == path_val[0]

        # Parse the json_ietf_val attribute of the update object
        json_value = json.loads(update.val.json_ietf_val)

        # Assert that the parsed json value matches the expected value
        assert json_value == path_val[1]

    @staticmethod
    def assert_set_response(response, path_op):
        assert (response.path == path_op[0])
        assert (response.op == path_op[1])

    @staticmethod
    def assert_updates(updates, path_vals):
        assert (len(updates) == len(path_vals))
        for i, u in enumerate(updates):
            AdapterTests.assert_update(u, path_vals[i])

    @staticmethod
    def assert_one_in_update(updates, pv):
        assert any(u.path == pv[0] and json.loads(u.val.json_ietf_val) == pv[1]
                   for u in updates)

    @staticmethod
    def assert_in_updates(updates, path_vals):
        log.debug("==> updates=%s path_vals=%s", updates, path_vals)
        assert (len(updates) == len(path_vals))
        for pv in path_vals:
            AdapterTests.assert_one_in_update(updates, pv)
        log.debug("<==")


    def verify_get_response_updates(self, prefix, paths, path_value,
                                    datatype, encoding, assert_fun=None):
        if assert_fun is None:
            assert_fun = AdapterTests.assert_updates
        log.debug("prefix=%s paths=%s pv_list=%s datatype=%s encoding=%s",
                  prefix, paths, path_value, datatype, encoding)
        time_before = get_timestamp_ns()
        get_response = self.client.get(prefix, paths, datatype, encoding)
        time_after = get_timestamp_ns()
        log.debug("notification=%s time_before=%i time_after=%i",
                  get_response.notification, time_before, time_after)
        for n in get_response.notification:
            log.debug("n=%s", n)
            if prefix:
                assert (n.prefix == prefix)
            assert(time_before <= n.timestamp and n.timestamp <= time_after)
            assert_fun(n.update, path_value)

    def verify_sub_sub_response_updates(self, prefix, paths, path_value,
                                        assert_fun=None,
                                        subscription_mode=gnmi_pb2.SubscriptionList.ONCE,
                                        poll_interval=0,
                                        poll_count=0, read_count=-1,
                                        sample_interval = None,
                                        encoding=gnmi_pb2.Encoding.JSON_IETF,
                                        allow_aggregation=True):
        '''
        Invoke subscription and verify received updates
        :param prefix:  gNMI prefix for subscription
        :param paths:   gNMI path for subscription
        :param path_value: array of tuples of expected (path,value) for each response
                           path is gNMI path, val is response value (in json)
        :param assert_fun: function to verify updates according to path_value
        :param subscription_mode:
        :param poll_interval: interval between polls (for gnmi_pb2.SubscriptionList.POLL only)
        :param poll_count: number of polls (for gnmi_pb2.SubscriptionList.POLL only)
        :param read_count: finish after the number of responses is read
              (for  gnmi_pb2.SubscriptionList.POLL or when sample_interval is used)
        :param sample_interval: interval for sample  in ms (for gnmi_pb2.SubscriptionList.STREAM only)
        :param encoding: encoding to use (implemented only JSON_IETF)
        :param allow_aggregation: allow aggregation of results in updates
        '''
        if assert_fun is None:
            assert_fun = AdapterTests.assert_updates

        stream_mode =  gnmi_pb2.SubscriptionMode.ON_CHANGE
        if sample_interval is not None:
            assert  subscription_mode == gnmi_pb2.SubscriptionList.STREAM
            stream_mode =  gnmi_pb2.SubscriptionMode.SAMPLE

        log.debug("paths=%s path_value=%s", paths, path_value)
        response_count = 0
        pv_idx = 0
        for pv in path_value:
            if not isinstance(pv, list):
                pv_idx = -1
                break
        log.debug("pv_idx=%s", pv_idx)

        def read_subscribe_responses(responses, read_count=-1):
            nonlocal response_count, pv_idx
            prev_response_time_ms = 0
            SAMPLE_THRESHOLD = 300
            for response in responses:
                time_after = get_timestamp_ns()
                log.debug("response=%s response_count=%i time_after=%i", response,
                          response_count, time_after)
                response_time_ms = time_after/1000000
                if response.sync_response:
                    log.debug("sync_response")
                    assert response_count == 1  # sync expected only after first response
                else:
                    response_count += 1
                    assert (time_before <= response.update.timestamp and
                            response.update.timestamp <= time_after)
                    if prefix:
                        assert (response.update.prefix == prefix)
                    pv_to_check = path_value
                    if pv_idx != -1:
                        assert pv_idx < len(path_value)
                        pv_to_check = path_value[pv_idx]
                        pv_idx += 1
                    if len(pv_to_check) > 0:  # skip empty arrays
                        assert_fun(response.update.update, pv_to_check)
                    log.debug("response_count=%i pv_idx=%i", response_count, pv_idx)
                    if sample_interval and response_count > 1:
                        assert (response_time_ms > (prev_response_time_ms + sample_interval - SAMPLE_THRESHOLD)) and (
                                response_time_ms < (prev_response_time_ms + sample_interval + SAMPLE_THRESHOLD))

                    if read_count > 0:
                        read_count -= 1
                        if read_count == 0:
                            log.info("read count reached")
                            break
                prev_response_time_ms = response_time_ms
                log.debug("Getting next response. read_count=%s response_count=%s",
                          read_count, response_count)
            assert read_count == -1 or read_count == 0

        read_fun = read_subscribe_responses
        subscription_list = \
            ConfDgNMIClient.make_subscription_list(prefix,
                                                   paths,
                                                   subscription_mode,
                                                   encoding,
                                                   stream_mode = stream_mode,
                                                   sample_interval_ms=sample_interval,
                                                   allow_aggregation=allow_aggregation)

        time_before = get_timestamp_ns()
        responses = self.client.subscribe(subscription_list,
                                          read_fun=read_fun,
                                          poll_interval=poll_interval,
                                          poll_count=poll_count,
                                          read_count=read_count)

        log.debug("responses=%s", responses)
        if poll_count:
            assert (poll_count + 1 == response_count)

    def _test_get_subscribe(self, is_subscribe=False,
                            datatype=gnmi_pb2.GetRequest.DataType.CONFIG,
                            subscription_mode=gnmi_pb2.SubscriptionList.ONCE,
                            poll_interval=0,
                            poll_count=0, read_count=-1,
                            sample_interval = None,
                            encoding = gnmi_pb2.Encoding.JSON_IETF,
                            allow_aggregation=True):

        kwargs = {"assert_fun": AdapterTests.assert_updates}
        if_state_str = prefix_state_str = ""
        db = GnmiDemoServerAdapter.get_adapter().demo_db
        if datatype == gnmi_pb2.GetRequest.DataType.STATE:
            prefix_state_str = "-state"
            if_state_str = "state_"
            db = GnmiDemoServerAdapter.demo_state_db
        map_db = GnmiDemoServerAdapter._demo_db_to_key_elem_map(db)
        prefix = make_gnmi_path("/ietf-interfaces:interfaces{}".format(prefix_state_str))
        kwargs["prefix"] = prefix
        if_id = 8
        leaf_paths = [AdapterTests.mk_gnmi_if_path(leaf_paths_str,
                                                   if_state_str,
                                                   if_id)
                      for leaf_paths_str in self.leaf_paths_str]
        list_paths = [
            AdapterTests.mk_gnmi_if_path(self.list_paths_str[0], if_state_str,
                                         if_id),
            AdapterTests.mk_gnmi_if_path(self.list_paths_str[1]),
            AdapterTests.mk_gnmi_if_path(self.list_paths_str[2].format(prefix_state_str)),
        ]
        ifname = "{}if_{}".format(if_state_str, if_id)

        if is_subscribe:
            verify_response_updates = self.verify_sub_sub_response_updates
            kwargs["subscription_mode"] = subscription_mode
            kwargs["poll_interval"] = poll_interval
            kwargs["poll_count"] = poll_count
            kwargs["read_count"] = read_count
            kwargs["sample_interval"] = sample_interval
            kwargs["allow_aggregation"] = allow_aggregation
        else:
            verify_response_updates = self.verify_get_response_updates
            kwargs["datatype"] = datatype

        kwargs["encoding"] = encoding
        kwargs["paths"] = [leaf_paths[0]]
        kwargs["path_value"] = [(leaf_paths[0], ifname)]
        verify_response_updates(**kwargs)
        kwargs["paths"] = [leaf_paths[1]]
        kwargs["path_value"] = [(leaf_paths[1], "iana-if-type:gigabitEthernet")]
        verify_response_updates(**kwargs)
        kwargs["paths"] = leaf_paths
        pv = [(leaf_paths[0], ifname),
              (leaf_paths[1], "iana-if-type:gigabitEthernet")]
        if datatype != gnmi_pb2.GetRequest.DataType.STATE:
            pv.append((leaf_paths[2], True))
        kwargs["path_value"] = pv
        verify_response_updates(**kwargs)
        kwargs["paths"] = [list_paths[0]]
        leaves = self.leaves
        vals = ["iana-if-type:gigabitEthernet", True]
        if datatype == gnmi_pb2.GetRequest.DataType.STATE:
            leaves = leaves[:-1]
            vals = vals[:-1]
        if allow_aggregation:
            kwargs["path_value"] = [(list_paths[0],
                                     dict(zip(leaves, [ifname] + vals)))]
        else:
            kwargs["path_value"] = pv

        verify_response_updates(**kwargs)
        kwargs["paths"] = [list_paths[1]]
        if allow_aggregation:
            pv = [(AdapterTests.mk_gnmi_if_path(self.list_paths_str[0], if_state_str, i),
                   dict(zip(leaves, [f"{if_state_str}if_{i}"] + vals)))
                  for i in range(1, GnmiDemoServerAdapter.num_of_ifs+1)]
        else:
            pv = []
            for i in range(1, GnmiDemoServerAdapter.num_of_ifs+1):
                pv.append((AdapterTests.mk_gnmi_if_path(self.leaf_paths_str[0], if_state_str, i),
                           f"{if_state_str}if_{i}"))
                pv.append((AdapterTests.mk_gnmi_if_path(self.leaf_paths_str[1], if_state_str, i),
                           "iana-if-type:gigabitEthernet"))
                if datatype != gnmi_pb2.GetRequest.DataType.STATE:
                    pv.append((AdapterTests.mk_gnmi_if_path(self.leaf_paths_str[2],
                                                            if_state_str,
                                                            i),
                               True))

        kwargs["path_value"] = pv
        kwargs["assert_fun"] = AdapterTests.assert_in_updates
        verify_response_updates(**kwargs)
        if allow_aggregation:
            kwargs["paths"] = [list_paths[2]]
            if allow_aggregation:
                kwargs["path_value"] = [(list_paths[2],
                                        {"interface": list(map_db.values())})]
            kwargs["assert_fun"] = None
            kwargs["prefix"] = None
            verify_response_updates(**kwargs)

    @pytest.mark.parametrize("data_type", ["CONFIG", "STATE"])
    def test_get(self, request, data_type):
        log.info("testing get")
        self._test_get_subscribe(datatype=datatype_str_to_int(data_type))

    def encoding_test_decorator(self, func):
        capabilities = self.client.get_capabilities()
        for encoding in [gnmi_pb2.Encoding.JSON, gnmi_pb2.Encoding.BYTES,
                         gnmi_pb2.Encoding.PROTO, gnmi_pb2.Encoding.ASCII,
                         gnmi_pb2.Encoding.JSON_IETF,
                         gnmi_pb2.Encoding.JSON_IETF + 100]:
            try:
                log.debug("testing encoding=%s", encoding)
                func(encoding)
            except grpc.RpcError as e:
                if encoding in capabilities.supported_encodings:
                    raise
                else:
                    assert e.code() == grpc.StatusCode.UNIMPLEMENTED

    @pytest.mark.parametrize("data_type", ["CONFIG", "STATE"])
    def test_get_encoding(self, request, data_type):
        log.info("testing get_encoding")

        @self.encoding_test_decorator
        def test_it(encoding):
            self._test_get_subscribe(datatype=datatype_str_to_int(data_type),
                                     encoding=encoding)

    @pytest.mark.parametrize("allow_aggregation", [True, False], ids=["aggr", "no-aggr"])
    @pytest.mark.parametrize("data_type", ["CONFIG", "STATE"])
    def test_subscribe_once(self, request, data_type, allow_aggregation):
        log.info("testing subscribe_once")
        self._test_get_subscribe(is_subscribe=True,
                                 datatype=datatype_str_to_int(data_type),
                                 allow_aggregation=allow_aggregation)

    @pytest.mark.parametrize("allow_aggregation", [True, False], ids=["aggr", "no-aggr"])
    @pytest.mark.parametrize("data_type", ["CONFIG", "STATE"])
    def test_subscribe_once_encoding(self, request, data_type, allow_aggregation):
        log.info("testing subscribe_once_encoding")

        @self.encoding_test_decorator
        def test_it(encoding):
            self._test_get_subscribe(is_subscribe=True,
                                     datatype=datatype_str_to_int(data_type),
                                     encoding=encoding,
                                     allow_aggregation=allow_aggregation)

    @pytest.mark.long
    @pytest.mark.parametrize("allow_aggregation", [True, False], ids=["aggr", "no-aggr"])
    @pytest.mark.parametrize("data_type", ["CONFIG", "STATE"])
    @pytest.mark.parametrize("poll_args",
                             [(0.2, 2), (0.5, 2), (1, 2), (0.2, 10)])
    def test_subscribe_poll(self, request, data_type, poll_args, allow_aggregation):
        log.info("testing subscribe_poll")
        self._test_get_subscribe(is_subscribe=True,
                                 datatype=datatype_str_to_int(data_type),
                                 subscription_mode=gnmi_pb2.SubscriptionList.POLL,
                                 poll_interval=poll_args[0],
                                 poll_count=poll_args[1],
                                 allow_aggregation=allow_aggregation)

    def _send_change_list_to_confd_thread(self, prefix_str, changes_list):
        log.info("==>")
        log.debug("prefix_str=%s change_list=%s", prefix_str, changes_list)
        path_prefix = make_gnmi_path(prefix_str)
        sleep(1)

        def confd_cmd_subprocess(confd_cmd):
            log.debug("confd_cmd=%s", confd_cmd)
            subprocess.run(f"confd_cmd -c '{confd_cmd}'", shell=True, check=True)

        def format_command(c):
            path = make_gnmi_path(c[0])
            return "mset {} {}".format(
                make_formatted_path(path, gnmi_prefix=path_prefix),
                c[1].split(":")[-1])  # remove json prefix

        for send, changes in itertools.groupby(changes_list, lambda c: c == "send"):
            if not send:
                confd_cmd_subprocess(";".join(format_command(c) for c in changes))
                sleep(1)

    @staticmethod
    def _changes_list_to_pv(changes_list):
        '''
        Return path_value_list created from changes_list.
        :param changes_list:
        :return:
        '''
        path_value = []
        pv_idx = 0
        for c in changes_list:
            if isinstance(c, str):
                if c == "send":
                    pv_idx += 1
            else:
                if len(path_value) < pv_idx + 1:
                    path_value.append([])
                path_value[pv_idx].append((make_gnmi_path(c[0]), c[1]))
        log.debug("path_value=%s", path_value)
        return path_value

    @staticmethod
    def _changes_list_to_xml(changes_list, prefix_str):
        demo = ET.Element("demo")
        sub = ET.SubElement(demo, "subscription")
        stream = ET.SubElement(sub, "STREAM")
        changes = ET.SubElement(stream, "changes")
        for c in changes_list:
            el = ET.SubElement(changes, "element")
            if isinstance(c, str):
                el.text = c
            else:
                ET.SubElement(el, "path").text = "{}/{}".format(prefix_str,
                                                                c[0])
                ET.SubElement(el, "val").text = c[1]
        xml_str = ET.tostring(demo, encoding='unicode')
        log.debug("xml_str=%s", xml_str)
        return xml_str

    @pytest.mark.long
    @pytest.mark.parametrize("data_type", ["CONFIG", "STATE"])
    def test_subscribe_stream(self, request, data_type):
        log.info("testing subscribe_stream")
        if_state_str = prefix_state_str = ""
        if data_type == "STATE":
            prefix_state_str = "-state"
            if_state_str = "state_"

        changes_list = [
            ("interface[name={}if_5]/type".format(if_state_str),
             "iana-if-type:fastEther"),
            ("interface[name={}if_6]/type".format(if_state_str),
             "iana-if-type:fastEther"),
            "send",
            ("interface[name={}if_5]/type".format(if_state_str),
             "iana-if-type:gigabitEthernet"),
            ("interface[name={}if_6]/type".format(if_state_str),
             "iana-if-type:gigabitEthernet"),
            "send",
        ]
        log.info("change_list=%s", changes_list)

        prefix_str = "{{prefix}}interfaces{}".format(prefix_state_str)
        paths = [AdapterTests.mk_gnmi_if_path(self.list_paths_str[1], if_state_str,
                                              "N/A")]
        self._test_subscribe(prefix_str, self.NS_IETF_INTERFACES,
                             paths, changes_list)

    def _test_subscribe(self, prefix_str, ns_prefix, paths, changes_list):
        path_value = [[]]  # empty element means no check
        path_value.extend(self._changes_list_to_pv(changes_list))
        prefix = make_gnmi_path("/" + prefix_str.format(prefix=ns_prefix))

        kwargs = {"assert_fun": AdapterTests.assert_in_updates}
        kwargs["prefix"] = prefix
        kwargs["paths"] = paths
        kwargs["path_value"] = path_value
        kwargs["subscription_mode"] = gnmi_pb2.SubscriptionList.STREAM
        kwargs["read_count"] = len(path_value)
        kwargs["assert_fun"] = AdapterTests.assert_in_updates

        if self.adapter_type == AdapterType.DEMO:
            prefix_pfx = prefix_str.format(prefix='')
            GnmiDemoServerAdapter.load_config_string(
                self._changes_list_to_xml(changes_list, prefix_pfx))
        if self.adapter_type == AdapterType.API:
            prefix_pfx = prefix_str.format(prefix=self.PREFIX_MAP[ns_prefix])
            thr = threading.Thread(
                target=self._send_change_list_to_confd_thread,
                args=(prefix_pfx, changes_list,))
            thr.start()

        self.verify_sub_sub_response_updates(**kwargs)

        if self.adapter_type == AdapterType.API:
            thr.join()
            # TODO reset ConfD DB to original values

    @pytest.mark.parametrize("data_type", ["CONFIG", "STATE"])
    def test_subscribe_stream_sample(self, request, data_type):
        log.info("testing subscribe_stream_sample")
        self._test_get_subscribe(is_subscribe=True,
                    subscription_mode=gnmi_pb2.SubscriptionList.STREAM,
                    datatype=datatype_str_to_int(data_type),
                                 sample_interval=1000, read_count=2)

    @pytest.mark.usefixtures("reset_cfg")
    def test_set(self, request):
        log.info("testing set")
        if_id = 8
        prefix = make_gnmi_path("/ietf-interfaces:interfaces")
        paths = [AdapterTests.mk_gnmi_if_path(self.leaf_paths_str[1], "", if_id)]
        vals = [gnmi_pb2.TypedValue(json_ietf_val=b"\"iana-if-type:fastEther\"")]
        time_before = get_timestamp_ns()
        response = self.client.set(prefix, list(zip(paths, vals)))
        time_after = get_timestamp_ns()
        assert (time_before <= response.timestamp and response.timestamp <= time_after)
        assert (response.prefix == prefix)
        AdapterTests.assert_set_response(response.response[0],
                                         (paths[0], gnmi_pb2.UpdateResult.UPDATE))

        # fetch with get and see value has changed
        datatype = gnmi_pb2.GetRequest.DataType.CONFIG
        encoding = gnmi_pb2.Encoding.JSON_IETF
        get_response = self.client.get(prefix, paths, datatype, encoding)
        for n in get_response.notification:
            log.debug("n=%s", n)
            assert (n.prefix == prefix)
            AdapterTests.assert_updates(n.update, [(paths[0], "iana-if-type:fastEther")])

        # put value back
        vals = [gnmi_pb2.TypedValue(json_ietf_val=b"\"iana-if-type:gigabitEthernet\"")]
        response = self.client.set(prefix, list(zip(paths, vals)))
        AdapterTests.assert_set_response(response.response[0],
                                         (paths[0], gnmi_pb2.UpdateResult.UPDATE))

    @pytest.mark.usefixtures("reset_cfg")
    def test_set_encoding(self, request):
        log.info("testing set_encoding")
        if_id = 8
        prefix = make_gnmi_path("/ietf-interfaces:interfaces")
        paths = [AdapterTests.mk_gnmi_if_path(self.leaf_paths_str[1], "", if_id)]

        @self.encoding_test_decorator
        def test_it(encoding):
            vals = [gnmi_pb2.TypedValue(
                json_ietf_val=b"\"iana-if-type:fastEther\"")]
            if encoding == gnmi_pb2.Encoding.JSON:
                vals = [
                    gnmi_pb2.TypedValue(json_val=b"\"iana-if-type:fastEther\"")]
            elif encoding == gnmi_pb2.Encoding.JSON_IETF:
                vals = [gnmi_pb2.TypedValue(
                    json_ietf_val=b"\"iana-if-type:fastEther\"")]
            elif encoding == gnmi_pb2.Encoding.PROTO:
                vals = [
                    gnmi_pb2.TypedValue(string_val="iana-if-type:fastEther")]
            elif encoding == gnmi_pb2.Encoding.ASCII:
                vals = [gnmi_pb2.TypedValue(ascii_val="iana-if-type:fastEther")]
            elif encoding == gnmi_pb2.Encoding.BYTES:
                vals = [
                    gnmi_pb2.TypedValue(bytes_val=b"iana-if-type:fastEther")]
            self.client.set(prefix, list(zip(paths, vals)))

        # put value back
        vals = [gnmi_pb2.TypedValue(
            json_ietf_val=b"\"iana-if-type:gigabitEthernet\"")]
        response = self.client.set(prefix, list(zip(paths, vals)))
        AdapterTests.assert_set_response(response.response[0],
                                         (paths[0], gnmi_pb2.UpdateResult.UPDATE))
