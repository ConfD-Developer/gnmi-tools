import json
import os
import socket
import sys
import threading
from time import sleep

import pytest

import grpc
import gnmi_pb2
from client_server_test_base import GrpcBase, AdapterTests
from confd_gnmi_common import make_gnmi_path, make_xpath_path
from confd_gnmi_server import AdapterType
from confd_gnmi_client import ConfDgNMIClient
from route_status import RouteData, RouteProvider, ChangeOp
from utils.utils import log
sys.path.append(os.getenv('CONFD_DIR')+"/src/confd/pyapi/confd")
from confd_gnmi_api_adapter import GnmiConfDApiServerAdapter

from confd import maapi, maagic
import _confd

_confd_DEBUG = 1

@pytest.mark.grpc
@pytest.mark.confd
@pytest.mark.usefixtures("fix_method")
class TestGrpcConfD(AdapterTests):

    def set_adapter_type(self):
        self.adapter_type = AdapterType.API

    @staticmethod
    def _route_change_thread(path_value, route_data, sleep_val=2):
        sleep(sleep_val)
        log.info("==> path_value=%s route_data=%s sleep_val=%s", path_value,
                 route_data, sleep_val)
        for pv_chunk in path_value:
            log.debug("pv_chunk=%s", pv_chunk)
            msgs = []
            for pv in pv_chunk:
                op = ChangeOp.MODIFIED.value
                xpath = make_xpath_path(pv[0])
                val_str = pv[1]
                msg = "{}\n{}\n{}".format(op, xpath, val_str)
                msgs.append(msg)
            # TODO update route_data
            # TODO reuse with route status
            # TODO port number
            log.debug("msgs=%s", msg)
            with socket.socket() as s:
                try:
                    s.connect(("localhost",
                               GnmiConfDApiServerAdapter.external_port))
                    log.debug("Connected to change server")
                    msg = ""
                    for m in msgs:
                        # log.debug("m=%s", m)
                        msg += m + '\n'
                    # remove last \n
                    msg = msg[:-1]
                    log.debug("msg=%s", msg)
                    s.sendall(msg.encode("utf-8"))
                except Exception:
                    log.debug("Cannot connect to change server!")

        log.info("<==")

    # TODO tests of gnmi_tools data model also for demo ?
    def _test_gnmi_tools_get_subscribe_gnmi_tools(self, is_subscribe=False,
                                                  datatype=gnmi_pb2.GetRequest.DataType.CONFIG,
                                                  subscription_mode=gnmi_pb2.SubscriptionList.ONCE,
                                                  poll_interval=0,
                                                  poll_count=0, read_count=-1,
                                                  sample_interval = None,
                                                  encoding = gnmi_pb2.Encoding.JSON_IETF,
                                                  allow_aggregation=True):

        kwargs = {"assert_fun": AdapterTests.assert_updates}
        kwargs["prefix"] = make_gnmi_path("/gnmi-tools:gnmi-tools")

        leaf_paths_val = [
            (AdapterTests.mk_gnmi_if_path(p[0]), p[1]) if len(p) == 2 else (
                AdapterTests.mk_gnmi_if_path(p[0]), p[1], p[2]) for p in
            self.leaf_paths_str_for_gnmi_tools]

        if is_subscribe:
            verify_response_updates = self.verify_sub_sub_response_updates
            kwargs["subscription_mode"] = subscription_mode
            kwargs["poll_interval"] = poll_interval
            kwargs["poll_count"] = poll_count
            kwargs["read_count"] = read_count
            kwargs["sample_interval"] = sample_interval
            kwargs["allow_aggregation"] = allow_aggregation
            # disabled subscription tests for double key lists as not handled by verify_sub_sub_response_updates
            if not allow_aggregation:
               leaf_paths_val = [ p for p in leaf_paths_val if not "double-key-list" in p[0].elem[0].name]
        else:
            verify_response_updates = self.verify_get_response_updates
            kwargs["datatype"] = datatype

        kwargs["encoding"] = encoding
        kwargs["paths"] = [ p[0] for p in leaf_paths_val]
        kwargs["path_value"] = [p for p in leaf_paths_val]
        verify_response_updates(**kwargs)

    @pytest.mark.confd
    def test_gnmi_tools_get(self, request):
        log.info("testing get")
        self._test_gnmi_tools_get_subscribe_gnmi_tools()

    @pytest.mark.confd
    @pytest.mark.parametrize("allow_aggregation", [True, False], ids=["aggr", "no-aggr"])
    def test_gnmi_tools_subscribe_once(self, request, allow_aggregation):
        log.info("testing subscribe_once")
        self._test_gnmi_tools_get_subscribe_gnmi_tools(is_subscribe=True, allow_aggregation=allow_aggregation)

    @pytest.mark.long
    @pytest.mark.confd
    @pytest.mark.parametrize("allow_aggregation", [True, False], ids=["aggr", "no-aggr"])
    @pytest.mark.parametrize("poll_args", [(0.2, 2), (0.5, 2), (1, 2)])
    def test_gnmi_tools_subscribe_poll(self, request, poll_args, allow_aggregation):
        log.info("testing subscribe_poll")
        self._test_gnmi_tools_get_subscribe_gnmi_tools(is_subscribe=True,
                                 subscription_mode=gnmi_pb2.SubscriptionList.POLL,
                                 poll_interval=poll_args[0],
                                 poll_count=poll_args[1],
                                 allow_aggregation=allow_aggregation)

    @pytest.mark.confd
    @pytest.mark.parametrize("allow_aggregation", [True, False], ids=["aggr", "no-aggr"])
    def test_gnmi_tools_subscribe_stream_sample(self, request, allow_aggregation):
        log.info("testing subscribe_stream_sample")
        self._test_gnmi_tools_get_subscribe_gnmi_tools(is_subscribe=True,
                                 subscription_mode=gnmi_pb2.SubscriptionList.STREAM,
                                 sample_interval=1000, read_count=2, allow_aggregation=allow_aggregation)

    @pytest.mark.long
    @pytest.mark.confd
    def test_subscribe_stream_with_state(self, request):
        log.info("testing subscribe_stream with state under config")
        changes_list = [
            ("interface[name=gig0]/state/loopback-mode", "NONE"),
            ("interface[name=gig2]/state/loopback-mode", "NONE"),
            "send",
            ("interface[name=gig0]/state/loopback-mode", "FACILITY"),
            ("interface[name=gig2]/state/loopback-mode", "FACILITY"),
            "send"]
        prefix_str = '{prefix}interfaces'
        paths = [make_gnmi_path("/interface")]
        self._test_subscribe(prefix_str, self.NS_OC_INTERFACES,
                             paths, changes_list)

    @pytest.mark.long
    @pytest.mark.confd
    def test_subscribe_stream_on_change_api_state(self, request):
        log.info("testing test_subscribe_stream_on_change_api_state")
        GnmiConfDApiServerAdapter.monitor_external_changes = True
        changes_list = [
            ("/route-status:route-status/route[id=rt5]/leaf1", 1010),
            ("/route-status:route-status/route[id=rt6]/leaf1", 1020),
            "send",
            ("/route-status:route-status/route[id=rt6]/leaf1", 1030),
            "send",
        ]
        path_value = [[]]  # empty element means no check
        path_value.extend(self._changes_list_to_pv(changes_list))

        prefix_str = ""
        prefix = make_gnmi_path(prefix_str)
        paths = [make_gnmi_path("route-status:route-status")]

        kwargs = {"assert_fun": AdapterTests.assert_in_updates}
        kwargs["prefix"] = prefix
        kwargs["paths"] = paths
        kwargs["path_value"] = path_value
        kwargs["subscription_mode"] = gnmi_pb2.SubscriptionList.STREAM
        kwargs["read_count"] = len(path_value)
        kwargs["assert_fun"] = AdapterTests.assert_in_updates

        route_data = RouteData(num=10, random=False)
        assert len(route_data.routes)
        RouteProvider.init_dp(route_data, confd_debug_level=_confd_DEBUG)

        confd_thread = threading.Thread(target=RouteProvider.confd_loop)
        change_thread = threading.Thread(
            target=self._route_change_thread,
            args=(path_value[1:], route_data,))
        confd_thread.start()
        change_thread.start()

        try:
            self.verify_sub_sub_response_updates(**kwargs)
            sleep(1)

        finally:
            change_thread.join()
            RouteProvider.stop_confd_loop()
            confd_thread.join()
            RouteProvider.close_dp()

    def _assert_auth(self, err_string, username="admin", password="admin"):
        client = ConfDgNMIClient(username=username, password=password, insecure=True)
        with pytest.raises(grpc.RpcError) as err:
            capabilities = client.get_capabilities()
        client.close()
        assert err_string in str(err)

    @pytest.mark.confd
    def test_authentication(self, request):
        log.info("testing authentication")
        self._assert_auth("Bad password", password="bad")
        self._assert_auth("No such local user", username="bad", password="bad")
        self._assert_auth("No such local user", username="bad")

    @pytest.mark.confd
    def test_set_trans_order(self, request):
        base = '/ietf-interfaces:interfaces/interface[name="if_8"]'
        path = make_gnmi_path(base + '/enabled')
        update = gnmi_pb2.Update(path=path,
                                 val=gnmi_pb2.TypedValue(json_ietf_val=b'false'))
        self.client.set_request(None, delete=[path], update=[update])
        datatype = gnmi_pb2.GetRequest.DataType.CONFIG
        encoding = gnmi_pb2.Encoding.JSON_IETF
        response = self.client.get(None, [path], datatype, encoding)
        assert response.notification[0].update[0].val.json_ietf_val == b'false'
        bad_update = gnmi_pb2.Update(path=make_gnmi_path(base + '/no-such-leaf'),
                                     val=gnmi_pb2.TypedValue(json_ietf_val=b'42'))
        with pytest.raises(grpc.RpcError):
            self.client.set_request(None, delete=[path], update=[bad_update])
        response = self.client.get(None, [path], datatype, encoding)
        assert response.notification[0].update[0].val.json_ietf_val == b'false'
        update = gnmi_pb2.Update(path=path,
                                 val=gnmi_pb2.TypedValue(json_ietf_val=b'true'))
        self.client.set_request(None, update=[update])


class TestGrpcConfDSet(GrpcBase):
    def set_adapter_type(self):
        self.adapter_type = AdapterType.API

    @pytest.fixture
    def reset_cfg(self, request):
        yield
        with maapi.single_write_trans('admin', 'system') as trans:
            gt = maagic.get_node(trans, '/gnmi-tools')
            for lst in (gt.top_list, gt.double_key_list):
                for inst in lst:
                    if inst.name.startswith('test-'):
                        trans.delete(inst._path)
            trans.apply()

    def assert_instance(self, lst='top-list', list_ix=1, leaf='down/str-leaf', value='abcd'):
        key = f'{{test-{list_ix} test}}' if lst == 'double-key-list' \
            else f'{{test-{list_ix}}}'
        path = f'/gnmi-tools/{lst}{key}/{leaf}'
        with maapi.single_read_trans('admin', 'system') as trans:
            assert trans.get_elem(path) == value

    def client_set(self, path, obj):
        self.client.set(None,
                        [(make_gnmi_path(path),
                          gnmi_pb2.TypedValue(json_ietf_val=json.dumps(obj).encode()))])

    @pytest.mark.confd
    @pytest.mark.usefixtures("reset_cfg")
    def test_set_list_create_instance(self, request):
        obj = {'top-list':
               {'name': 'test-1',
                'down': {'str-leaf': 'abcd'}}}
        self.client_set('/gnmi-tools:gnmi-tools', obj)
        self.assert_instance()

    @pytest.mark.confd
    @pytest.mark.usefixtures("reset_cfg")
    def test_set_list_update_instance(self, request):
        obj = {'top-list':
               {'name': 'test-1',
                'down': {'str-leaf': 'abcd'}}}
        self.client_set('/gnmi-tools:gnmi-tools', obj)
        self.assert_instance()
        obj['top-list']['down']['str-leaf'] = 'efgh'
        self.client_set('/gnmi-tools:gnmi-tools', obj)
        self.assert_instance(value='efgh')

    @pytest.mark.confd
    @pytest.mark.usefixtures("reset_cfg")
    def test_set_list_2key_instance(self, request):
        obj = {'double-key-list':
               {'name': 'test-1',
                'type': 'test',
                'admin-state': 'tested'}}
        self.client_set('/gnmi-tools:gnmi-tools', obj)
        self.assert_instance(lst='double-key-list', leaf='admin-state', value='tested')

    @pytest.mark.confd
    @pytest.mark.usefixtures("reset_cfg")
    def test_set_list_two_instances(self, request):
        base = {'down': {'str-leaf': 'abcd'}}
        insts = [{'name': 'test-1'}, {'name': 'test-2'}]
        for inst in insts:
            inst.update(base)
        obj = {'top-list': insts}
        self.client_set('/gnmi-tools:gnmi-tools', obj)
        self.assert_instance()
        self.assert_instance(list_ix=2)
        for inst in insts:
            inst['down']['str-leaf'] = 'efgh'
        self.client_set('/gnmi-tools:gnmi-tools', obj)
        self.assert_instance(value='efgh')
        self.assert_instance(list_ix=2, value='efgh')
