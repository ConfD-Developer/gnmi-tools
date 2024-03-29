from contextlib import contextmanager
import logging
import functools
import itertools
import os
import re
import select
import sys
import threading
import typing as t
import json
from enum import Enum
from socket import create_server, socket
import gnmi_pb2
from confd_gnmi_adapter import GnmiServerAdapter, UpdateTransaction
from confd_gnmi_api_adapter_defaults import ApiAdapterDefaults
from confd_gnmi_common import make_xpath_path, make_formatted_path, \
    add_path_prefix, remove_path_prefix, make_gnmi_path, parse_instance_path, \
    get_timestamp_ns

import tm
_tm = __import__(tm.TM)

maapi = __import__(tm.TM[1:]).maapi
maagic = __import__(tm.TM[1:]).maagic
cdb = __import__(tm.TM[1:]).cdb.cdb

log = logging.getLogger('confd_gnmi_api_adapter')


INT_VALS = {_tm.C_INT8, _tm.C_INT16, _tm.C_INT32,
            _tm.C_UINT8, _tm.C_UINT16, _tm.C_UINT32}

EXT_SPOINT = -1  # no subscription point for external changes


class GnmiConfDApiServerAdapter(GnmiServerAdapter):
    addr: str = ApiAdapterDefaults.ADDR
    port: int = _tm.PORT
    monitor_external_changes: bool = ApiAdapterDefaults.MONITOR_EXTERNAL_CHANGES
    external_port: int = ApiAdapterDefaults.EXTERNAL_PORT

    class AuthenticationException(Exception):
        def __init__(self, user, reason="N/A"):
            self.user = user
            self.reason = reason
        def __str__(self):
            return f"Authentication failed for user '{self.user}': {self.reason}."

    def __init__(self):
        self.addr: str = GnmiConfDApiServerAdapter.addr
        self.port: int = GnmiConfDApiServerAdapter.port
        self.username: str = ""
        self.password: str = ""
        self.mp_inst = None
        # make sure schemas are loaded
        with maapi.Maapi(ip=self.addr, port=self.port):
            pass
        nslist = _tm.get_nslist()
        self.module_to_pfx = {nsentry[-1]: nsentry[1] for nsentry in nslist}
        self.pfx_to_module = {nsentry[1]: nsentry[-1] for nsentry in nslist}
        self.ns_to_module = {nsentry[0]: nsentry[-1] for nsentry in nslist}

    # call only once!
    @staticmethod
    def set_tm_debug_level(level):
        if level == "debug":
            debug_level = _tm.DEBUG
        elif level == "trace":
            debug_level = _tm.TRACE
        elif level == "proto":
            debug_level = _tm.PROTO_TRACE
        elif level == "silent":
            debug_level = _tm.SILENT
        else:
            debug_level = _tm.TRACE
            log.warning("Unknown debug level %s", level)
        _tm.set_debug(debug_level, sys.stderr)

    @staticmethod
    def set_addr(addr):
        GnmiConfDApiServerAdapter.addr = addr

    @staticmethod
    def set_port(port):
        if port is None:
            port = _tm.PORT
        GnmiConfDApiServerAdapter.port = port

    @staticmethod
    def set_external_port(port):
        GnmiConfDApiServerAdapter.external_port = port

    @staticmethod
    def set_monitor_external_changes(val=True):
        GnmiConfDApiServerAdapter.monitor_external_changes = val

    @classmethod
    def get_adapter(cls) -> GnmiServerAdapter:
        """
        This is classmethod on purpose, see GnmiDemoServerAdapter
        """
        return GnmiConfDApiServerAdapter()

    class SubscriptionHandler(GnmiServerAdapter.SubscriptionHandler):

        def __init__(self, adapter, subscription_list):
            super().__init__(adapter, subscription_list)
            # TODO reuse with demo adapter?
            self.monitored_paths = []
            self.change_db = []
            self.change_db_lock = threading.Lock()
            self.change_thread = None
            self.stop_pipe = None
            self.subpoint_paths = {}

        def get_subscription_notifications(self) -> list[gnmi_pb2.Notification]:
            return [gnmi_pb2.Notification(timestamp=get_timestamp_ns(),
                                          prefix=prefix,
                                          update=updates,
                                          delete=deletes,
                                          atomic=False)
                    for prefix, updates, deletes in self._get_subscription_notifications()]

        def _get_subscription_notifications(self):
            with self.change_db_lock:
                log.debug("self.change_db=%s", self.change_db)
                assert len(self.change_db) > 0
                for sub_point, changes in self.change_db:
                    sub_prefix = self.subpoint_paths[sub_point]
                    pfxlen = len(sub_prefix.elem)
                    groups = itertools.groupby(changes, key=lambda change: change[1].elem[:pfxlen])
                    for elems, changegroup in groups:
                        prefix = gnmi_pb2.Path(elem=elems,
                                               target=sub_prefix.target,
                                               origin=sub_prefix.origin)
                        changegroup = list(changegroup)
                        updates = [gnmi_pb2.Update(path=remove_path_prefix(path, prefix),
                                                   val=value)
                                   for op, path, value in changegroup
                                   if op is self.ChangeOp.MODIFIED]
                        deletes = [remove_path_prefix(path, prefix)
                                   for op, path, _ in changegroup
                                   if op is self.ChangeOp.DELETED]
                        log.debug("update=%s deletes=%s", updates, deletes)
                        yield prefix, updates, deletes
                self.change_db = []

        def get_sample(self, path, prefix, allow_aggregation=False,
                       start_change_processing=False):
            log.debug("==>")
            pathstr = make_xpath_path(path, prefix, quote_val=True)
            datatype = gnmi_pb2.GetRequest.DataType.ALL
            updates = self.adapter.get_updates_with_maapi(pathstr, datatype,
                                                          allow_aggregation=allow_aggregation)
            sample = [gnmi_pb2.Update(path=remove_path_prefix(u.path, prefix),
                                      val=u.val)
                      for u in updates]
            log.debug("<== sample=%s", sample)
            return sample

        def add_path_for_monitoring(self, path, prefix):
            log.debug("==>")
            self.monitored_paths.append(add_path_prefix(path, prefix))
            log.debug("<==")

        @staticmethod
        def kp_to_xpath(kp):
            log.debug("==> kp=%s", kp)
            xpath = _tm.xpath_pp_kpath(kp)
            xpath = xpath.replace('"', '')
            # for now, remove possible prefix
            # (TODO for now handled only prefix in first elem)
            starts_slash = xpath.startswith('/')
            xplist = xpath.split(':', 1)
            if len(xplist) == 2:
                xpath = xplist[1]
                if starts_slash:
                    xpath = '/' + xpath
            log.debug("<== xpath=%s", xpath)
            return xpath

        class ChangeOp(Enum):
            MODIFIED = "mod"
            DELETED = "del"

        def _append_changes(self, sub_point, changes):
            """
            :param sub_point:
            :param change_tuple: 3 elem tuple (o, gnmi path, val)
            :return:
            """
            log.debug("==> change_tuple=%s", changes)
            with self.change_db_lock:
                self.change_db.append((sub_point, changes))
            log.debug("<==")

        def process_subscription(self, sub_sock, sub_point):
            log.debug("==>")

            def cdb_iter(kp, op, oldv, newv, changes):
                log.debug("==> kp=%s, op=%r, oldv=%s, newv=%s, state=%r", kp,
                          op, oldv, newv, changes)
                csnode = _tm.cs_node_cd(None, str(kp))
                if op == _tm.MOP_CREATED:
                    log.debug("_tm.MOP_CREATED")
                    # TODO in case of empty leaves or possibly presence
                    # containers, something needs to be done; the rest is
                    # handled after ITER_RECURSE
                if op == _tm.MOP_VALUE_SET:
                    log.debug("_tm.MOP_VALUE_SET")
                    changes.append((self.ChangeOp.MODIFIED,
                                    self.adapter.make_gnmi_keypath(kp, csnode),
                                    self.adapter.make_gnmi_json_value(newv, csnode)))
                    # TODO MOP_VALUE_SET implement
                elif op == _tm.MOP_DELETED:
                    log.debug("_tm.MOP_DELETED")
                    changes.append((self.ChangeOp.DELETED,
                                    self.adapter.make_gnmi_keypath(kp, csnode),
                                    None))
                elif op == _tm.MOP_MODIFIED:
                    log.debug("_tm.MOP_MODIFIED")
                    # nothing to do, will be handled after ITER_RECURSE
                else:
                    log.warning(
                        "Operation op=%d is not expected, kp=%s. Skipping!",
                        op, kp)
                return _tm.ITER_RECURSE

            changes = []
            cdb.diff_iterate(sub_sock, sub_point, cdb_iter, 0, changes)
            self._append_changes(sub_point, changes)
            self.put_event(self.SubscriptionEvent.SEND_CHANGES)
            log.debug("self.change_db=%s", self.change_db)
            log.debug("<==")

        def _external_changes(self, data):
            data_iter = iter(data)
            for op, xpath, value in zip(data_iter, data_iter, data_iter):
                fxpath = self.adapter.fix_path_prefixes(xpath)
                csnode = _tm.cs_node_cd(None, fxpath)
                path = make_gnmi_path(xpath)
                cval = _tm.Value.str2val(value, csnode.info().type())
                json_value = self.adapter.make_gnmi_json_value(cval, csnode)
                yield self.ChangeOp(op), path, json_value

        def process_external_change(self, ext_sock):
            log.info("==>")
            connection, client_address = ext_sock.accept()
            with connection:
                # TODO make const
                msg = connection.recv(1024)
                log.debug("msg=%s", msg)
                # simple protocol (just for illustration, real implementation
                # should be more robust as not everything may come at once)
                # the msg string should contain N strings separated by \n
                # op1\nxpath1\nval1\nop2\nxpath2\nval2 .....
                # op1 .. first operation1, xpath1 .... first xpath, ...
                # op is string used in ChangeOp Enum class
                # currently operation can be only "modified"
                # the size must be smaller than size in recv
                data = msg.decode().split('\n')
                assert len(data) % 3 == 0
                self._append_changes(EXT_SPOINT, list(self._external_changes(data)))
                self.put_event(self.SubscriptionEvent.SEND_CHANGES)
                log.debug("data=%s", data)
            log.info("<==")

        def subscribe_monitored_paths_cdb(self, sub_sock):
            """
            Subscribe to monitored paths
            :param sub_sock:
            :return: True, if some path is not in CDB
            """
            log.debug("==> sub_sock=%s", sub_sock)
            prio = 10
            subscribed = has_non_cdb = False
            # make subscription for all self.monitored_paths in CDB
            for path in self.monitored_paths:
                log.debug("subscribing config path=%s", path)
                # TODO hash - breaks generic usage
                # TODO for now we subscribe path for both, config and oper,
                # TODO subscribe only for paths that exist
                # it may be more efficient to find out type of path and subscribe
                # only for one type
                path_str = self.adapter.fix_path_prefixes(make_formatted_path(path))
                cs_node = _tm.cs_node_cd(None, path_str)
                if cs_node.info().flags() & _tm.CS_NODE_IS_LIST != 0 \
                   and not path.elem[-1].key:
                    subpoint_path = gnmi_pb2.Path(elem=path.elem[:-1],
                                                  origin=path.origin,
                                                  target=path.target)
                else:
                    subpoint_path = path
                flags = cs_node.info().flags()
                is_cdb = flags & _tm.CS_NODE_IS_CDB
                if is_cdb:
                    subscribed = True
                    spoint = cdb.subscribe2(sub_sock, cdb.SUB_OPERATIONAL, 0, prio, 0, path_str)
                    self.subpoint_paths[spoint] = subpoint_path
                    if flags & _tm.CS_NODE_IS_WRITE:
                        spoint = cdb.subscribe2(sub_sock, cdb.SUB_RUNNING, 0, prio, 0, path_str)
                        self.subpoint_paths[spoint] = subpoint_path
                else:
                    has_non_cdb = True
            if subscribed:
                cdb.subscribe_done(sub_sock)
            log.debug("<== has_non_cdb=%s", has_non_cdb)
            return has_non_cdb

        @staticmethod
        def start_external_change_server():
            """
            Start external change server
            :return: socket to listen for changes
            """
            log.debug("==>")
            log.info("Starting external change server!")
            # TODO port (host) as const or command line option
            ext_server_sock = create_server(("localhost", GnmiConfDApiServerAdapter.external_port),
                                            reuse_port=True)
            ext_server_sock.listen(5)
            log.debug("<== ext_server_sock=%s", ext_server_sock)
            return ext_server_sock

        def socket_loop(self, sub_sock, ext_server_sock=None):
            log.debug("==> sub_sock=%s ext_server_sock=%s", sub_sock,
                      ext_server_sock)
            rlist = [sub_sock, self.stop_pipe[0]]
            if ext_server_sock is not None:
                rlist.append(ext_server_sock)
            wlist = elist = []
            while True:
                log.debug("rlist=%s", rlist)
                r, w, e = select.select(rlist, wlist, elist)
                log.debug("r=%s", r)
                if ext_server_sock is not None and ext_server_sock in r:
                    self.process_external_change(ext_server_sock)
                if sub_sock in r:
                    try:
                        sub_info = cdb.read_subscription_socket2(
                            sub_sock)
                        for s in sub_info[2]:
                            self.process_subscription(sub_sock, s)
                        cdb.sync_subscription_socket(sub_sock,
                                                     cdb.DONE_PRIORITY)
                    except _tm.error.Error as e:
                        # Callback error
                        if e.confd_errno is _tm.ERR_EXTERNAL:
                            log.exception(e)
                        else:
                            raise e
                if self.stop_pipe[0] in r:
                    v = os.read(self.stop_pipe[0], 1)
                    assert v == b'x'
                    log.debug("Stopping ConfD loop")
                    break
            log.debug("<==")

        def process_changes(self, external_changes=False):
            log.debug("==> external_changes=%s", external_changes)
            with socket() as sub_sock:
                cdb.connect(sub_sock, cdb.SUBSCRIPTION_SOCKET, self.adapter.addr,
                            self.adapter.port)
                has_non_cdb = self.subscribe_monitored_paths_cdb(sub_sock)
                log.debug("subscribe_done")
                assert self.stop_pipe is not None
                try:
                    if external_changes and has_non_cdb:
                        ext_server_sock = self.start_external_change_server()
                        self.subpoint_paths[EXT_SPOINT] = gnmi_pb2.Path()
                        with ext_server_sock:
                            self.socket_loop(sub_sock, ext_server_sock)
                    else:
                        self.socket_loop(sub_sock)
                except Exception as e:
                    log.exception(e)
                    self.async_stop()
            log.debug("<==")

        def start_monitoring(self):
            log.debug("==>")
            assert self.change_thread is None
            assert self.stop_pipe is None
            log.debug("** creating change_thread")
            self.stop_pipe = os.pipe()
            # TODO external change server always started,
            # make optional by passing False
            self.change_thread = \
                threading.Thread(target=self.process_changes,
                                 args=(
                                     GnmiConfDApiServerAdapter.monitor_external_changes,))
            log.debug("** starting change_thread")
            self.change_thread.start()
            log.debug("** change_thread started")
            log.debug("<==")

        def stop_monitoring(self):
            log.debug("==>")
            # if there is an error during fetch of first subs. sample,
            # we do not start change thread
            if self.change_thread is None:
                log.warning("Cannot stop change thread! Not started?")
            else:
                assert self.stop_pipe is not None
                log.debug("** stopping change_thread")
                # https://stackoverflow.com/a/4661284
                os.write(self.stop_pipe[1], b'x')
                self.change_thread.join()
                log.debug("** change_thread joined")
                self.change_thread = None
                os.close(self.stop_pipe[0])
                os.close(self.stop_pipe[1])
                self.stop_pipe = None
            self.monitored_paths = []
            log.debug("<==")

    def get_subscription_handler(self,
                                 subscription_list) -> SubscriptionHandler:
        log.debug("==>")
        handler = self.SubscriptionHandler(self, subscription_list)
        log.debug("<== handler=%s", handler)
        return handler

    def authenticate(self, username="admin", password="admin"):
        log.info("==> username=%s password=:-)", username)
        self.username = username
        self.password = password
        auth_status = maapi.Maapi(ip=self.addr, port=self.port).authenticate(
            self.username, self.password, 1)
        reason = "N/A"
        if not isinstance(auth_status, int):
            reason = auth_status[1]
            auth_status = auth_status[0]
        if auth_status == 1:
            log.info(f"Authenticated {self.username}.")
        else:
            e =  self.AuthenticationException(self.username, str(reason))
            log.warning(e)
            raise e
        log.info("<== self.username=%s self.password=:-)", self.username)

    # https://tools.ietf.org/html/rfc6022#page-8
    # TODO pass username from request context
    def get_netconf_capabilities(self):
        log.info("==>")
        context = "netconf"
        groups = [self.username]
        try:
            with maapi.single_read_trans(self.username, context, groups,
                                         ip=self.addr, port=self.port) as t:
                root = maagic.get_root(t)
                values = []
                count = 0
                for module in root.modules_state.module:
                    log.debug("val=%s", module.name)
                    name = f'{module.name}'
                    revision = str(module.revision) if module.revision else "N/A"
                    values.append((module.namespace, name, "", revision))
                    count += 1
                    log.debug("Value element count=%d" % count)
            log.debug("values=%s", values)
        except Exception as e:
            log.exception(e)
            raise e

        log.info("<==")
        return values

    def capabilities(self):
        log.info("==>")
        ns_list = self.get_netconf_capabilities()
        log.debug("ns_list=%s", ns_list)
        models = []
        for ns in ns_list:
            models.append(GnmiServerAdapter.CapabilityModel(name=ns[1],
                                                            organization="",
                                                            version=ns[3]))

        log.info("<== models=%s", models)
        return models

    def encodings(self):
        return [gnmi_pb2.Encoding.JSON, gnmi_pb2.Encoding.JSON_IETF]

    def make_gnmi_keypath_elems(self, keypath, csnode):
        i = len(keypath) - 1
        ns = 0

        def csnode_list():
            node = csnode
            while node is not None:
                yield node
                node = node.parent()

        for node in reversed(list(csnode_list())):
            assert i >= 0
            keys = {}
            name = _tm.hash2str(keypath[i].tag)
            if keypath[i].ns != ns:
                ns = keypath[i].ns
                name = f'{self.ns_to_module[ns]}:{name}'
            i -= 1
            if node.info().flags() & _tm.CS_NODE_IS_LIST != 0:
                assert i >= 0 and isinstance(keypath[i], tuple)
                keys = {_tm.hash2str(key): str(val)
                        for (key, val) in zip(node.info().keys(), keypath[i])}
                i -= 1
            yield gnmi_pb2.PathElem(name=name, key=keys)

    def make_gnmi_keypath(self, keypath, csnode=None):
        if csnode is None:
            csnode = _tm.cs_node_cd(None, str(keypath))
        return gnmi_pb2.Path(elem=list(self.make_gnmi_keypath_elems(keypath, csnode)))

    def make_gnmi_json_value(self, value, csnode):
        if value.confd_type() in INT_VALS:
            json_value = int(value)
        elif value.confd_type() == _tm.C_BOOL:
            json_value = bool(value)
        elif value.confd_type() == _tm.C_IDENTITYREF:
            # JSON formatting is different from what ConfD does by default
            [prefix, idref] = value.val2str(csnode.info().type()).split(":")
            json_value = f"{self.pfx_to_module[prefix]}:{idref}"
        # empty, leaf-lists...
        else:
            json_value = value.val2str(csnode.info().type())
        gnmi_value = gnmi_pb2.TypedValue(json_ietf_val=json.dumps(json_value).encode())
        return gnmi_value

    def append_update(self, tr, keypath, csnode=None):
        if csnode is None:
            csnode = _tm.cs_node_cd(None, keypath)

        def append_update_inner():
            if tr.exists(keypath):
                gnmi_value = None
                if csnode.is_empty_leaf():
                    # See https://datatracker.ietf.org/doc/html/rfc7951#section-6.9
                    gnmi_value = gnmi_pb2.TypedValue(
                        json_ietf_val=json.dumps([None]).encode())
                elif csnode.is_p_container():
                    gnmi_value = gnmi_pb2.TypedValue(
                        json_ietf_val=json.dumps({}).encode())
                else:
                    elem = tr.get_elem(keypath)
                    gnmi_value = self.make_gnmi_json_value(elem, csnode)
                if gnmi_value is not None:
                    tr.pushd(keypath)
                    kp = tr.getcwd_kpath()
                    tr.popd()
                    gnmi_path = self.make_gnmi_keypath(kp, csnode)
                    yield gnmi_pb2.Update(path=gnmi_path, val=gnmi_value)

        # tr.get_elem(path_str) throws exception even though tr.exists(path_str)
        # returns True, special handling for oper is needed
        try:
            yield from append_update_inner()
        except _tm.error.Error:
            pass

    def make_updates_with_maagic_rec(self, tr, node):
        if isinstance(node, maagic.Node):
            if isinstance(node, maagic.List):
                for n in node:
                    yield from self.make_updates_with_maagic_rec(tr, n)
            elif isinstance(node, maagic.Leaf):
                yield from self.append_update(tr, node._path, node._cs_node)
            else:
                if hasattr(node, "_children"):  # skip nodes w/o children, e.g. Action
                    children = node._children.get_children(node._backend, node)
                    if len(children) == 0 and isinstance(node,
                                                         maagic.PresenceContainer):
                        yield from self.append_update(tr, node._path, node._cs_node)
                    else:
                        for n in children:
                            yield from self.make_updates_with_maagic_rec(tr, n)

    def make_updates_with_maagic(self, tr, keypath):
        node = maagic.get_node(tr, keypath)
        if not isinstance(node, maagic.Node):
            yield from self.append_update(tr, keypath)
        else:
            yield from self.make_updates_with_maagic_rec(tr, node)


    def get_updates(self, trans, path_str, save_flags, allow_aggregation=False):
        log.debug("==> path_str=%s allow_aggregation=%s", path_str,
                  allow_aggregation)
        tagpath = '/' + '/'.join(tag for tag, _ in parse_instance_path(path_str))
        log.debug("tagpath=%s", tagpath)
        if tagpath != '/':
            csnode = _tm.cs_node_cd(None, tagpath)
        else:
            raise Exception("Query for root path / not supported by NSO/ConfD adapter.")
        updates = []

        def add_update_json(keypath, _value):
            save_id = trans.save_config(save_flags, str(keypath))
            with socket() as save_sock:
                _tm.stream_connect(sock=save_sock, id=save_id, flags=0,
                                   ip=self.addr, port=self.port)
                max_msg_size = 1024
                save_str = b''.join(iter(lambda: save_sock.recv(max_msg_size), b''))
                if not save_str:
                    return
                saved_data = json.loads(save_str)
                log.debug("data=%s", saved_data)
                save_result = trans.maapi.save_config_result(save_id)
                log.debug("save_result=%s", save_result)
                assert save_result == 0
                gnmi_path = self.make_gnmi_keypath(keypath, csnode)
                # the format of saved_data is {"node": {data}}, can be
                # {} if the container is empty; we need only the data
                # part
                if not saved_data:
                    data = {}
                else:
                    [data] = saved_data.values()
                gnmi_value = gnmi_pb2.TypedValue(json_ietf_val=json.dumps(data).encode())
                updates.append(gnmi_pb2.Update(path=gnmi_path, val=gnmi_value))

        if allow_aggregation:
            if csnode is None:
                log.warning('failed to find the cs-node')
            else:
                trans.xpath_eval(path_str, add_update_json, None, '/')
        else:
            keypath = str(trans.maapi.xpath2kpath(path_str))
            updates = list(self.make_updates_with_maagic(trans, keypath))

        log.debug("<== save_str=%s", updates)
        return updates

    def fix_path_prefixes(self, path):
        def module_to_prefix(match):
            name = match.groups()[0]
            return self.module_to_pfx.get(name, name) + ':'
        return re.sub(r'([^/:]+):', module_to_prefix, path)

    def get_updates_with_maapi(self, path, data_type, allow_aggregation=False):
        log.debug("==> path=%s data_type=%s", path, data_type)

        pfx_path = self.fix_path_prefixes(path)
        save_flags = _tm.maapi.CONFIG_JSON | _tm.maapi.CONFIG_NO_PARENTS \
                     | _tm.maapi.CONFIG_WITH_DEFAULTS
        db = _tm.OPERATIONAL

        if data_type == gnmi_pb2.GetRequest.DataType.ALL:
            save_flags |= _tm.maapi.CONFIG_WITH_OPER
        elif data_type == gnmi_pb2.GetRequest.DataType.CONFIG:
            db = _tm.RUNNING
        elif data_type == gnmi_pb2.GetRequest.DataType.STATE:
            save_flags |= _tm.maapi.CONFIG_OPER_ONLY
        elif data_type == gnmi_pb2.GetRequest.DataType.OPERATIONAL:
            save_flags |= _tm.maapi.CONFIG_OPER_ONLY

        context = "netconf"
        groups = [self.username]
        updates = []
        try:
            with maapi.single_read_trans(self.username, context, groups, db=db,
                                         ip=self.addr, port=self.port) as t:
                updates = self.get_updates(t, pfx_path, save_flags,
                                           allow_aggregation=allow_aggregation)
        except ValueError:
            pass
        except Exception as e:
            log.exception(e)
            raise e

        log.debug("<== up=%s", updates)
        return updates

    def get(self, prefix, paths, data_type, use_models):
        log.info("==> prefix=%s, paths=%s, data_type=%s, use_models=%s",
                 prefix, paths, data_type, use_models)
        notifications = []
        updates2 = [self.get_updates_with_maapi(make_xpath_path(path, prefix, quote_val=True),
                                                data_type, allow_aggregation=True)
                    for path in paths]
        updates = [gnmi_pb2.Update(path=remove_path_prefix(update.path, prefix), val=update.val)
                   for u_list in updates2
                   for update in u_list]
        notif = gnmi_pb2.Notification(timestamp=get_timestamp_ns(), prefix=prefix,
                                      update=updates,
                                      delete=[],
                                      atomic=True)
        notifications.append(notif)
        log.info("<== notifications=%s", notifications)
        return notifications

    @contextmanager
    def update_transaction(self, prefix) -> t.Iterator["ApiTransaction"]:
        log.info("==> prefix=%s", prefix)
        context = "netconf"
        groups = [self.username]
        with maapi.single_write_trans(self.username, context, groups,
                                      ip=self.addr, port=self.port) as trans:
            at = ApiTransaction(self, trans, prefix)
            yield at
            trans.apply()
        log.info("<==")


class ApiTransaction(UpdateTransaction):
    """Update message handler.  One instance is suppposed to handle
    all Update messages in one SetRequest.
    """
    def __init__(self, adapter, trans, prefix):
        self.adapter = adapter
        self.trans = trans
        self.prefix = prefix

    def apply_update(self, path, value):
        if value.HasField('json_ietf_val'):
            obj = json.loads(value.json_ietf_val)
        elif value.HasField('json_val'):
            log.warning('using json_val as json_ietf_val')
            obj = json.loads(value.json_val)
        else:
            raise Exception(f'{value.ListFields()[0][0].name} not supported for updates')
        elems = [gnmi_pb2.PathElem(name='data')] + list(self.prefix.elem) + list(path.elem)
        data = functools.reduce(self.build_obj, reversed(elems), obj)
        log.debug('sending %s to JSON load', data)
        sid = self.trans.load_config_stream(_tm.maapi.CONFIG_JSON | _tm.maapi.CONFIG_MERGE)
        with socket() as sock:
            _tm.stream_connect(sock, sid, 0, self.adapter.addr, self.adapter.port)
            sock.send(json.dumps(data).encode())
        if self.trans.maapi.load_config_stream_result(sid) != _tm.OK:
            raise Exception('load_config_stream failed')
        return gnmi_pb2.UpdateResult.UPDATE

    def build_obj(self, obj, elem):
        if elem.key:
            assert isinstance(obj, dict), 'cannot apply keys to a non-container'
            obj.update(elem.key)
        return {elem.name: obj}

    def update(self, updates):
        log.debug("==> updates=%s", updates)
        ops = [(up.path, self.apply_update(up.path, up.val)) for up in updates]
        log.debug("<== ops=%s", ops)
        return ops

    def delete(self, paths):
        def do_delete(path):
            gpath = self.adapter.fix_path_prefixes(make_formatted_path(path, self.prefix))
            self.trans.delete(gpath)
            return (path, gnmi_pb2.UpdateResult.DELETE)
        log.debug("==> paths=%s", paths)
        ops = [do_delete(path) for path in paths]
        log.debug("<== ops=%s", ops)
        return ops
