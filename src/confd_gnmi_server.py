#!/usr/bin/env python3
import argparse
import logging
import logging.config
import sys
import os

from confd_gnmi_api_adapter_defaults import ApiAdapterDefaults


def parse_args(args, parser=None):
    if parser is None:
        parser = argparse.ArgumentParser(description="gNMI Adapter server")
    parser.add_argument("-t", "--type", action="store", dest="type",
                        choices=["demo", "confd", 'nso'],
                        help="gNMI server type",
                        default="demo")
    parser.add_argument("--logging", action="store", dest="logging",
                        help=f"Either logging level (one of {', '.join(logging_levels.keys())}) "
                        "or logging config filename",
                        default="warning")
    parser.add_argument("--insecure", action="store_true", dest="insecure",
                        help="Use insecure connection",
                        default=False)
    parser.add_argument("--port", action="store", dest="port",
                        type=int,
                        help="port number (default: {})".format(50061),
                        default=50061)
    parser.add_argument("--host", action="store", dest="host",
                        help="host (name, ip, default: {})".format('localhost'),
                        default='localhost')
    parser.add_argument("-d", "--confd-nso-debug", action="store",
                        dest="confd_nso_debug",
                        choices=["trace", "debug", "silent", "proto"],
                        help="ConfD or NSO debug level",
                        default="debug")
    parser.add_argument("--confd-nso-addr", action="store", dest="confd_nso_addr",
                        help="ConfD or NSO IP address (default is {})".format(
                            ApiAdapterDefaults.ADDR),
                        default=ApiAdapterDefaults.ADDR)
    parser.add_argument("--confd-nso-port", action="store", dest="confd_nso_port",
                        help="IPC port (ConfD or NSO)")
    parser.add_argument("--monitor-external-changes", action="store_true",
                        dest="monitor_external_changes",
                        help="start external changes service",
                        default=ApiAdapterDefaults.MONITOR_EXTERNAL_CHANGES)
    parser.add_argument("--external-port", action="store", dest="external_port",
                        help="Port of external changes service (default is {})".format(
                            ApiAdapterDefaults.EXTERNAL_PORT),
                        default=ApiAdapterDefaults.EXTERNAL_PORT, type=int)
    parser.add_argument("--cfg", action="store", dest="cfg",
                        help="config file")
    parser.add_argument("--key", action="store", dest="key",
                        help="Path to the server key.",
                        default="server.key")
    parser.add_argument("--crt", action="store", dest="crt",
                        help="Path to the server certificate.",
                        default="server.crt")
    opt = parser.parse_args(args=args)
    return opt


def config_logging(
        logging_option,
        format='%(asctime)s %(levelname)s:%(filename)s:%(lineno)s:%(funcName)s '
               '%(message)s [%(threadName)s]',
        force=False):
    if logging_option in logging_levels:
        level = logging_levels[logging_option]
        logging.basicConfig(format=format, level=level, force=force)
    else:
        filename = logging_option
        logging.config.fileConfig(filename)


logging_levels = {"error": logging.ERROR,
                  "warning": logging.WARNING,
                  "info": logging.INFO,
                  "debug": logging.DEBUG}

if __name__ == '__main__':
    opt = parse_args(args=sys.argv[1:])
    config_logging(opt.logging)
    log = logging.getLogger('confd_gnmi_server')
    from confd_gnmi_service import AdapterType, ConfDgNMIServicer
    adapter_type = AdapterType.DEMO
    if opt.type == "confd" or opt.type == "nso":
        if opt.type == "confd":
            sys.path.append(os.getenv('CONFD_DIR') + "/src/confd/pyapi/confd")
        else:
            sys.path.append(os.getenv('NCS_DIR') + "/src/ncs/pyapi/ncs")
        from confd_gnmi_api_adapter import GnmiConfDApiServerAdapter
        adapter_type = AdapterType.API
        GnmiConfDApiServerAdapter.set_tm_debug_level(opt.confd_nso_debug)
        GnmiConfDApiServerAdapter.set_addr(opt.confd_nso_addr)
        if opt.confd_nso_port:
            GnmiConfDApiServerAdapter.set_port(int(opt.confd_nso_port))
        GnmiConfDApiServerAdapter.set_external_port(int(opt.external_port))
        GnmiConfDApiServerAdapter.set_monitor_external_changes(
            bool(opt.monitor_external_changes))
    # elif opt.type == "netconf":
    #     adapter_type = AdapterType.NETCONF
    elif opt.type == "demo":
        adapter_type = AdapterType.DEMO
        if opt.cfg:
            log.info("processing config file opt.cfg=%s", opt.cfg)
            with open(opt.cfg, "r") as cfg_file:
                cfg = cfg_file.read()
            log.debug("cfg=%s", cfg)
            from confd_gnmi_demo_adapter import GnmiDemoServerAdapter

            GnmiDemoServerAdapter.load_config_string(cfg)
    else:
        log.warning("Unknown server type %s", opt.type)

    server = ConfDgNMIServicer.serve(opt.port, adapter_type, insecure=opt.insecure,
                                     key_file=opt.key, crt_file=opt.crt)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log.info('exit on interrupt')
