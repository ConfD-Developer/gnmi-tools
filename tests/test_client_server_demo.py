import pytest

from client_server_test_base import AdapterTests
from confd_gnmi_server import AdapterType
from confd_gnmi_demo_adapter import GnmiDemoServerAdapter

_confd_DEBUG = 1


@pytest.mark.grpc
@pytest.mark.demo
@pytest.mark.usefixtures("fix_method")
class TestGrpcDemo(AdapterTests):

    def set_adapter_type(self):
        self.adapter_type = AdapterType.DEMO

    def _do_reset_cfg(self):
        yield
        GnmiDemoServerAdapter.reset()
