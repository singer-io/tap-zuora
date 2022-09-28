"""Test tap discovery mode and metadata/annotated-schema."""
import unittest
#from tap_tester import menagerie, connections

from test_zuora_discovery_rest import DiscoveryTest


class DiscoveryTestAQUA(DiscoveryTest):
    """Test tap discovery mode with the AQUA API selected"""

    @staticmethod
    def name():
        return "tap_tester_zuora_discovery_aqua"

    def test_discovery(self):
        self.zuora_api_type = "AQUA"
        self.discovery_test()