import unittest
from unittest.mock import Mock, MagicMock
from infrastructure.client import *
from util.builder import awsbuilder
from infrastructure.tests.test_region import * 

class TestClientRegion(TestRegion):
    CLASS = ClientRegion
    IdentifierCLASS = Identifier
    MessageCLASS = Message
    InfrastructureServiceCLASS = InfrastructureService


    def test_setup(self):
        a = super(TestClientRegion, self).test_setup()
        self.assertTrue(isinstance(a, ClientRegion))

class TestClient(TestService):
    RegionCLASS = ClientRegion
    ServiceCLASS = Client
