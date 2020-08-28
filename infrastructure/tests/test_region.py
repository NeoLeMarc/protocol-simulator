import unittest
from unittest.mock import Mock, MagicMock
from infrastructure.region import *
from infrastructure.network import *
from util.builder import awsbuilder

class TestRegion(unittest.TestCase):
    CLASS = Region
    IdentifierCLASS = Identifier
    MessageCLASS = Message
    InfrastructureServiceCLASS = InfrastructureService

    def test_setup(self):
        """Class can be correctly instantiated"""
        a = self.CLASS(None, regionName = 'test-region')
        self.assertTrue(True)
        self.assertTrue(isinstance(a, Region))
        return a

    def test_givenAregionAndANetworkPathWhenTryingToConnectToAnotherRegionThenRegionsAreCorrectlyConnected(self):
        """Given a region and a network path - when trying to connect to another region - then regions are correcly connected"""
        env = Mock()
        r1 = self.CLASS(env, regionName = 'region1')
        r2 = self.CLASS(env, regionName = 'region2')

        r1ToR2 = Mock()

        r1.connectRegion(r2, r1ToR2)

        r1ToR2.connectLeftSide.assert_called_with(r1)
        r1ToR2.connectRightSide.assert_called_with(r2)

    def test_givenTwoRegionsConnectedOverNetworkPathWhenSendingThenSendIsTriggerdOnConnection(self):
        """Given two regions connected over network path when sending - then send is triggered on connection"""
        env = Mock()
        r1 = self.CLASS(env, regionName = 'region1')
        r2 = self.CLASS(env, regionName = 'region2')

        r1ToR2 = Mock()
        r2ToR1 = Mock()

        r1.connectRegion(r2, r1ToR2)
        r2.connectRegion(r1, r2ToR1)

        sender = AWSIdentifier('region1', 'Sender')
        receiver = AWSIdentifier('region2', 'Receiver')
        message = AWSMessage(sender, receiver, 'test')

        r1.sendToRegion('region2', 'test', message)

        r1ToR2.send.assert_called_with(message)

    def test_givenTwoRegionsConnectedOverNetworkPathWhenSendingThenSendIsTriggerdOnConnection(self):
        """Given two regions connected over network path when sending - then send is triggered on connection"""
        env = Mock()
        r1 = self.CLASS(env, regionName = 'region1')
        r2 = self.CLASS(env, regionName = 'region2')

        r1ToR2 = Mock()
        r2ToR1 = Mock()

        r1.connectRegion(r2, r1ToR2)
        r2.connectRegion(r1, r2ToR1)

        sender = self.IdentifierCLASS('region1', 'Sender')
        receiver = self.IdentifierCLASS('region2', 'Receiver')
        message = self.MessageCLASS(sender, receiver, 'test')

        r1.sendToRegion('region2', 'test', message)

        r1ToR2.send.assert_called_with(message)

    def test_givenARegionWhenRegisteringAServiceThenServiceIsCorrectlyRegistered(self):
        """Given a region - when registering a service - then service is correctly registered"""
        env = Mock()
        r1 = self.CLASS(env, regionName = 'region1')
        service = Mock(self.InfrastructureServiceCLASS)

        r1.registerService(service, 'test_service')

        self.assertTrue('test_service' in r1.services)
        self.assertTrue(r1.services['test_service'] == service)

    def test_givenARegionWithARegisteredServiceWhenCallingNotifyNetworkReceiveThenRegionCorrectlyDeliversMessageToService(self):
        """Given a region with a registered service - when calling notify network receive - then region correclty delivers message to service"""
        env = Mock()
        r1 = self.CLASS(env, regionName = 'region1')
        service = self.InfrastructureServiceCLASS(r1)
        service.receive = MagicMock()

        np = Mock()
        
        sender = Mock(self.IdentifierCLASS)
        receiver = self.IdentifierCLASS(r1.regionName, service.serviceName) 
        message = self.MessageCLASS(sender, receiver, 'testMessage')

        np.receive = MagicMock(return_value=(message))

        generator = r1.notifyNetworkReceive(np)
        message = next(generator)
        try:
            generator.send(message)
        except StopIteration:
            pass

        service.receive.assert_called_with(message)

    def test_givenARegionWithARegisteredServiceWhenTryingToSendToLocalRegionAndLocalServiceThenMessageIsLocallyDelivered(self):
        """Given a region with a registered service - when trying to send to local region and local service - then message is delivered locally"""
        env = Mock()
        r1 = self.CLASS(env, regionName = 'region1')
        service = self.InfrastructureServiceCLASS(r1)
        service.receive = MagicMock()
        sender = Mock(self.IdentifierCLASS)
        receiver = self.IdentifierCLASS(r1.regionName, service.serviceName) 
        message = self.MessageCLASS(sender, receiver, 'testMessage')
        r1.sendToRegion('region1', message.receiver.receiverName, message)
        service.receive.assert_called_with(message)

    def test_givenTwoConnectedRegionsWhenCallingGetRoundtripThenCorrectRoundtripIsReturned(self):
        """Given two connected regions - when calling get roundtrip - then correct roundtrip is returned"""
        env = Mock()
        LATENCY1 = 11
        LATENCY2 = 22
        ROUNDTRIP = LATENCY1 + LATENCY2
        REGION_NAME1 = 'region1'
        REGION_NAME2 = 'region2'

        r1 = self.CLASS(env, regionName = REGION_NAME1)
        r2 = self.CLASS(env, regionName = REGION_NAME2)

        np1 = NetworkPath(Mock(), 'r1-to-r2', LATENCY1)
        np2 = NetworkPath(Mock(), 'r2-to-r1', LATENCY2)

        r1.connectRegion(r2, np1)
        r2.connectRegion(r1, np2)
        
        self.assertEqual(r1.getRoundtrip(REGION_NAME2), ROUNDTRIP)
        self.assertEqual(r2.getRoundtrip(REGION_NAME1), ROUNDTRIP)

    def test_givenTwoConnectedRegionsWhenCallingGetLatencyThenCorrectLatencyIsReturned(self):
        """Given two connected regions - when calling get latency - then correct latency is returend"""
        env = Mock()
        LATENCY1 = 11
        LATENCY2 = 22
        REGION_NAME1 = 'region1'
        REGION_NAME2 = 'region2'

        r1 = self.CLASS(env, regionName = REGION_NAME1)
        r2 = self.CLASS(env, regionName = REGION_NAME2)

        np1 = NetworkPath(Mock(), 'r1-to-r2', LATENCY1)
        np2 = NetworkPath(Mock(), 'r2-to-r1', LATENCY2)

        r1.connectRegion(r2, np1)
        r2.connectRegion(r1, np2)
        
        self.assertEqual(r1.getLatency(REGION_NAME2), LATENCY1)
        self.assertEqual(r2.getLatency(REGION_NAME1), LATENCY2)



class TestService(unittest.TestCase):
    RegionCLASS = Region
    ServiceCLASS = Service

    def test_setup(self):
        """Service can be correctly instantiated"""
        SERVICE_NAME = 'DUMMYSERVICE'
        env = Mock()
        r1 = Mock(self.RegionCLASS)

        service = self.ServiceCLASS(r1, SERVICE_NAME)

        r1.registerService.assert_called_with(service, SERVICE_NAME)
        self.assertTrue(isinstance(service, Service))
        return service


class TestInfrastructureService(unittest.TestCase):
    RegionCLASS = Region
    ServiceCLASS = InfrastructureService

    def test_setup(self):
        """AWSInfrastructureService can be correctly instantiated"""
        env = Mock()
        r1 = Mock(self.RegionCLASS)

        service = self.ServiceCLASS(r1)

        r1.registerService.assert_called_with(service, service.serviceName)
        self.assertTrue(isinstance(service, InfrastructureService))
        return service





if __name__ == '__main__':
    unittest.main()

