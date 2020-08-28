import unittest
import sys
sys.path.append(".")
from unittest.mock import Mock, MagicMock
from infrastructure.aws import *
from infrastructure.client import *
from infrastructure.network import *
from components.sim import *
import simpy
from util.builder import awsbuilder 
from util.builder import clientbuilder
from util.printer import p

class TestClientIntegration(unittest.TestCase):

    def test_setup(self):
        """Client Region with clients can be correctly initialized"""
        env = simpy.Environment()

        cr1 = clientbuilder.b.buildClientRegionWithClients(env, 'APAC-Clients', 10)
        b = awsbuilder.AWSBuilderWithLossyNetwork()
        r1, r2 = b.buildTwoConnectedRegions(env, 'us-west-1', 'us-east-2', 10, 10)

        b.connectTwoRegions(env, cr1, r1, 130, 130)
        b.connectTwoRegions(env, cr1, r2, 120, 120)

       
    def test_givenAClientRegionAndAnAwsRegionWithALoadbalancerWithTwoRegisteredServicesWhenSendingMessagesToTheLoadbalancerThenMessagesAreDistributedOverBothServices(self):
        """Given a client region and an AWS Region with a Loadbalancer with two registered services - when sending messages to the load balancer - then messages are distributed over both services"""

        class TestService(AWSPublicService):
            pass

        env = simpy.Environment()

        cr1 = clientbuilder.b.buildClientRegionWithClients(env, 'APAC-Clients', 10)
        b = awsbuilder.AWSBuilderWithLossyNetwork()
        r1, r2 = b.buildTwoConnectedRegions(env, 'us-west-1', 'us-east-2', 10, 10)

        b.connectTwoRegions(env, cr1, r1, 130, 130)
        b.connectTwoRegions(env, cr1, r2, 120, 120)

        lb1 = AWSLoadBalancer(r1, 'Test-LB1')
        srv1 = TestService(r1, "Public Service 1")
        srv2 = TestService(r1, "Public Service 2")
        lb1.registerInstance(srv1)
        lb1.registerInstance(srv2)


        m1 = Message(Mock(Identifier), Mock(Identifier), 'test 1')

        srv1Called = 0
        srv2Called = 0

        for i in range(0, 100):
            srv1.publicReceive = Mock()
            srv2.publicReceive = Mock()

            lb1.receive(m1)

            self.assertNotEqual(srv1.publicReceive.called, srv2.publicReceive.called)

            if srv1.publicReceive.called:
                srv1Called += 1

            if srv2.publicReceive.called:
                srv2Called += 1 

        self.assertEqual(100, srv1Called + srv2Called)
        self.assertTrue(srv1Called in range(40, 60))
        self.assertTrue(srv2Called in range(40, 60))

    def test_givenMultipleClientRegionsAndMultipleAWSRegionsAndRoute53WhenTryingToUseServiceThenEachClientRegionGetsClosesAWSRegionForAServiceAndCommunicationWorks(self):
        """Given multiple client regions and multiple AWS regions and Route 53 - when trying to use service - then each client region gets closest AWS region for a service and communication works"""

        
        class TestService(AWSPublicService):
            MESSAGES = []

            def publicReceive(self, data):
                self.MESSAGES.append(data)
        
        env = simpy.Environment()
        SERVICE_NAME = 'test-service'


        route53 = AWSRoute53() 

        ## Start with building 2 instances of a service in 3 regions
        regionsToBuild = [
                {'regionName' : 'us-west-1',    'latency' : 10},
                {'regionName' : 'us-east-1',    'latency' : 10},
                {'regionName' : 'eu-central-1', 'latency' : 10}
        ]

        awsRegions = awsbuilder.b.buildFullyMeshedRegions(env, regionsToBuild)

        for region in awsRegions:
            srv1 = TestService(region, 'Public Service 1')
            srv2 = TestService(region, 'Public Service 2')
            lb = AWSLoadBalancer(region, 'Test-LB1')
            lb.registerInstance(srv1)
            lb.registerInstance(srv2)
            route53.registerLoadBalancer(SERVICE_NAME, lb)


        ## Build 4 client regions
        clientRegionsToBuild = [
                {'regionName' : 'clients-us-west-1',      'connectedAWSRegions' : [('us-west-1'    , 10),
                                                                                   ('us-east-1'    , 20),
                                                                                   ('eu-central-1' , 40)]},
                {'regionName' : 'clients-us-east-1',      'connectedAWSRegions' : [('us-west-1'    , 20),
                                                                                   ('us-east-1'    , 10),
                                                                                   ('eu-central-1' , 30)]},

                {'regionName' : 'clients-eu-central-1',   'connectedAWSRegions' : [('us-west-1'    , 50),
                                                                                   ('us-east-1'    , 40),
                                                                                   ('eu-central-1' , 10)]},

                {'regionName' : 'clients-apac-central-1', 'connectedAWSRegions' : [('us-west-1'    , 90),
                                                                                   ('us-east-1'    , 80),
                                                                                   ('eu-central-1' , 40)]}
        ]

        clientRegions = clientbuilder.b.buildClientRegionsAndConnectAWSRegions(env, clientRegionsToBuild, awsRegions)

        ## Lookup loadbalancers from route 53 from all regions
        lbLists = []
        expectedLBCount = len(regionsToBuild)

        sentMessages = []
        for i in range(0, len(clientRegions)):
            lbList = route53.lookup(SERVICE_NAME, clientRegions[i])
            self.assertEqual(len(lbList), expectedLBCount)
            lbLists.append(lbList)


            ## Assert that the loadbalancers are sorted by distance
            regionNal = list(filter(lambda r: r['regionName'] == clientRegions[i].regionName, clientRegionsToBuild))[0] 
            closestRegion = sorted(regionNal['connectedAWSRegions'], key = lambda x: x[1])

            self.assertEqual(lbList[0].regionName, closestRegion[0][0])
            self.assertEqual(lbList[1].regionName, closestRegion[1][0])
            self.assertEqual(lbList[2].regionName, closestRegion[2][0])

            ## Now send a message to the closest loadbalancer
            region = clientRegions[i]
            sender = list(region.services.values())[0].getIdentifier()
            receiver = lbList[0]
            message = Message(sender, receiver, 'Test')
            region.send(message) 
            sentMessages.append(message)

        ## Assert that all messages are correctly delivered
        env.run(until = 200)
        for message in sentMessages:
            self.assertIn(message, TestService.MESSAGES)

if __name__ == '__main__':
    unittest.main()
