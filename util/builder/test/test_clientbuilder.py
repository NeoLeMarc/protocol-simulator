import unittest
from unittest.mock import Mock, MagicMock
from infrastructure.aws import *
from util.builder import awsbuilder, clientbuilder
from infrastructure.tests.test_region import * 
from infrastructure.region import *

class TestClientBuilder(unittest.TestCase):

    def __init__(self, foo):
        super(TestClientBuilder, self).__init__(foo)
        self.clientRegionsToBuild = [
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

        self.awsRegionsToBuild = [
                {'regionName' : 'us-west-1',    'latency' : 10},
                {'regionName' : 'us-east-1',    'latency' : 10},
                {'regionName' : 'eu-central-1', 'latency' : 10}
        ]


    def test_buildClientRegionsAndConnectAWSRegions(self):
        clientRegions = self.prepareClientRegions()
        self.assertEqual(len(clientRegions), len(self.clientRegionsToBuild))

        for awsRegion in self.awsRegionsToBuild:
            for region in clientRegions:
                self.assertIn(awsRegion['regionName'], region.connectedRegions)

        regionNames = [region.regionName for region in clientRegions] 

        for regionNal in self.clientRegionsToBuild:
            self.assertIn(regionNal['regionName'], regionNames) 

    def prepareClientRegions(self):
        env = Mock()
        ## Start with building 2 instances of a service in 3 regions
        awsRegions = awsbuilder.b.buildFullyMeshedRegions(env, self.awsRegionsToBuild)

        ## Build 4 client regions
        clientRegions = clientbuilder.b.buildClientRegionsAndConnectAWSRegions(env, self.clientRegionsToBuild, awsRegions)
        return clientRegions

if __name__ == '__main__':
    unittest.main()
