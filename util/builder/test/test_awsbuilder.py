#!/usr/bin/env python3
#
# Unit tests for AWS Builder

import unittest
from unittest.mock import Mock, MagicMock
from util.builder.awsbuilder import *

class TestAWSBuilder(unittest.TestCase):

    def test_buildTwoConnectedRegions(self):
        """Test that buildTwoConnectedRegions() builds two regions as specified and both of them are connected"""
        REGION_NAME1 = 'region1'
        REGION_NAME2 = 'region2'
        LATENCY1 = 100
        LATENCY2 = 200

        regions = b.buildTwoConnectedRegions(Mock(), REGION_NAME1, REGION_NAME2, LATENCY1, LATENCY2)


        self.assertEqual(2, len(regions))
        self.assertTrue(isinstance(regions[0], AWSRegion))
        self.assertTrue(isinstance(regions[1], AWSRegion))
        self.assertEqual(REGION_NAME1, regions[0].regionName)
        self.assertEqual(REGION_NAME2, regions[1].regionName)
        region1, region2 = regions
        self.verifyTwoRegionsAreConnected(region1, region2, LATENCY1, LATENCY2)

    def verifyTwoRegionsAreConnected(self, region1, region2, LATENCY1 = -1, LATENCY2 = -1):
        REGION_NAME1 = region1.regionName
        REGION_NAME2 = region2.regionName

        self.assertTrue(REGION_NAME2 in region1.connectedRegions)
        self.assertTrue(REGION_NAME1 in region2.connectedRegions)

        np1 = region1.connectedRegions[REGION_NAME2][1]
        np2 = region2.connectedRegions[REGION_NAME1][1]

        self.assertEqual(np1.leftSide, region1)
        self.assertEqual(np1.rightSide, region2)

        self.assertEqual(np2.leftSide, region2)
        self.assertEqual(np2.rightSide, region1)

        if LATENCY1 >= 0:
            self.assertEqual(np1.latency, LATENCY1)

        if LATENCY2 >= 0:
            self.assertEqual(np2.latency, LATENCY2)

    def test_buildFullyMeshedRegions(self):
        """buildFullyMeshedRegions() can build 4 regions that are fully meshed and correctly set up"""

        regionsNAL = self.buildRegionsNAL()
        regions = b.buildFullyMeshedRegions(Mock(), regionsNAL)

        self.assertEqual(len(regions), len(regionsNAL))
        self.assertTrue(len(regionsNAL) > 0)

        regionNames = [region.regionName for region in regions]

        count = 0
        for regionNAL in regionsNAL:
            self.assertTrue(regionNAL['regionName'] in regionNames)
            region = self.getRegionFromRegions(regionNAL['regionName'], regions) 
            
            for otherRegion in regions:
                if otherRegion != region:
                    self.verifyTwoRegionsAreConnected(region, otherRegion)
                    count += 1

        self.assertEqual(count, (len(regionsNAL) - 1) * len(regionsNAL))


    def getRegionFromRegions(self, regionName, regions):
        for region in regions:
            if regionName == region.regionName:
                return region


    def buildRegionsNAL(self):
        regionsNAL = []
        regionsNAL.append({'regionName' : 'us-west-1', 'latency' : 10})
        regionsNAL.append({'regionName' : 'us-east-1', 'latency' : 10})
        regionsNAL.append({'regionName' : 'eu-central-1', 'latency' : 40})
        regionsNAL.append({'regionName' : 'apac-central-1', 'latency' : 60})
        return regionsNAL


    def test_buildNetworkPaths(self):
        regionsNAL = self.buildRegionsNAL()
        myRegionNAL = regionsNAL[1]
        paths = b.buildNetworkPaths(Mock(), myRegionNAL, regionsNAL)

        self2self = "%s-to-%s" % (myRegionNAL['regionName'], myRegionNAL['regionName'])
        self.assertTrue(self2self not in paths)

        count = 0
        for region in regionsNAL:
            if region != myRegionNAL:
                pathname = "%s-to-%s" % (myRegionNAL['regionName'], region['regionName'])
                self.assertTrue(pathname in paths)
                count += 1

        self.assertEqual(count, len(regionsNAL) - 1) # Path to each other region exists


    def test_buildNetworkPath(self):
        """buildNetworkPath() builds a valid network path"""
        REGION_NAME1 = "bla-foo-bar-region-02"
        REGION_NAME2 = "magic-fuzzy-region-01"
        sourceRegion = {'regionName' : REGION_NAME1, 'latency' : 10}
        targetRegion = {'regionName' : REGION_NAME2, 'latency' : 20}
        np = b.buildNetworkPath(Mock(), sourceRegion, targetRegion, 10)

        self.assertTrue(isinstance(np, NetworkPath))
        self.assertEqual(np.latency, 10)
        self.assertEqual(np.name, "%s-to-%s" % (REGION_NAME1, REGION_NAME2))
        self.assertEqual(np.leftSide, None) 
        self.assertEqual(np.rightSide, None) 

    def test_buildRegion(self):
        """buildRegion() builds a correcly set up region"""
        REGION_NAME = 'testRegion'
        r = b.buildRegion(regionName = REGION_NAME)
        
        self.assertTrue(isinstance(r, AWSRegion))
        self.assertEqual(REGION_NAME, r.regionName)

    def test_buildCustomerNetworkPath(self):
        class MyNetworkPath(NetworkPath):

            def test(self):
                return True

        """When network path is replaced with own class, buildNetworkPath returns an instance of this class"""
        b = AWSBuilder()
        nal = self.buildRegionsNAL()
        np = b.buildNetworkPath(Mock(), nal[0], nal[1], 10)

        self.assertTrue(isinstance(np, NetworkPath))
        b.getNetworkPathInstance = lambda env, name, latency : MyNetworkPath(env, name, latency)

        mnp = b.buildNetworkPath(Mock(), nal[0], nal[1], 10)
        self.assertTrue(isinstance(mnp, MyNetworkPath))
        self.assertTrue(mnp.test())


if __name__ == '__main__':
    unittest.main()
