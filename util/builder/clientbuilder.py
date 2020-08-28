#!/usr/bin/eny python3
from infrastructure.network import *
from infrastructure.client import *
from unittest.mock import Mock, MagicMock
from util.builder import regionbuilder

class ClientBuilder(object):

    def buildClientRegionsAndConnectAWSRegions(self, env, clientRegionsToBuild, awsRegions):
        regionByName = {}
        crs = []
        for region in awsRegions:
            regionByName[region.regionName] = region

        for c in clientRegionsToBuild:
            cr = self.buildClientRegionWithClients(env, c['regionName'], 10)

            for cregion in c['connectedAWSRegions']:
                regionbuilder.b.connectTwoRegions(env, cr, regionByName[cregion[0]], latency1 = cregion[1], latency2 = cregion[1])
            crs.append(cr)
        return crs

    def buildClientRegionWithClients(self, env, regionName, numberOfClients):

        cr = ClientRegion(env, regionName)

        for i in range(0, numberOfClients):
            c = Client(cr, 'Client-%i' % i)

        return cr


b = ClientBuilder()
