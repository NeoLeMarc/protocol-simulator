#!/usr/bin/eny python3
from util.builder import regionbuilder
from infrastructure.aws import *
from infrastructure.network import *
from unittest.mock import Mock, MagicMock

class AWSBuilder(object):

    def __init__(self):
        self.getNetworkPathInstance = lambda env, name, latency: NetworkPath(env, name, latency)
        self.regionBuilder = regionbuilder.RegionBuilder(AWSRegion)
        self.regionBuilder.getNetworkPathInstance = lambda env, name, latency: self.getNetworkPathInstance(env, name, latency)

    def buildTwoConnectedRegions(self, env, regionName1, regionName2, latency1 = 10, latency2 = 10):
        return self.regionBuilder.buildTwoConnectedRegions(env, regionName1, regionName2, latency1, latency2)

    def connectTwoRegions(self, env, region1, region2, latency1 = 10, latency2 = 10):
        return self.regionBuilder.connectTwoRegions(env, region1, region2, latency1, latency2)


    def buildFullyMeshedRegionsWithKinesis(self, env, regionNamesAndLatencies, TTL = 0):
        """Build fully meshed regions with kinesis services in each

        Each region will have one primary kinesis and remote kinesis to each other region"""

        regions = self.buildFullyMeshedRegions(env, regionNamesAndLatencies)

        for region in regions:
            kinesis = AWSKinesis(region, env, TTL)
            for otherRegion in regions:
                if otherRegion != region:
                    AWSRemoteKinesis(region, otherRegion.regionName)

        return regions


    def buildFullyMeshedRegions(self, env, regionNamesAndLatencies):
        return self.regionBuilder.buildFullyMeshedRegions(env, regionNamesAndLatencies)

    def buildNetworkPaths(self, env, regionNAL, regionNamesAndLatencies):
        return self.regionBuilder.buildNetworkPaths(env, regionNAL, regionNamesAndLatencies)

    def buildNetworkPath(self, env, sourceRegion, targetRegion, latency):
        return self.regionBuilder.buildNetworkPath(env, sourceRegion, targetRegion, latency)

    def buildRegion(self, env = None, regionName = 'myregion'):
        return self.regionBuilder.buildRegion(env, regionName)

    def buildRemoteService(self, localRegion = None, remoteRegionName = 'remote-region', remoteServiceName = 'Kinesis'):
        if localRegion == None:
            localRegion = self.buildRegion(regionName = 'localRegion')
        return AWSRemoteService(localRegion, remoteRegionName, remoteServiceName)

    def buildRemoteKinesis(self, localRegion = None, remoteRegionName = 'remote-region'):
        if localRegion == None:
            localRegion = self.buildRegion(regionName = 'localRegion')
        return AWSRemoteKinesis(localRegion, remoteRegionName)

    def buildLocalService(self, localRegion = None, serviceName = 'localService'):
        if localRegion == None:
            localRegion = self.buildRegion(regionName = 'localRegion')
        return AWSService(localRegion, serviceName)

class AWSBuilderWithLossyNetwork(AWSBuilder):

    def __init__(self):
        super(AWSBuilderWithLossyNetwork, self).__init__()
        self.getNetworkPathInstance = lambda env, name, latency: LossyNetworkPath(env, name, latency, lossProbability = 0)



b = AWSBuilder()
