#!/usr/bin/eny python3
from infrastructure.region import *
from infrastructure.network import *
from unittest.mock import Mock, MagicMock
from simpy import *

class RegionBuilder(object):

    def __init__(self, RegionCLASS):
        self.RegionCLASS = RegionCLASS
        self.getNetworkPathInstance = lambda env, name, latency: NetworkPath(env, name, latency)

    def buildTwoConnectedRegions(self, env, regionName1, regionName2, latency1 = 10, latency2 = 10):
        """Builds two connected regions having two distinct network paths between them"""
        r1 = self.buildRegion(env = env, regionName = regionName1)
        r2 = self.buildRegion(env = env, regionName = regionName2)

        r1ToR2 = self.getNetworkPathInstance(env, name = 'r1-to-r2', latency = latency1)
        r2ToR1 = self.getNetworkPathInstance(env, name = 'r2-to-r1', latency = latency2) 

        r1.connectRegion(r2, r1ToR2)
        r2.connectRegion(r1, r2ToR1)

        return r1, r2

    def buildRegion(self, env = None, regionName = 'myregion'):
        if env == None:
            env = Mock(Environment)
        return self.RegionCLASS(env, regionName = regionName)

    def connectTwoRegions(self, env, region1, region2, latency1 = 10, latency2 = 10):
        r1ToR2 = self.getNetworkPathInstance(env, name = "%s-to-%s" % (region1.regionName, region2.regionName), latency = latency1)
        r2ToR1 = self.getNetworkPathInstance(env, name = "%s-to-%s" % (region2.regionName, region1.regionName), latency = latency2) 

        region1.connectRegion(region2, r1ToR2)
        region2.connectRegion(region1, r2ToR1)

    def buildFullyMeshedRegions(self, env, regionNamesAndLatencies):
        """Builds fully meshed regions

        About latencies: it is assumed, that the further regions in 
        regionNamesAndLatencies are sorted by relative distance. Each of them
        carry a value (latency) by which the 'relative latency counter' 
        is increased

        Args:
            env: simpy environment
            regionNamesAndLatencies: [{'regionName' : <name of region>,
                                       'latency'    : <latency to add to latency counter>}]
        """

        regionKeys = []
        regions = {}
        paths = {}
        connectedRegions = []
       
        for regionNAL in regionNamesAndLatencies:
            paths.update(self.buildNetworkPaths(env, regionNAL, regionNamesAndLatencies))
            region = self.buildRegion(env = env, regionName = regionNAL['regionName'])
            regions[region.regionName] = region
            regionKeys.append(region.regionName)

        self.meshRegions(regions, paths)
        return [regions[x] for x in regionKeys]

    def meshRegions(self, regions, paths):
        for sourceRegionName in regions:
            for targetRegionName in regions:
                if sourceRegionName != targetRegionName:
                    sourceRegion = regions[sourceRegionName]
                    targetRegion = regions[targetRegionName]
                    path = paths["%s-to-%s" % (sourceRegionName, targetRegionName)]
                    sourceRegion.connectRegion(targetRegion, path)

    def buildNetworkPaths(self, env, regionNAL, regionNamesAndLatencies):
        pos = regionNamesAndLatencies.index(regionNAL)
        beforeRegion = regionNamesAndLatencies[:pos]
        afterRegion = regionNamesAndLatencies[pos+1:]

        paths = {} 
        relativeLatency = regionNAL['latency']
        for reg in reversed(beforeRegion):
            path = self.buildNetworkPath(env, regionNAL, reg, relativeLatency + reg['latency']/2)
            paths[path.name] = path
            relativeLatency += reg['latency']

        relativeLatency = regionNAL['latency'] 
        for reg in afterRegion:
            path = self.buildNetworkPath(env, regionNAL, reg, relativeLatency + reg['latency']/2)
            paths[path.name] = path
            relativeLatency += reg['latency']

        return paths

    def buildNetworkPath(self, env, sourceRegion, targetRegion, latency):
        """Build a network path from sourceRegion to targetRegion with latency"""
        name = "%s-to-%s" % (sourceRegion['regionName'], targetRegion['regionName'])
        return self.getNetworkPathInstance(env, name = name, latency = latency)





b = RegionBuilder(Region)
