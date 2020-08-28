#!/usr/bin/env python
#
# Generic region functionality
from util.helper import singleton

class RegionError(Exception):
    pass

class Region(object):

    def __init__(self, env, regionName):
        self.env = env
        self.regionName = regionName
        self.connectedRegions = {}
        self.services = {}

    def connectRegion(self, otherRegion, networkPath):
        if otherRegion.regionName not in self.connectedRegions:
            self.connectedRegions[otherRegion.regionName] = (otherRegion, networkPath)
            networkPath.connectLeftSide(self)
            networkPath.connectRightSide(otherRegion)
        else:
            raise RegionError("Region already connected")

    def getLatency(self, otherRegionName):
        if otherRegionName not in self.connectedRegions:
            raise RegionError("Region not connected")
        else:
            np = self.connectedRegions[otherRegionName][1]
            return np.latency

    def getRoundtrip(self, otherRegionName):
        if otherRegionName not in self.connectedRegions:
            raise RegionError("Region not connected")
        else:
            otherRegion = self.connectedRegions[otherRegionName][0]
            return self.getLatency(otherRegionName) + otherRegion.getLatency(self.regionName)

    def send(self, message):
        if not isinstance(message, Message):
            raise RegionError("Message is not of type Message")
        else:
            self.sendToRegion(message.receiver.regionName, message.receiver.receiverName, message)

    def sendToRegion(self, regionName, service, message):
        if not isinstance(message, Message):
            raise RegionError("Message is not of type AWSMessage")
        elif regionName != message.receiver.regionName:
            raise RegionError("Receiver region in mesage does not match target region")
        elif regionName == self.regionName:
            self.localReceive(message)
        elif regionName not in self.connectedRegions:
            raise RegionError('No path to region %s' % regionName)
        else:
            networkPath = self.connectedRegions[regionName][1]
            networkPath.send(message)

    def registerService(self, service, serviceName):
        if not isinstance(service, Service):
            raise RegionError("Service is not a instance of Service")

        if serviceName not in self.services:
            self.services[serviceName] = service
        else:
            raise RegionError("Service already registered")

    def notifyNetworkReceive(self, networkPath):
        message = yield networkPath.receive()

        if not isinstance(message, Message):
            raise RegionError("Message is not of type Message")
        else:
            self.localReceive(message)

        receiver = message.receiver.receiverName
        if receiver in self.services:
            self.services[receiver].receive(message)
        else:
            raise RegionError("Service %s not registered" % receiver)

    def localReceive(self, message):
        receiver = message.receiver.receiverName
        if receiver in self.services:
            self.services[receiver].receive(message)
        else:
            raise RegionError("Service %s not registered" % receiver)

    def getServiceByName(self, serviceName):
        if serviceName in self.services:
            return self.services[serviceName]
        else:
            raise RegionError("Service not registered")


class Service(object):

    def __init__(self, region, name):
        if not isinstance(region, Region):
            raise RegionError("Type error: region needs to be of type AWSRegion")
        region.registerService(self, name)
        self.serviceName = name
        self.region = region

    def receive(self, message):
        raise RegionError("Unhandled receive")

    def getIdentifier(self):
        a = Identifier(self.region.regionName, self.serviceName)
        return a


class InfrastructureService(Service):

    def __init__(self, region, name = 'Infrastructure Service'):
        super(InfrastructureService, self).__init__(region, name)



class Identifier(object):

    def __new__(cls, regionName, receiverName):
        return IdentifierInstance(regionName, receiverName)

    @classmethod
    def getInstance(regionName, receiverName):
        return IdentifierInstance(self.regionName, self.receiverName)

class IdentifierBehaviour(object):

    def __new__(cls, regionName, receiverName):
        return object.__new__(cls)

    def __init__(self, regionName, receiverName):
        self.regionName = regionName
        self.receiverName = receiverName

@singleton
class IdentifierInstance(IdentifierBehaviour, Identifier):
    pass


class Message(object):
    
    def __init__(self, sender, receiver, payload):
        if not isinstance(sender, Identifier):
            raise RegionError("Type error: expected Identifier for sender")

        if not isinstance(receiver, Identifier):
            raise RegionError("Type error: expected Identifier for receiver")

        self.sender = sender
        self.receiver = receiver
        self.payload = payload

    def makeReply(self, payload):
        return self.__class__(self.receiver, self.sender, payload)


