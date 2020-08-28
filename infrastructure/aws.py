#!/usr/bin/env python3
#
# AWS components for SIM simulator

from infrastructure.region import *
from util.helper import singleton
import random

class AWSError(RegionError):
    pass

class AWSServiceNotLocalOrRemoteServiceError(AWSError):
    pass

class AWSServiceNotLocalError(AWSServiceNotLocalOrRemoteServiceError):
    pass


class AWSRegion(Region):

    def __init__(self, env, regionName):
        super(AWSRegion, self).__init__(env, regionName)

    def notifyNetworkReceive(self, networkPath):
        message = yield networkPath.receive()

        if not isinstance(message, Message):
            raise AWSError("Message is not of type Message")
        else:
            self.localReceive(message)

    def send(self, message):
        if not isinstance(message, AWSMessage):
            raise AWSError("Message is not of type AWSMessage")
        else:
            super(AWSRegion, self).send(message)

    def sendToRegion(self, regionName, service, message):
        if not isinstance(message, AWSMessage):
            raise AWSError("Message is not of type AWSMessage")
        else:
            super(AWSRegion, self).sendToRegion(regionName, service, message)

    def registerService(self, service, serviceName):
        if not isinstance(service, AWSService):
            raise AWSError("Service is not a instance of AWSService")
        else:
            super(AWSRegion, self).registerService(service, serviceName)


class AWSService(Service):

    def __init__(self, region, name):
        if not isinstance(region, AWSRegion):
            raise AWSError("Type error: region needs to be of type AWSRegion")
        super(AWSService, self).__init__(region, name)

    def getAWSIdentifier(self):
        a = AWSIdentifier(self.region.regionName, self.serviceName)
        return a


class AWSPublicService(AWSService):

    def __init__(self, region, name):
        super(AWSPublicService, self).__init__(region, name)

    def publicReceive(self, message):
        raise AWSError("Not implemented")


class AWSInfrastructureService(AWSService, InfrastructureService):

    def __init__(self, region, name = 'Infrastructure Service'):
        super(AWSInfrastructureService, self).__init__(region, name)


class AWSLoadBalancer(AWSInfrastructureService):

    def __init__(self, region, name = 'AWSLoadBalancer'):
        self.instances = []
        super(AWSLoadBalancer, self).__init__(region, name)

    def registerInstance(self, service):
        if not isinstance(service, AWSPublicService):
            raise AWSError("Only public services can be registered to loadbalancer")

        if service not in self.instances:
            self.instances.append(service)

    def receive(self, message):
        if not isinstance(message, Message):
            raise AWSError("Invalid message format")

        else:
            instance = self.getRandomInstance()
            instance.publicReceive(message)

    def getRandomInstance(self):
        if len(self.instances) == 0:
            raise AWSError("No registered instances")

        instanceNo = random.randint(0, len(self.instances) - 1)
        return self.instances[instanceNo]



class AWSKinesisError(AWSError):
    pass

class AWSKinesisStreamExistsError(AWSKinesisError):
    pass

class AWSKinesis(AWSInfrastructureService):
    serviceName = "Kinesis"

    def __init__(self, region, env, ttl = 0):
        self.streams = {}
        self.subscriptions = {}
        self.env = env
        self.ttl = ttl 
        super(AWSKinesis, self).__init__(region, self.serviceName)
        self.startBehaviour()

    def startBehaviour(self):
        if self.ttl > 0:
            self.env.process(self.ttlBehaviour())

    def ttlBehaviour(self):
        yield self.env.timeout(self.ttl)
        self.cleanupStreams()
        yield self.env.process(self.ttlBehaviour())

    def cleanupStreams(self):
        for streamName in self.streams:
            stream = self.streams[streamName]
            self.streams[streamName] = [x for x in stream if x.timestamp + self.ttl > self.env.now]

    def createStream(self, streamName):
        if streamName in self.streams:
            raise AWSKinesisStreamExistsError("Stream (%s) already registered" % streamName)
        else:
            self.streams[streamName] = []
            self.subscriptions[streamName] = []

    def publish(self, streamName, message):
        if not isinstance(message, AWSKinesisPayload):
            raise AWSKinesisError("Invalid message format")

        self.verifyStreamExists(streamName)
        self.streams[streamName].append(message)
        self.notifySubscribers(streamName)

    def consume(self, streamName):
        self.verifyStreamExists(streamName)
        return list(self.streams[streamName])

    def subscribe(self, streamName, service):
       if not isinstance(service, AWSService):
           raise AWSKinesisError('Service not of type AWSService')
       else:
           identifier = service.getAWSIdentifier()
           self.subscribeIdentifier(streamName, identifier)

    def subscribeIdentifier(self, streamName, identifier):
        if streamName not in self.subscriptions:
            raise AWSKinesisError("Stream (%s) does not exist" % streamName)
 
        self.verifyRemoteKinesis(identifier)

        if identifier not in self.subscriptions[streamName]:
            self.subscriptions[streamName].append(identifier)

    def notifySubscribers(self, streamName):
        for subscriber in self.subscriptions[streamName]:
            sender = self.getAWSIdentifier() 
            payload = AWSKinesisSubscriberNotification(sender, subscriber, streamName)
            self.region.sendToRegion(subscriber.regionName, subscriber.receiverName, payload)

    def verifyRemoteKinesis(self, identifier):
        if identifier.regionName != self.region.regionName:
            if identifier.receiverName != 'RemoteKinesis_%s' % self.region.regionName:
                raise AWSServiceNotLocalOrRemoteServiceError("Only proxy services can subscribe to remote Kinesis")

    def verifyStreamExists(self, streamName):
        if streamName not in self.streams or streamName not in self.subscriptions:
            raise AWSKinesisError("Stream (%s) does not exist" % streamName)

    def receive(self, message):
        self.verifyRemoteKinesis(message.sender)

        if(type(message.payload) != type(tuple()) or len(message.payload) < 2):
            raise AWSKinesisError("Could not parse message")

        if message.payload[0] == 'subscribe':
            self.subscribeIdentifier(message.payload[1], message.sender)
        elif message.payload[0] == 'create stream':
            self.createStream(message.payload[1])
        elif message.payload[0] == 'publish':
            self.publish(message.payload[1], message.payload[2])
        elif message.payload[0] == 'request stream content':
            content = self.consume(message.payload[1])
            reply = message.makeReply(('stream content', message.payload[1], content))
            self.region.send(reply)
        else:
            raise AWSKinesisError("Not implemented")

class AWSRemoteService(AWSService):

    def __init__(self, localRegion, remoteRegionName, remoteServiceName):
        self.region = localRegion
        self.remoteRegionName = remoteRegionName
        self.remoteServiceName = remoteServiceName
        self.serviceName = 'Remote%s_%s' % (remoteServiceName, remoteRegionName) 
        super(AWSRemoteService, self).__init__(self.region, self.serviceName)

    def receive(self, message):
        raise AWSError("Not implemented")

    def make_sender(self):
        return AWSIdentifier(self.region.regionName, self.serviceName)

    def make_receiver(self):
        return AWSIdentifier(self.remoteRegionName, self.remoteServiceName)

    def make_message(self, payload):
        s = self.make_sender()
        r = self.make_receiver()
        return AWSMessage(s, r, payload)

    def verifyLocalService(self, service):
        if not service.region == self.region:
            raise AWSServiceNotLocalError("Service does not reside in same region") 

class AWSRemoteKinesis(AWSRemoteService):

    def __init__(self, localRegion, remoteRegionName):
        self.localSubscriptions = {}
        self.bufferedStreams = {}
        super(AWSRemoteKinesis, self).__init__(localRegion, remoteRegionName, 'Kinesis')

    def subscribe(self, streamName, service):
        self.verifyLocalService(service)

        if streamName not in self.localSubscriptions:
            self.localSubscriptions[streamName] = []

        if streamName not in self.bufferedStreams:
            self.bufferedStreams[streamName] = []

        if service not in self.localSubscriptions[streamName]:
            self.localSubscriptions[streamName].append(service)

        message = self.make_message(('subscribe', streamName)) 
        self.region.send(message)

    def consume(self, streamName):
        self.verifyStreamExists(streamName)
        return list(self.bufferedStreams[streamName])

    def publish(self, streamName, message):
        if not isinstance(message, AWSKinesisPayload):
            raise AWSError("Invalid message format")

        payload = self.make_message(('publish', streamName, message)) 
        self.region.send(payload)

    def createStream(self, streamName):
        payload = self.make_message(('create stream', streamName)) 
        self.region.send(payload)

    def receive(self, message):
        self.checkMessageIntegrity(message)

        if message.payload[0] == 'notify':
            payload = self.make_message(('request stream content', message.payload[1])) 
            self.region.send(payload)
        elif message.payload[0] == 'stream content':
            streamName = message.payload[1]
            self.bufferedStreams[streamName] = message.payload[2]
            self.notifySubscribers(streamName)
        else:
            raise AWSError('Could not parse message')

    def notifySubscribers(self, streamName):
        for subscriber in self.localSubscriptions[streamName]:
            sender = self.getAWSIdentifier()
            subscriberId = subscriber.getAWSIdentifier() 
            payload = AWSKinesisSubscriberNotification(sender, subscriberId, streamName)
            self.region.send(payload)

    def checkMessageIntegrity(self, message):
        if message.sender.regionName != self.remoteRegionName:
            raise AWSError('Remote region name mismatch')

        if message.sender.receiverName != 'Kinesis':
            raise AWSError('Message does not come from Kinesis')

        if message.receiver.regionName != self.region.regionName:
            raise AWSError('Local region name mismatch')

        if message.receiver.receiverName != self.serviceName:
            raise AWSError('Service name mismatch')

        if type(message.payload) != type(tuple()):
            raise AWSError('Unknown payload format')

    def verifyStreamExists(self, streamName):
        if streamName not in self.bufferedStreams or streamName not in self.localSubscriptions:
            raise AWSError("Stream (%s) does not exist" % streamName)

class AWSIdentifier(Identifier):

    def __new__(cls, regionName, receiverName):
        return AWSIdentifierInstance(regionName, receiverName)

    @classmethod
    def getInstance(regionName, receiverName):
        return AWSIdentifierInstance(self.regionName, self.receiverName)

@singleton
class AWSIdentifierInstance(IdentifierBehaviour, AWSIdentifier):
    pass

class AWSMessage(Message):
    
    def __init__(self, sender, receiver, payload):
        if not isinstance(sender, AWSIdentifier):
            raise AWSError("Type error: expected AWSIdentifier for sender")

        if not isinstance(receiver, AWSIdentifier):
            raise AWSError("Type error: expected AWSIdentifier for receiver")

        super(AWSMessage, self).__init__(sender, receiver, payload)


class AWSKinesisMessage(AWSMessage):
    pass

class AWSKinesisSubscriberNotification(AWSKinesisMessage):

    def __init__(self, sender, receiver, streamName):
        payload = ("notify", streamName)
        self.streamName = streamName
        super(AWSKinesisSubscriberNotification, self).__init__(sender, receiver, payload)

class AWSKinesisPayload(object):
    def __init__(self, timestamp, sender, payload):
        self.timestamp = timestamp
        self.sender = sender
        self.payload = payload

class AWSKinesisStreamContent(AWSKinesisMessage):

    def __init__(self, sender, receiver, streamName, content):
        payload = ("stream content", streamName, content)
        super(AWSKinesisStreamContent, self).__init__(sender, receiver, payload)


class AWSRoute53(object):
    """A class that represents the Route 53 functionality. Route 53 is supposed to be a global service, 
    therefore only one instance should exist at a given time."""

    def __init__(self):
        self.registeredLoadBalancers = {}

    def registerLoadBalancer(self, serviceName, loadBalancer):
        """Register a loadbalancer for a given service name
        A service name can have an arbitrary numbers of loadbalancers registered to it."""
        if not isinstance(loadBalancer, AWSLoadBalancer):
            raise AWSError('Currently only loadbalancers can be registered in AWSRoute53')

        if serviceName not in self.registeredLoadBalancers:
            self.registeredLoadBalancers[serviceName] = []

        identifier = loadBalancer.getIdentifier()

        if identifier in self.registeredLoadBalancers[serviceName]:
            raise AWSError('Loadbalancer is already registered')
        else:
            self.registeredLoadBalancers[serviceName].append(identifier)

    def lookup(self, serviceName, clientRegion):
        """Lookup the loadbalancers for a given serviceName
        The list will be ordered by distance to the requesting client"""
        if not isinstance(clientRegion, Region):
            raise AWSError("ClientRegion needs to be of type Region")

        if serviceName not in self.registeredLoadBalancers:
            raise AWSError("ServiceName not registered")
    
        return self.getLoadBalancersOrderedByRoundtripLatency(serviceName, clientRegion)

    def getLoadBalancersOrderedByRoundtripLatency(self, serviceName, clientRegion):
        roundtrips = {}
        loadbalancers = []

        for loadbalancer in self.registeredLoadBalancers[serviceName]:
            roundtrip = clientRegion.getLatency(loadbalancer.regionName) 
            roundtrips[loadbalancer] = roundtrip
            loadbalancers.append(loadbalancer)

        return sorted(loadbalancers, key = lambda x: roundtrips[x])
