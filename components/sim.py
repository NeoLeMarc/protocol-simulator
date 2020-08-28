#!/usr/bin/env python3
#
# SIM component for SIM simulator

from infrastructure.aws import *
from util.helper import sort


class SimStrategy(object):

    def setSIM(self, sim):
        if not isinstance(sim, Sim):
            raise Exception("SIM needs to be an instance of Sim")
        else:
            self.sim = sim

class SimPersistencyStrategy(SimStrategy):

    def receive(self, message):
        raise Exception("Not implemented")

    def startBehaviour(self):
        raise Exception("Not implemented")

    def attachRemoteRegions(self, regions):
        raise Exception("Not implemented")

    def attachRemoteRegion(self, region):
        raise Exception("Not implemented")

    def subscribeRegion(self, region):
        raise Exception("Not implemented")

    def heartbeatBehaviour(self):
        raise Exception("Not implemented")

    def sendHeartbeatMessage(self):
        raise Exception("Not implemented")

    def getSystemStatus(self):
        raise Exception("Not implemented")

    def printInfo(self, p):
         raise Exception("Not implemented")

class SimJoinStrategy(SimStrategy):

    def publicReceive(self, message):
         raise Exception("Not implemented")

    def receive(self, message):
        pass

class Sim(AWSPublicService):
    SERVICE_NAME = "SIM"
    maxHeartbeatAge = 200

    def __init__(self, env, region, instance_id, persistency_strategy = SimPersistencyStrategy(), update_interval = 50):
        self.region = region
        self.instance_id = instance_id
        self.env = env
        self.update_interval = update_interval

        self.otherSIM = []
        self.lastHeartbeat = {}
        self.regionsInSync = 0
        self.attachedRegions = []

        super(Sim, self).__init__(region, "%s_%i" % (self.SERVICE_NAME, instance_id))
        self.setPersistencyStrategy(persistency_strategy)
        self.setJoinStrategy(SimJoinStrategy())

    def setPersistencyStrategy(self, strategy):
        if not isinstance(strategy, SimPersistencyStrategy):
            raise Exception("Strategy not of type SimPersistencyStrategy")
        else:
            strategy.setSIM(self)
            self.persistencyStrategy = strategy

    def setJoinStrategy(self, strategy):
        if not isinstance(strategy, SimJoinStrategy):
            raise Exception("Strategy not of type SimJoinStrategy")
        else:
            strategy.setSIM(self)
            self.joinStrategy = strategy

    def publicReceive(self, message):
        self.joinStrategy.publicReceive(message)

    def receive(self, message):
        ## Messags are sent to all strategies - if they don't understand the message, they have to discard them
        self.persistencyStrategy.receive(message)
        self.joinStrategy.receive(message)

    def startBehaviour(self):
        self.persistencyStrategy.startBehaviour()

    def attachRemoteRegions(self, regions):
        self.persistencyStrategy.attachRemoteRegions(regions)

    def attachRemoteRegion(self, region):
        self.persistencyStrategy.attachRemoteRegion(region)

    def subscribeRegion(self, region):
        self.persistencyStrategy.subscribeRegion(region)

    def heartbeatBehaviour(self):
        self.persistencyStrategy.heartbeatBehaviour()

    def sendHeartbeatMessage(self):
        self.persistencyStrategy.sendHeartbeatMessage()

    def getSystemStatus(self):
        self.persistencyStrategy.getSystemStatus()

    def printInfo(self, p):
        self.persistencyStrategy.printInfo(p)
    
class SimMessage(AWSKinesisPayload):
    pass

class SimHeartbeatMessage(SimMessage):

    def __init__(self, timestamp, sender):
        super(SimHeartbeatMessage, self).__init__(timestamp, sender, 'heartbeat')

class KinesisBasedSimPersistencyStrategy(SimPersistencyStrategy):

    KINESIS_STREAM = "sim_control"

    def __init__(self, kinesis):
        self.kinesis = kinesis
        try:
            kinesis.createStream(self.KINESIS_STREAM)
        except AWSKinesisStreamExistsError:
            pass

        self.kinesisPosition = {} 
        self.kinesisPosition['Kinesis'] = -1

    def setSIM(self, sim):
        super(KinesisBasedSimPersistencyStrategy, self).setSIM(sim)
        self.kinesis.subscribe(self.KINESIS_STREAM, self.sim)

    def attachRemoteRegions(self, regions):
        for region in regions:
            if region != self.sim.region:
                self.attachRemoteRegion(region)

    def attachRemoteRegion(self, region):
        remoteKinesis = self.subscribeRegion(region)
        self.sim.attachedRegions.append(region)
        self.kinesisPosition[remoteKinesis.serviceName] = -1

    def subscribeRegion(self, region):
        remoteKinesis = self.sim.region.getServiceByName("RemoteKinesis_%s" % region.regionName)
        remoteKinesis.subscribe(self.KINESIS_STREAM, self.sim)
        return remoteKinesis

    def startBehaviour(self):
        ## Periodically send heartbeats
        self.sim.env.process(self.heartbeatBehaviour())

    def heartbeatBehaviour(self):
        yield self.sim.env.timeout(self.sim.update_interval)
        self.sendHeartbeatMessage()
        self.sim.env.process(self.heartbeatBehaviour())

    def sendHeartbeatMessage(self):
        message = SimHeartbeatMessage(self.sim.env.now, self.sim.getAWSIdentifier())
        self.kinesis.publish(self.KINESIS_STREAM, message)

    def handleSubscriberNotification(self, message):

        if message.streamName != self.KINESIS_STREAM:
            raise Exception("Message from unknown stream")

        if message.sender.regionName != self.sim.region.regionName:
            raise Exception("This behaviour won't handle messages from remote regions")

        kinesis = self.sim.region.getServiceByName(message.sender.receiverName)

        if kinesis.region != self.sim.region:
            raise Exception("Kinesis region mismatch - something went horribly wrong")

        position = self.kinesisPosition[message.sender.receiverName]
        streamData = kinesis.consume(self.KINESIS_STREAM)

        for event in streamData:
            if event.timestamp >= self.kinesisPosition[message.sender.receiverName]:
                # Only process unseen events
                self.handleEvent(kinesis.serviceName, message.sender.regionName, event)
                if position < event.timestamp:
                    position = event.timestamp 

        self.kinesisPosition[message.sender.receiverName] = position

    def handleEvent(self, kinesisName, sourceRegion, event):
        if isinstance(event, SimHeartbeatMessage):
            identifier = "%s" % (event.sender.regionName)

            if event.sender.regionName != self.sim.region.regionName and not kinesisName.startswith('Remote'):
                raise Exception('Consumed remote event from local kinesis')
            if identifier not in self.sim.lastHeartbeat:
                self.sim.lastHeartbeat[identifier] = (-1, -1)
            if event.timestamp > self.sim.lastHeartbeat[identifier][0]:
                self.sim.lastHeartbeat[identifier] = (event.timestamp, self.sim.env.now)
        else:
            raise Exception("Unknown event")

    def receive(self, message):
        if isinstance(message, AWSKinesisSubscriberNotification):
            self.handleSubscriberNotification(message)
        else:
            pass # Discard message

    def getSystemStatus(self):
        status = 'OUT OF SYNC'

        regionsInSync = 0
        for region in self.sim.attachedRegions:
            if region.regionName in self.sim.lastHeartbeat:
                heartbeat = self.sim.lastHeartbeat[region.regionName]
                if heartbeat[0] + self.sim.maxHeartbeatAge >= self.sim.env.now:
                    regionsInSync += 1

        if regionsInSync == len(self.sim.attachedRegions):
            status = 'IN SYNC (%i/%i)' % (regionsInSync, len(self.sim.attachedRegions))
        else:
            status = 'OUT OF SYNC (%i/%i)' % (regionsInSync, len(self.sim.attachedRegions))

        self.sim.regionsInSync = regionsInSync
        return status

    def printInfo(self, p):
        p.printHeader("***********  SIM INFO: ", end = '')
        p.printBold("%s - %s" % (self.sim.region.regionName, self.sim.serviceName), end = ' ')
        p.printHeader("")
        p.print("Current simulation time: ", end = '')
        p.printBold("%i" % self.sim.env.now)

        status = self.getSystemStatus()

        p.printBold("+ Status: ", end = '')
        if status.startswith('IN SYNC'):
            p.printOK(status)
        else:
            p.printError(status)

        p.printBold("+ Heartbeats: ", end = '')

        for identifier in sort(self.sim.lastHeartbeat):
            if not identifier.startswith(self.sim.region.regionName):
                sent = self.sim.lastHeartbeat[identifier][0]
                received = self.sim.lastHeartbeat[identifier][1]
                age = self.sim.env.now - self.sim.lastHeartbeat[identifier][0]
                if age > self.sim.maxHeartbeatAge:
                    p.printWarning("!![%s: %i (%i - Age: %i)]!!\t" % (identifier, sent, received, age), end = ' ')
                else:
                    p.printOKB(" [%s: %i (%i - Age: %i)]  \t" % (identifier, sent, received, age), end = ' ')

        p.print()
        p.print()

class ResubscribingKinesisBasedSimPersistencyStrategy(KinesisBasedSimPersistencyStrategy):
    resubscriptionInterval = 1000

    def resubscriptionBehaviour(self):
        yield self.sim.env.timeout(self.resubscriptionInterval)
        for region in self.sim.attachedRegions:
            self.subscribeRegion(region)
        self.sim.env.process(self.resubscriptionBehaviour())

    def startBehaviour(self):
        super(ResubscribingKinesisBasedSimPersistencyStrategy, self).startBehaviour()
        self.sim.env.process(self.resubscriptionBehaviour())
