import unittest
import sys
sys.path.append(".")
from unittest.mock import Mock, MagicMock
from infrastructure.aws import *
from infrastructure.network import *
from components.sim import *
import simpy
from util.builder import awsbuilder 

class TestSIMIntegration(unittest.TestCase):

    def test_setup(self):
        """SIM object can be correctly initialized"""
        env = simpy.Environment()
        r1, r2 = awsbuilder.b.buildTwoConnectedRegions(env, 'us-west-1', 'us-east-2', 10, 10)
        sim = Sim(env, r2, 0)
        sim.receive = Mock()

        sender = AWSIdentifier('us-west-1', 'Sender1')
        receiver = AWSIdentifier('us-east-2', 'SIM_0')
        message = AWSMessage(sender, receiver, SimHeartbeatMessage(env.now, sim))

        r1.send(message)

        env.run(until = 11)

    def test_givenTwoRegionsAndSIMInOneRegionWhenSendingAMessageFromOneRegionToSIMInOtherRegionThenMessageArrives(self):
        """Given two regions and SIM is in one of those regions - when sending a message from one region to SIM in other region - then message arrives"""
        env = simpy.Environment()
        r1, r2 = awsbuilder.b.buildTwoConnectedRegions(env, 'us-west-1', 'us-east-2', 10, 10)

        sim = Sim(env, r2, 0)
        sim.receive = Mock()

        sender = AWSIdentifier('us-west-1', 'Sender1')
        receiver = AWSIdentifier('us-east-2', 'SIM_0')
        message = AWSMessage(sender, receiver, 'Payload')

        r1.sendToRegion('us-east-2', 'SIM_0', message)

        env.run(until = 11)
        sim.receive.assert_called_with(message)

    def test_givenTwoRegionsAndSIMInOneRegionWhenSendingAMessageFromOneRegionToSIMInOtherRegionThenLatencyWorksAsExpected(self):
        """Given two regions and SIM is in one of those regions - when sending a message from one region to SIM in other region - then latency works as expected"""
        env = simpy.Environment()
        r1, r2 = awsbuilder.b.buildTwoConnectedRegions(env, 'us-west-1', 'us-east-2', 10, 10)

        sim = Sim(env, r2, 0)
        sim.receive = Mock()

        sender = AWSIdentifier('us-west-1', 'Sender1')
        receiver = AWSIdentifier('us-east-2', 'SIM_0')
        message = AWSMessage(sender, receiver, 'Payload')

        r1.sendToRegion('us-east-2', 'SIM_0', message)

        env.run(until = 10)
        sim.receive.assert_not_called()

        env.run(until = 11)
        sim.receive.assert_called_with(message)

    def test_givenFourFullyMeshedSIMWhenSendingHeartbeatsThenHeartbeatsArriveWithAppropriateDelay(self):
        """Given four fully meshed sim - when sending heartbeats - then hearbeats arrive with appropriate delay"""
        env = simpy.Environment()

        regionsToBuild = [
                {'regionName' : 'us-west-1', 'latency' : 10},
                {'regionName' : 'us-east-1', 'latency' : 10},
                {'regionName' : 'eu-central-1', 'latency' : 50},
                {'regionName' : 'apac-central-1', 'latency' : 40}
        ]        

        self.regions = awsbuilder.b.buildFullyMeshedRegionsWithKinesis(env, regionsToBuild)

        ## Create SIM pair in each Region
        self.sims = []
        for region in self.regions:
            sim1 = Sim(env, region, 0, KinesisBasedSimPersistencyStrategy(region.getServiceByName('Kinesis')))
            sim2 = Sim(env, region, 1, KinesisBasedSimPersistencyStrategy(region.getServiceByName('Kinesis')))
            self.sims.append(sim1)
            self.sims.append(sim2)

        for sim in self.sims:
            sim.attachRemoteRegions(self.regions)
            sim.startBehaviour()

        i = 1 

        for i in range(1, 1000):
            env.run(until = i)

            for region in self.regions:
                kinesis = region.getServiceByName('Kinesis')
                for event in kinesis.consume('sim_control'):
                    self.assertEqual(event.sender.regionName, region.regionName)

            for sim in self.sims:
                for heartbeatRegion in sim.lastHeartbeat:
                    if heartbeatRegion != sim.region.regionName:
                        ht = sim.lastHeartbeat[heartbeatRegion]
                        self.assertNotEqual(ht[0], ht[1])



if __name__ == '__main__':
    unittest.main()
