import unittest
from unittest.mock import Mock, MagicMock
from infrastructure.network import *
from util.builder import awsbuilder
import util.simpy

class TestNetworkPath(unittest.TestCase):

    CLASS = NetworkPath

    def test_setup(self):
        """Network path can be correctly initiated"""
        ENV = Mock()
        NAME = "my-network-path"
        LATENCY = 1234

        np = self.CLASS(ENV, NAME, LATENCY)

        self.assertTrue(isinstance(np, NetworkPath))
        self.assertEqual(ENV, np.env)
        self.assertEqual(NAME, np.name)
        self.assertEqual(LATENCY, np.latency)

    def test_givenANetworkPathWhenTryingToConnectLeftAndRightSideThanBothSidesAreConnectedCorrectly(self):
        """Given a network path - when trying to connect left and right side - then both sides are connected correctly"""
        LEFT_SIDE = Mock()
        RIGHT_SIDE = Mock()

        np = self.buildNetworkPathAndConnectBothSides(LEFT_SIDE, RIGHT_SIDE)

        self.assertEqual(np.leftSide, LEFT_SIDE)
        self.assertEqual(np.rightSide, RIGHT_SIDE)
        self.assertNotEqual(LEFT_SIDE, RIGHT_SIDE)

    def buildNetworkPathAndConnectBothSides(self, LEFT_SIDE = Mock(), RIGHT_SIDE = Mock(), ENV = Mock()):
        NAME = "my-network-path"
        LATENCY = 1234

        np = self.CLASS(ENV, NAME, LATENCY)

        np.connectLeftSide(LEFT_SIDE)
        np.connectRightSide(RIGHT_SIDE)
        return np


    def test_givenANetworkPathWithBothSidesConnectedWhenTryingToConnectEitherSideThenConnectIsRejected(self):
        """Given a network path with both sides connected - when trying to connect either side - then connect is rejected""" 
        np = self.buildNetworkPathAndConnectBothSides()

        LEFT_SIDE = Mock()
        RIGHT_SIDE = Mock()

        with self.assertRaises(SideAlreadyConnectedError) as context:
            np.connectLeftSide(LEFT_SIDE)

        with self.assertRaises(SideAlreadyConnectedError) as context:
            np.connectRightSide(RIGHT_SIDE)

    def test_givenAFullyConnectedNetworkPathWhenTryingToSendThenLatencyBehaviourIsTriggeredAndPayloadCanBeRetrievedViaReceive(self):
        """Given a fully connected network path - when trying to send - then latency behaviour is triggered and payload can be retrieved via receive"""

        leftSide = Mock()
        rightSide = Mock()
        env = Mock()
        payload = 'test'

        np = self.buildNetworkPathAndConnectBothSides(leftSide, rightSide, env)

        np.send(payload)
        self.assertTrue(env.process.called)

        generator = env.process.call_args[0][0]
        next(generator)
        env.timeout.assert_called_with(np.latency)
        self.assertTrue(env.process.called)

        generator = env.process.call_args[0][0]
        try:
            next(generator)
        except StopIteration:
            pass
        
        self.assertTrue(rightSide.notifyNetworkReceive.called)

        # this is ugly, but I currently don't know how simpy.Environment
        # exactly triggers store.Get() events. Therefore we extract the
        # payload directly from the underlying store.
        event = util.simpy.ExtractorEvent() 
        np.buffer._do_get(event)
        self.assertEqual(payload, event._item)


class TestLossyNetworkPath(TestNetworkPath):
    CLASS = LossyNetworkPath

    def test_givenALossyNetworkPathWithAProabilityOf50PercentWhenSendingThenApproximately50PercentOftheSendsGetLost(self):
        """Given a lossy network path with a probability of 50 percent - when sending - then approximately 50 percent of the sends get lost""" 
        ENV = Mock()
        NAME = "my-network-path"
        LATENCY = 1234
        PROBABILITY = 0.5
        RUNS = 1000
        ACCEPTABLE_DEVIATION = 0.1

        np = self.CLASS(ENV, NAME, LATENCY, PROBABILITY)

        successfulSends = 0

        for i in range(0, RUNS):
            np.sendLatency = Mock()
            np.send('test')
            if np.sendLatency.called:
                successfulSends += 1

        self.assertLess(PROBABILITY - ACCEPTABLE_DEVIATION, round(successfulSends/RUNS, 2))
        self.assertGreater(PROBABILITY + ACCEPTABLE_DEVIATION, round(successfulSends/RUNS, 2))


if __name__ == '__main__':
    unittest.main()
