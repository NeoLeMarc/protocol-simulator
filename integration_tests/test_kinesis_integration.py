import unittest
import sys
sys.path.append(".")
from unittest.mock import Mock, MagicMock
from infrastructure.aws import *
from infrastructure.network import *
import simpy
from util.builder import awsbuilder

class TestKinesisIntegration(unittest.TestCase):

    def test_setup(self):
        """Kinesis object can be correctly initialized"""
        env = simpy.Environment()
        REGION_NAME1 = 'us-east-1'
        REGION_NAME2 = 'us-west-2'
        LATENCY = 10

        r1, r2 = awsbuilder.b.buildTwoConnectedRegions(env, REGION_NAME1, REGION_NAME2, LATENCY, LATENCY)
        kinesis = AWSKinesis(r2, env)
        self.assertTrue(True)

    def test_givenAKinesisWithATTLForMessagesWhenPublishingAMessageThenMessageIsDeletedAfterTimeout(self):
        """Given a kinesis with a TTL for messages - when publishing a message - then message is deleted after timeout"""
        env = simpy.Environment()
        REGION_NAME1 = 'us-east-1'
        STREAM_NAME = 'test-stream'
        TTL = 10

        r1 = awsbuilder.b.buildRegion(env, REGION_NAME1)
        kinesis = AWSKinesis(r1, env, TTL)
        kinesis.createStream(STREAM_NAME)

        TEST_MESSAGE = AWSKinesisPayload(0, Mock(), 'test-message')
        env.run(until=1)
        kinesis.publish(STREAM_NAME, TEST_MESSAGE)

        self.assertTrue(TEST_MESSAGE in kinesis.consume(STREAM_NAME))
        env.run(until=11)
        self.assertFalse(TEST_MESSAGE in kinesis.consume(STREAM_NAME))

    def test_givenAKinesisWithoutATTLForMessagesWhenPublishingAMessageThenMessageIsNotDeleted(self):
        """Given a kinesis without a TTL for messages - when publishing a message - then message is not deleted"""
        env = simpy.Environment()
        REGION_NAME1 = 'us-east-1'
        STREAM_NAME = 'test-stream'
        TTL = 0

        r1 = awsbuilder.b.buildRegion(env, REGION_NAME1)
        kinesis = AWSKinesis(r1, env, TTL)
        kinesis.createStream(STREAM_NAME)

        env.run(until=1)
        TEST_MESSAGE = AWSKinesisPayload(1, Mock(), 'test-message')
        kinesis.publish(STREAM_NAME, TEST_MESSAGE)

        self.assertTrue(TEST_MESSAGE in kinesis.consume(STREAM_NAME))
        env.run(until=1000)
        self.assertTrue(TEST_MESSAGE in kinesis.consume(STREAM_NAME))





    def test_givenAKinesisInOneRegionAndASubscriberInAnotherRegionWhenPublishingToKinesisThenTheSubscriberIsNotified(self):
        """Given a kinesis in one region and a subscriber in another region - when publishing to kinesis - then the subscriber is notified"""
        env = simpy.Environment()
        REGION_NAME1 = 'us-east-1'
        REGION_NAME2 = 'us-west-2'
        STREAM_NAME = 'test-stream'
        SUBSCRIBER_NAME = 'RemoteKinesis_%s' % REGION_NAME2
        LATENCY = 10
        TEST_MESSAGE = AWSKinesisPayload(0, Mock(), 'test-message')

        r1, r2 = awsbuilder.b.buildTwoConnectedRegions(env, REGION_NAME1, REGION_NAME2, LATENCY, LATENCY)
        kinesis = AWSKinesis(r2, env)
        kinesis.createStream(STREAM_NAME)
        subscriber = AWSRemoteKinesis(r1, r2.regionName)
        subscriber.receive = Mock()
        kinesis.subscribe(STREAM_NAME, subscriber)
        kinesis.publish(STREAM_NAME, TEST_MESSAGE)

        env.run(until=11)
        self.assertTrue(subscriber.receive.called)
        arguments = subscriber.receive.call_args

        self.verifyRemoteAWSSubscriberNotification(arguments, REGION_NAME1, REGION_NAME2, AWSIdentifier, SUBSCRIBER_NAME, STREAM_NAME, 'Kinesis')
       
    def verifyRemoteAWSSubscriberNotification(self, arguments, REGION_NAME1, REGION_NAME2, AWSIdentifier, SUBSCRIBER_NAME, STREAM_NAME, SENDER_NAME):
        self.assertTrue(isinstance(arguments[0][0], AWSKinesisSubscriberNotification))
        self.assertTrue(isinstance(arguments[0][0].sender, AWSIdentifier))
        self.assertTrue(arguments[0][0].sender.regionName == REGION_NAME2)
        self.assertTrue(arguments[0][0].sender.receiverName == SENDER_NAME)
        self.assertTrue(isinstance(arguments[0][0].receiver, AWSIdentifier))
        self.assertTrue(arguments[0][0].receiver.regionName == REGION_NAME1)
        self.assertTrue(arguments[0][0].receiver.receiverName == SUBSCRIBER_NAME)
        self.assertTrue(arguments[0][0].payload == ("notify", STREAM_NAME))

    def test_givenAKinesisAndARemoteKinesisWhenPublishingOnTheRemoteKinesisThenMessageShowsUpInRealKinesis(self):
        """Given a kinesis and a remote kinesis - when publishing on the remote kinesis - then message shows up in real kinesis"""
        env = simpy.Environment()
        REGION_NAME1 = 'us-east-1'
        REGION_NAME2 = 'us-west-2'
        STREAM_NAME = 'test-stream'
        SUBSCRIBER_NAME = 'test-subscriber'
        LATENCY = 10
        TEST_MESSAGE = AWSKinesisPayload(0, Mock(), 'test-message')

        r1, r2 = awsbuilder.b.buildTwoConnectedRegions(env, REGION_NAME1, REGION_NAME2, LATENCY, LATENCY)
        kinesis = AWSKinesis(r2, env)
        kinesis.createStream(STREAM_NAME)
        remoteKinesis = AWSRemoteKinesis(r1, r2.regionName)
        subscriber = AWSService(r1, SUBSCRIBER_NAME)
        remoteKinesis.subscribe(STREAM_NAME, subscriber)

        remoteKinesis.publish(STREAM_NAME, TEST_MESSAGE)

        env.run(until=10)
        self.assertFalse(TEST_MESSAGE in kinesis.consume(STREAM_NAME))

        env.run(until=11)
        self.assertTrue(TEST_MESSAGE in kinesis.consume(STREAM_NAME))

    def test_givenAKinesisAndARemoteKinesisHavingASubscriberWhenPublishingAMessageThenSubscriberIsNotifiedAnCanFetchMessage(self):
        """Given a kinesis and a remote kinesis having a subscriber - when publishing a message - then subscriber is notified and can fetch message"""
        env = simpy.Environment()
        REGION_NAME1 = 'us-east-1'
        REGION_NAME2 = 'us-west-2'
        STREAM_NAME = 'test-stream'
        SUBSCRIBER_NAME = 'test-subscriber'
        LATENCY = 10
        TEST_MESSAGE = AWSKinesisPayload(0, Mock(), 'test-message')

        r1, r2 = awsbuilder.b.buildTwoConnectedRegions(env, REGION_NAME1, REGION_NAME2, LATENCY, LATENCY)
        kinesis = AWSKinesis(r2, env)
        kinesis.createStream(STREAM_NAME)
        remoteKinesis = AWSRemoteKinesis(r1, r2.regionName)
        subscriber = AWSService(r1, SUBSCRIBER_NAME)
        subscriber.receive = Mock()
        remoteKinesis.subscribe(STREAM_NAME, subscriber)

        env.run(until=11) # Wait for subscription message to be delivered to 'real kinesis'

        kinesis.publish(STREAM_NAME, TEST_MESSAGE)

        env.run(until=22) # Wait for content request to arrive 
        env.run(until=44) # Wait for content to be delivered 

        self.assertTrue(subscriber.receive.called)
        arguments = subscriber.receive.call_args

        self.verifyRemoteAWSSubscriberNotification(arguments, REGION_NAME1, REGION_NAME1, AWSIdentifier, SUBSCRIBER_NAME, STREAM_NAME, 'RemoteKinesis_%s' % REGION_NAME2)

        self.assertTrue(TEST_MESSAGE in remoteKinesis.consume(STREAM_NAME))

    def test_givenAKinesisAndARemoteKinesisWhenCreatingAStreamOnRemoteKinesisThenStreamInRealKinesisIsBeingCreated(self):
        """Given a kinesis and a remote kinesis - when creating a stream on remote kinesis - then stream on real kinesis is being created"""

        env = simpy.Environment()
        REGION_NAME1 = 'us-east-1'
        REGION_NAME2 = 'us-west-2'
        STREAM_NAME = 'test-stream'
        SUBSCRIBER_NAME = 'test-subscriber'
        LATENCY = 10
        TEST_MESSAGE = 'test-message'

        r1, r2 = awsbuilder.b.buildTwoConnectedRegions(env, REGION_NAME1, REGION_NAME2, LATENCY, LATENCY)
        kinesis = AWSKinesis(r2, env)
        remoteKinesis = AWSRemoteKinesis(r1, r2.regionName)
        subscriber = AWSService(r1, SUBSCRIBER_NAME)
        subscriber.receive = Mock()
        remoteKinesis.createStream(STREAM_NAME)

        self.assertFalse(STREAM_NAME in kinesis.streams)
        env.run(until=11) # Wait for message to be delivered to 'real kinesis'
        self.assertTrue(STREAM_NAME in kinesis.streams)

