import unittest
from unittest.mock import Mock, MagicMock
from infrastructure.aws import *
from util.builder import awsbuilder, clientbuilder
from infrastructure.tests.test_region import * 
from infrastructure.region import *

class TestAWSRegion(TestRegion):
    CLASS = AWSRegion
    IdentifierCLASS = AWSIdentifier
    MessageCLASS = AWSMessage
    InfrastructureServiceCLASS = AWSInfrastructureService


    def test_setup(self):
        a = super(TestAWSRegion, self).test_setup()
        self.assertTrue(isinstance(a, AWSRegion))


class TestAWSService(TestService):
    RegionCLASS = AWSRegion
    ServiceCLASS = AWSService

    def test_setup(self):
        a = super(TestAWSService, self).test_setup()
        self.assertTrue(isinstance(a, AWSService))


class TestAWSInfrastructureService(TestInfrastructureService):
    RegionCLASS = AWSRegion
    ServiceCLASS = AWSInfrastructureService

    def test_setup(self):
        a = super(TestAWSInfrastructureService, self).test_setup()
        self.assertTrue(isinstance(a, AWSInfrastructureService))


class TestAWSKinesis(unittest.TestCase):
    
    def test_setup(self):
        """AWSKinesis can be correctly instantiated"""
        r1 = Mock(AWSRegion)
        kinesis = AWSKinesis(r1, Mock())
        r1.registerService.assert_called_with(kinesis, 'Kinesis')

    def test_givenAKinesisServiceWhenCreatingAStreamThenStreamIsCreated(self):
        """Given a kinesis - when creating a stream - then stream is created"""
        r1 = Mock(AWSRegion)
        kinesis = AWSKinesis(r1, Mock())
        kinesis.createStream('testStream')
        self.assertTrue('testStream' in kinesis.streams)

    def test_givenAKinesisServiceWithAStreamWhenPublishingAMessageThenMessageGetsAddedToStream(self):
        """Given a kinesis with a stream - when publishing a message - then message gets added to stream"""
        STREAM_NAME = 'testStream'
        TEST_MESSAGE = AWSKinesisPayload(0, Mock(), 'Test Message')

        r1 = Mock(AWSRegion)
        kinesis = AWSKinesis(r1, Mock())
        kinesis.createStream(STREAM_NAME)
        
        kinesis.publish(STREAM_NAME, TEST_MESSAGE)
        self.assertTrue(TEST_MESSAGE in kinesis.consume(STREAM_NAME))

    def test_givenAKinesisServiceWithMultipleStreamsWhenPublishingAMessageThenMessageGetsAddedToCorrectStream(self):
        """Given a kinesis with multiple streams - when publishing a message - then message gets added to correct stream"""
        STREAM_NAME1 = 'testStream1'
        STREAM_NAME2 = 'testStream2'
        TEST_MESSAGE1 = AWSKinesisPayload(0, Mock(), 'Test Message1')
        TEST_MESSAGE2 = AWSKinesisPayload(0, Mock(), 'Test Message2')

        r1 = Mock(AWSRegion)
        kinesis = AWSKinesis(r1, Mock())
        kinesis.createStream(STREAM_NAME1)
        kinesis.createStream(STREAM_NAME2)
       
        kinesis.publish(STREAM_NAME1, TEST_MESSAGE1)
        kinesis.publish(STREAM_NAME2, TEST_MESSAGE2)

        self.assertTrue(TEST_MESSAGE1 in kinesis.consume(STREAM_NAME1))
        self.assertTrue(TEST_MESSAGE2 in kinesis.consume(STREAM_NAME2))

        self.assertFalse(TEST_MESSAGE1 in kinesis.consume(STREAM_NAME2))
        self.assertFalse(TEST_MESSAGE2 in kinesis.consume(STREAM_NAME1))

    def test_givenAKinesisServiceWithAregisteredStreamWhenTryingToCreateTheSameStreamAgainThenAnExceptionGetsThrown(self):
        """Given a kinesis with a registered stream - when trying to create the same stream again - then an exception gets thrown"""
        STREAM_NAME = 'testStream'
        TEST_MESSAGE = 'Test Message'

        r1 = Mock(AWSRegion)
        kinesis = AWSKinesis(r1, Mock())
        kinesis.createStream(STREAM_NAME)
        
        with self.assertRaises(Exception) as context:
            kinesis.createStream(STREAM_NAME)

        self.assertTrue("Stream (%s) already registered" % STREAM_NAME in str(context.exception))

    def test_givenAKinesisHavinAStreamWhenAServiceThatIsNotARemoteKinesisServiceTriesToSubscribeThenExceptionIsThrown(self):
        '''Given a Kinesis having a stream - when a service, that is not a remote kinesis tries to subscribe - then a exception is thrown'''
        STREAM_NAME  = 'testStream'
        TEST_MESSAGE = 'Test Message'
        TARGET_REGION = Mock(AWSRegion)
        TARGET_REGION.regionName = 'target-region'
        TARGET_SERVICE = AWSService(TARGET_REGION, 'TEST-Service-1')

        r1 = Mock(AWSRegion)
        r1.regionName = "foo-region"
        r1.sendToRegion = Mock()
        kinesis = AWSKinesis(r1, Mock())
        kinesis.createStream(STREAM_NAME)

        with self.assertRaises(AWSServiceNotLocalOrRemoteServiceError) as context:
            kinesis.subscribe(STREAM_NAME, TARGET_SERVICE) 

        self.assertTrue("Only proxy services can subscribe to remote Kinesis" in str(context.exception))


    def test_givenAKinesisHavinAStreamWithASubscriberWhenPublishingToStreamThenSubscriberIsNotified(self):
        '''Given a Kinesis having a stream with a subscriber - when publishing to stream - then subscriber is notified'''
        r1 = Mock(AWSRegion)
        r1.regionName = "foo-region"

        STREAM_NAME  = 'testStream'
        TEST_MESSAGE = AWSKinesisPayload(0, Mock(), 'Test Message')
        TARGET_REGION = Mock(AWSRegion)
        TARGET_REGION.regionName = 'target-region'
        TARGET_SERVICE = AWSService(TARGET_REGION, 'RemoteKinesis_%s' % r1.regionName)

        r1.sendToRegion = Mock()
        kinesis = AWSKinesis(r1, Mock())
        kinesis.createStream(STREAM_NAME)
        kinesis.subscribe(STREAM_NAME, TARGET_SERVICE) 
        kinesis.publish(STREAM_NAME, TEST_MESSAGE)

        self.assertTrue(r1.sendToRegion.called)
        arguments = r1.sendToRegion.call_args
        self.verifyNotifyArguments(TARGET_REGION.regionName, TARGET_SERVICE.serviceName, STREAM_NAME, arguments)

    def test_givenAKinesisHavingAStreamWithMultipleSubscribersWhenPublishingToStreamThenAllSubscribersAreNotified(self):
        '''Given a Kinesis having a stream with multiple subscribers - when publishing to stream - then all subscribers are notified'''
        STREAM_NAME  = 'testStream'
        TEST_MESSAGE = AWSKinesisPayload(0, Mock(), 'Test Message')
        KINESIS_REGION = "foo-region"
        TARGET_REGION1 = Mock(AWSRegion)
        TARGET_REGION1.regionName = 'target-region1'
        TARGET_REGION2 = Mock(AWSRegion)
        TARGET_REGION2.regionName = 'target-region2'
        TARGET_REGION3 = Mock(AWSRegion)
        TARGET_REGION3.regionName = 'target-region3'
        TARGET_SERVICE1 = AWSRemoteKinesis(TARGET_REGION1, KINESIS_REGION)
        TARGET_SERVICE2 = AWSRemoteKinesis(TARGET_REGION2, KINESIS_REGION)
        TARGET_SERVICE3 = AWSRemoteKinesis(TARGET_REGION3, KINESIS_REGION)

        r1 = Mock(AWSRegion)
        r1.regionName = KINESIS_REGION
        r1.sendToRegion = Mock()
        kinesis = AWSKinesis(r1, Mock())
        kinesis.createStream(STREAM_NAME)
        kinesis.subscribe(STREAM_NAME, TARGET_SERVICE1) 
        kinesis.subscribe(STREAM_NAME, TARGET_SERVICE2) 
        kinesis.subscribe(STREAM_NAME, TARGET_SERVICE3) 
        kinesis.publish(STREAM_NAME, TEST_MESSAGE)

        self.assertTrue(r1.sendToRegion.called)
        arguments = r1.sendToRegion.call_args_list
        self.verifyNotifyArguments(TARGET_REGION1.regionName, TARGET_SERVICE1.serviceName, STREAM_NAME, arguments[0])
        self.verifyNotifyArguments(TARGET_REGION2.regionName, TARGET_SERVICE2.serviceName, STREAM_NAME, arguments[1])
        self.verifyNotifyArguments(TARGET_REGION3.regionName, TARGET_SERVICE3.serviceName, STREAM_NAME, arguments[2])

    def verifyNotifyArguments(self, TARGET_REGION, TARGET_SERVICE, STREAM_NAME, arguments, typ = 'notify'):
        self.assertTrue(arguments[0][0] == TARGET_REGION)
        self.assertTrue(arguments[0][1] == TARGET_SERVICE)
        self.assertTrue(isinstance(arguments[0][2], AWSKinesisSubscriberNotification))
        self.assertTrue(arguments[0][2].payload[0] == typ)
        self.assertTrue(arguments[0][2].payload[1] == STREAM_NAME)


    def test_givenAKinesisHavingAStreamWhenReceivingASubscribeMessageThenRemoteSubscriberIsSubscribed(self):
        """Given a kinesis having a stream - when receiving a subscribe message - then remote subscriber is subscribed"""
        STREAM_NAME = 'testStream'
        KINESIS_REGION = 'foo-region'
        REMOTE_REGION = 'remote-region'
        REMOTE_SERVICE = 'RemoteKinesis_%s' % KINESIS_REGION

        r1 = Mock(AWSRegion)
        r1.regionName = KINESIS_REGION
        kinesis = AWSKinesis(r1, Mock())
        kinesis.createStream(STREAM_NAME)

        sender = AWSIdentifier(REMOTE_REGION, REMOTE_SERVICE)
        receiver = AWSIdentifier(KINESIS_REGION, 'Kinesis')
        payload = ('subscribe', STREAM_NAME)
        subscribeMessage = AWSMessage(sender, receiver, payload)

        kinesis.receive(subscribeMessage)

        self.assertTrue(sender in kinesis.subscriptions[STREAM_NAME])

    def test_givenAKinesisWhenReceivingACreateMessageThenStreamIsCreated(self):
        """Given a kinesis - when receiving a create stream message - then stream is created"""
        STREAM_NAME = 'testStream'
        KINESIS_REGION = 'foo-region'
        REMOTE_REGION = 'remote-region'
        REMOTE_SERVICE = 'RemoteKinesis_%s' % KINESIS_REGION

        r1 = Mock(AWSRegion)
        r1.regionName = KINESIS_REGION
        kinesis = AWSKinesis(r1, Mock())

        sender = AWSIdentifier(REMOTE_REGION, REMOTE_SERVICE)
        receiver = AWSIdentifier(KINESIS_REGION, 'Kinesis')
        payload = ('create stream', STREAM_NAME)
        createMessage = AWSMessage(sender, receiver, payload)

        kinesis.receive(createMessage)

        self.assertTrue(STREAM_NAME in kinesis.streams)


    def test_givenAKinesisWhenReceivingAPublishMessageThenPayloadIsPublished(self):
        """Given a kinesis - when receiving a publish message - then payload is published"""
        STREAM_NAME = 'testStream'
        KINESIS_REGION = 'foo-region'
        REMOTE_REGION = 'remote-region'
        REMOTE_SERVICE = 'RemoteKinesis_%s' % KINESIS_REGION
        PUBLISH_TEXT = AWSKinesisPayload(0, Mock(), 'a test message')

        r1 = Mock(AWSRegion)
        r1.regionName = KINESIS_REGION
        kinesis = AWSKinesis(r1, Mock())
        kinesis.createStream(STREAM_NAME)

        sender = AWSIdentifier(REMOTE_REGION, REMOTE_SERVICE)
        receiver = AWSIdentifier(KINESIS_REGION, 'Kinesis')
        payload = ('publish', STREAM_NAME, PUBLISH_TEXT)
        publishMessage = AWSMessage(sender, receiver, payload)

        kinesis.receive(publishMessage)

        self.assertTrue(PUBLISH_TEXT in kinesis.streams[STREAM_NAME])

    def test_givenAKinesiswhenReceivingARequestStreamContentMessageThenContentIsSent(self):
        """Given a kinesis - when receiving a request stream content message - then content is sent"""
        STREAM_NAME = 'testStream'
        KINESIS_REGION = 'foo-region'
        REMOTE_REGION = 'remote-region'
        REMOTE_SERVICE = 'RemoteKinesis_%s' % KINESIS_REGION
        STREAM_CONTENT = [AWSKinesisPayload(0, Mock(), 1), AWSKinesisPayload(0, Mock(), 2), AWSKinesisPayload(0, Mock(), 3)]

        r1 = Mock(AWSRegion)
        r1.regionName = KINESIS_REGION
        kinesis = AWSKinesis(r1, Mock())
        kinesis.createStream(STREAM_NAME)

        sender = AWSIdentifier(REMOTE_REGION, REMOTE_SERVICE)
        receiver = AWSIdentifier(KINESIS_REGION, 'Kinesis')

        for content in STREAM_CONTENT:
            kinesis.publish(STREAM_NAME, content)

        payload = ('request stream content', STREAM_NAME)
        requestContentMessage = AWSMessage(sender, receiver, payload)

        kinesis.receive(requestContentMessage)

        self.assertTrue(r1.send.called)
        arguments = r1.send.call_args[0][0]
        
        self.assertEqual(arguments.receiver, sender)
        self.assertEqual(arguments.sender, receiver)
        self.assertEqual(arguments.payload, ('stream content', STREAM_NAME, STREAM_CONTENT))

    def test_givenAKinesisWithATTLSetThenTTLBehaviourIsTriggered(self):
        """Given a kinesis - with a TTL set - then TTL behaviour is triggered"""
        STREAM_NAME = 'testStream'
        KINESIS_REGION = 'foo-region'
        TIMEOUT = 1
        SAMPLE_MESSAGE = AWSKinesisPayload(0, Mock(), 'test-message')

        r1 = Mock(AWSRegion)
        r1.regionName = KINESIS_REGION
        env = Mock()
        kinesis = AWSKinesis(r1, env, TIMEOUT)
        kinesis.createStream(STREAM_NAME)
        kinesis.publish(STREAM_NAME, SAMPLE_MESSAGE)

        generator = kinesis.startBehaviour()
        self.assertTrue(env.timeout.called_with(TIMEOUT))

        self.assertTrue(SAMPLE_MESSAGE in kinesis.consume(STREAM_NAME))
        generator = env.process.call_args[0][0]
        env.now = 30
        next(generator)
        next(generator)
        self.kinesis = kinesis
        self.assertFalse(SAMPLE_MESSAGE in kinesis.consume(STREAM_NAME))
        


        


 


class TestAWSIdentifier(unittest.TestCase):

    def test_setup(self):
        """AWSIdentifier can be correctly instantiated"""
        a = AWSIdentifier("test-region", "test-service")

    def test_singletonBehaviourSame(self):
        """Singleton Behaviour: AWS identifiers with same parameters are the same object"""
        a = AWSIdentifier("a", "b")
        b = AWSIdentifier("a", "b")
        self.assertEqual(a, b)

    def test_singletonBehaviourDifferent(self):
        """Singleton Behaviour: AWS identifiers with different parameters are different objects"""
        a = AWSIdentifier("a", "b")
        b = AWSIdentifier("b", "a")
        self.assertNotEqual(a, b)


class TestAWSMessage(unittest.TestCase):

    def test_setup(self):
        """AWSMessage can be correctly instantiated"""
        sender = AWSIdentifier("test-region1", "sender-service")
        receiver = AWSIdentifier("test-region2", "receiver-service")
        payload = "TEST"
        a = AWSMessage(sender, receiver, payload)

    def test_makeReply(self):
        """MakeReply returns correct AWS Message"""
        sender = AWSIdentifier("test-region1", "sender-service")
        receiver = AWSIdentifier("test-region2", "receiver-service")
        payload = "TEST"
        a = AWSMessage(sender, receiver, payload)
        r = a.makeReply(payload)

        self.assertEqual(r.sender, receiver)
        self.assertEqual(r.receiver, sender)
        self.assertEqual(r.payload, payload)



class TestAWSKinesisMessage(unittest.TestCase):

    def test_setup(self):
        """AWSKinesisMessage can be correctly instantiated"""
        sender = AWSIdentifier("test-region1", "sender-service")
        receiver = AWSIdentifier("test-region2", "receiver-service")
        payload = "TEST"
        a = AWSKinesisMessage(sender, receiver, payload)

class TestAWSKinesisSubscriberNotification(unittest.TestCase):
    
    def test_setup(self):
        """AWSKinesisSubscriberNotification can be correctly instantiated"""
        sender = AWSIdentifier("test-region1", "sender-service")
        receiver = AWSIdentifier("test-region2", "receiver-service")
        streamName = "TEST"
        a = AWSKinesisSubscriberNotification(sender, receiver, streamName)

class TestAWSRemoteService(unittest.TestCase):

    def test_setup(self):
        """AWSRemoteService object can be correctly instantiated"""
        REMOTE_SERVICE_NAME = 'Kinesis'
        a = awsbuilder.b.buildRemoteService()

        self.assertTrue(isinstance(a, AWSRemoteService))
        self.assertTrue(a.serviceName == 'RemoteKinesis_%s' % a.remoteRegionName)
        self.assertTrue(a.remoteServiceName == REMOTE_SERVICE_NAME)

    def test_makeSenderReturnsCorrectAWSIdentity(self):
        """makeSender() returns AWSIdentity with correct values"""
        a = awsbuilder.b.buildRemoteService()
        s = a.make_sender()
        self.verifySender(a, s)

    def verifySender(self, a, s):
        self.assertTrue(isinstance(s, AWSIdentifier))
        self.assertTrue(s.regionName == a.region.regionName)
        self.assertTrue(s.receiverName == a.serviceName)

    def test_makeReceiverReturnsCorrectAWSIdentity(self):
        """makeReceiver() returns AWSIdentity with correct values"""
        a = awsbuilder.b.buildRemoteService()
        r = a.make_receiver()
        self.verifyReceiver(a, r)

    def verifyReceiver(self, a, r):
        self.assertTrue(isinstance(r, AWSIdentifier))
        self.assertTrue(r.regionName == a.remoteRegionName)
        self.assertTrue(r.receiverName == a.remoteServiceName)

    def test_makeMessageReturnsCorrectAWSMessage(self):
        """makeMessage() returns AWSMessage with correct values"""
        MESSAGE_PAYLOAD = "Testmessage"
        a = awsbuilder.b.buildRemoteService()
        m = a.make_message(MESSAGE_PAYLOAD)

        self.assertTrue(isinstance(m, AWSMessage))
        self.verifySender(a, m.sender)
        self.verifyReceiver(a, m.receiver)
        self.assertTrue(m.payload == MESSAGE_PAYLOAD)

class TestAWSRemoteKinesis(unittest.TestCase):

    def test_setup(self):
        """AWSRemoteKinesis can be correctly instantiated"""
        r = awsbuilder.b.buildRemoteKinesis()

        self.assertTrue(isinstance(r, AWSRemoteService))
        self.assertTrue(r.serviceName == 'RemoteKinesis_%s' % r.remoteRegionName)
        self.assertTrue(r.remoteServiceName == 'Kinesis')

    def test_givenARemoteKinesisAndAServiceInAnotherRegionWhenServiceTriesToSubscribeToRemoteKinesisThenSubscribeIsRejected(self):
        """Given a remote kinesis and a services in another region - when subscribing - then subscribe is rejected"""
        STREAM_NAME = 'remote-stream'
        REMOTE_REGION_NAME = 'remote-region'
        ANOTHER_REGION_NAME = 'another-region'

        region = awsbuilder.b.buildRegion()
        region.sendToRegion = Mock()

        anotherRegion = awsbuilder.b.buildRegion(regionName = ANOTHER_REGION_NAME)
        anotherService = awsbuilder.b.buildLocalService(localRegion = anotherRegion)

        remoteKinesis = awsbuilder.b.buildRemoteKinesis(localRegion = region, remoteRegionName = REMOTE_REGION_NAME)

        with self.assertRaises(AWSServiceNotLocalError) as context:
            remoteKinesis.subscribe(STREAM_NAME, anotherService)



    def test_givenARemoteKinesisAndALocalServiceWhenSubscribingThenServiceIsAddedToSubscriberAndMessageToOtherRegionIsSent(self):
        """Given a remote kinesis and a local service - when subscribing - then service is added to subscriber and message to other region is sent"""
        STREAM_NAME = 'remote-stream'
        REMOTE_REGION_NAME = 'remote-region'

        region = awsbuilder.b.buildRegion()
        region.sendToRegion = Mock()

        remoteKinesis = awsbuilder.b.buildRemoteKinesis(localRegion = region, remoteRegionName = REMOTE_REGION_NAME)
        localService = awsbuilder.b.buildLocalService(localRegion = region)

        remoteKinesis.subscribe(STREAM_NAME, localService)

        self.assertTrue(region.sendToRegion.called)
        arguments = region.sendToRegion.call_args

        self.assertEqual(arguments[0][0], REMOTE_REGION_NAME)
        self.assertEqual(arguments[0][1], 'Kinesis')
        self.assertEqual(arguments[0][2].payload, ('subscribe', STREAM_NAME))
        self.assertTrue(STREAM_NAME in remoteKinesis.localSubscriptions)
        self.assertTrue(STREAM_NAME in remoteKinesis.bufferedStreams)
        self.assertTrue(localService in remoteKinesis.localSubscriptions[STREAM_NAME])

    def test_givenARemoteKinesisHavingBufferedContentWhenConsumingThenBufferedContentIsReturned(self):
        """Given a remote kinesis having buffered content - when consuming - then the buffered content is returned"""
        STREAM_NAME = 'remote-stream'
        REMOTE_REGION_NAME = 'remote-region'
        BUFFERED_CONTENT = ['a', 'b', 'c']

        region = awsbuilder.b.buildRegion()
        region.sendToRegion = Mock()

        remoteKinesis = awsbuilder.b.buildRemoteKinesis(localRegion = region, remoteRegionName = REMOTE_REGION_NAME)
        localService = awsbuilder.b.buildLocalService(localRegion = region)

        remoteKinesis.subscribe(STREAM_NAME, localService)
        remoteKinesis.bufferedStreams[STREAM_NAME] = BUFFERED_CONTENT

        content = remoteKinesis.consume(STREAM_NAME)
        self.assertEqual(content, BUFFERED_CONTENT)

    def test_givenARemoteKinesisWhenPublishingThenMessageToOtherRegionIsSent(self):
        """Given a remote kinesis - when publishing - then a message to the other region is sent"""
        STREAM_NAME = 'remote-stream'
        REMOTE_REGION_NAME = 'remote-region'
        MESSAGE = AWSKinesisPayload(0, Mock(), 'A nice test message!')

        region = awsbuilder.b.buildRegion()
        region.sendToRegion = Mock()

        remoteKinesis = awsbuilder.b.buildRemoteKinesis(localRegion = region, remoteRegionName = REMOTE_REGION_NAME)

        remoteKinesis.publish(STREAM_NAME, MESSAGE)

        self.assertTrue(region.sendToRegion.called)
        arguments = region.sendToRegion.call_args

        self.assertEqual(arguments[0][0], REMOTE_REGION_NAME)
        self.assertEqual(arguments[0][1], 'Kinesis')
        self.assertEqual(arguments[0][2].payload, ('publish', STREAM_NAME, MESSAGE))

    def test_givenARemoteKinesisWithSubscribersWhenNotifySubscribersIsCalledThenCorrectSubscribersAreNotified(self):
        """Given a remote kinesis with subscribers - when notify subscribers is called - then correct subscribers are notified"""
        STREAM_NAME = 'remote-stream'
        OTHER_STREAM_NAME = 'other-stream'
        REMOTE_REGION_NAME = 'remote-region'

        region = awsbuilder.b.buildRegion()
        region.sendToRegion = Mock()

        remoteKinesis = awsbuilder.b.buildRemoteKinesis(localRegion = region, remoteRegionName = REMOTE_REGION_NAME)
        localService1 = awsbuilder.b.buildLocalService(localRegion = region, serviceName = 'localService1')
        localService2 = awsbuilder.b.buildLocalService(localRegion = region, serviceName = 'localService2')

        remoteKinesis.subscribe(STREAM_NAME, localService1)
        remoteKinesis.subscribe(OTHER_STREAM_NAME, localService2)

        region.sendToRegion = Mock()
        remoteKinesis.notifySubscribers(STREAM_NAME)

        self.assertTrue(region.sendToRegion.called)
        self.assertEqual(region.sendToRegion.call_count, 1)
        arguments = region.sendToRegion.call_args

        self.assertEqual(arguments[0][0], region.regionName)
        self.assertEqual(arguments[0][1], localService1.serviceName)
        self.assertTrue(isinstance(arguments[0][2], AWSKinesisSubscriberNotification))
        self.assertEqual(arguments[0][2].payload, ('notify', STREAM_NAME))

    def test_givenARemoteKinesisWhenReceivingANotifyThenStreamContentIsRequested(self):
        """Given a remote kinesis - when receiving a notification - then stream content is requested"""
        STREAM_NAME = 'remote-stream'
        OTHER_STREAM_NAME = 'other-stream'
        REMOTE_REGION_NAME = 'remote-region'

        region = awsbuilder.b.buildRegion()
        region.sendToRegion = Mock()

        remoteKinesis = awsbuilder.b.buildRemoteKinesis(localRegion = region, remoteRegionName = REMOTE_REGION_NAME)

        receiver = AWSIdentifier(region.regionName, remoteKinesis.serviceName)
        sender = AWSIdentifier(REMOTE_REGION_NAME, 'Kinesis')
        message = AWSKinesisSubscriberNotification(sender, receiver, STREAM_NAME)
        remoteKinesis.receive(message)

        self.assertTrue(region.sendToRegion.called)
        self.assertEqual(region.sendToRegion.call_count, 1)
        arguments = region.sendToRegion.call_args

        self.assertEqual(arguments[0][0], REMOTE_REGION_NAME)
        self.assertEqual(arguments[0][1], 'Kinesis')
        self.assertTrue(isinstance(arguments[0][2], AWSMessage))
        self.assertEqual(arguments[0][2].payload, ('request stream content', STREAM_NAME))

    def test_givenARemoteKinesisWhenReceivingStreamContentThenStreamContentIsStoredInBufferAndSubscribersNotified(self):
        """Given a remote kinesis - when receiving stream content - then stream content is stored in buffer and subscribers are notified"""
        STREAM_NAME = 'remote-stream'
        OTHER_STREAM_NAME = 'other-stream'
        REMOTE_REGION_NAME = 'remote-region'
        STREAM_CONTENT1 = ['1', '2', '3']
        STREAM_CONTENT2 = ['1', '2', '3']

        region = awsbuilder.b.buildRegion()
        region.sendToRegion = Mock()

        remoteKinesis = awsbuilder.b.buildRemoteKinesis(localRegion = region, remoteRegionName = REMOTE_REGION_NAME)
        localService1 = awsbuilder.b.buildLocalService(localRegion = region, serviceName = 'localService1')
        localService1.receive = Mock()
        remoteKinesis.subscribe(STREAM_NAME, localService1)

        receiver = AWSIdentifier(region.regionName, remoteKinesis.serviceName)
        sender = AWSIdentifier(REMOTE_REGION_NAME, 'Kinesis')
        message1 = AWSKinesisStreamContent(sender, receiver, STREAM_NAME, STREAM_CONTENT1)
        message2 = AWSKinesisStreamContent(sender, receiver, STREAM_NAME, STREAM_CONTENT2)

        remoteKinesis.receive(message1)
        self.assertEqual(remoteKinesis.bufferedStreams[STREAM_NAME], STREAM_CONTENT1)

        remoteKinesis.receive(message2)
        self.assertEqual(remoteKinesis.bufferedStreams[STREAM_NAME], STREAM_CONTENT2)

        self.assertTrue(region.sendToRegion.called)
        args = region.sendToRegion.call_args
        self.assertEqual(args[0][0], region.regionName)
        self.assertEqual(args[0][1], localService1.serviceName)
        self.assertEqual(args[0][2].payload, ('notify', STREAM_NAME))
        self.assertEqual(args[0][2].sender.receiverName, 'RemoteKinesis_%s' % REMOTE_REGION_NAME)
        self.assertEqual(args[0][2].sender.regionName, region.regionName)


class TestAWSLoadBalancer(unittest.TestCase):

    def test_setup(self):
        """AWSLoadBalancer can be instantiated"""
        SERVICE_NAME = "test-service"
        region = awsbuilder.b.buildRegion()
        p = AWSPublicService(region, SERVICE_NAME)
        lb = AWSLoadBalancer(region)
        lb.registerInstance(p)


    def test_givenALoadbalancerWithTwoRegisteredInstancesWhenDoingMultipleSendsThenBothInstancesGetData(self):
        """Given a LoadBalancer with two registered instances - when doing multiple sends - then both instances get data"""
        SERVICE_NAME = "test-service"
        region = awsbuilder.b.buildRegion()

        p1 = AWSPublicService(region, SERVICE_NAME + '1')
        p1.publicReceive = Mock()
        p2 = AWSPublicService(region, SERVICE_NAME + '2')
        p2.publicReceive = Mock()

        lb = AWSLoadBalancer(region)
        lb.registerInstance(p1)
        lb.registerInstance(p2)

        m = Mock(Message)

        for i in range(0, 10):
            lb.receive(m)

        self.assertTrue(p1.publicReceive.called)
        self.assertTrue(p2.publicReceive.called)


class TestAWSPublicService(unittest.TestCase):

    def test_setup(self):
        """AWSPublicService can be instantiated"""
        SERVICE_NAME = 'test-service'
        region = awsbuilder.b.buildRegion()
        s = AWSPublicService(region, SERVICE_NAME)


class TestAWSRoute53(unittest.TestCase):

    def test_setup(self):
        """AWSRoute53 can be instantiated"""
        r = AWSRoute53()

    def test_givenARoute53AndTwoLoadbalancersWhenRegisteringLoadbalancersThenBothLoadbalancersAreRegistered(self):
        """Given a route 53 and two loadbalancers - when registering loadbalancers - then both loadbalancers are registered"""
        SERVICE_NAME = 'test-service'
        r = AWSRoute53()

        region1 = awsbuilder.b.buildRegion(regionName = 'region1')
        lb1 = AWSLoadBalancer(region1)

        region2 = awsbuilder.b.buildRegion(regionName = 'region2')
        lb2 = AWSLoadBalancer(region2)

        r.registerLoadBalancer(SERVICE_NAME, lb1)
        r.registerLoadBalancer(SERVICE_NAME, lb2)

        self.assertTrue(SERVICE_NAME in r.registeredLoadBalancers)
        self.assertTrue(lb1.getIdentifier() in r.registeredLoadBalancers[SERVICE_NAME])
        self.assertTrue(lb2.getIdentifier() in r.registeredLoadBalancers[SERVICE_NAME])

    def test_givenARoute53WithLoadbalancersRegisteredUnderSameNameWhenTryingToLookupServiceThenBothLoadBalancersAreReturned(self):
        """Given a route 53 with two load balancers reigstered under the same name - when trying to lookup service - then both load balancers are returned"""
        SERVICE_NAME = 'test-service'
        r = AWSRoute53()

        region1 = awsbuilder.b.buildRegion(regionName = 'region1')
        lb1 = AWSLoadBalancer(region1)

        region2 = awsbuilder.b.buildRegion(regionName = 'region2')
        lb2 = AWSLoadBalancer(region2)

        r.registerLoadBalancer(SERVICE_NAME, lb1)
        r.registerLoadBalancer(SERVICE_NAME, lb2)

        clientRegion = Mock(Region)
        clientRegion.getLatency = Mock(return_value = 10)

        ret = r.lookup(SERVICE_NAME, clientRegion)

        self.assertTrue(lb1.getIdentifier() in ret)
        self.assertTrue(lb2.getIdentifier() in ret)

    def test_givenARoute53WithMultipleLodbalancersRegisteredUnderSameNameWhenTryingToLookupServiceThenClosestLoadBalancersAreReturnedFirst(self):
        """Given a route 53 with multiple loadbalancers registered under the same name - when trying to lookup service - then closest load balancers are returned first"""
        SERVICE_NAME = 'test-service'
        r = AWSRoute53()

        region1 = awsbuilder.b.buildRegion(regionName = 'region1')
        lb1 = AWSLoadBalancer(region1)

        region2 = awsbuilder.b.buildRegion(regionName = 'region2')
        lb2 = AWSLoadBalancer(region2)

        region3 = awsbuilder.b.buildRegion(regionName = 'region3')
        lb3 = AWSLoadBalancer(region3)

        cr = clientbuilder.b.buildClientRegionWithClients(Mock(), 'client-region', 1)

        awsbuilder.b.connectTwoRegions(Mock(), cr, region3, 10, 10)
        awsbuilder.b.connectTwoRegions(Mock(), cr, region1, 20, 20)
        awsbuilder.b.connectTwoRegions(Mock(), cr, region2, 30, 30)

        r.registerLoadBalancer(SERVICE_NAME, lb1)
        r.registerLoadBalancer(SERVICE_NAME, lb2)
        r.registerLoadBalancer(SERVICE_NAME, lb3)

        ret = r.lookup(SERVICE_NAME, cr)

        self.assertEqual(ret[0].regionName, region3.regionName)
        self.assertEqual(ret[1].regionName, region1.regionName)
        self.assertEqual(ret[2].regionName, region2.regionName)


if __name__ == '__main__':
    unittest.main()
