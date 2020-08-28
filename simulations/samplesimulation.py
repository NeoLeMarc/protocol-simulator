#!/usr/bin/env python
#
# Simulation test implementation


from simulations.simulation import Simulation
from infrastructure.aws import AWSRegion, AWSService, AWSMessage
from infrastructure.network import NetworkPath
import simpy

class SampleService(AWSService):

    def receive(self, message):
        print("Received a message: " + message.payload)

class SampleSimulation(Simulation):

    def run(self):
        env = simpy.Environment()

        westToEast = NetworkPath(env, name = 'ConnectingAWSWestToEast', latency = 10)
        eastToWest = NetworkPath(env, name = 'ConnectingAWSEastToWest', latency = 10)

        usWest1 = AWSRegion(env, regionName = 'us-west-1')
        usEast1 = AWSRegion(env, regionName = 'us-east-1')

        usWest1.connectRegion(usEast1, westToEast)
        usEast1.connectRegion(usWest1, eastToWest)

        serviceWest = SampleService(usWest1, 'ServiceWest')
        serviceEast = SampleService(usEast1, 'ServiceEast')

        sender = serviceWest.getAWSIdentifier()
        receiver = serviceEast.getAWSIdentifier()
        message = AWSMessage(sender, receiver, 'This is a Test!')

        usWest1.send(message)

        print("Starting simulation")
        env.run(until = 100)
