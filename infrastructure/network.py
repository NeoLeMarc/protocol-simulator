#!/usr/bin/env python
#
# Networking components for SIM simulator

import simpy
import random

class NetworkError(Exception):
    pass

class NetworkPathError(NetworkError):
    pass

class SideAlreadyConnectedError(NetworkPathError):
    pass

class NetworkPath(object):
    """A standard, unidirectional network path, that connects two objects"""

    def __init__(self, env, name = None, latency = 0):
        self.env = env
        self.name = name 
        self.latency = latency
        self.leftSide = None
        self.rightSide = None
        self.buffer = simpy.Store(env)

    def connectLeftSide(self, leftSide):
        if self.leftSide != None:
            raise SideAlreadyConnectedError("LeftSide already connected")
        else:
            self.leftSide = leftSide

    def connectRightSide(self, rightSide):
        if self.rightSide != None:
            raise SideAlreadyConnectedError("RightSide already connected")
        else:
            self.rightSide = rightSide

    def send(self, payload):
        self.env.process(self.sendLatency(payload))

    def sendLatency(self, payload):
        yield self.env.timeout(self.latency)

        self.buffer.put(payload)
        self.env.process(self.rightSide.notifyNetworkReceive(self))

    def receive(self):
        return self.buffer.get()

class LossyNetworkPath(NetworkPath):
    """Same as NetworkPath but randomly loses events"""

    def __init__(self, env, name = None, latency = 0, lossProbability = 0, randomGenerator = random.random):
        super(LossyNetworkPath, self).__init__(env, name, latency)
        self.lossProbability = lossProbability
        self.randomGenerator = randomGenerator

    def send(self, payload):
        if self.randomGenerator() <= self.lossProbability:
            pass # Packet was lost
        else:
            super(LossyNetworkPath, self).send(payload)
