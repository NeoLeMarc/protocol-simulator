#!/usr/bin/env python
#
# Simulation of Kinesis based SIM 


from simulations.simulation import Simulation
from infrastructure.aws import AWSRegion, AWSService, AWSMessage
from infrastructure.network import NetworkPath, LossyNetworkPath
from components.sim import *
from util.builder import awsbuilder
from util.helper import sort
from util.printer import p
import simpy, os, time

class KinesisBasedSIMSimulationWithClients(Simulation):

    def run(self):
        ################################# Scenario 
        regionsToBuild = [
                {'regionName' : 'us-west-1',      'latency' : 10},
                {'regionName' : 'us-east-1',      'latency' : 10},
                {'regionName' : 'eu-central-1',   'latency' : 50},
                {'regionName' : 'apac-central-1', 'latency' : 40}
        ] 

        TTL                  = 1000    ## TTL for messages in Kinesis
        maxstep              = 1000000 ## When does simulation end?
        packetLoss           = 0.0     ## Percentage of packets to loose
        sleepTime            = 0.0     ## how long to sleep between each XX steps
        step                 = 1000     ## How big are the steps between outputs?
        simHeartbeatInterval = 10      ## How often does SIM sends its heartbeats
        badNetworkInAPAC     = True    ## Really bad network between APAC and us-west-1 during start up
        waitForReturn        = False   ## Wait for return after each XX steps

        ## Which SIM behaviour to test:
        #SIM = KinesisBasedSim
        SIM = ResubscribingKinesisBasedSim
        ################################# END OF Scenario 

        env = simpy.Environment()
        b = awsbuilder.AWSBuilder()
        b.getNetworkPathInstance = lambda env, name, latency: LossyNetworkPath(env, name, latency, packetLoss)
        self.regions = b.buildFullyMeshedRegionsWithKinesis(env, regionsToBuild, TTL)

        if badNetworkInAPAC:
            self.regions[3].connectedRegions['us-west-1'][1].lossProbability = 0.9 # Network connectivity in ASIA is lossy
    
        ## Create SIM pair in each Region
        self.sims = []
        for region in self.regions:
            self.sims.append(SIM(env, region, 0, region.getServiceByName('Kinesis'), simHeartbeatInterval))
            self.sims.append(SIM(env, region, 1, region.getServiceByName('Kinesis'), simHeartbeatInterval))

        for sim in self.sims:
            sim.attachRemoteRegions(self.regions)
            sim.startBehaviour()

        ## Print status
        os.system("clear")
        p.printHeader("+++++++++++++++++++++++++++++ Simulation Scenario ++++++++++++++++++++++++++++++")
        self.printRegions()
        self.input("Press [ENTER] to continue")

        curtime = 200
        env.run(until = (curtime - 1))
        self.regions[3].connectedRegions['us-west-1'][1].lossProbability = packetLoss # Network magically improved 

        while curtime < maxstep:
            env.run(until = curtime) 
            if sleepTime > 0:
                time.sleep(sleepTime)
            if waitForReturn:
                input("Press [ENTER] to continue")

            os.system("clear")
            progress = curtime*100/maxstep
            p.printHeader("+++++++++++++++++++++++++++++ Simulation Step ++++++++++++++++++++++++++++++")
            p.print("Current time: %i - Step width: %i - Simulation ends at: %i (%i%% completed)" % (curtime, step, maxstep, progress))
            p.print()

            self.printSIMS()
            curtime += step 


    def printRegions(self):
        for region in self.regions:
            p.printRegion(region)

    def printSIMS(self):
        p.printSIMS(self.sims)

