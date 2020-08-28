#!/usr/bin/env python3
#
# Helper Class for console output - part of SIM Simulator

from util.output import bcolors
from util.helper import sort

class Printer(object):
    def printRegion(self, region):
        self.printHeader("*********** REGION INFO *******************")
        self.printBold(region.regionName)
        self.printBold("+ Services: ")
        for service in sort(region.services):
            self.print("  %s" % service)

        self.printBold("+ Connected Regions: ")
        for connectedRegion in sort(region.connectedRegions):
            if hasattr(region.connectedRegions[connectedRegion][1], 'lossProbability'):
                self.print("  %s:\n\tLatency: %s ticks\tPacket loss: %i%%" % (connectedRegion, region.connectedRegions[connectedRegion][1].latency, region.connectedRegions[connectedRegion][1].lossProbability * 100))
            else:
                self.print("  %s:\n\tLatency: %s ticks\tPacket loss: %i%%" % (connectedRegion, region.connectedRegions[connectedRegion][1].latency, 0))

        self.print()

    def printSIMS(self, sims):
        lastRegion = None
        for sim in sims:
            if sim.region != lastRegion:
                self.print()
                self.printHeader("########### ", end = '')
                self.printBold("Region: %s " % sim.region.regionName, end = '')
                self.printHeader("###########") 
            lastRegion = sim.region
            sim.printInfo(self)

class ConsolePrinter(Printer):
    def print(self, inp = '', end = '\n'):
        print(inp, end = end)

    def printError(self, inp = '', end = '\n'):
        print(bcolors.FAIL, end = '')
        print(inp, end = ' ')
        print(bcolors.ENDC, end = end)

    def printOK(self, inp = '', end = '\n'):
        print(bcolors.OKGREEN, end = '')
        print(inp, end = ' ')
        print(bcolors.ENDC, end = end)

    def printOKB(self, inp = '', end = '\n'):
        print(bcolors.OKBLUE, end = '')
        print(inp, end = ' ')
        print(bcolors.ENDC, end = end)

    def printBold(self, inp = '', end = '\n'):
        print(bcolors.BOLD, end = '')
        print(inp, end = ' ')
        print(bcolors.ENDC, end = end)

    def printWarning(self, inp = '', end = '\n'):
        print(bcolors.WARNING, end = '')
        print(inp, end = ' ')
        print(bcolors.ENDC, end = end)

    def printHeader(self, inp = '', end = '\n'):
        print(bcolors.HEADER, end = '')
        print(inp, end = ' ')
        print(bcolors.ENDC, end = end)


p = ConsolePrinter()
