#!/usr/bin/env python3
#
# Simulation runner for SIM Simulator


import sys, string, cProfile

if len(sys.argv) < 2:
    print("Usage: %s <simulation to run>" % sys.argv[0])

else:
    simulationName = sys.argv[1]

    try:
        exec("from simulations.%s import %s as sim" % (simulationName.lower(), simulationName)) 
        mySim = sim()
        mySim.run()
    except IOError:
        sys.stderr.write("Error: Could not find simulation \"%s\".\n" % simulationName)
        sys.exit(1)
    finally:
        import debug
