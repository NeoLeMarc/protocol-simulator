def dumpStream(stream):
    ret = []
    for event in stream:
        sender = event.sender.receiverName
        region = event.sender.regionName
        timestamp = event.timestamp
        ret.append((sender, region, timestamp))
    return ret

import resource
print('Memory usage: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

import gc
gc.collect()

import objgraph
objgraph.show_most_common_types()
