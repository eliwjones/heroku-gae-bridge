import os, time, sys
from queue import filesystemqueue
from queue.consumers import *

try:
    log = sys.argv[1] == 'log'
except:
    log = False

if __name__ == '__main__':
    while True:
        try:
            results = filesystemqueue.work()
            if results and log:
                with open('pmq.log', 'a') as pmqlog:
                    pmqlog.write("[%s]:  %s\n" % (time.asctime(), results))
        except Exception as e:
            print e
        time.sleep(1)
