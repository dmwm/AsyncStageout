from threading import Thread
import os
from Queue import Queue
import subprocess
import json

myList = Queue(maxsize=0)
lockFile = '/data/srv/asyncstageout/current/install/asyncstageout/Monitor/Cleanup-locked'
lastCounter = '/data/srv/asyncstageout/current/install/asyncstageout/Monitor/Cleanup-counter'

if os.path.isfile(lockFile):
    print 'Clean up lock file exists. Exiting and not running on top.'
    exit

#Touch the file to let know other that lock is available.
open(lockFile, 'a').close()

counter = []
if os.path.isfile(lastCounter):
    with open(lastCounter) as fd:
        counter = json.loads(fd.read())

if len(counter) == 0:
    counter.append(0)

def deleteFile(i, q):
    while True:
        success = False
        fileName = q.get()
        fileName = "/data/srv/asyncstageout/current/install/asyncstageout/Monitor/" + fileName
        #print fileName
        if os.path.isfile(fileName):
            os.remove(fileName)
            success = True
            print '%s %s %s' % (i, fileName, success)
        q.task_done()

print 'grep all in ended state'
returnCode = subprocess.call(["grep 'ended in state' /data/srv/asyncstageout/current/install/asyncstageout/Monitor/aso-monitor.log | awk '{print $4}' > /data/srv/asyncstageout/current/install/asyncstageout/Monitor/cleanup_out1.txt"], shell=True)
print 'grep all in ended state exit %s' % returnCode
print 'grep all JOBIDs'
returnCode = subprocess.call(["grep 'JOBID' /data/srv/asyncstageout/current/install/asyncstageout/Monitor/cleanup_out1.txt | awk -F= '{print $2}' > /data/srv/asyncstageout/current/install/asyncstageout/Monitor/cleanup_out2.txt"], shell=True)
print 'grep all JOBIDs exit %s' % returnCode

with open('/data/srv/asyncstageout/current/install/asyncstageout/Monitor/cleanup_out2.txt') as fd:
    lines = fd.readlines()

size = len(lines)
print 'Number of files in monitor log file: %s' % size
print 'Last time cleanup processed: %s' % counter[0]
while size != counter[0]:
    size -= 1
    myList.put("work/Monitor.%s.json" % lines[size].rstrip())

for i in range(24):
    worker = Thread(target=deleteFile, args=(i, myList))
    worker.setDaemon(True)
    worker.start()
print 'Main wait'
myList.join()
print 'Main done'

# Cleanup
counter[0] = len(lines)
with open(lastCounter, 'w') as fd:
    json.dump(counter, fd)
os.remove('/data/srv/asyncstageout/current/install/asyncstageout/Monitor/cleanup_out1.txt')
os.remove('/data/srv/asyncstageout/current/install/asyncstageout/Monitor/cleanup_out2.txt')
os.remove(lockFile)

