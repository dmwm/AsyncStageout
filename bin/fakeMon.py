import threading
import Queue
import sys
import os

def do_work(in_queue,):
    while True:
        item = in_queue.get()
        # process
        with open(item) as json_file:
            json_data = json.load(json_file)

        jobid = item.split(".")[1]

        reporter = {
                    "LFNs": [],
                    "transferStatus": [],
                    "failure_reason": [],
                    "timestamp": [],
                    "username": ""
        }
        reporter["LFNs"] = json_data["LFNs"]
        reporter["transferStatus"] = ['Finished' for x in range(len(reporter["LFNs"]))]
        reporter["username"] = json_data["username"]
        reporter["reasons"] = ['' for x in range(len(reporter["LFNs"]))]
        reporter["timestamp"] = 10000 
        report_j = json.dumps(reporter)

        try:
            if not os.path.exists("/data/srv/asyncstageout/current/install/asyncstageout/Monitor/work/%s" %user):
                os.makedirs("/data/srv/asyncstageout/current/install/asyncstageout/Monitor/work/%s" %user)
            out_file = open("/data/srv/asyncstageout/current/install/asyncstageout/AsyncTransfer/dropbox/inputs/%s/Reporter.%s.json"%(user,jobid),"w")
            out_file.write(report_j)
            out_file.close()
            os.remove('/%s/Monitor.%s.json' %(self.user,self.jobid))
        except Exception as ex:
            msg="Cannot create fts job report: %s" %ex
            self.logger.error(msg)

        in_queue.task_done()

if __name__ == "__main__":
    self.f
    work = Queue.Queue()
    results = Queue.Queue()
    total = 20

    # start for workers
    for i in xrange(4):
        t = threading.Thread(target=do_work, args=(work,))
        t.daemon = True
        t.start()

    # produce data
    for i in os.listdir("/data/srv/asyncstageout/current/install/asyncstageout/AsyncTransfer/dropbox/outputs"):
        work.put(i)

    work.join()

    sys.exit()
