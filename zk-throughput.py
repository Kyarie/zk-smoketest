#!/usr/bin/env python

import datetime, time, os
from optparse import OptionParser
from multiprocessing.pool import ThreadPool

import zkclient
from zkclient import ZKClient, CountingWatcher, zookeeper

SESSIONS_NUM = 12
total_writes = 0

usage = "usage: %prog [options]"
parser = OptionParser(usage=usage)
parser.add_option("", "--servers", dest="servers",
                  default="localhost:2181", help="comma separated list of host:port (default %default), test each in turn")
parser.add_option("", "--cluster", dest="cluster",
                  default=None, help="comma separated list of host:port, test as a cluster, alternative to --servers")
parser.add_option("", "--config",
                  dest="configfile", default=None,
                  help="zookeeper configuration file to lookup cluster from")
parser.add_option("", "--timeout", dest="timeout", type="int",
                  default=5000, help="session timeout in milliseconds (default %default)")
parser.add_option("", "--root_znode", dest="root_znode",
                  default="/zk-latencies", help="root for the test, will be created as part of test (default /zk-latencies)")
parser.add_option("", "--znode_size", dest="znode_size", type="int",
                  default=25, help="data size when creating/setting znodes (default %default)")
parser.add_option("", "--znode_count", dest="znode_count", default=10000, type="int",
                  help="the number of znodes to operate on in each performance section (default %default)")
parser.add_option("", "--watch_multiple", dest="watch_multiple", default=1, type="int",
                  help="number of watches to put on each znode (default %default)")

parser.add_option("", "--force",
                  action="store_true", dest="force", default=False,
                  help="force the test to run, even if root_znode exists - WARNING! don't run this on a real znode or you'll lose it!!!")

parser.add_option("", "--synchronous",
                  action="store_true", dest="synchronous", default=False,
                  help="by default asynchronous ZK api is used, this forces synchronous calls")

parser.add_option("-v", "--verbose",
                  action="store_true", dest="verbose", default=False,
                  help="verbose output, include more detail")
parser.add_option("-q", "--quiet",
                  action="store_true", dest="quiet", default=False,
                  help="quiet output, basically just success/failure")
parser.add_option("", "--type", dest="type", default=False,
                  help="quiet output, basically just success/failure")

(options, args) = parser.parse_args()

zkclient.options = options

zookeeper.set_log_stream(open("cli_log_%d.txt" % (os.getpid()),"w"))

class SmokeError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class ZkData:

    def __init__(self, session, data, startTime, id):
        self.session = session
        self.data = data
        self.startTime = startTime
        self.id = id

def child_path(i, id):
    return "%s/session_%d_%d" % (options.root_znode, i, id)

def create_asynchronous_latency_test(zd):
    # create znode_count znodes (perm)
    
    global SESSIONS_NUM
    callbacks = []
    for j in xrange(options.znode_count/SESSIONS_NUM):
        cb = zkclient.CreateCallback()
        cb.cv.acquire()
        zd.session.acreate(child_path(j, zd.id), cb, zd.data)
        callbacks.append(cb)
        
    count = 0
    for j, cb in enumerate(callbacks):
        cb.waitForSuccess()
        #print(child_path(j, zd.id))
        if cb.path != child_path(j, zd.id):
            raise SmokeError("invalid path %s for operation %d on handle %d" %
                            (cb.path, j, cb.handle))
        count += 1
        if time.time() - zd.startTime >= 10:
            break
    return count

def get_asynchronous_latency_test(zd):
    # create znode_count znodes (perm)
    global SESSIONS_NUM
    callbacks = []
    for j in xrange(options.znode_count/SESSIONS_NUM):
        cb = zkclient.GetCallback()
        cb.cv.acquire()
        zd.session.aget(child_path(j, zd.id), cb)
        callbacks.append(cb)

    count = 0
    for cb in callbacks:
        cb.waitForSuccess()
        if cb.value != data:
            raise SmokeError("invalid data %s for operation %d on handle %d" %
                             (cb.value, j, cb.handle))
        count += 1
        if time.time() - zd.startTime >= 10:
            break
    return count

def set_asynchronous_latency_test(zd):
    # create znode_count znodes (perm)
    global SESSIONS_NUM
    callbacks = []
    for j in xrange(options.znode_count/SESSIONS_NUM):
        cb = zkclient.SetCallback()
        cb.cv.acquire()
        zd.session.aset(child_path(j, zd.id), cb, zd.data)
        callbacks.append(cb)

    count = 0
    for cb in callbacks:
        cb.waitForSuccess()
        count += 1
        if time.time() - zd.startTime >= 10:
            break
    return count

def log_result(result):
    global total_writes
    #print("done")
    total_writes += result

def apply_async_with_callback(sessions, data):
    global total_writes
    pool = ThreadPool(processes=SESSIONS_NUM)
    startTime = time.time()
    print("Start time: ", str(startTime))
    for i, s in enumerate(sessions):
        zd = ZkData(s, data, startTime, i)
        if options.type == "create":    
            pool.apply_async(create_asynchronous_latency_test, args=(zd,), callback=log_result)
        elif options.type == "set":
            pool.apply_async(set_asynchronous_latency_test, args=(zd,), callback=log_result)
        elif options.type == "get":
            pool.apply_async(get_asynchronous_latency_test, args=(zd,), callback=log_result)
    pool.close()
    pool.join()
    print("Duration: ", str(time.time()-startTime))
    print("Total writes: ", total_writes)

if __name__ == '__main__':
    data = options.znode_size * "x"
    servers = options.servers.split(",")
    # create all the sessions first to ensure that all servers are
    # at least available & quorum has been formed. otw this will
    # fail right away (before we start creating nodes)
    sessions = []
    # create sessions to each of the servers in the ensemble
    for server in servers:
        for i in range(SESSIONS_NUM):
            sessions.append(ZKClient(server, options.timeout))

    # ensure root_znode doesn't exist
    if sessions[0].exists(options.root_znode) and options.type == "create":
        # unless forced by user
        if not options.force:
            raise SmokeError("Node %s already exists!" % (options.root_znode))

        children = sessions[0].get_children(options.root_znode)
        for child in children:
            sessions[0].delete("%s/%s" % (options.root_znode, child))
    elif options.type == "create":
        sessions[0].create(options.root_znode,
                           "smoketest root, delete after test done, created %s" %
                           (datetime.datetime.now().ctime()))

    apply_async_with_callback(sessions, data)

    sessions[0].delete(options.root_znode)

    # close sessions
    for s in sessions:
        s.close()

    print("Latency test complete")
