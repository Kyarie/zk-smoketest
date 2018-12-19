#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime, time, os
from optparse import OptionParser
import multiprocessing as mp

import zkclient
from zkclient import ZKClient, CountingWatcher, zookeeper

SESSIONS_NUM = 3
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

(options, args) = parser.parse_args()

zkclient.options = options

zookeeper.set_log_stream(open("cli_log_%d.txt" % (os.getpid()),"w"))

class SmokeError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class ZkTest:

    def __init__(self, data, startTime):
        self.data = data
        self.startTime = startTime

    def child_path(self, i):
        return "%s/session_%d" % (options.root_znode, i)

    def asynchronous_latency_test(self, s):
        # create znode_count znodes (perm)
        
        callbacks = []
        for j in xrange(options.znode_count/SESSIONS_NUM):
            cb = zkclient.CreateCallback()
            cb.cv.acquire()
            s.acreate(self.child_path(j), cb, self.data)
            callbacks.append(cb)
        print("Callback created")
        count = 0
        for j, cb in enumerate(callbacks):
            cb.waitForSuccess()
            if cb.path != self.child_path(j):
                raise SmokeError("invalid path %s for operation %d on handle %d" %
                                (cb.path, j, cb.handle))
            count += 1
            if time.time() - self.startTime >= 10000:
                break
        return count


def log_result(result):
    global total_writes
    total_writes += result

def apply_async_with_callback(sessions, data):
    global total_writes
    zkTest = []
    pool = mp.Pool()
    startTime = time.time()
    print("Start time: ", str(startTime))
    for i, s in enumerate(sessions):
        zt = ZkTest(s, data)
        pool.apply_async(zt.asynchronous_latency_test, args=(s,), callback=zt.log_result)
        zkTest.append(zt)
    pool.close()
    pool.join()
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
    if sessions[0].exists(options.root_znode):
        # unless forced by user
        if not options.force:
            raise SmokeError("Node %s already exists!" % (options.root_znode))

        children = sessions[0].get_children(options.root_znode)
        for child in children:
            sessions[0].delete("%s/%s" % (options.root_znode, child))
    else:
        sessions[0].create(options.root_znode,
                           "smoketest root, delete after test done, created %s" %
                           (datetime.datetime.now().ctime()))

    apply_async_with_callback(sessions, data)

    sessions[0].delete(options.root_znode)

    # close sessions
    for s in sessions:
        s.close()

    print("Latency test complete")

'''
class ZkOptions:

    def __init__(self, root_znode):
        self.servers = "localhost:2181"
        self.cluster = None
        self.configfile = None
        self.timeout = 5000
        self.root_znode = root_znode
        self.znode_size = 25
        self.watch_multiple = 1
        self.force = False
        self.synchronous = False
        self.verbose = False
        self.quiet = False
'''
