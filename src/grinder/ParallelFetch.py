#!/usr/bin/env python
#
# Copyright (c) 2010 Red Hat, Inc.
#
# This software is licensed to you under the GNU General Public License,
# version 2 (GPLv2). There is NO WARRANTY for this software, express or
# implied, including the implied warranties of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. You should have received a copy of GPLv2
# along with this software; if not, see
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.
#
# Red Hat trademarks are not licensed under GPLv2. No permission is
# granted to use or replicate Red Hat trademarks that are incorporated
# in this software or its documentation.
#
import time
import logging
import threading
from threading import Thread, Lock
import traceback
import Queue

from BaseFetch import BaseFetch
from GrinderCallback import ProgressReport

LOG = logging.getLogger("grinder.ParallelFetch")

class SyncReport:
    def __init__(self):
        self.successes = 0
        self.downloads = 0
        self.errors = 0
    def __str__(self):
        return "%s successes, %s downloads, %s errors" % (self.successes, self.downloads, self.errors)

class ParallelFetch(object):
    def __init__(self, fetcher, numThreads=3, callback=None):
        self.fetcher = fetcher
        self.numThreads = numThreads
        self.callback = callback
        self.sizeTotal = 0
        self.sizeLeft = 0
        self.itemTotal = 0
        self.statusLock = Lock()
        self.syncStatusDict = dict()
        self.syncStatusDict[BaseFetch.STATUS_NOOP] = 0
        self.syncStatusDict[BaseFetch.STATUS_DOWNLOADED] = 0
        self.syncStatusDict[BaseFetch.STATUS_SIZE_MISSMATCH] = 0
        self.syncStatusDict[BaseFetch.STATUS_MD5_MISSMATCH] = 0
        self.syncStatusDict[BaseFetch.STATUS_ERROR] = 0
        self.toSyncQ = Queue.Queue()
        self.syncCompleteQ = Queue.Queue()
        self.syncErrorQ = Queue.Queue()
        self.threads = []
        for i in range(self.numThreads):
            wt = WorkerThread(self, fetcher)
            self.threads.append(wt)

    def addItem(self, item):
        if item.has_key("size") and item['size'] is not None:
            self.sizeTotal = self.sizeTotal + int(item['size'])
        self.toSyncQ.put(item)

    def addItemList(self, items):
        for p in items:
            if p.has_key("size") and p['size'] is not None:
                self.sizeTotal = self.sizeTotal + int(p['size'])
            self.toSyncQ.put(p)

    def getWorkItem(self):
        """
        Returns an item, or throws Queue.Empty exception if queue is empty
        """
        item = None
        # Usage of statusLock is to ensure that reporting of items
        # left to work on are reported accurately through markStatus
        self.statusLock.acquire()
        try:
            item = self.toSyncQ.get_nowait()
        finally:
            self.statusLock.release()
        return item

    def markStatus(self, itemInfo, status):
        self.statusLock.acquire()
        try:
            if status in self.syncStatusDict:
                self.syncStatusDict[status] = self.syncStatusDict[status] + 1
            else:
                self.syncStatusDict[status] = 1
            if status != BaseFetch.STATUS_ERROR:
                self.syncCompleteQ.put(itemInfo)
            else:
                self.syncErrorQ.put(itemInfo)
            LOG.debug("%s status updated, %s success %s error" % (itemInfo,
                self.syncCompleteQ.qsize(), self.syncErrorQ.qsize()))
            if itemInfo.has_key("size")  and itemInfo['size'] is not None:
                self.sizeLeft = self.sizeLeft - int(itemInfo['size'])
            if self.callback is not None:
                itemsLeft = self.itemTotal - (self.syncErrorQ.qsize() + self.syncCompleteQ.qsize())
                r = ProgressReport(self.sizeTotal, self.sizeLeft, self.itemTotal, itemsLeft)
                if itemInfo.has_key("fileName"):
                    r.item_name = itemInfo["fileName"]
                r.status = status
                r.num_error = self.syncErrorQ.qsize()
                r.num_success = self.syncCompleteQ.qsize()
                self.callback(r)
        finally:
            self.statusLock.release()

    def start(self):
        # Assumption is all adds to toSyncQ have been completed at this point
        # We will grab the size of the items for total number of items to sync
        # before we kick off the threads to start fetching
        self.sizeLeft = self.sizeTotal
        self.itemTotal = self.toSyncQ.qsize()
        if self.callback is not None:
            r = ProgressReport(self.sizeTotal, self.sizeLeft, self.itemTotal, self.toSyncQ.qsize())
            r.status = "STARTED"
            self.callback(r)
        for t in self.threads:
            t.start()

    def stop(self):
        for t in self.threads:
            t.stop()

    def _running(self):
        working = 0
        for t in self.threads:
            if (t.isAlive()):
                working += 1
        return (working > 0)

    def _waitForThreads(self):
        num_alive_threads = self._running()
        while (num_alive_threads):
            LOG.debug("Waiting for threads to finish, %s still active" % (num_alive_threads))
            time.sleep(0.5)
            num_alive_threads = self._running()

    def waitForFinish(self):
        """
        Will wait for all worker threads to finish
        Returns (successList, errorList)
         successList is a list of all items successfully synced
         errorList is a list of all items which couldn't be synced
        """
        self._waitForThreads()

        LOG.info("All threads have finished.")
        successList = []
        while not self.syncCompleteQ.empty():
            p = self.syncCompleteQ.get_nowait()
            successList.append(p)
        errorList = []
        while not self.syncErrorQ.empty():
            p = self.syncErrorQ.get_nowait()
            errorList.append(p)
        report = SyncReport()
        report.successes = self.syncStatusDict[BaseFetch.STATUS_DOWNLOADED]
        report.successes = report.successes + self.syncStatusDict[BaseFetch.STATUS_NOOP]
        report.downloads = self.syncStatusDict[BaseFetch.STATUS_DOWNLOADED]
        report.errors = self.syncStatusDict[BaseFetch.STATUS_ERROR]
        report.errors = report.errors + self.syncStatusDict[BaseFetch.STATUS_MD5_MISSMATCH]
        report.errors = report.errors + self.syncStatusDict[BaseFetch.STATUS_SIZE_MISSMATCH]

        LOG.info("ParallelFetch: %s items successfully processed, %s downloaded, %s items had errors" %
            (report.successes, report.downloads, report.errors))
        if self.callback is not None:
            r = ProgressReport(self.sizeTotal, self.sizeLeft, self.itemTotal, self.toSyncQ.qsize())
            r.status = "FINISHED"
            r.num_error = report.errors
            r.num_success = report.successes
            r.num_download = report.downloads
            self.callback(r)
        return report

class WorkerThread(Thread):

    def __init__(self, pFetch, fetcher):
        """
        pFetch - reference to ParallelFetch instance
        fetcher - reference to a class instantiating BaseFetch
        """
        Thread.__init__(self)
        self.pFetch = pFetch
        self.fetcher = fetcher
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def run(self):
        LOG.debug("Run has started")
        while not self._stop.isSet():
            try:
                itemInfo = self.pFetch.getWorkItem()
            except Queue.Empty:
                LOG.debug("Queue is empty, thread will end")
                break
            if itemInfo is None:
                break
            try:
                status = self.fetcher.fetchItem(itemInfo)
            except Exception, e:
                LOG.error("%s" % (traceback.format_exc()))
                LOG.error(e)
                self.pFetch.markStatus(itemInfo, BaseFetch.STATUS_ERROR)
                break
            self.pFetch.markStatus(itemInfo, status)
        LOG.debug("Thread ending")

if __name__ == "__main__":
    import GrinderLog
    GrinderLog.setup(True)
    # This a very basic test just to feel out the flow of the threads 
    # pulling items from a shared Queue and exiting cleanly
    # Create a simple fetcher that sleeps every few items
    class SimpleFetcher(object):
        def fetchItem(self, x):
            print "Working on item %s" % (x)
            if x % 3 == 0:
                print "Sleeping 1 second"
                time.sleep(1)
            return BaseFetch.STATUS_NOOP

    pf = ParallelFetch(SimpleFetcher(), 3)
    numPkgs = 20
    pkgs = range(0, numPkgs)
    pf.addItemList(pkgs)
    pf.start()
    report = pf.waitForFinish()
    print "Success: ", report.successes
    print "Error: ", report.errors
    assert(report.successes == numPkgs)

