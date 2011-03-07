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
import sys
import Queue

from BaseFetch import BaseFetch
from GrinderCallback import ProgressReport

LOG = logging.getLogger("grinder.ParallelFetch")

class SyncReport:
    def __init__(self):
        self.successes = 0
        self.downloads = 0
        self.errors = 0
        self.last_progress = None
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
        self.details = {}
        self.error_details = []
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
        self.step = None
        self.stopping = False
        for i in range(self.numThreads):
            wt = WorkerThread(self, fetcher)
            self.threads.append(wt)

    def _update_totals(self, item):
        if item.has_key("item_type"):
            item_type = item["item_type"]
            if not self.details.has_key(item_type):
                self.details[item_type] = {}
            # How many items of this type
            if not self.details[item_type].has_key("total_count") or \
                not self.details[item_type]["total_count"]:
                self.details[item_type]["total_count"] = 1
            else:
                self.details[item_type]["total_count"] += 1
            self.details[item_type]["items_left"] = self.details[item_type]["total_count"]
            # Note for some items we may not know the item size, example tree_files
            # in that case we will skip updating the size related fields
            if item["size"] or item["size"] <= 0:
                # Total size in bytes of this type
                if not self.details[item_type].has_key("total_size_bytes") or \
                    not self.details[item_type]["total_size_bytes"]:
                    self.details[item_type]["total_size_bytes"] = item["size"]
                else:
                    self.details[item_type]["total_size_bytes"] += item["size"]
                # How many bytes are left to fetch of this type
                if not self.details[item_type].has_key("size_left"):
                    self.details[item_type]["size_left"] = self.details[item_type]["total_size_bytes"]
                else:
                    self.details[item_type]["size_left"] += item['size']
            else:
                if not self.details[item_type].has_key("total_size_bytes") or \
                    not self.details[item_type]["total_size_bytes"]:
                    self.details[item_type]["total_size_bytes"] = 0
                if not self.details[item_type].has_key("size_left") or \
                    not self.details[item_type]["size_left"]:
                    self.details[item_type]["size_left"] = 0
            # Initialize 'num_success'
            if not self.details[item_type].has_key("num_success"):
                self.details[item_type]["num_success"] = 0
            if not self.details[item_type].has_key("num_error"):
                self.details[item_type]["num_error"] = 0
            
    def addItem(self, item):
        if item.has_key("size") and item['size'] is not None:
            self.sizeTotal = self.sizeTotal + int(item['size'])
        self._update_totals(item)
        self.toSyncQ.put(item)

    def addItemList(self, items):
        for p in items:
            self.addItem(p)

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

    def processCallback(self, step, itemInfo=None):
        if not self.callback:
            return
        r = self.formProgressReport(step, itemInfo)
        self.callback(r)

    def addErrorDetails(self, itemInfo, errorInfo=None):
        self.error_details.append((itemInfo, errorInfo))

    def formProgressReport(self, step=None, itemInfo=None, status=None):
        itemsLeft = self.itemTotal - (self.syncErrorQ.qsize() + self.syncCompleteQ.qsize())
        r = ProgressReport(self.sizeTotal, self.sizeLeft, self.itemTotal, itemsLeft)
        r.item_name = None
        if itemInfo:
            if itemInfo.has_key("fileName"):
                r.item_name = itemInfo["fileName"]
        r.status = None
        if status:
            r.status = status
        r.num_error = self.syncErrorQ.qsize()
        r.num_success = self.syncCompleteQ.qsize()
        r.sync_status = self.syncStatusDict
        r.details = self.details
        r.error_details = self.error_details
        if step:
            self.step = step
        r.step = self.step
        return r

    def markStatus(self, itemInfo, status, errorInfo=None):
        LOG.info("%s threads are active" % (self._running()))
        self.statusLock.acquire()
        try:
            if status in self.syncStatusDict:
                self.syncStatusDict[status] = self.syncStatusDict[status] + 1
            else:
                self.syncStatusDict[status] = 1
            if status not in (BaseFetch.STATUS_ERROR, BaseFetch.STATUS_UNAUTHORIZED):
                self.syncCompleteQ.put(itemInfo)
            else:
                self.syncErrorQ.put(itemInfo)
                self.addErrorDetails(itemInfo, {"error_type":status, "error":errorInfo})
            LOG.debug("%s status updated, %s success %s error" % (itemInfo,
                self.syncCompleteQ.qsize(), self.syncErrorQ.qsize()))
            if itemInfo.has_key("size")  and itemInfo['size'] is not None and itemInfo['size'] >= 0:
                self.sizeLeft = self.sizeLeft - int(itemInfo['size'])
                if itemInfo.has_key("item_type"):
                    item_type = itemInfo["item_type"]
                    if not self.details[item_type].has_key("size_left"):
                        self.details[item_type]["size_left"] = self.details[item_type]["total_size_bytes"] - int(itemInfo['size'])
                    else:
                        self.details[item_type]["size_left"] -= int(itemInfo['size'])
            if itemInfo.has_key("item_type"):
                item_type = itemInfo["item_type"]
                if not self.details[item_type].has_key("items_left"):
                    self.details[item_type]["items_left"] = self.details[item_type]["total_count"] - 1
                else:
                    self.details[item_type]["items_left"] -= 1
                if status != BaseFetch.STATUS_ERROR:
                    # Mark success for item
                    if not self.details[item_type].has_key("num_success"):
                        self.details[item_type]["num_success"] = 1
                    else:
                        self.details[item_type]["num_success"] += 1
                else:
                    # Mark failure
                    if not self.details[item_type].has_key("num_error"):
                        self.details[item_type]["num_error"] = 1
                    else:
                        self.details[item_type]["num_error"] += 1
            if self.callback is not None:
                r = self.formProgressReport(ProgressReport.DownloadItems, itemInfo, status)
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
            r.details = self.details
            self.callback(r)
        for t in self.threads:
            t.start()

    def stop(self):
        self.stopping = True
        for t in self.threads:
            t.stop()

    def _running(self):
        working = 0
        for t in self.threads:
            if (t.isAlive()):
                working += 1
        return working

    def _waitForThreads(self):
        num_alive_threads = self._running()
        counter = 0
        while (num_alive_threads):
            counter += 1
            if self.stopping and counter % 10 == 0:
                LOG.info("Waiting for threads to finish, %s still active" % (num_alive_threads))
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
        r = self.formProgressReport()
        r.status = "FINISHED"
        r.num_error = report.errors
        r.num_success = report.successes
        r.num_download = report.downloads
        r.items_left = 0 
        r.details = self.details
        if self.callback is not None:
            self.callback(r)
        report.last_progress = r
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
                status,msg = self.fetcher.fetchItem(itemInfo)
                self.pFetch.markStatus(itemInfo, status, msg)
            except Exception, e:
                LOG.error("%s" % (traceback.format_exc()))
                LOG.error(e)
                errorInfo = {}
                exctype, value = sys.exc_info()[:2]
                errorInfo["error_type"] = str(exctype)
                errorInfo["error"] = str(value)
                errorInfo["traceback"] = traceback.format_exc().splitlines()
                self.pFetch.markStatus(itemInfo, BaseFetch.STATUS_ERROR, errorInfo)
                LOG.debug("Thread ending")
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

