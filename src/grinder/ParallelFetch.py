#
# Copyright (c) 2011 Red Hat, Inc.
#
# This software is licensed to you under the GNU General Public
# License as published by the Free Software Foundation; either version
# 2 of the License (GPLv2) or (at your option) any later version.
# There is NO WARRANTY for this software, express or implied,
# including the implied warranties of MERCHANTABILITY,
# NON-INFRINGEMENT, or FITNESS FOR A PARTICULAR PURPOSE. You should
# have received a copy of GPLv2 along with this software; if not, see
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.
#
import logging
import threading
import time
import traceback
import sys
import Queue
from threading import Thread, Lock
from grinder.BaseFetch import BaseFetch
from grinder.GrinderCallback import ProgressReport
from grinder.activeobject import ActiveObject

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
        self.tracker = fetcher.tracker
        self.tracker.callback = self.incremental_progress_update
        self.numThreads = numThreads
        self.callback = callback
        self.error_details = []
        self.statusLock = Lock()
        self.itemTotal = 0
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
        self.startTime = time.time()

    def addItem(self, item, requeue=False):
        self.toSyncQ.put(item)
        if not requeue:
            if item.has_key("item_type") and item.has_key("downloadurl") and item.has_key("size"):
                self.tracker.add_item(item["downloadurl"], item['size'], item["item_type"])

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

    def incremental_progress_update(self, progress):
        """
        @param progress: from ProgressTracker.get_progress, example:
           {"total_size_bytes":"", "remaining_bytes":"", "total_num_items":"", "remaining_num_items":"", "type_info":{}}
           example of 'type_info' 'type_info':{"rpm":{"total_size_bytes":"", "size_left":"", "total_count":"",
                                                "items_left":"", "num_success":"", "num_error":""}}
        @type progress: dict
        """
        r = self.formProgressReport(step=ProgressReport.DownloadItems, progress=progress)
        if self.callback:
            self.callback(r)

    def processCallback(self, step, itemInfo=None):
        if not self.callback:
            return
        r = self.formProgressReport(step, itemInfo)
        self.callback(r)

    def addErrorDetails(self, itemInfo, errorInfo=None):
        # When we communicate these errors back to pulp
        # we want each error to be a single json object
        # so we will combine the 2 dicts
        for key in errorInfo:
            itemInfo[key] = errorInfo[key]
        self.error_details.append(itemInfo)

    def formProgressReport(self, step=None, itemInfo=None, status=None, progress=None):
        if not progress:
            progress = self.tracker.get_progress()

        itemsLeft = self.itemTotal - (self.syncErrorQ.qsize() + self.syncCompleteQ.qsize())
        r = ProgressReport(progress["total_size_bytes"], progress["remaining_bytes"], self.itemTotal, itemsLeft)
        r.item_name = None
        if itemInfo:
            if itemInfo.has_key("fileName"):
                r.item_name = itemInfo["fileName"]
            if itemInfo.has_key("item_type"):
                r.item_type = itemInfo["item_type"]
        r.status = None
        if status:
            r.status = status
        r.num_error = self.syncErrorQ.qsize()
        r.num_success = self.syncCompleteQ.qsize()
        r.sync_status = self.syncStatusDict
        r.details = progress["type_info"]
        r.error_details = self.error_details
        if step:
            self.step = step
        r.step = self.step
        return r

    def markStatus(self, itemInfo, status, errorInfo=None):
        LOG.info("%s threads are active. %s items left to be fetched" % (self._running(), (self.toSyncQ.qsize() + self._running())))
        self.statusLock.acquire()
        try:
            if status == BaseFetch.STATUS_REQUEUE:
                LOG.info("Requeueing: %s" % (itemInfo))
                self.addItem(itemInfo, requeue=True)
                return
            if status in self.syncStatusDict:
                self.syncStatusDict[status] = self.syncStatusDict[status] + 1
            else:
                self.syncStatusDict[status] = 1
            if status not in (BaseFetch.STATUS_ERROR, BaseFetch.STATUS_UNAUTHORIZED,
                BaseFetch.STATUS_SIZE_MISSMATCH, BaseFetch.STATUS_MD5_MISSMATCH):
                # Handle Success
                self.syncCompleteQ.put(itemInfo)
            else:
                # Handle Errors
                if not errorInfo and itemInfo.has_key("downloadurl"):
                    msg = "%s on %s" % (status, itemInfo["downloadurl"])
                    # Keeping "error" for backwards compatibility with Pulp V1
                    self.addErrorDetails(itemInfo, {"error_type":status, "value":msg, "error":msg,
                                                    "exception": "", "traceback": ""})
                elif isinstance(errorInfo, dict):
                    self.addErrorDetails(itemInfo, errorInfo)
                else:
                    # Handle case when errorInfo is a string such as:
                    #  'HTTP status code of 403 received for http://blah.../foo.rpm'
                    self.addErrorDetails(itemInfo, {"error_type":status, "value":errorInfo, "error":errorInfo,
                                                    "exception": "", "traceback": ""})
                self.syncErrorQ.put(itemInfo)
            LOG.debug("%s status updated, %s success %s error" % (itemInfo,
                self.syncCompleteQ.qsize(), self.syncErrorQ.qsize()))
            if itemInfo.has_key("downloadurl"):
                fetchURL = itemInfo["downloadurl"]
                success = True
                if status in (BaseFetch.STATUS_ERROR, BaseFetch.STATUS_UNAUTHORIZED,
                              BaseFetch.STATUS_SIZE_MISSMATCH, BaseFetch.STATUS_MD5_MISSMATCH):
                    success = False
                LOG.debug("Calling item_complete(%s,%s) status = %s" % (fetchURL, success, status))
                self.tracker.item_complete(fetchURL, success)
            else:
                LOG.info("Skipping item_complete for %s" % (itemInfo))
            if self.callback is not None:
                r = self.formProgressReport(ProgressReport.DownloadItems, itemInfo, status)
                self.callback(r)
        finally:
            self.statusLock.release()

    def start(self):
        # Assumption is all adds to toSyncQ have been completed at this point
        # We will grab the size of the items for total number of items to sync
        # before we kick off the threads to start fetching
        progress = self.tracker.get_progress()
        self.itemTotal = self.toSyncQ.qsize()
        LOG.info("%s items are marked to be fetched" % (self.itemTotal))
        if self.callback is not None:
            r = ProgressReport(progress["total_size_bytes"], progress["remaining_bytes"], self.itemTotal, self.toSyncQ.qsize())
            r.step = ProgressReport.DownloadItems
            r.status = "STARTED"
            r.details = progress["type_info"]
            self.callback(r)
        for t in self.threads:
            t.start()

    def stop(self):
        LOG.info("Grinder stopping")
        self.stopping = True
        for t in self.threads:
            t.stop()
            LOG.info("Told thread <%s> to stop" % (t))

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
        self.endTime = time.time()
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
        progress = self.tracker.get_progress()
        for item_type in progress["type_info"]:
            type_info = progress["type_info"][item_type]
            LOG.info("Transferred [%s] bytes of [%s]" %
                     (type_info["total_size_bytes"] - type_info["size_left"], item_type))
        LOG.info("Transferred [%s] total bytes in %s seconds" % (progress["total_size_bytes"] - progress["remaining_bytes"], (self.endTime - self.startTime)))
        r = self.formProgressReport()
        r.status = "FINISHED"
        r.num_error = report.errors
        r.num_success = report.successes
        r.num_download = report.downloads
        r.items_left = 0 
        r.details = progress["type_info"]
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
        self.fetcher = ActiveObject(fetcher, "update_bytes_transferred", "reset_bytes_transferred")
        self._stop = threading.Event()
        self.fetcher_lock = Lock()

    def stop(self):
        self._stop.set()
        LOG.info("stop() invoked")
        self.fetcher_lock.acquire()
        try:
            try:
                if hasattr(self, "fetcher"):
                    self.fetcher.fetchItem.abort()
            except Exception, e:
                LOG.error("%s" % (traceback.format_exc()))
        finally:
            self.fetcher_lock.release()
        LOG.info("stop() completed")

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
                result = self.fetcher.fetchItem(itemInfo)
                if result:
                    status, msg = result
                    self.pFetch.markStatus(itemInfo, status, msg)
                    if status == BaseFetch.STATUS_REQUEUE:
                        time.sleep(2)
            except Exception, e:
                LOG.error("%s" % (traceback.format_exc()))
                LOG.error(e)
                errorInfo = {}
                exctype, value = sys.exc_info()[:2]
                errorInfo["error_type"] = str(exctype)
                errorInfo["value"] = str(value)
                # Keeping "error" for backward compatibility with Pulp v1
                errorInfo["error"] = str(value)
                errorInfo["traceback"] = traceback.format_exc().splitlines()
                errorInfo["exception"] = e
                self.pFetch.markStatus(itemInfo, BaseFetch.STATUS_ERROR, errorInfo)

        LOG.info("WorkerThread deleting ActiveObject")
        self.fetcher_lock.acquire()
        try:
            try:
                #Note: We want to explicitly kill the child activeobject
                # so will use abort() on activeobject's Method class
                self.fetcher.dummy_method.abort()
                # We were seeing the invocation of __del__() on activeobject
                # being delayed, hence child processes weren't dying when they should.
                # therefore we added the explicit abort()
                del self.fetcher
            except Exception, e:
                LOG.error("%s" % (traceback.format_exc()))
        finally:
            self.fetcher_lock.release()
        LOG.info("Thread ending")

if __name__ == "__main__":
    from grinder import GrinderLog
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

