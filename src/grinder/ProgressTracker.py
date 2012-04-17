#
# Copyright (c) 2011 Red Hat, Inc.
#
# Module to fetch content from yum repos
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

from grinder.GrinderCallback import ProgressReport

LOG = logging.getLogger(__name__)

class ProgressTracker(object):
    """
    Responsible for tracking progress information for all download objects
    Tracks total size versus amount downloaded
    includes information on item types, rpm, delta_rpm, tree_file
    """

    def __init__(self, callback=None):
        self.lock = threading.RLock()
        self.items = {}
        self.total_size_bytes = 0
        self.remaining_bytes = self.total_size_bytes
        self.total_num_items = 0
        self.remaining_num_items = 0
        self.type_info = {}
        self.callback = None

    def get_progress(self):
        self.lock.acquire()
        try:
            progress = {}
            progress["total_size_bytes"] = self.total_size_bytes
            progress["remaining_bytes"] = self.remaining_bytes
            progress["total_num_items"] = self.total_num_items
            progress["remaining_num_items"] = self.remaining_num_items
            progress["type_info"] = self.type_info.copy()
            return progress
        finally:
            self.lock.release()

    def add_item(self, fetchURL, size, item_type):
        """
        @param fetchURL: unique URL identifying where to fetch this item
        @type fetchURL: str

        @param size: expected size, 0 if unknown
        @type size: int

        @param item_type: type of item being fetch, example could be "rpm"
        @type item_type: str
        """
        try:
            size = int(size)
        except ValueError:
            LOG.error("Error in add_item casting '%s' to an integer on %s  <%s>" % (size, item_type, fetchURL))
            size = 0

        self.lock.acquire()
        try:
            if size < 0:
                LOG.error("%s size = %s, setting to 0 instead" % (fetchURL, size))
                size = 0
            # Total Information for all Items
            self.total_size_bytes += size
            self.remaining_bytes = self.total_size_bytes

            self.total_num_items += 1
            self.remaining_num_items += 1

            # Account for Group info based on type of items, (rpm, drpm, etc)
            # Using older term of 'size_left', 'total_count', 'items_left' to avoid breaking
            # expectations Katello has.
            if not self.type_info.has_key(item_type):
                self.type_info[item_type] = {}
                self.type_info[item_type]["total_size_bytes"] = 0
                self.type_info[item_type]["size_left"] = 0
                self.type_info[item_type]["total_count"] = 0
                self.type_info[item_type]["items_left"] = 0
                self.type_info[item_type]["num_success"] = 0
                self.type_info[item_type]["num_error"] = 0
            self.type_info[item_type]["total_size_bytes"] += size
            self.type_info[item_type]["size_left"] += size
            self.type_info[item_type]["total_count"] += 1
            self.type_info[item_type]["items_left"] += 1

            # Account for Individual Item Information
            # Note: If fetchURL already existed it is overwritten
            self.items[fetchURL] = {}
            self.items[fetchURL]["total_size_bytes"] = size
            self.items[fetchURL]["remaining_bytes"] = size
            self.items[fetchURL]["item_type"] = item_type
        finally:
            self.lock.release()

    def modify_item_size(self, fetchURL, size):
        """
        @param fetchURL: unique URL identifying where to fetch this item
        @type fetchURL: str

        @param size: expected size
        @type size: int
        """
        # This method would not be called on typical operations
        # Typically we would know the size of a file ahead of time when we read metadata
        # sometimes this metadata may be inaccurate, for those situations this method will
        # be invoked when we are told from pycurl that the size of the file is different
        # than what we expected, we will update our progress counters to reflect the
        # new information.
        try:
            size = int(size)
        except ValueError:
            LOG.error("Error in modify_item_size when casting '%s' to an integer on <%s>" % (size, fetchURL))
            size = 0

        self.lock.acquire()
        try:
            if size < 0:
                LOG.error("size is negative, fetchURL=<%s> size=<%s>" % (fetchURL, size))
                return
            if not self.items.has_key(fetchURL):
                LOG.error("Unable to find <%s>" % (fetchURL))
                return
            item = self.items[fetchURL]
            # diff_in_size could be negative or positive, both are valid
            diff_in_size = size - item["total_size_bytes"]
            LOG.warning("Modifying size information for <%s>. original size = <%s>, new size = <%s>, diff_in_size = <%s>" % (fetchURL, item["total_size_bytes"], size, diff_in_size))
            item_type = item["item_type"]
            if self.type_info.has_key(item_type):
                LOG.warning("%s changing tracker for %s total_size_bytes from %s to %s" % (fetchURL, item_type,
                    self.type_info[item_type]["total_size_bytes"], self.type_info[item_type]["total_size_bytes"] + diff_in_size))
                self.type_info[item_type]["total_size_bytes"] += diff_in_size
                self.type_info[item_type]["size_left"] += diff_in_size
            self.items[fetchURL]["total_size_bytes"] = size
            self.items[fetchURL]["remaining_bytes"] += diff_in_size

            # Total Information for all Items
            self.total_size_bytes += diff_in_size
            self.remaining_bytes += diff_in_size
        finally:
            self.lock.release()

    def item_complete(self, fetchURL, status):
        """
        @param fetchURL: url of an item
        @type fetchURL: str

        @param status: True means successful transfer, False means an error occurred
        @type status: boolean
        """
        # This may be called to mark an item errored out and is complete
        # it's progress from a tracking perspective should be fully realized
        # 
        # Typically the curl PROGRESSFUNCTION callback will be called to adjust the
        # remaining_bytes to 0 for successful downloads.
        #
        # Also note, some items will be downloaded where we don't know the size
        # for those cases we will not adjust the progress since we don't know what we are tracking.
        self.lock.acquire()
        try:
            if not self.items.has_key(fetchURL):
                return
            item_type = self.items[fetchURL]["item_type"]
            prev_remaining_bytes = self.items[fetchURL]["remaining_bytes"]
            if prev_remaining_bytes > 0:
                # Adjust for incomplete transfers which are now finished
                # Remember to remove their remaining bytes from our counters
                self.remaining_bytes -= prev_remaining_bytes
                self.type_info[item_type]["size_left"] -= prev_remaining_bytes

            self.type_info[item_type]["items_left"] -= 1
            if status:
                self.type_info[item_type]["num_success"] += 1
            else:
                self.type_info[item_type]["num_error"] += 1

            del self.items[fetchURL]
            self.remaining_num_items -= 1
        finally:
            self.lock.release()

    def update_progress_download(self, fetchURL, download_total, downloaded):
        """
        @param fetchURL url of the item, must be unique against all known items being downloaded
        @type fetchURL: str

        @param download_total expected total number of bytes for this item
        @type download_total: int

        @param downloaded number of bytes downloaded up till now for this item
        @type downloaded: int
        """
        try:
            download_total = int(download_total)
        except ValueError:
            LOG.error("Error in update_progress_download when casting '%s' to an integer on <%s>" % (download_total, fetchURL))
            download_total = 0
        try:
            downloaded = int(downloaded)
        except ValueError:
            LOG.error("Error in update_progress_download when casting '%s' to an integer on <%s>" % (downloaded, fetchURL))
            downloaded = 0

        # This method is used to note progress made while a specific item is being downloaded
        # Example, it can note that 100kb of a file has been downloaded of a 1GB file
        self.lock.acquire()
        try:
            if not self.items.has_key(fetchURL):
                return
            # Note curl will invoke this method initially with download_total=0 and downloaded=0, ignore that invokation
            if download_total == 0 or downloaded == 0:
                return
            if download_total != self.items[fetchURL]["total_size_bytes"]:
                self.modify_item_size(fetchURL, download_total)
            prev_remaining_bytes = self.items[fetchURL]["remaining_bytes"]
            remaining_bytes = download_total - downloaded
            delta_bytes = prev_remaining_bytes - remaining_bytes
            if delta_bytes < 0:
                LOG.error("Negative delta_bytes <%s>. download_total=<%s>, downloaded=<%s>, prev_remaining_bytes=<%s>, remaining_bytes=<%s>, total_size_bytes=<%s>, %s" % \
                        (delta_bytes, download_total, downloaded, prev_remaining_bytes, remaining_bytes, self.items[fetchURL]["total_size_bytes"], fetchURL))
                return
            else:
                self.items[fetchURL]["remaining_bytes"] = remaining_bytes
                # Adjust cumulative remaining bytes for all items
                self.remaining_bytes -= delta_bytes
                # Adjust remaining bytes for all of this type of item
                item_type = self.items[fetchURL]["item_type"]
                self.type_info[item_type]["size_left"] -= delta_bytes
        finally:
            self.lock.release()

        if self.callback:
            progress = self.get_progress()
            self.callback(progress)


    def reset_progress(self, fetchURL):
        """
        @param fetchURL url of the item, must be unique against all known items being downloaded
        @type fetchURL: str
        """
        self.lock.acquire()
        try:
            if not self.items.has_key(fetchURL):
                return
            item = self.items[fetchURL]
            item_type = item["item_type"]
            LOG.warning("Resetting progress for <%s>" % (fetchURL))
            # Reset total byte counter
            # Remove prev remaining, then add in total size
            self.remaining_bytes -= item["remaining_bytes"]
            self.remaining_bytes += item["total_size_bytes"]
            # Reset bytes counter for item_type, example 'rpms', 'tree_files', etc
            if self.type_info.has_key(item_type):
                self.type_info[item_type]["size_left"] -= item["remaining_bytes"]
                self.type_info[item_type]["size_left"] += item["total_size_bytes"]
            # Reset bytes counter for actual item
            self.items[fetchURL]["remaining_bytes"] = item["total_size_bytes"]
        finally:
            self.lock.release()

        if self.callback:
            progress = self.get_progress()
            self.callback(progress)