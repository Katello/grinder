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
from logging import Formatter
from logging.handlers import RotatingFileHandler


class ProgressReport(object):
    DownloadMetadata = "Downloading Metadata"
    DownloadItems = "Downloading Items or Verifying"
    PurgeOrphanedPackages = "Purging Orphaned Packages"
    RemoveOldPackages = "Removing Old Packages"

    
    def __init__(self, sizeTotal, sizeLeft, itemTotal, itemLeft, itemName="", status="", itemType=""):
        self.items_total = itemTotal    # Total number of items
        self.items_left = itemLeft      # Number of items left to process
        self.size_total = sizeTotal     # Total number of bytes 
        self.size_left = sizeLeft       # Bytes left to process
        self.item_name = itemName       # Name of last item worked on
        self.status = status            # Status Message
        self.item_type = itemType       # Type of item fetched
        self.num_error = 0              # Number of Errors
        self.num_success = 0            # Number of Successes
        self.num_download = 0          # Number of actual downloads
        self.details = {}               # Details about specific file types
        self.error_details = []         # Details about specific errors that were observed
                                        # List of tuples. Tuple format [0] = item info, [1] = exception details
        self.step = None

    def __str__(self):
        s = "Step: %s, " % (self.step)
        #if not self.item_name:
        s += "Item<%s>: %s, Status: %s, " % (self.item_type, self.item_name, self.status)
        s += "%s/%s items remaining, %s/%s size remaining, " % (self.items_left,
                self.items_total, self.size_left, self.size_total)
        s += "%s num_error, %s num_success, %s num_download, " % (self.num_error, 
                self.num_success, self.num_download)
        s += "details = %s, " % (self.details)
        s += "error_details = %s, " % (self.error_details)
        return s


