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

import logging
from logging import Formatter
from logging.handlers import RotatingFileHandler


class ProgressReport(object):
    DownloadMetadata = "Downloading Metadata"
    DownloadItems = "Downloading Items"
    PurgeOrphanedPackages = "Purging Orphaned Packages"
    RemoveOldPackages = "Removing Old Packages"

    
    def __init__(self, sizeTotal, sizeLeft, itemTotal, itemLeft, itemName="", status=""):
        self.items_total = itemTotal    # Total number of items
        self.items_left = itemLeft      # Number of items left to process
        self.size_total = sizeTotal     # Total number of bytes 
        self.size_left = sizeLeft       # Bytes left to process
        self.item_name = itemName       # Name of last item worked on
        self.status = status            # Status Message
        self.num_error = 0              # Number of Errors
        self.num_success = 0            # Number of Successes
        self.num_download = 0          # Number of actual downloads
        self.details = {}               # Details about specific file types
        self.step = None

    def __str__(self):
        s = ""
        if not self.item_name:
            s += "Item: %s, Status: %s" % (self.item_name, self.status)
        s += "%s/%s items remaining, %s/%s size remaining" % (self.items_left,
                self.items_total, self.size_left, self.size_total)
        return s


