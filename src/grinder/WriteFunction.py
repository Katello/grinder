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
import pycurl
import os
import logging

LOG = logging.getLogger("grinder.WriteFunction")

class WriteFunction(object):
    """ utility callback to acumulate response"""
    def __init__(self, path, size=None):
        """
        @param path: path to where the file is written to disk.
        @type path: str
        @param size: file size if available to compare.
        @type size: int
        """
        self.wfile = path
        self.size = size
        self.fp = None
        self.offset = 0
        self.chunk_read = 0
        self.setup()

    def setup(self):
        if os.path.exists(self.wfile):
            self.offset = self.get_offset()
            LOG.debug("File exists; offset at %s" % self.offset)
        self.fp = open(self.wfile, 'a+')
        self.chunk_read = self.offset
        
    def callback(self, chunk):
        """
        @param chunk: data chunk buffer to write or append to a file object
        @type chunk: str
        """
        #LOG.debug("processing chunk %s" % len(chunk))
        self.chunk_read += len(chunk)
        if self.size and self.size == self.offset:
            # "File already exists with right size
            #  include checksum compare
            return
        if self.offset <= self.chunk_read:
            self.fp.seek(self.offset)
        self.fp.write(chunk)
        #LOG.debug("Total chunk size read %s" % self.chunk_read)

    def get_offset(self):
        self.offset = os.stat(self.wfile).st_size
        return self.offset

    def cleanup(self):
        self.fp.close()
        self.offset = 0
        self.chunk_read = 0
