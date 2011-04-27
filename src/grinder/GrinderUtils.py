#!/usr/bin/python
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
import os
import logging
import sys
import glob
import rpm
import rpmUtils
import rpmUtils.miscutils


LOG = logging.getLogger("grinder.GrinderUtils")

def get_relative_path(source_path, dest_path):
    rel_path = ""
    # Need to account for spare '/' which result in "" being in the array
    src_parts = [x for x in source_path.split("/") if x]
    dst_parts = [x for x in dest_path.split("/") if x]
    similar_index = 0
    for index in range(0,len(dst_parts)):
        if index < len(src_parts):
            if dst_parts[index] != src_parts[index]:
                break
            similar_index += 1
    num_ellipses = len(dst_parts) - 1 - similar_index
    rel_path = os.path.join("../"*num_ellipses, *src_parts[similar_index:])
    return rel_path

class GrinderUtils(object):

    def __init__(self):
        self.numOldPkgsKeep = 2

    def getNEVRA(self, filename):
        fd = os.open(filename, os.O_RDONLY)
        ts = rpm.TransactionSet()
        ts.setVSFlags((rpm._RPMVSF_NOSIGNATURES|rpm._RPMVSF_NODIGESTS))
        h = ts.hdrFromFdno(fd)
        os.close(fd)
        ts.closeDB()
        d = {}
        d["filename"] = filename
        for key in ["name", "epoch", "version", "release", "arch"]:
            d[key] = h[key]
        return d

    def getListOfSyncedRPMs(self, path):
        """
         Returns a dictionary where the key is name.arch
          and the value is a list of dict entries
          [{"name", "version", "release", "epoch", "arch", "filename"}]
        """
        rpms = {}
        rpmFiles = glob.glob(path+"/*.rpm")
        for filename in rpmFiles:
            info = self.getNEVRA(filename)
            key = info["name"] + "." + info["arch"]
            if not rpms.has_key(key):
                rpms[key] = []
            rpms[key].append(info)
        return rpms

    def sortListOfRPMS(self, rpms):
        for key in rpms:
            rpms[key].sort(lambda a, b: 
                    rpmUtils.miscutils.compareEVR(
                        (a["epoch"], a["version"], a["release"]), 
                        (b["epoch"], b["version"], b["release"])), reverse=True)
        return rpms

    def getSortedListOfSyncedRPMs(self, path):
        """
         Returns a dictionary with key of 'name.arch' which has values sorted in descending order
         i.e. latest rpm is the first element on the list
          Values in dictionary are a list of:
          [{"name", "version", "release", "epoch", "arch", "filename"}]
        """
        rpms = self.getListOfSyncedRPMs(path)
        return self.sortListOfRPMS(rpms)

    def runRemoveOldPackages(self, path, numOld=None):
        """
          Will scan 'path'.
          The current RPM and 'numOld' or prior releases are kept, all other RPMs are deleted
        """
        if numOld == None:
            numOld = self.numOldPkgsKeep
        if numOld < 0:
            numOld = 0
        LOG.info("Will keep latest package and %s older packages" % (numOld))
        rpms = self.getSortedListOfSyncedRPMs(path)
        for key in rpms:
            values = rpms[key]
            if len(values) > numOld:
                # Remember to keep the latest package
                for index in range(1+numOld, len(values)):
                    fname = values[index]['filename']
                    LOG.info("Removing: %s" % (fname))
                    os.remove(fname)


