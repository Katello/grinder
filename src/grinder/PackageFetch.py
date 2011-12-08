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
from grinder.BaseFetch import BaseFetch
from grinder.RHNComm import RHNComm

LOG = logging.getLogger("grinder.PackageFetch")


class PackageFetch(BaseFetch):
    
    def __init__(self, systemId, baseURL, channelLabel, savePath):
        BaseFetch.__init__(self)
        self.systemId = systemId
        self.baseURL = baseURL
        self.rhnComm = RHNComm(baseURL, self.systemId)
        self.channelLabel = channelLabel
        self.savePath = savePath

    def login(self, refresh=False):
        """
        Returns authentication headers needed for RHN 'GET' requests.
        auth data is cached, if data needs to be updated, pass in refresh=True
        """
        return self.rhnComm.login(refresh)

    def getFetchURL(self, channelLabel, fetchName):
        return self.baseURL + "/SAT/$RHN/" + channelLabel + "/getPackage/" + fetchName;

    def fetchItem(self, itemInfo):
        authMap = self.login()
        fileName = itemInfo['fileName']
        fetchName = itemInfo['fetch_name']
        itemSize = itemInfo['size']
        md5sum = itemInfo['md5sum']
        hashType = itemInfo['hashtype']
        if itemInfo.has_key('pkgpath'):
            pkgPath = itemInfo['pkgpath']
        else:
            pkgPath = None
        fetchURL = self.getFetchURL(self.channelLabel, fetchName)
        status = self.fetch(fileName, fetchURL, self.savePath, itemSize, hashType, md5sum,  headers=authMap)
        if status == BaseFetch.STATUS_UNAUTHORIZED:
            LOG.warn("Unauthorized request from fetch().  Will attempt to update authentication credentials and retry")
            authMap = self.login(refresh=True)
            return self.fetch(fileName, fetchURL, self.savePath, itemSize, hashType, md5sum, headers=authMap, pkgpath=pkgPath)
        return status

if __name__ == "__main__":
    from grinder import GrinderLog
    GrinderLog.setup(True)
    systemId = open("/etc/sysconfig/rhn/systemid").read()
    baseURL = "http://satellite.rhn.redhat.com"
    channelLabel = "rhel-i386-server-vt-5"
    savePath = "./test123"
    pf = PackageFetch(systemId, baseURL, channelLabel, savePath)
    pkg = {}
    pkg['nevra'] = "Virtualization-es-ES-5.2-9.noarch.rpm"
    pkg['fetch_name'] = "Virtualization-es-ES-5.2-9:.noarch.rpm"
    pkg['size'] = "1731195"
    pkg['md5sum'] = "91b0f20aeeda88ddae4959797003a173" 
    pkg['hashtype'] = 'md5'
    pkg['fileName'] = "Virtualization-es-ES-5.2-9.noarch.rpm"
    status = pf.fetchItem(pkg)
    print "Package fetch status is %s" % (status)

