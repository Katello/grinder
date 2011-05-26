#!/usr/bin/env python
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
import os
import httplib
import urlparse
import tempfile
import time
import pycurl
import logging
import traceback
import hashlib
import types
import unicodedata

from GrinderExceptions import GrinderException
import GrinderUtils
LOG = logging.getLogger("grinder.BaseFetch")



class BaseFetch(object):
    STATUS_NOOP = 'noop'
    STATUS_DOWNLOADED = 'downloaded'
    STATUS_SIZE_MISSMATCH = 'size_missmatch'
    STATUS_MD5_MISSMATCH = 'md5_missmatch'
    STATUS_ERROR = 'error'
    STATUS_UNAUTHORIZED = "unauthorized"
    STATUS_SKIP_VALIDATE = "skip_validate"

    RPM = 'rpm'
    DELTA_RPM = 'delta_rpm'
    TREE_FILE = 'tree_file'
    FILE      = 'file'

    def __init__(self, cacert=None, clicert=None, clikey=None, 
            proxy_url=None, proxy_port=None, proxy_user=None, 
            proxy_pass=None, sslverify=1, max_speed = None):
        self.sslcacert = cacert
        self.sslclientcert = clicert
        self.sslclientkey = clikey
        self.proxy_url = proxy_url
        self.proxy_port = proxy_port
        self.proxy_user = proxy_user
        self.proxy_pass = proxy_pass
        self.sslverify  = sslverify
        self.max_speed = max_speed

    def validateDownload(self, filePath, size, hashtype, checksum, verbose=False):
        statinfo = os.stat(filePath)
        fileName = os.path.basename(filePath)
        calchecksum = getFileChecksum(hashtype, filename=filePath)
        # validate fetched data
        if statinfo.st_size != int(size) and int(size) > 0:
            LOG.error("%s size mismatch, read: %s bytes, was expecting %s bytes" \
                      % (fileName, statinfo.st_size, size))
            os.remove(filePath)
            return BaseFetch.STATUS_SIZE_MISSMATCH
        elif calchecksum != checksum:
            LOG.error("%s md5sum mismatch, read md5sum of: %s expected md5sum of %s" \
                      %(fileName, calchecksum, checksum))
            os.remove(filePath)
            return BaseFetch.STATUS_MD5_MISSMATCH
        LOG.debug("Package [%s] is valid with checksum [%s] and size [%s]" % (fileName, checksum, size))
        return BaseFetch.STATUS_DOWNLOADED
    
    def makeDirSafe(self, path):
        try:
            os.makedirs(path)
        except OSError, e:
            # Another thread may have created the dir since we checked,
            # if that's the case we'll see errno=17, so ignore that exception
            if e.errno != 17:
                tb_info = traceback.format_exc()
                LOG.debug("%s" % (tb_info))
                LOG.critical(e)
                raise e

    def makeTempDir(self):
        return tempfile.mkdtemp()
    
    def fetch(self, fileName, fetchURL, savePath, itemSize=None, hashtype=None, checksum=None, 
             headers=None, retryTimes=2, packages_location=None):
        """
        Input:
            itemInfo = dict with keys: 'file_name', 'fetch_url', 'item_size', 'hashtype', 'checksum'
            retryTimes = how many times to retry fetch if an error occurs
            max_speed = Optional param, limit download bandwidth in KB/sec 

        Will return a true/false if item was fetched successfully 
        """
        if packages_location is not None:
            # this option is to store packages in a central location
            # and symlink pkgs to individual repo directories
            filePath = os.path.join(packages_location, fileName)
            repofilepath = os.path.join(savePath, fileName)
            basedir = os.path.dirname(repofilepath)
            if basedir and not os.path.exists(basedir):
                self.makeDirSafe(basedir)
        else:
            repofilepath = None
            filePath = os.path.join(savePath, fileName)
        tempDirPath = os.path.dirname(filePath)
        if not os.path.isdir(tempDirPath):
            LOG.info("Creating directory: %s" % tempDirPath)
            self.makeDirSafe(tempDirPath)

        if os.path.exists(filePath) and \
            verifyChecksum(filePath, hashtype, checksum):
            LOG.info("%s exists with correct size and md5sum, no need to fetch." % (filePath))
            if repofilepath is not None and not os.path.exists(repofilepath):
                relFilePath = GrinderUtils.get_relative_path(filePath, repofilepath)
                LOG.info("Symlink missing in repo directory. Creating link %s to %s" % (repofilepath, relFilePath))
                if not os.path.islink(repofilepath):
                    os.symlink(relFilePath, repofilepath)
            return (BaseFetch.STATUS_NOOP,None)

        try:
            f = open(filePath, "wb")
            curl = pycurl.Curl()
            #def item_progress_callback(download_total, downloaded, upload_total, uploaded):
            #    LOG.debug("%s status %s/%s bytes" % (fileName, downloaded, download_total))
            #curl.setopt(pycurl.PROGRESSFUNCTION, item_progress_callback)
            if self.max_speed:
                #Convert KB/sec to Bytes/sec for MAC_RECV_SPEED_LARGE
                limit = self.max_speed*1024
                curl.setopt(curl.MAX_RECV_SPEED_LARGE, limit)
            curl.setopt(curl.VERBOSE,0)
            if type(fetchURL) == types.UnicodeType:
                #pycurl does not accept unicode strings for a URL, so we need to convert
                fetchURL = unicodedata.normalize('NFKD', fetchURL).encode('ascii','ignore')
            curl.setopt(curl.URL, fetchURL)
            if self.sslcacert and self.sslclientcert and self.sslclientkey:
                curl.setopt(curl.CAINFO, self.sslcacert)
                curl.setopt(curl.SSLCERT, self.sslclientcert)
                curl.setopt(curl.SSLKEY, self.sslclientkey)
            if not self.sslverify:
                curl.setopt(curl.SSL_VERIFYPEER, 0)
            if headers:
                curl.setopt(pycurl.HTTPHEADER, curlifyHeaders(headers))
            if self.proxy_url:
                if not self.proxy_port:
                    raise GrinderException("Proxy url defined, but no port specified")
                curl.setopt(pycurl.PROXY, self.proxy_url)
                curl.setopt(pycurl.PROXYPORT, int(self.proxy_port))
                curl.setopt(pycurl.PROXYTYPE, pycurl.PROXYTYPE_HTTP)
                if self.proxy_user:
                    if not self.proxy_pass:
                        raise GrinderException("Proxy username is defined, but no password was specified")
                    curl.setopt(pycurl.PROXYAUTH, pycurl.HTTPAUTH_BASIC)
                    curl.setopt(pycurl.PROXYUSERPWD, "%s:%s" % (self.proxy_user, self.proxy_pass))
            curl.setopt(curl.WRITEFUNCTION, f.write)
            curl.setopt(curl.FOLLOWLOCATION, 1)
            LOG.info("Fetching %s bytes: %s from %s" % (itemSize or "Unknown", fileName, fetchURL))
            curl.perform()
            status = curl.getinfo(curl.HTTP_CODE)
            curl.close()
            f.close()
            # validate the fetched bits
            if itemSize is not None and hashtype is not None and checksum is not None:
                vstatus = self.validateDownload(filePath, int(itemSize), hashtype, checksum)
            else:
                vstatus = BaseFetch.STATUS_SKIP_VALIDATE
            if status == 401:
                LOG.error("Unauthorized request from: %s" % (fetchURL))
                cleanup(filePath)
                return (BaseFetch.STATUS_UNAUTHORIZED, "HTTP status code of %s received for %s" % (status, fetchURL))
            if status != 200:
                if retryTimes > 0:
                    retryTimes -= 1
                    LOG.warn("Retrying fetch of: %s with %s retry attempts left." % (fileName, retryTimes))
                    return self.fetch(fileName, fetchURL, savePath, itemSize, hashtype, 
                                      checksum , headers, retryTimes, packages_location)
                cleanup(filePath)
                LOG.warn("ERROR: Response = %s fetching %s." % (status, fetchURL))
                return (BaseFetch.STATUS_ERROR, "HTTP status code of %s received for %s" % (status, fetchURL))
            if vstatus in [BaseFetch.STATUS_ERROR, BaseFetch.STATUS_SIZE_MISSMATCH, 
                BaseFetch.STATUS_MD5_MISSMATCH] and retryTimes > 0:
                #
                # Incase of a network glitch or issue with RHN, retry the rpm fetch
                #
                retryTimes -= 1
                LOG.error("Retrying fetch of: %s with %s retry attempts left." % (fileName, retryTimes))
                cleanup(filePath)
                return self.fetch(fileName, fetchURL, savePath, itemSize, hashtype, 
                                  checksum, headers, retryTimes, packages_location)
            if packages_location and os.path.exists(filePath):
                relFilePath = GrinderUtils.get_relative_path(filePath, repofilepath)
                LOG.info("Create a link in repo directory for the package at %s to %s" % (repofilepath, relFilePath))
                if os.path.islink(repofilepath):
                    os.unlink(repofilepath)
                os.symlink(relFilePath, repofilepath)
            LOG.debug("Successfully Fetched Package - [%s]" % filePath)
            return (vstatus, None)
        except Exception, e:
            tb_info = traceback.format_exc()
            LOG.debug("%s" % (tb_info))
            LOG.error("Caught exception<%s> in fetch(%s, %s)" % (e, fileName, fetchURL))
            if retryTimes > 0:
                retryTimes -= 1
                LOG.error("Retrying fetch of: %s with %s retry attempts left." % (fileName, retryTimes))
                return self.fetch(fileName, fetchURL, savePath, itemSize, hashtype, 
                                  checksum, headers, retryTimes, packages_location)
            cleanup(filePath)
            raise

def cleanup(filepath):
    if os.path.exists(filepath):
        os.unlink(filepath)

def getFileChecksum(hashtype, filename=None, fd=None, file=None, buffer_size=None):
    """ Compute a file's checksum
    """
    if hashtype in ['sha', 'SHA']:
        hashtype = 'sha1'

    if buffer_size is None:
        buffer_size = 65536

    if filename is None and fd is None and file is None:
        raise ValueError("no file specified")
    if file:
        f = file
    elif fd is not None:
        f = os.fdopen(os.dup(fd), "r")
    else:
        f = open(filename, "r")
    # Rewind it
    f.seek(0, 0)
    m = hashlib.new(hashtype)
    while 1:
        buffer = f.read(buffer_size)
        if not buffer:
            break
        m.update(buffer)

    # cleanup time
    if file is not None:
        file.seek(0, 0)
    else:
        f.close()
    return m.hexdigest()

def verifyChecksum(filePath, hashtype, checksum):
    if hashtype is None or checksum is None:
        #Nothing to perform
        return False
    if getFileChecksum(hashtype, filename=filePath) == checksum:
        return True
    return False

def curlifyHeaders(headers):
    # pycurl drops empty header. Combining headers
    cheaders = ""
    for key,value in headers.items():
        cheaders += key +": "+ str(value) + "\r\n"
    return [cheaders]



if __name__ == "__main__":
    import GrinderLog
    GrinderLog.setup(True)
    systemId = open("/etc/sysconfig/rhn/systemid").read()
    baseURL = "http://satellite.rhn.redhat.com"
    bf = BaseFetch()
    itemInfo = {}
    fileName = "Virtualization-es-ES-5.2-9.noarch.rpm"
    fetchName = "Virtualization-es-ES-5.2-9:.noarch.rpm"
    channelLabel = "rhel-i386-server-vt-5"
    fetchURL = baseURL + "/SAT/$RHN/" + channelLabel + "/getPackage/" + fetchName;
    itemSize = "1731195"
    md5sum = "91b0f20aeeda88ddae4959797003a173" 
    hashtype = "md5"
    savePath = "./test123"
    from RHNComm import RHNComm
    rhnComm = RHNComm(baseURL, systemId)
    authMap = rhnComm.login()
    status = bf.fetch(fileName, fetchURL, itemSize, hashtype, md5sum, savePath, headers=authMap, retryTimes=2)
    print status
    assert(status in [BaseFetch.STATUS_DOWNLOADED, BaseFetch.STATUS_NOOP])
    print "Test Download or NOOP passed"
    status = bf.fetch(fileName, fetchURL, itemSize, hashtype, md5sum, savePath, headers=authMap, retryTimes=2)
    assert(status == BaseFetch.STATUS_NOOP)
    print "Test for NOOP passed"
    authMap['X-RHN-Auth'] = "Bad Value"
    fileName = "Virtualization-en-US-5.2-9.noarch.rpm"
    fetchName = "Virtualization-en-US-5.2-9:.noarch.rpm"
    status = bf.fetch(fileName, fetchURL, itemSize, hashtype, md5sum, savePath, headers=authMap, retryTimes=2)
    print status
    assert(status == BaseFetch.STATUS_UNAUTHORIZED)
    print "Test for unauthorized passed"
    print "Repo Sync Test"
    baseURL = "http://download.fedora.devel.redhat.com/pub/fedora/linux/releases/12/Everything/x86_64/os/"
    bf = BaseFetch(baseURL)
    itemInfo = {}
    
