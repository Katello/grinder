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
import time
import pycurl
import logging
import traceback
import hashlib
import types
import unicodedata
from grinder.GrinderExceptions import GrinderException
from grinder.ProgressTracker import ProgressTracker
from grinder import GrinderUtils
from WriteFunction import WriteFunction 
from grinder.GrinderLock import GrinderLock

LOG = logging.getLogger("grinder.BaseFetch")


class BaseFetch(object):
    STATUS_NOOP = 'noop'
    STATUS_DOWNLOADED = 'downloaded'
    STATUS_SIZE_MISSMATCH = 'size_missmatch'
    STATUS_MD5_MISSMATCH = 'md5_missmatch'
    STATUS_ERROR = 'error'
    STATUS_UNAUTHORIZED = "unauthorized"
    STATUS_SKIP_VALIDATE = "skip_validate"
    STATUS_REQUEUE = "requeue"

    RPM = 'rpm'
    DELTA_RPM = 'delta_rpm'
    TREE_FILE = 'tree_file'
    FILE      = 'file'

    def __init__(self, cacert=None, clicert=None, clikey=None, 
            proxy_url=None, proxy_port=None, proxy_user=None, 
            proxy_pass=None, sslverify=1, max_speed = None,
            verify_options = None, tracker = None):
        self.sslcacert = cacert
        self.sslclientcert = clicert
        self.sslclientkey = clikey
        self.proxy_url = proxy_url
        self.proxy_port = proxy_port
        self.proxy_user = proxy_user
        self.proxy_pass = proxy_pass
        self.sslverify  = sslverify
        self.max_speed = max_speed
        self.verify_options = verify_options
        if not tracker:
            tracker = ProgressTracker()
        self.tracker = tracker

    def validateDownload(self, filePath, size, hashtype, checksum):
        """
        @param filePath path to file
        @type filePath str

        @param size expected size of file
        @type size int

        @param hashtype type of checksum of file
        @type hashtype

        @param checksum value of file
        @type checksum
        """
        fileName = os.path.basename(filePath)
        calchecksum = getFileChecksum(hashtype, filename=filePath)
        # validate fetched data
        statinfo = os.stat(filePath)
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
    
    def makeSafeSymlink(self, src_path, dst_path):
        try:
            if os.path.islink(dst_path):
                os.unlink(dst_path)
            os.symlink(src_path, dst_path)
        except OSError, e:
            # Another thread may have created the symlink since we checked,
            # if that's the case we'll see errno=17, so ignore that exception
            if e.errno != 17:
                tb_info = traceback.format_exc()
                LOG.debug("%s" % (tb_info))
                LOG.critical(e)
                raise e

    def reset_bytes_transferred(self, fetchURL):
        # Intended to be invoked on parent, not in ActiveObject Child
        if hasattr(self, "tracker"):
            #LOG.debug("self=<%s>, fetchURL = %s, download_total = %s, downloaded = %s" % (self, fetchURL, download_total, downloaded))
            self.tracker.reset_progress(fetchURL)

    def update_bytes_transferred(self, fetchURL, download_total, downloaded):
        # Intended to be invoked on parent, not in ActiveObject Child
        if hasattr(self, "tracker"):
            #LOG.debug("self=<%s>, fetchURL = %s, download_total = %s, downloaded = %s" % (self, fetchURL, download_total, downloaded))
            self.tracker.update_progress_download(fetchURL, download_total, downloaded)

    def fetch(self, fileName, fetchURL, savePath, itemSize=None, hashtype=None, checksum=None, 
             headers=None, retryTimes=2, packages_location=None, verify_options=None, probing=None, force=False):
        """
        @param fileName file name
        @type fileName str

        @param fetchURL url
        @type fetchURL str

        @param savePath path to save file
        @type savePath str

        @param itemSize expected size of file
        @type itemSize str (will be cast to int)

        @param hashtype type of checksum
        @type hashtype str

        @param checksum value of file
        @type checksum str

        @param headers optional data to include in headers
        @type headers dict{str, str}

        @param retryTimes number of times to retry if a problem occurs
        @type retryTimes int

        @param packages_location path where packages get stored
        @type packages_location str

        @param verify_options optional parameter to limit the verify operations run on existing files
        @type verify_options dict{option=value}, where option is one of "size", "checksum" and value is True/False

        @param probing if True will be silent and not log warnings/errors, useful when we are probing to see if a file exists
        @type probing bool

        @param force if True will force fetch the download file; use this for downloads that dont have checksum and size info for verification
        @type force bool

        @return true/false if item was fetched successfully
        @rtype bool

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
            verifyExisting(filePath, itemSize, hashtype, checksum, verify_options) and not force:
            LOG.debug("%s exists with expected information, no need to fetch." % (filePath))
            if repofilepath is not None and not os.path.exists(repofilepath):
                relFilePath = GrinderUtils.get_relative_path(filePath, repofilepath)
                LOG.info("Symlink missing in repo directory. Creating link %s to %s" % (repofilepath, relFilePath))
                if not os.path.islink(repofilepath):
                    self.makeSafeSymlink(relFilePath, repofilepath)
            return (BaseFetch.STATUS_NOOP,None)

        # Acquire a write lock so no other process duplicates the effort
        grinder_write_locker = GrinderLock(filePath + '.lock')
        existing_lock_pid = grinder_write_locker.readlock()
        new_pid = os.getpid()
        if existing_lock_pid and int(existing_lock_pid) != new_pid and grinder_write_locker.isvalid(existing_lock_pid):
            # If there is an existing write pid
            # and if the pid is not same as the current pid
            # and pid is valid there is another process alive
            # and handling this, exit here.
            LOG.debug("another process is already handling this path [%s] and is alive; no need to process this again " % filePath)
            return (BaseFetch.STATUS_REQUEUE,None)
        # this means either there is no pid or a pid matching current pid exists(retry case),
        # verify lock and either skip or re-acquire it
        try:
            grinder_write_locker.acquire()
            existing_lock_pid = grinder_write_locker.readlock()
        except:
            LOG.debug("Unable to acquire lock.")
            return (BaseFetch.STATUS_REQUEUE,None)
        if not existing_lock_pid or existing_lock_pid != str(os.getpid()):
            # This means, either we still dont have a lock and hence not safe to proceed or
            # the acquired lock doesnt match current pid, return and let the next process handle it
            return (BaseFetch.STATUS_NOOP,None)
        try:
            #f = open(filePath, "wb")
            curl = pycurl.Curl()
            def item_progress_callback(download_total, downloaded, upload_total, uploaded):
                #LOG.debug("%s status %s/%s bytes" % (fileName, downloaded, download_total))
                self.update_bytes_transferred(fetchURL, download_total, downloaded)
            curl.setopt(curl.NOPROGRESS, False)
            curl.setopt(curl.PROGRESSFUNCTION, item_progress_callback)
            if self.max_speed:
                #Convert KB/sec to Bytes/sec for MAC_RECV_SPEED_LARGE
                limit = self.max_speed*1024
                curl.setopt(curl.MAX_RECV_SPEED_LARGE, limit)
            curl.setopt(curl.VERBOSE,0)
            # We have seen rare and intermittent problems with grinder syncing against a remote server
            #  the remote server leaves a socket open but does not send back data.  Grinder has been stuck
            #  for several days looping over a poll of the socket with no data being sent.
            #   Slower than 1000 byes over 5 minutes will mark the connection as too slow and abort
            curl.setopt(curl.LOW_SPEED_LIMIT,1000)
            curl.setopt(curl.LOW_SPEED_TIME,60*5)
            # When using multiple threads you should set the CURLOPT_NOSIGNAL option to 1 for all handles
            # May impact DNS timeouts
            curl.setopt(curl.NOSIGNAL, 1)
            
            if type(fetchURL) == types.UnicodeType:
                #pycurl does not accept unicode strings for a URL, so we need to convert
                fetchURL = unicodedata.normalize('NFKD', fetchURL).encode('ascii','ignore')
            curl.setopt(curl.URL, fetchURL)
            if self.sslcacert:
                curl.setopt(curl.CAINFO, self.sslcacert)
            if self.sslclientcert:
                curl.setopt(curl.SSLCERT, self.sslclientcert)
            if self.sslclientkey:
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
            # callback logic to save and resume bits
            tmp_write_file = get_temp_file_name(filePath)
            if itemSize is not None:
                itemSize = int(itemSize)
            wf = WriteFunction(tmp_write_file, itemSize)
            if wf.offset > 0:
                # setup file resume
                LOG.info("A partial download file already exists; prepare to resume download.")
                curl.setopt(pycurl.RESUME_FROM, wf.offset)
            curl.setopt(curl.WRITEFUNCTION, wf.callback)
            curl.setopt(curl.FOLLOWLOCATION, 1)
            if not probing:
                LOG.info("Fetching %s bytes: %s from %s" % (itemSize or "Unknown", fileName, fetchURL))
            curl.perform()
            status = curl.getinfo(curl.HTTP_CODE)
            curl.close()
            wf.cleanup()
            # this tmp file could be closed by other concurrent processes
            if os.path.exists(tmp_write_file):
                # download complete rename the .part file
                os.rename(tmp_write_file, filePath)
            # validate the fetched bits
            if itemSize is not None and hashtype is not None and checksum is not None:
                vstatus = self.validateDownload(filePath, int(itemSize), hashtype, checksum)
            else:
                vstatus = BaseFetch.STATUS_SKIP_VALIDATE
            if status == 401:
                LOG.error("Unauthorized request from: %s" % (fetchURL))
                grinder_write_locker.release()
                cleanup(filePath)
                return (BaseFetch.STATUS_UNAUTHORIZED, "HTTP status code of %s received for %s" % (status, fetchURL))
            if status not in (0, 200, 206, 226):
                # 0 - for local syncs
                # 200 - is typical http return code, yet 206 and 226 have also been seen to be returned and valid
                if retryTimes > 0 and not fetchURL.startswith("file:"):
                    retryTimes -= 1
                    LOG.warn("Retrying fetch of: %s with %s retry attempts left. HTTP status was %s" % (fileName, retryTimes, status))
                    cleanup(filePath)
                    self.reset_bytes_transferred(fetchURL)
                    return self.fetch(fileName, fetchURL, savePath, itemSize, hashtype,
                                      checksum , headers, retryTimes, packages_location)
                grinder_write_locker.release()
                cleanup(filePath)
                LOG.warn("ERROR: Response = %s fetching %s." % (status, fetchURL))
                return (BaseFetch.STATUS_ERROR, "HTTP status code of %s received for %s" % (status, fetchURL))
            if vstatus in [BaseFetch.STATUS_ERROR, BaseFetch.STATUS_SIZE_MISSMATCH, 
                BaseFetch.STATUS_MD5_MISSMATCH] and retryTimes > 0:
                #
                # Incase of a network glitch or issue with RHN, retry the rpm fetch
                #
                retryTimes -= 1
                LOG.error("Retrying fetch of: %s with %s retry attempts left.  VerifyStatus was %s" % (fileName, retryTimes, vstatus))
                cleanup(filePath)
                self.reset_bytes_transferred(fetchURL)
                return self.fetch(fileName, fetchURL, savePath, itemSize, hashtype, 
                                  checksum, headers, retryTimes, packages_location)
            if packages_location and os.path.exists(filePath):
                relFilePath = GrinderUtils.get_relative_path(filePath, repofilepath)
                LOG.info("Create a link in repo directory for the package at %s to %s" % (repofilepath, relFilePath))
                self.makeSafeSymlink(relFilePath, repofilepath)
            grinder_write_locker.release()
            LOG.debug("Successfully Fetched Package - [%s]" % filePath)
            return (vstatus, None)
        except Exception, e:
            cleanup(tmp_write_file)
            cleanup(filePath)
            if probing:
                LOG.info("Probed for %s and determined it is missing." % (fetchURL))
                grinder_write_locker.release()
                return BaseFetch.STATUS_ERROR, None
            tb_info = traceback.format_exc()
            LOG.error("Caught exception<%s> in fetch(%s, %s)" % (e, fileName, fetchURL))
            LOG.error("%s" % (tb_info))
            if retryTimes > 0 and not fetchURL.startswith("file:"):
                retryTimes -= 1
                #grinder_write_locker.release()
                LOG.error("Retrying fetch of: %s with %s retry attempts left." % (fileName, retryTimes))
                self.reset_bytes_transferred(fetchURL)
                return self.fetch(fileName, fetchURL, savePath, itemSize, hashtype, 
                                  checksum, headers, retryTimes, packages_location)
            grinder_write_locker.release()
            raise

    def __getstate__(self):
        """
        Get the object state for pickling.
        The (tracker) attribute cannot be pickled.
        """
        state = self.__dict__.copy()
        state.pop('tracker', None)
        return state

def get_temp_file_name(file_name):
    return "%s.%s" % (file_name, "part")

def cleanup(filepath):
    LOG.info("Cleanup %s" % (filepath))
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

def verifyExisting(filePath, expectedSize, hashtype, checksum, options=None):
    """
    @param filePath file path of an existing file
    @type filePath str

    @param hashtype checksum type
    @type hashtype str

    @param checksum value
    @type checksum str

    @param options Optional dictionary of validation steps, expects boolean for values: size, checksum
    @type options dict{str, str}
    """
    size_check = True
    checksum_check = True
    if options and isinstance(options, dict):
        if options.has_key("size"):
            size_check = options["size"]
        if options.has_key("checksum"):
            checksum_check = options["checksum"]

    if size_check and expectedSize:
        statinfo = os.stat(filePath)
        if statinfo.st_size != int(expectedSize) and int(expectedSize) > 0:
            return False

    if checksum_check and hashtype and checksum:
        if getFileChecksum(hashtype, filename=filePath) != checksum:
            return False
        
    return True

def curlifyHeaders(headers):
    # pycurl drops empty header. Combining headers
    cheaders = ""
    for key,value in headers.items():
        cheaders += key +": "+ str(value) + "\r\n"
    return [cheaders]



if __name__ == "__main__":
    from grinder import GrinderLog
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
    
