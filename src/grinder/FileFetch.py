#
# Copyright (c) 2011 Red Hat, Inc.
#
# Module to fetch file based content
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
import logging
from grinder.BaseFetch import BaseFetch
from grinder.GrinderCallback import ProgressReport
from grinder.GrinderUtils import parseManifest
from grinder.ParallelFetch import ParallelFetch

LOG = logging.getLogger("grinder.FileFetch")

class FileFetch(BaseFetch):
    def __init__(self, repo_label, url, cacert=None, clicert=None, clikey=None,
                 download_dir='./', proxy_url=None,
                 proxy_port=None, proxy_user=None, proxy_pass=None, sslverify=1,
                 max_speed=None):
        BaseFetch.__init__(self, cacert=cacert, clicert=clicert, clikey=clikey,
                proxy_url=proxy_url, proxy_port=proxy_port,
                proxy_user=proxy_user, proxy_pass=proxy_pass, sslverify=sslverify,
                max_speed=max_speed)
        self.repo_label = repo_label
        self.url = url.encode('ascii', 'ignore')
        self.local_dir = download_dir
        self.repo_dir = os.path.join(self.local_dir, self.repo_label)

    def fetchItem(self, info):
        return self.fetch(info['fileName'],
                          str(info['downloadurl']),
                          info['savepath'],
                          itemSize=info['size'],
                          hashtype=info['checksumtype'],
                          checksum=info['checksum'],
                          packages_location=info['pkgpath'] or None)


class FileGrinder(object):
    """
      Driver module to initiate the file fetching
    """
    def __init__(self, repo_label, url, parallel=50, cacert=None, clicert=None, clikey=None, \
                       proxy_url=None, proxy_port=None, proxy_user=None, \
                       proxy_pass=None, sslverify=1, files_location=None, max_speed=None ):
        self.repo_label = repo_label
        self.repo_url = url
        self.numThreads = int(parallel)
        self.downloadinfo = []
        self.sslcacert = cacert
        self.sslclientcert = clicert
        self.sslclientkey = clikey
        self.proxy_url = proxy_url
        self.proxy_port = proxy_port
        self.proxy_user = proxy_user
        self.proxy_pass = proxy_pass
        # set this if you want all packages to be stored in a central location
        self.filepath = files_location
        self.sslverify  = sslverify
        self.max_speed = max_speed
        self.fileFetch = None

    def prepareFiles(self):
        file_manifest = "PULP_MANIFEST"
        file_url = self.fileFetch.url + '/' + file_manifest
        file_name   = file_manifest
        file_path   = self.fileFetch.repo_dir
        info = {
                'downloadurl'   : file_url,
                'fileName'      : file_name,
                'savepath'      : file_path,
                'checksumtype'  : None,
                'checksum'      : None,
                'size'          : None,
                'pkgpath'       : None,
                }
        self.fileFetch.fetchItem(info)
        file_info = {}
        file_manifest_path = os.path.join(file_path, file_manifest)
        if os.path.exists(file_manifest_path):
            file_info = parseManifest(file_manifest_path)
        else:
            LOG.info("File Metadata Not Found at url %s" % self.repo_url)
        for fileinfo in file_info:
            info = {}
            info['downloadurl'] = self.repo_url + '/' + fileinfo['filename']
            info['fileName']    = os.path.basename(fileinfo['filename'])
            info['savepath']    = file_path #+ '/' + os.path.dirname(info['filename'])
            info['checksumtype'] = 'sha256'
            info['checksum'] = fileinfo['checksum']
            info['size']        = int(fileinfo['size'])
            if self.filepath:
                info['pkgpath']  = "%s/%s/%s/%s/" % (self.filepath, os.path.basename(fileinfo['filename'])[:3], \
                                        os.path.basename(fileinfo['filename']), fileinfo['checksum'])
            else:
                info['pkgpath'] = None
            info['item_type'] = BaseFetch.FILE
            self.downloadinfo.append(info)
        LOG.info("%s files have been marked to be fetched" % len(file_info))
    
    def fetch(self,basepath="./", callback=None):
        LOG.info("fetch basepath = %s" % (basepath))
        startTime = time.time()
        self.fileFetch = FileFetch(self.repo_label, self.repo_url, cacert=self.sslcacert, \
                                   clicert=self.sslclientcert, clikey=self.sslclientkey, \
                                   download_dir=basepath, proxy_url=self.proxy_url, \
                                   proxy_port=self.proxy_port, proxy_user=self.proxy_user, \
                                   proxy_pass=self.proxy_pass, sslverify=self.sslverify, max_speed=self.max_speed)
        self.parallel_fetch_files = ParallelFetch(self.fileFetch, self.numThreads, callback=callback)
        LOG.info("Determining downloadable Content bits...")
        self.parallel_fetch_files.processCallback(ProgressReport.DownloadMetadata)
        self.prepareFiles()
        # prepare for download
        self.parallel_fetch_files.addItemList(self.downloadinfo)
        self.parallel_fetch_files.start()
        report = self.parallel_fetch_files.waitForFinish()
        endTime = time.time()
        LOG.info("Processed <%s> items in [%d] seconds" % (len(self.downloadinfo), \
                  (endTime - startTime)))
        return report

    def stop(self, block=True):
        if self.parallel_fetch_files:
            self.parallel_fetch_files.stop()
            if block:
                self.parallel_fetch_files._waitForThreads()


if __name__ == "__main__":
    file_fetch = FileGrinder("test_file_repo", "http://pkilambi.fedorapeople.org/test_file_repo/", 5)
    file_fetch.fetch()
