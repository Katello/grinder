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
import os
import time
import logging
import shutil
from grinder.ParallelFetch import ParallelFetch
from grinder.BaseFetch import BaseFetch
from grinder.DistroInfo import DistroInfo
from grinder.GrinderCallback import ProgressReport
from grinder.YumInfo import YumInfo

LOG = logging.getLogger("grinder.RepoFetch")

class RepoFetch(BaseFetch):
    """
    Needed by ParallelFetch
    - Main purpose is to override fetchItem so ParallelFetch::WorkerThread knows how to call fetch()
    """
    def __init__(self, cacert=None, clicert=None, clikey=None,
                 proxy_url=None, proxy_port=None, proxy_user=None, proxy_pass=None,
                 sslverify=1, max_speed=None, verify_options=None):
        BaseFetch.__init__(self, cacert=cacert, clicert=clicert, clikey=clikey,
                proxy_url=proxy_url, proxy_port=proxy_port, 
                proxy_user=proxy_user, proxy_pass=proxy_pass, sslverify=sslverify,
                max_speed=max_speed, verify_options=verify_options)

    def stop(self, state=True):
        self.stopped = state

    def fetchItem(self, info, probing=None, force=False):
        return self.fetch(info['fileName'], 
                          str(info['downloadurl']), 
                          info['savepath'],
                          itemSize=info['size'], 
                          hashtype=info['checksumtype'], 
                          checksum=info['checksum'],
                          packages_location=info['pkgpath'] or None,
                          verify_options=self.verify_options, probing=probing, force=force)


class YumRepoGrinder(object):
    """
      Driver class to fetch content from a Yum Repository
    """
    def __init__(self, repo_label, repo_url, parallel=10, mirrors=None,
                 newest=False, cacert=None, clicert=None, clikey=None,
                 proxy_url=None, proxy_port=None, proxy_user=None,
                 proxy_pass=None, sslverify=1, packages_location=None,
                 remove_old=False, numOldPackages=2, skip=None, max_speed=None,
                 purge_orphaned=True, distro_location=None, tmp_path=None,
                 filter=None):
        self.repo_label = repo_label
        self.repo_url = repo_url
        self.repo_dir = None
        self.mirrors = mirrors
        self.numThreads = int(parallel)
        self.fetchPkgs = None
        self.downloadinfo = []
        self.repoFetch = None
        self.fetchPkgs = None
        self.sslcacert = cacert
        self.sslclientcert = clicert
        self.sslclientkey = clikey
        self.temp_ssl_client_cert = None
        self.temp_ssl_client_key = None
        self.proxy_url = proxy_url
        self.proxy_port = proxy_port
        self.proxy_user = proxy_user
        self.proxy_pass = proxy_pass
        self.newest = newest
        # set this if you want all packages to be stored in a central location
        self.pkgpath = packages_location
        self.numOldPackages = numOldPackages
        self.pkgsavepath = ''
        self.remove_old = remove_old
        self.skip = skip
        if not self.skip:
            self.skip = []
        self.sslverify  = sslverify
        self.max_speed = max_speed
        self.purge_orphaned = purge_orphaned
        self.stopped = False
        self.distropath = distro_location
        self.rpmlist = []
        self.drpmlist = []
        self.tmp_path = tmp_path
        self.filter = filter

    def getRPMItems(self):
        return self.rpmlist

    def getDeltaRPMItems(self):
        return self.drpmlist

    def getDistroItems(self):
        return self.distro_items

    def setup(self, basepath="./", callback=None, verify_options=None):
        """
        Fetches yum metadata and determines what object should be downloaded.

        @param basepath: path to store repo data
        @type basepath: str

        @param callback: progress callback function
        @type callback: function which accepts a grinder.GrinderCallback.ProgressReport

        @param verify_options: controls verification checks on "size" and "checksum".
        @type verify_options: dict{"size":bool,"checksum":bool}
        """
        self.repo_dir = os.path.join(basepath, self.repo_label)
        LOG.info("%s, %s, Calling RepoFetch with: cacert=<%s>, clicert=<%s>, clikey=<%s>, proxy_url=<%s>, proxy_port=<%s>, proxy_user=<%s>, proxy_pass=<NOT_LOGGED>, sslverify=<%s>, max_speed=<%s>, verify_options=<%s>, filter=<%s>" %\
             (self.repo_label, self.repo_url, self.sslcacert, self.sslclientcert, self.sslclientkey, self.proxy_url, self.proxy_port, self.proxy_user, self.sslverify, self.max_speed, verify_options, self.filter))

        self.repoFetch = RepoFetch(cacert=self.sslcacert, clicert=self.sslclientcert, clikey=self.sslclientkey,\
        proxy_url=self.proxy_url, proxy_port=self.proxy_port,
        proxy_user=self.proxy_user, proxy_pass=self.proxy_pass,
        sslverify=self.sslverify,
        max_speed=self.max_speed,
        verify_options=verify_options)
        self.fetchPkgs = ParallelFetch(self.repoFetch, self.numThreads, callback=callback)
        self.fetchPkgs.processCallback(ProgressReport.DownloadMetadata)

        info = YumInfo(
            repo_label=self.repo_label, repo_url=self.repo_url, 
            mirrors = self.mirrors, repo_dir=self.repo_dir, 
            packages_location=self.pkgpath, newest=self.newest,
            remove_old=self.remove_old, numOldPackages=self.numOldPackages,
            cacert=self.sslcacert, clicert=self.sslclientcert, 
            clikey=self.sslclientkey, proxy_url=self.proxy_url, 
            proxy_port=self.proxy_port, proxy_user=self.proxy_user, 
            proxy_pass=self.proxy_pass, sslverify=self.sslverify, skip=self.skip,
            tmp_path=self.tmp_path, filter=self.filter)
        info.setUp()
        self.rpmlist = info.rpms
        self.drpmlist = info.drpms

    def setupDistroInfo(self):
        info = DistroInfo(repo_url=self.repo_url, repo_dir=self.repo_dir,
                          distropath=self.distropath)
        distro_items = info.prepareTrees(self.repoFetch)
        self.distro_items = {}
        if distro_items:
            self.distro_items = distro_items

    def addItems(self, items):
        self.fetchPkgs.addItemList(items)

    def download(self):
        """
        Synchronous call, initiates download and waits for all items to finish before returning

        @return: A SyncReport
        @rtype: grinder.ParallelFetch.SyncReport
        """
        try:
            startTime = time.time()
            self.fetchPkgs.start()
            self.fetchPkgs.processCallback(ProgressReport.DownloadItems)
            report = self.fetchPkgs.waitForFinish()
            self.finalizeMetadata()
            if 'rpm' not in self.skip:
                if self.purge_orphaned:
                    # Includes logic of:
                    # 1) removing previously existing packages that have been
                    #    removed from repository metadata
                    # 2) removing old packages that are part of repository metadata,
                    #    but we want removed because of remove_old/numOldPackages option
                    LOG.info("Cleaning any orphaned packages..")
                    self.fetchPkgs.processCallback(ProgressReport.PurgeOrphanedPackages)
                    self.purgeOrphanPackages()
                if self.remove_old:
                    # Need to re-test remove_old is functioning
                    # We added the ability to limit the old packages from being downloaded
                    # I think we need to address the case of existing packages from a prior sync
                    self.fetchPkgs.processCallback(ProgressReport.RemoveOldPackages)
            endTime = time.time()
            LOG.info("Processed <%s>,<%s> with <%s> items in [%d] seconds. Report: %s" % (self.repo_label, self.repo_url, len(self.downloadinfo),\
                                                                                          (endTime - startTime), report))
            return report
        finally:
            if self.fetchPkgs:
                self.fetchPkgs.stop()
                self.fetchPkgs = None

    def fetchYumRepo(self, basepath="./", callback=None, verify_options=None):
        LOG.info("fetchYumRepo() repo_label = %s, repo_url = %s, basepath = %s, verify_options = %s" % \
                 (self.repo_label, self.repo_url, basepath, verify_options))
        self.setup(basepath, callback, verify_options)
        if 'distribution' not in self.skip:
            self.setupDistroInfo()
            if self.distro_items:
                self.addItems(self.distro_items['files'])
        else:
            LOG.debug("skipping distributions from sync")
        self.addItems(self.rpmlist)
        self.addItems(self.drpmlist)
        return self.download()

    def stop(self, block=True):
        LOG.info("Stopping")
        self.stopped = True
        if self.fetchPkgs:
            self.fetchPkgs.stop()
            if block:
                LOG.info("Block is <%s> so waiting" % (block))
                self.fetchPkgs._waitForThreads()
            
    def purgeOrphanPackages(self):
        """
        While re-sync purge any orphaned packages in the Repo that we did not intend to sync.
        Includes removal of packages no longer in primary.xml
         as well as older packages filtered out from remove_old/numOldPackages logic
        """
        dpkgs = []
        if self.rpmlist:
            for pkg in self.rpmlist:
                dpkgs.append(os.path.join(self.repo_dir, os.path.dirname(pkg['relativepath']), pkg['fileName']))
        if os.path.exists(self.repo_dir):
            for root, dirs, files in os.walk(self.repo_dir):
                for f in files:
                    tmp_path = os.path.join(root, f)
                    if tmp_path.endswith('.rpm') and tmp_path not in dpkgs:
                        LOG.info("Removing orphan package: %s" % (tmp_path))
                        os.remove(tmp_path)

    def finalizeMetadata(self):
        local_repo_path = "%s/%s" % (self.repo_dir, "repodata")
        local_new_path  = "%s/%s" % (self.repo_dir, "repodata.new")
        if not os.path.exists(local_new_path):
            LOG.info("No new metadata to finalize.")
            return
        try:
            LOG.info("Finalizing metadata, moving %s to %s" % (local_new_path, local_repo_path))
            if os.path.exists(local_repo_path):
                # remove existing metadata before copying
                shutil.rmtree(local_repo_path)
            shutil.copytree(local_new_path, local_repo_path)
            shutil.rmtree(local_new_path)
        except Exception, e:
            LOG.error("An error occurred while finalizing metadata:\n%s" % str(e))

if __name__ == "__main__":
    yfetch = YumRepoGrinder("testrepo", "http://download.fedora.redhat.com/pub/fedora/linux/releases/13/Fedora/i386/os/",
                            10)
    yfetch.fetchYumRepo()
