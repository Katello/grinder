#!/usr/bin/env python
#
# Copyright (c) 2010 Red Hat, Inc.
#
# Module to fetch content from yum repos
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
import yum
import time
import logging
import shutil
import pycurl
import traceback

from PrestoParser import PrestoParser
from ParallelFetch import ParallelFetch
from BaseFetch import BaseFetch
LOG = logging.getLogger("grinder.RepoFetch")

class RepoFetch(BaseFetch):
    """
     Module to fetch content from remote yum repos
    """
    def __init__(self, repo_label, repourl, cacert=None, clicert=None, clikey=None, 
                 mirrorlist=None, download_dir='./', proxy_url=None, 
                 proxy_port=None, proxy_user=None, proxy_pass=None):
        BaseFetch.__init__(self, cacert=cacert, clicert=clicert, clikey=clikey,
                proxy_url=proxy_url, proxy_port=proxy_port, 
                proxy_user=proxy_user, proxy_pass=proxy_pass)
        self.repo_label = repo_label
        self.repourl = repourl.encode('ascii', 'ignore')
        self.mirrorlist = mirrorlist
        self.local_dir = download_dir
        self.repo_dir = os.path.join(self.local_dir, self.repo_label)
        self.proxy_url = proxy_url
        self.proxy_port = proxy_port
        self.proxy_user = proxy_user
        self.proxy_pass = proxy_pass

    def setupRepo(self):
        self.repo = yum.yumRepo.YumRepository(self.repo_label)
        self.repo.basecachedir = self.local_dir
        self.repo.cache = 0
        self.repo.metadata_expire = 0
        if self.mirrorlist:
            self.repo.mirrorlist = self.repourl
        else:
            self.repo.baseurl = [self.repourl]
        if self.proxy_url:
            if not self.proxy_port:
                raise GrinderException("Proxy url is defined, but proxy_port is not specified")
            self.repo.proxy = "%s:%s" % (self.proxy_url, self.proxy_port)
            self.repo.proxy_username = self.proxy_user
            self.repo.proxy_password = self.proxy_pass
        self.repo.baseurlSetup()
        self.deltamd = None
        self.repo.sslcacert = self.sslcacert
        self.repo.sslclientcert = self.sslclientcert
        self.repo.sslclientkey = self.sslclientkey
        self.repo.sslverify = 1

    def getPackageList(self, newest=False):
        sack = self.repo.getPackageSack()
        sack.populate(self.repo, 'metadata', None, 0)
        if newest:
            download_list = sack.returnNewestByNameArch()
        else:
            download_list = sack.returnPackages()
        return download_list

    def getDeltaPackageList(self):
        if not self.deltamd:
            return []
        sack = PrestoParser(self.deltamd).getDeltas()
        return sack.values()
    
    def fetchItem(self, info):
        return self.fetch(info['fileName'], 
                          str(info['downloadurl']), 
                          info['size'], 
                          info['checksumtype'], 
                          info['checksum'],
                          info['savepath'], packages_location=info['pkgpath'] or None)

    def fetchAll(self):
        plist = self.getPackageList()
        total = len(plist)
        seed = 1
        for pkg in plist:
            print("Fetching [%d/%d] Packages - %s" % (seed, total, pkg))
            check = (self.validatePackage, (pkg ,1), {})
            self.repo.getPackage(pkg, checkfunc=check)
            seed += 1

    def getRepoData(self):
        local_repo_path = os.path.join(self.repo_dir, "repodata")
        if not os.path.exists(local_repo_path):
            try:
                os.makedirs(local_repo_path)
            except IOError, e:
                LOG.error("Unable to create repo directory %s" % local_repo_path)
        for ftype in self.repo.repoXML.fileTypes():
            try:
                if ftype == "primary_db":
                    self.repo.retrieved["primary_db"] = 0
                ftypefile = self.repo.retrieveMD(ftype)
                basename  = os.path.basename(ftypefile)
                destfile  = "%s/%s" % (local_repo_path, basename)
                shutil.copyfile(ftypefile, destfile)
                if ftype == "prestodelta": 
                    self.deltamd = destfile 
            except Exception, e:
                tb_info = traceback.format_exc()
                LOG.debug("%s" % (tb_info))
                LOG.error("Unable to Fetch Repo data file %s" % ftype)
        shutil.copyfile(self.repo_dir + "/repomd.xml", "%s/%s" % (local_repo_path, "repomd.xml"))
        LOG.debug("Fetched repo metadata for %s" % self.repo_label)

    def validatePackage(self, fo, pkg, fail):
        return pkg.verifyLocalPkg()

class YumRepoGrinder(object):
    """
      Driver module to initiate the repo fetching
    """
    def __init__(self, repo_label, repo_url, parallel, mirrors=None, \
                       newest=False, cacert=None, clicert=None, clikey=None, \
                       proxy_url=None, proxy_port=None, proxy_user=None, \
                       proxy_pass=None, packages_location=None):
        self.repo_label = repo_label
        self.repo_url = repo_url
        self.mirrors = mirrors
        self.numThreads = int(parallel)
        self.fetchPkgs = None
        self.downloadinfo = []
        self.yumFetch = None
        self.sslcacert = cacert
        self.sslclientcert = clicert
        self.sslclientkey = clikey
        self.proxy_url = proxy_url
        self.proxy_port = proxy_port
        self.proxy_user = proxy_user
        self.proxy_pass = proxy_pass
        self.newest = newest
        # set this if you want all packages to be stored in a central location
        self.pkgpath = packages_location

    def prepareRPMS(self):
        pkglist = self.yumFetch.getPackageList(newest=self.newest)
        for pkg in pkglist:
            info = {}
            #urljoin doesnt like epoch in rpm name so using string concat
            info['fileName'] = pkg.name + "-" + pkg.version + "-" + \
                                pkg.release + "." + pkg.arch + ".rpm"

            info['downloadurl'] = self.yumFetch.repourl + '/' + pkg.relativepath
            info['savepath'] = self.yumFetch.repo_dir + '/' + os.path.dirname(pkg.relativepath)
            info['checksumtype'], info['checksum'], status = pkg.checksums[0]
            info['size'] = pkg.size
            if self.pkgpath:
                info['pkgpath']  = "%s/%s/%s/%s/%s/%s" % (self.pkgpath, info['checksum'][:3], pkg.name, pkg.version, pkg.release, pkg.arch)
            else:
                info['pkgpath'] = None 
            self.downloadinfo.append(info)
        LOG.info("%s packages have been marked to be fetched" % len(pkglist))

    def prepareDRPMS(self):
        deltarpms = self.yumFetch.getDeltaPackageList()
        if not deltarpms:
            return

        for dpkg in deltarpms:
            info = {}
            relativepath = dpkg.deltas.values()[0].filename
            info['fileName'] = dpkg.deltas.values()[0].filename
            info['downloadurl'] = self.yumFetch.repourl + '/' + relativepath
            info['savepath'] = self.yumFetch.repo_dir
            info['checksumtype'] = dpkg.deltas.values()[0].checksum_type
            info['checksum'] = dpkg.deltas.values()[0].checksum
            info['size'] = dpkg.deltas.values()[0].size
            info['pkgpath']  = self.pkgpath
            self.downloadinfo.append(info)
        LOG.info("%s delta rpms have been marked to be fetched" % len(deltarpms))

    def fetchYumRepo(self, basepath="./", callback=None):
        LOG.info("fetchYumRepo() basepath = %s" % (basepath))
        startTime = time.time()
        self.yumFetch = RepoFetch(self.repo_label, repourl=self.repo_url, \
                            cacert=self.sslcacert, clicert=self.sslclientcert, \
                            clikey=self.sslclientkey, mirrorlist=self.mirrors, \
                            download_dir=basepath, proxy_url=self.proxy_url, \
                            proxy_port=self.proxy_port, proxy_user=self.proxy_user, \
                            proxy_pass=self.proxy_pass)
        self.yumFetch.setupRepo()
        LOG.info("Fetching repo metadata...")
        # first fetch the metadata
        self.yumFetch.getRepoData()
        LOG.info("Determining downloadable Content bits...")
        # get rpms to fetch
        self.prepareRPMS()
        # get drpms to fetch
        self.prepareDRPMS()
        # prepare for download
        self.fetchPkgs = ParallelFetch(self.yumFetch, self.numThreads, callback=callback)
        self.fetchPkgs.addItemList(self.downloadinfo)
        self.fetchPkgs.start()
        report = self.fetchPkgs.waitForFinish()
        endTime = time.time()
        LOG.info("Processed <%s> packages in [%d] seconds" % (len(self.downloadinfo), \
                  (endTime - startTime)))
        LOG.info("Cleaning any orphaned packages..")
        self.purgeOrphanPackages(self.yumFetch.getPackageList(), self.yumFetch.repo_dir)
        return report

    def stop(self):
        if self.fetchPkgs:
            self.fetchPkgs.stop()
            
    def purgeOrphanPackages(self, downloadlist, repo_dir):
        """
        While re-sync purge any orphaned packages in the Repo that are not in
        updated primary.xml download list. 
        """
        dpkgs = []
        save_path = repo_dir
        for pkg in downloadlist:
            rpmName = pkg.name + "-" + pkg.version + "-" + \
                                pkg.release + "." + pkg.arch + ".rpm"
            save_path = repo_dir + '/' + os.path.dirname(pkg.relativepath)
            dpkgs.append(rpmName)
        for ppkg in os.listdir(save_path):
            if not ppkg.endswith('.rpm'):
                continue
            if ppkg not in dpkgs:
                os.remove(os.path.join(save_path, ppkg))
                

if __name__ == "__main__":
#    yfetch = YumRepoGrinder("fedora12", \
#         "http://download.fedora.devel.redhat.com/pub/fedora/linux/releases/12/Everything/x86_64/os/", 10, newest=True, pkg_path="/var/lib/pulp/packages/")
#    yfetch.fetchYumRepo()
    
    yfetch = YumRepoGrinder("testrepo", "http://mmccune.fedorapeople.org/pulp/", 
                            10, packages_location="/var/lib/pulp/packages/")
    yfetch.fetchYumRepo()
