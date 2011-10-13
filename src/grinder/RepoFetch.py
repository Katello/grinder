#!/usr/bin/env python
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
import yum
import time
import logging
import shutil
import tempfile
import threading
import traceback
import ConfigParser
from grinder.PrestoParser import PrestoParser
from grinder.ParallelFetch import ParallelFetch
from grinder.BaseFetch import BaseFetch
from grinder.GrinderCallback import ProgressReport
from grinder.GrinderUtils import GrinderUtils, splitPEM

LOG = logging.getLogger("grinder.RepoFetch")

GRINDER_YUM_LOCK = threading.RLock()
TREE_INFO_LOCK = threading.RLock()

class RepoFetch(BaseFetch):
    """
     Module to fetch content from remote yum repos
    """
    def __init__(self, repo_label, repourl, cacert=None, clicert=None, clikey=None, 
                 mirrorlist=None, download_dir='./', proxy_url=None, 
                 proxy_port=None, proxy_user=None, proxy_pass=None, sslverify=1,
                 max_speed=None, verify_options=None):
        BaseFetch.__init__(self, cacert=cacert, clicert=clicert, clikey=clikey,
                proxy_url=proxy_url, proxy_port=proxy_port, 
                proxy_user=proxy_user, proxy_pass=proxy_pass, sslverify=sslverify,
                max_speed=max_speed, verify_options=verify_options)
        self.repo = None
        self.repo_label = repo_label
        self.repourl = repourl.encode('ascii', 'ignore')
        self.mirrorlist = mirrorlist
        self.local_dir = download_dir
        self.repo_dir = os.path.join(self.local_dir, self.repo_label)
        self.proxy_url = proxy_url
        self.proxy_port = proxy_port
        self.proxy_user = proxy_user
        self.proxy_pass = proxy_pass
        self.sslverify  = sslverify
        self.stopped = False

    def stop(self, state=True):
        self.stopped = state

    def setupRepo(self):
        self.repo = yum.yumRepo.YumRepository(self.repo_label)
        self.repo.basecachedir = self.makeTempDir()
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
        self.repo.sslverify = self.sslverify
        
    def closeRepo(self):
        if self.repo is not None:
            self.repo.close()
            self.repo = None

    def getPackageList(self, newest=False):
        GRINDER_YUM_LOCK.acquire()
        try:
            sack = self.repo.getPackageSack()
            sack.populate(self.repo, 'metadata', None, 0)
            if newest:
                download_list = sack.returnNewestByNameArch()
            else:
                download_list = sack.returnPackages()
            return download_list
        finally:
            GRINDER_YUM_LOCK.release()
            
    def getDeltaPackageList(self):
        if not self.deltamd:
            return []
        sack = PrestoParser(self.deltamd).getDeltas()
        return sack.values()
    
    def fetchItem(self, info):
        return self.fetch(info['fileName'], 
                          str(info['downloadurl']), 
                          info['savepath'],
                          itemSize=info['size'], 
                          hashtype=info['checksumtype'], 
                          checksum=info['checksum'],
                          packages_location=info['pkgpath'] or None,
                          verify_options=self.verify_options)

    def fetchAll(self):
        plist = self.getPackageList()
        total = len(plist)
        seed = 1
        for pkg in plist:
            print("Fetching [%d/%d] Packages - %s" % (seed, total, pkg))
            check = (self.validatePackage, (pkg ,1), {})
            self.repo.getPackage(pkg, checkfunc=check)
            seed += 1

    def getRepoXmlFileTypes(self):
        try:
            return self.repo.repoXML.fileTypes()
        except Exception, e:
            LOG.error("Caught exception when trying to fetch content from [%s]: %s" % (self.repourl, e))
            raise

    def getRepoData(self):
        GRINDER_YUM_LOCK.acquire()
        try:
            local_repo_path = "%s/%s" % (self.repo_dir, "repodata.new")
            if not os.path.exists(local_repo_path):
                try:
                    os.makedirs(local_repo_path)
                except IOError, e:
                    LOG.error("Unable to create repo directory %s" % local_repo_path)
                    raise
            for ftype in self.getRepoXmlFileTypes():
                if self.stopped:
                    break
                try:
                    if ftype == "primary_db":
                        self.repo.retrieved["primary_db"] = 0
                    time_a = time.time()
                    ftypefile = self.repo.retrieveMD(ftype)
                    time_b = time.time()
                    LOG.debug("self.repo.retrieveMD(%s) took %s seconds" % (ftype, (time_b-time_a)))
                    basename  = os.path.basename(ftypefile)
                    destfile  = "%s/%s" % (local_repo_path, basename)
                    shutil.copyfile(ftypefile, destfile)
                    if ftype == "prestodelta":
                        self.deltamd = destfile
                except Exception, e:
                    tb_info = traceback.format_exc()
                    LOG.debug("%s" % (tb_info))
                    LOG.error("Unable to Fetch Repo data file %s" % ftype)
                    raise
            src = os.path.join(self.repo.basecachedir, self.repo_label, "repomd.xml")
            dst = os.path.join(local_repo_path, "repomd.xml")
            LOG.debug("Copy %s to %s" % (src, dst))
            shutil.copyfile(src, dst)
            LOG.debug("Fetched repo metadata for %s" % self.repo_label)
        finally:
            GRINDER_YUM_LOCK.release()

    def validatePackage(self, fo, pkg, fail):
        return pkg.verifyLocalPkg()
    
    def deleteBaseCacheDir(self):
        # Delete the temporary directory we are using to store repodata files
        # for grinders own usage.  This is not the endpoint destination
        # for sync'd content
        try:
            # yum is currently deleting this for us, below check is a precaution
            if os.path.exists(self.repo.basecachedir) and \
                    os.path.isdir(self.repo.basecachedir):
                shutil.rmtree(self.repo.basecachedir)
            return True
        except Exception, e:
            tb_info = traceback.format_exc()
            LOG.error("%s" % (tb_info))
            LOG.error(e)
            return False

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
        shutil.rmtree(self.repo.basecachedir)
        
    def __getstate__(self):
        """
        Get the object state for pickling.
        The (repo) attribute cannot be pickled.
        """
        state = self.__dict__.copy()
        state.pop('repo', None)
        return state


class YumRepoGrinder(object):
    """
      Driver module to initiate the repo fetching
    """
    def __init__(self, repo_label, repo_url, parallel=10, mirrors=None, \
                       newest=False, cacert=None, clicert=None, clikey=None, \
                       proxy_url=None, proxy_port=None, proxy_user=None, \
                       proxy_pass=None, sslverify=1, packages_location=None, \
                       remove_old=False, numOldPackages=2, skip=None, max_speed=None, \
                       purge_orphaned=True):
        self.repo_label = repo_label
        self.repo_url = repo_url
        self.mirrors = mirrors
        self.numThreads = int(parallel)
        self.fetchPkgs = None
        self.downloadinfo = []
        self.yumFetch = None
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
            self.skip = {}
        self.sslverify  = sslverify
        self.max_speed = max_speed
        self.purge_orphaned = purge_orphaned
        self.stopped = False

    def __deleteTempCerts(self):
        # Goal is to delete temporary cert files we generate if a PEM cert was used
        if self.temp_ssl_client_cert:
            os.unlink(self.temp_ssl_client_cert)
        if self.temp_ssl_client_key:
            os.unlink(self.temp_ssl_client_key)
            
    def _prune_package_list(self, pkglist, numold):
        """
        pkglist: list of packages as returned from yum's package sack
        numold: number of old versions of package to keep
        """
        rpms = {}
        # Format data for sorting
        for pkg in pkglist:
            pkg_key = "%s.%s" % (pkg.name, pkg.arch)
            if not rpms.has_key(pkg_key):
                rpms[pkg_key] = []
            item = {}
            for key in ["name", "release", "epoch", "version", "arch"]:
                item[key] = getattr(pkg, key)
            item["pkg"] = pkg
            rpms[pkg_key].append(item)
        grinderUtils = GrinderUtils()
        grinderUtils.numOldPkgsKeep = numold
        rpms = grinderUtils.sortListOfRPMS(rpms)
        # Prune data
        for key in rpms:
            values = rpms[key]
            if len(values) > numold:
                rpms[key] = rpms[key][:numold+1]
        # Flatten data
        pkglist = []
        for key in rpms:
            pkgs = [item["pkg"] for item in rpms[key]]
            pkglist.extend(pkgs)
        return pkglist

    def prepareRPMS(self):
        pkglist = self.yumFetch.getPackageList(newest=self.newest)
        if self.remove_old and not self.newest:
            pkglist = self._prune_package_list(pkglist, self.numOldPackages)
        for pkg in pkglist:
            info = {}
            #urljoin doesnt like epoch in rpm name so using string concat
            info['fileName'] = pkg.name + "-" + pkg.version + "-" + \
                                pkg.release + "." + pkg.arch + ".rpm"
            info['downloadurl'] = pkg.remote_url or self.yumFetch.repourl + '/' + pkg.relativepath
            info['savepath'] = self.yumFetch.repo_dir + '/' + os.path.dirname(pkg.relativepath)
            info['checksumtype'], info['checksum'], status = pkg.checksums[0]
            info['size'] = pkg.size
            if self.pkgpath:
                info['pkgpath']  = "%s/%s/%s/%s/%s/%s" % (self.pkgpath, pkg.name, pkg.version, \
                                                          pkg.release, pkg.arch, info['checksum'][:3])
            else:
                info['pkgpath'] = None
            info['item_type'] = BaseFetch.RPM
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
            info['item_type'] = BaseFetch.DELTA_RPM
            self.downloadinfo.append(info)
        LOG.info("%s delta rpms have been marked to be fetched" % len(deltarpms))

    def prepareTrees(self):
        # Below is a problem we saw with protected syncs, resulting from NSS not being thread safe
        # https://bugzilla.redhat.com/show_bug.cgi?id=737614
        # Bug 737614 - glibc backtrace during repo sync
        # NSS is not thread safe, we are seeing a "double free" error after looping
        # several hundred grinder syncs.
        # Need to restrict pycurl/NSS transfers to one per process
        # Alternative solution is to move the pycurl fetch to ActiveObject
        TREE_INFO_LOCK.acquire()
        try:
            return self.__prepareTrees()
        finally:
            TREE_INFO_LOCK.release()

    def __prepareTrees(self):
        LOG.info("Preparing to fetch any available trees..")
        # In certain cases, treeinfo is not a hidden file. Try if one fails..
        for treeinfo in ['.treeinfo', 'treeinfo']:
            tree_manifest = treeinfo
            treeinfo_url = self.yumFetch.repourl + '/' + tree_manifest
            treeinfo_name   = tree_manifest
            treeinfo_path   = self.yumFetch.repo_dir
            info = {
                    'downloadurl'   : treeinfo_url,
                    'fileName'      : treeinfo_name,
                    'savepath'      : treeinfo_path,
                    'checksumtype'  :  None,
                    'checksum'      : None,
                    'size'          : None,
                    'pkgpath'       : None,
                    }

            self.yumFetch.fetchItem(info)
            if os.path.exists(os.path.join(treeinfo_path, tree_manifest)):
                LOG.info("Tree info fetched from %s" % treeinfo_url)
                break
        
        cfgparser = ConfigParser.ConfigParser()
        cfgparser.optionxform = str # prevent cfgparser to converts data to lowercase.
        try:
            treecfg = open(os.path.join(treeinfo_path, tree_manifest))
            cfgparser.readfp(treecfg)
        except Exception, e:
            LOG.info("Unable to read the tree info config.")
            LOG.info(e)
            return

        tree_info = {}
        if cfgparser.has_section('checksums'):
            # This should give us all the kernel/image files
            for opt_fn in cfgparser.options('checksums'):
                (csum_type, csum) = cfgparser.get('checksums', opt_fn).split(':')
                tree_info[opt_fn] = (csum_type, csum)
        else:
            #No checksum section, look manually for images
            arch = None
            if cfgparser.has_section('general'):
                arch = cfgparser.get('general', 'arch')
            if cfgparser.has_section('images-%s' % arch):
                try:
                    imgs = 'images-%s' % arch
                    for fn in cfgparser.options(imgs):
                        fileinf = cfgparser.get(imgs, fn)
                        tree_info[fileinf] = (None, None)
                    if cfgparser.has_section('stage2'):
                        mainimage = cfgparser.get('stage2', 'mainimage')
                    else:
                        mainimage = 'images/stage2.img'
                    tree_info[mainimage] = (None, None)
                except ConfigParser.NoOptionError, e:
                    LOG.info("Invalid treeinfo: %s" % str(e))
                    return
        treecfg.close()
        for relpath, hashinfo in tree_info.items():
            info = {}
            info['downloadurl'] = self.yumFetch.repourl + '/' + relpath
            info['fileName']    = os.path.basename(relpath)
            info['savepath']    = treeinfo_path + '/' + os.path.dirname(relpath)
            (info['checksumtype'], info['checksum']) = hashinfo
            info['size']        = 0
            info['pkgpath'] = None
            info['item_type'] = BaseFetch.TREE_FILE
            self.downloadinfo.append(info)
        LOG.info("%s Tree files have been marked to be fetched" % len(tree_info))

    def convertCert(self):
        try:
            # If only a ssl client cert is passed in, attempt to split it out
            if self.sslclientcert and not self.sslclientkey:
                data = open(self.sslclientcert).read()
                key, cert = splitPEM(data)
                if cert and key:
                    temp_cert_file = None
                    temp_key_file = None
                    try:
                        # We found a cert and a key, so assuming this was a PEM format
                        temp_cert_file, self.temp_ssl_client_cert  = tempfile.mkstemp(prefix="temp_ssl_cert")
                        temp_key_file, self.temp_ssl_client_key = tempfile.mkstemp(prefix="temp_ssl_key")
                        # Write out the cert/key to separate files
                        # Need to write out to a file, since curl expects a filename and doesn't work with a string
                        os.write(temp_cert_file, cert)
                        os.write(temp_key_file, key)
                        LOG.debug("Converted PEM <%s> to Cert <%s> Key <%s>" % \
                                  (self.sslclientcert, self.temp_ssl_client_cert, self.temp_ssl_client_key))
                    finally:
                        if temp_cert_file:
                            os.close(temp_cert_file)
                        if temp_key_file:
                            os.close(temp_key_file)
                    self.sslclientkey = self.temp_ssl_client_key
                    self.sslclientcert = self.temp_ssl_client_cert
                    LOG.debug("sslclientcert = <%s>, sslclientkey = <%s>" % (self.sslclientcert, self.sslclientkey))
        except Exception, e:
            LOG.error(e)
            LOG.error(traceback.format_exc())
            try:
                if self.temp_ssl_client_cert:
                    os.unlink(self.temp_ssl_client_cert)
                    self.temp_ssl_client_cert = None
                if self.temp_ssl_client_key:
                    os.unlink(self.temp_ssl_client_key)
                    self.temp_ssl_client_key = None
            except:
                pass
            raise



    def fetchYumRepo(self, basepath="./", callback=None, verify_options=None):
        LOG.info("fetchYumRepo() basepath = %s" % (basepath))
        startTime = time.time()
        self.yumFetch = RepoFetch(self.repo_label, repourl=self.repo_url, \
                            cacert=self.sslcacert, clicert=self.sslclientcert, \
                            clikey=self.sslclientkey, mirrorlist=self.mirrors, \
                            download_dir=basepath, proxy_url=self.proxy_url, \
                            proxy_port=self.proxy_port, proxy_user=self.proxy_user, \
                            proxy_pass=self.proxy_pass, sslverify=self.sslverify,
                            max_speed=self.max_speed,
                            verify_options=verify_options)
        self.fetchPkgs = ParallelFetch(self.yumFetch, self.numThreads, callback=callback)
        try:
            self.yumFetch.setupRepo()
            LOG.info("Fetching repo metadata...")
            # first fetch the metadata
            self.fetchPkgs.processCallback(ProgressReport.DownloadMetadata)
            self.yumFetch.getRepoData()
            if self.stopped:
                return None
            LOG.info("Determining downloadable Content bits...")
            if not self.skip.has_key('packages') or self.skip['packages'] != 1:
                # get rpms to fetch
                self.prepareRPMS()
                # get drpms to fetch
                self.prepareDRPMS()
            else:
                LOG.info("Skipping packages preparation from sync process")
            if not self.skip.has_key('distribution') or self.skip['distribution'] != 1:
                # get Trees to fetch
                self.prepareTrees()
            else:
                LOG.info("Skipping distribution preparation from sync process")
            # prepare for download
            self.fetchPkgs.addItemList(self.downloadinfo)
            self.fetchPkgs.start()
            report = self.fetchPkgs.waitForFinish()
            self.yumFetch.finalizeMetadata()
            endTime = time.time()
            LOG.info("Processed <%s> items in [%d] seconds" % (len(self.downloadinfo), \
                  (endTime - startTime)))
            if not self.skip.has_key('packages') or self.skip['packages'] != 1:
                if self.purge_orphaned:
                    LOG.info("Cleaning any orphaned packages..")
                    self.fetchPkgs.processCallback(ProgressReport.PurgeOrphanedPackages)
                    self.purgeOrphanPackages(self.yumFetch.getPackageList(), self.yumFetch.repo_dir)
                if self.remove_old:
                    LOG.info("Removing old packages to limit to %s" % self.numOldPackages)
                    self.fetchPkgs.processCallback(ProgressReport.RemoveOldPackages)
                    gutils = GrinderUtils()
                    gutils.runRemoveOldPackages(self.pkgsavepath, self.numOldPackages)
            return report
        finally:
            self.fetchPkgs.stop()
            self.yumFetch.deleteBaseCacheDir()
            self.yumFetch.closeRepo()

    def stop(self, block=True):
        LOG.info("Stopping")
        self.stopped = True
        if self.yumFetch:
            self.yumFetch.stop()
        if self.fetchPkgs:
            self.fetchPkgs.stop()
            if block:
                LOG.info("Block is <%s> so waiting" % (block))
                self.fetchPkgs._waitForThreads()
            
    def purgeOrphanPackages(self, downloadlist, repo_dir):
        """
        While re-sync purge any orphaned packages in the Repo that are not in
        updated primary.xml download list. 
        """
        dpkgs = []
        self.pkgsavepath = repo_dir
        for pkg in downloadlist:
            rpmName = pkg.name + "-" + pkg.version + "-" + \
                                pkg.release + "." + pkg.arch + ".rpm"
            pkg_path = os.path.dirname(pkg.relativepath)
            self.pkgsavepath = repo_dir + '/' + pkg_path
            dpkgs.append(rpmName)
        if os.path.exists(self.pkgsavepath):
            for ppkg in os.listdir(self.pkgsavepath):
                if not ppkg.endswith('.rpm'):
                    continue
                if ppkg not in dpkgs:
                    os.remove(os.path.join(self.pkgsavepath, ppkg))
                

if __name__ == "__main__":
    yfetch = YumRepoGrinder("testrepo", "http://download.fedora.redhat.com/pub/fedora/linux/releases/13/Fedora/i386/os/",
                            10)
    yfetch.fetchYumRepo()
