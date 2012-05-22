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
import traceback
from grinder.activeobject import ActiveObject
from grinder.BaseFetch import BaseFetch
from grinder.PrestoParser import PrestoParser
from grinder.GrinderExceptions import GrinderException
from grinder.GrinderUtils import GrinderUtils
from grinder.Retry import Retry
from grinder.tmpdir import TmpDir

LOG = logging.getLogger("grinder.YumInfo")

class YumMetadataObj(object):
    """
    Responsible for
    1) Fetching yum metadata
    2) Retrieving information about RPMs and DelataRPMs from yum metadata

    Note:  We intend this functionality to run in a separate process through 'ActiveObject'
            This will isolate the main grinder process from memory leaks and timing issues
            See bz: Bug 737523 - RHUA ran out of memory and triggered the oom killer
            https://bugzilla.redhat.com/show_bug.cgi?id=737523
    """
    def __init__(self, repo_label, repo_url,
                 cacert=None, clicert=None, clikey=None,
                 mirrorlist=None,
                 proxy_url=None, proxy_port=None,
                 proxy_user=None, proxy_pass=None,
                 sslverify=1, tmp_path=None, filter=None):
        self.repo = None
        self.repo_label = repo_label
        self.repo_url = repo_url.encode('ascii', 'ignore')
        self.mirrorlist = mirrorlist
        self.repo_dir = None
        self.pkgpath = None
        self.sslcacert = cacert
        self.sslclientcert = clicert
        self.sslclientkey = clikey
        self.proxy_url = proxy_url
        self.proxy_port = proxy_port
        self.proxy_user = proxy_user
        self.proxy_pass = proxy_pass
        self.sslverify  = sslverify
        self.tmp_path = tmp_path
        self.filter = filter

    def getDownloadItems(self, repo_dir="./", packages_location=None,
                         skip=None, newest=False, remove_old=False, numOldPackages=None):
        """
        @param repo_dir path to store repository files
        @type repo_dir: str

        @param packages_location path to store the RPMs, a symlink will be stored under the download_dir/repo_label
                allows a RPM to be saved once on disk and shared between multiple repositories
        @type packages_location: str

        @param skip a dictionary allowing steps to be skipped, {"packages":1} would skip downloading of packages
        @type skip: dict

        @param newest: forces only the newest packages to be downloaded
        @type newest: boolean

        @param remove_old: if true will remove old versions from disk
        @type remove_old: boolean

        @param numOldPackages: If removing old packages, this specifies how many OLD packages will be kept.
        @type numOldPackages: int

        @return info required to fetch "rpms" and "drpms"
        @rtype: dict  {"rpms":[], "drpms":[]}
        """
        download_items = {}
        try:
            if self.tmp_path:
                TmpDir.ROOT = os.path.join(self.tmp_path, "grinder")
            TmpDir.clean()
            tmpdir = TmpDir()
            tmpdir.create(self.repo_label)
            self.__setupRepo(repo_dir, tmpdir.path(), packages_location)
            self.__getRepoData()
            if not skip:
                skip = {}
            if 'rpm' not in skip:
                rpms = self.__getRPMs(newest, remove_old, numOldPackages)
                if 'drpm' not in skip:
                    drpms = self.__getDRPMs()
                    download_items["drpms"] = drpms
                download_items["rpms"] = rpms
        finally:
            self.__closeRepo()
            tmpdir.delete()
            self.repo = None
        return download_items

    def __setupRepo(self, repo_dir, tmp_dir, packages_location=None):
        if packages_location:
            self.pkgpath = packages_location
        self.repo_dir = repo_dir
        self.repo = yum.yumRepo.YumRepository(self.repo_label)
        self.repo.basecachedir = tmp_dir
        self.repo.cache = 0
        self.repo.metadata_expire = 0
        if self.mirrorlist:
            self.repo.mirrorlist = self.repo_url
        else:
            self.repo.baseurl = [self.repo_url]
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

    def __closeRepo(self):
        if self.repo is not None:
            self.repo.close()

    def __getPackageList(self, newest=False):
        sack = self.repo.getPackageSack()
        sack.populate(self.repo, 'metadata', None, 0)
        if newest:
            download_list = sack.returnNewestByNameArch()
        else:
            download_list = sack.returnPackages()
        return download_list

    def __getDeltaPackageList(self):
        if not self.deltamd:
            return []
        sack = PrestoParser(self.deltamd).getDeltas()
        return sack

    @Retry()
    def __getRepoXmlFileTypes(self):
        try:
            return self.repo.repoXML.fileTypes()
        except Exception, e:
            LOG.error("Caught exception when trying to fetch repomd.xml from [%s]: %s" % (self.repo_url, e))
            raise

    @Retry()
    def __retrieveMD(self, ftype):
        try:
            return self.repo.retrieveMD(ftype)
        except Exception, e:
            LOG.error("Caught exception when trying to fetch metadata file %s from [%s]: %s" % (ftype, self.repo_url, e))
            raise

    def __getRepoData(self):
        local_repo_path = "%s/%s" % (self.repo_dir, "repodata.new")
        if not os.path.exists(local_repo_path):
            try:
                os.makedirs(local_repo_path)
            except IOError, e:
                LOG.error("Unable to create repo directory %s" % local_repo_path)
                raise
        for ftype in self.__getRepoXmlFileTypes():
            try:
                if ftype == "primary_db":
                    self.repo.retrieved["primary_db"] = 0
                time_a = time.time()
                ftypefile = self.__retrieveMD(ftype)
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
    
    def __getRPMs(self, newest=False, remove_old=False, numOldPackages=None):
        LOG.debug("YumInfo.__getRPMs(newest=%s, remove_old=%s, numOldPackages=%s)" % \
                  (newest, remove_old, numOldPackages))
        items = []
        pkglist = self.__getPackageList(newest)
        if remove_old and not newest:
            pkglist = self._prune_package_list(pkglist, numOldPackages)
        if self.filter:
            pkglist = self._filter_package_list(pkglist)
        for pkg in pkglist:
            info = {}
            #urljoin doesnt like epoch in rpm name so using string concat
            info['fileName'] = pkg.name + "-" + pkg.version + "-" + \
                                pkg.release + "." + pkg.arch + ".rpm"
            # Adding a 'filename' to make life easier for Pulp v2 client support
            # keeping 'fileName' to not break Pulp v1
            info["filename"] = info["fileName"]

            info['downloadurl'] = pkg.remote_url or self.repo_url + '/' + pkg.relativepath
            info['savepath'] = self.repo_dir + '/' + os.path.dirname(pkg.relativepath)
            info['checksumtype'], info['checksum'], status = pkg.checksums[0]
            info['size'] = pkg.size
            if self.pkgpath:
                info['pkgpath']  = "%s/%s/%s/%s/%s/%s" % (self.pkgpath, pkg.name, pkg.version, \
                                                          pkg.release, pkg.arch, info['checksum'])
            else:
                info['pkgpath'] = None
            info['item_type'] = BaseFetch.RPM
            info["name"] = pkg.name
            info["version"] = pkg.version
            info["arch"] = pkg.arch
            info["release"] = pkg.release
            info["epoch"] = pkg.epoch
            info["relativepath"] = pkg.relativepath
            info["vendor"] = pkg.vendor
            info["license"] = pkg.license
            info["requires"] = pkg.requires
            info["provides"] = pkg.provides
            info["buildhost"] = pkg.buildhost
            info["description"] = pkg.description
            items.append(info)
        LOG.info("%s packages have been marked to be fetched" % len(items))
        return items

    def __getDRPMs(self):
        items = []
        deltarpms = self.__getDeltaPackageList()
        if not deltarpms:
            return
        for nevra, dpkg in deltarpms.items():
            for drpm in dpkg.deltas.values():
                info = {}
                relativepath = drpm.filename
                info['new_package'] = nevra
                info['fileName'] = drpm.filename
                info['downloadurl'] = self.repo_url + '/' + relativepath
                info['savepath'] = self.repo_dir
                info['epoch'] = drpm.epoch
                info['release'] = drpm.release
                info['version'] = drpm.version
                info['sequence'] = drpm.sequence
                info['checksumtype'] = drpm.checksum_type
                info['checksum'] = drpm.checksum
                info['size'] = drpm.size
                info['pkgpath']  = self.pkgpath
                info['item_type'] = BaseFetch.DELTA_RPM
                items.append(info)
                LOG.info("delta rpm added %s" % info)
        LOG.info("%s delta rpms have been marked to be fetched" % len(items))
        return items

    def _prune_package_list(self, pkglist, numold):
        """
        pkglist: list of packages as returned from yum's package sack
        numold: number of old versions of package to keep
        """
        if pkglist:
            LOG.debug("YumInfo._prune_package_list(pkglist=<%s packages>, numold=%s)" % (len(pkglist), numold))
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
        LOG.debug("_prune_package_list() returning %s pruned package list" % (len(pkglist)))
        return pkglist

    def _filter_package_list(self, pkglist):
        """
        run pkglist through self.filter
        pkglist: list of packages as returned from yum's package sack
        """
        if pkglist:
            LOG.debug("YumInfo._filter_package_list(pkglist=<%s packages>)" 
                      % (len(pkglist)))
        if not self.filter:
            LOG.debug("_filter_package_list() called with no filter")
            return pkglist
        pkglist_filtered = [ pkg for pkg in pkglist if self.filter.test(pkg) ]
        LOG.debug("_filter_package_list():  %s packages after filtering" % 
                  (len(pkglist_filtered)))
        return pkglist_filtered

    def __getstate__(self):
        """
        Get the object state for pickling.
        The (repo) attribute cannot be pickled.
        """
        state = self.__dict__.copy()
        state.pop('repo', None)
        return state


class YumInfo(object):
    def __init__(self, repo_label, repo_url, repo_dir="./",
                 packages_location=None,
                 mirrors=None, newest=False,
                 cacert=None, clicert=None, clikey=None,
                 proxy_url=None, proxy_port=None, proxy_user=None,
                 proxy_pass=None, sslverify=1,
                 remove_old=False, numOldPackages=2, skip=None, max_speed=None,
                 purge_orphaned=True, distro_location=None, tmp_path=None,
                 filter=None):
        self.rpms = []
        self.drpms = []
        self.repo_label = repo_label
        self.repo_url = repo_url
        self.mirrors = mirrors
        self.repo_dir=repo_dir
        self.downloadinfo = []
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
        self.tmp_path = tmp_path
        self.filter = filter

    def setUp(self):
        yum_metadata_obj = YumMetadataObj(
            repo_label=self.repo_label, repo_url=self.repo_url,
            mirrorlist=self.mirrors,
            cacert=self.sslcacert, clicert=self.sslclientcert, 
            clikey=self.sslclientkey,
            proxy_url=self.proxy_url, proxy_port=self.proxy_port,
            proxy_user=self.proxy_user, proxy_pass=self.proxy_pass,
            sslverify=self.sslverify, tmp_path=self.tmp_path,
            filter=self.filter)
        yumAO = None
        try:
            yumAO = ActiveObject(yum_metadata_obj)
            download_items = yumAO.getDownloadItems(repo_dir=self.repo_dir,
                                                    packages_location=self.pkgpath,
                                                    newest=self.newest,
                                                    remove_old=self.remove_old,
                                                    numOldPackages=self.numOldPackages,
                                                    skip=self.skip)
            if download_items.has_key("rpms"):
                if download_items["rpms"]:
                    self.rpms.extend(download_items["rpms"])
            if download_items.has_key("drpms"):
                if download_items["drpms"]:
                    self.drpms.extend(download_items["drpms"])
        finally:
            # Force __del__ to be called on ActiveObject
            del yumAO
