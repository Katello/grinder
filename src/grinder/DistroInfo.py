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
import time
import logging
import shutil
import tempfile
import threading
import traceback
import ConfigParser
from grinder.activeobject import ActiveObject
from grinder.BaseFetch import BaseFetch

LOG = logging.getLogger("grinder.DistroInfo")

class DistroInfo(object):

    def __init__(self, repo_url, repo_dir, distropath):
        self.repo_url = repo_url
        self.repo_dir = repo_dir
        self.distropath = distropath

    def prepareTrees(self, fetcher):
        """
        @param fetcher instance of a BaseFetch instance capable of retrieving .treeinfo/treeinfo metadata
        @type fetcher: Instance of grinder.BaseFetch.BaseFetch

        @return List of dicts representing distribution tree files to fetch
        """
        distro_items = []
        fetcherAO = ActiveObject(fetcher)
        LOG.info("Preparing to fetch any available trees..")
        # In certain cases, treeinfo is not a hidden file. Try if one fails..
        for treeinfo in ['.treeinfo', 'treeinfo']:
            tree_manifest = treeinfo
            treeinfo_url = self.repo_url + '/' + tree_manifest
            treeinfo_name   = tree_manifest
            treeinfo_path   = self.repo_dir
            info = {
                    'downloadurl'   : treeinfo_url,
                    'fileName'      : treeinfo_name,
                    'savepath'      : treeinfo_path,
                    'checksumtype'  :  None,
                    'checksum'      : None,
                    'size'          : None,
                    'pkgpath'       : None,
                    }
            # Will fetch tree metadata through ActiveObject
            # This avoids a timing issue seen with NSS and multiple threads
            fetcherAO.fetchItem(info)
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
        arch = variant = version = family = None
        if cfgparser.has_section('general'):
            for field in ['arch', 'variant', 'version', 'family']:
                if (cfgparser.has_option('general', field) and \
                    len(cfgparser.get('general', field)) > 0):
                    vars(self)[field] = cfgparser.get('general', field)
                else:
                    vars(self)[field] = None
        ks_label = "ks-%s-%s-%s-%s" % (self.family, self.variant, self.version, self.arch)
        tree_info = {}
        if cfgparser.has_section('checksums'):
            # This should give us all the kernel/image files
            for opt_fn in cfgparser.options('checksums'):
                (csum_type, csum) = cfgparser.get('checksums', opt_fn).split(':')
                tree_info[opt_fn] = (csum_type, csum)
        else:
            #No checksum section, look manually for images
            if cfgparser.has_section('images-%s' % self.arch):
                try:
                    imgs = 'images-%s' % self.arch
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
            info['downloadurl'] = self.repo_url + '/' + relpath
            info['fileName']    = os.path.basename(relpath)
            info['savepath']    = treeinfo_path + '/' + os.path.dirname(relpath)
            (info['checksumtype'], info['checksum']) = hashinfo
            info['size']        = 0
            info['pkgpath'] = None
            if self.distropath:
                info['pkgpath'] = "%s/%s" % (self.distropath, ks_label)
            info['item_type'] = BaseFetch.TREE_FILE
            distro_items.append(info)
        LOG.info("%s Tree files have been marked to be fetched" % len(tree_info))
        # write the treeinfo file to distro location for reuse
        tree_repo_location = os.path.join(treeinfo_path, tree_manifest)
        if self.distropath and os.path.exists(tree_repo_location) and not os.path.islink(tree_repo_location) :
            tree_distro_location = os.path.join(info['pkgpath'], tree_manifest)
            if not os.path.exists(info['pkgpath']):
                os.makedirs(info['pkgpath'])
            shutil.move(tree_repo_location, tree_distro_location)
            LOG.info("creating symlink from [%s] to [%s]" % (tree_distro_location, tree_repo_location))
            os.symlink(tree_distro_location, tree_repo_location)
        return distro_items

        
