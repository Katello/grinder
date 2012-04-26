#!/usr/bin/python
#
# Copyright (c) 2012 Red Hat, Inc.
#
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

# Python
import glob
import os
import shutil
import sys
import tempfile
import unittest

srcdir = os.path.abspath(os.path.dirname(__file__)) + "/../../src/"
sys.path.insert(0, srcdir)
from grinder.DistroInfo import DistroInfo
from grinder.RepoFetch import RepoFetch, YumRepoGrinder

class TestDistributionSync(unittest.TestCase):

    def clean(self):
        shutil.rmtree(self.temp_dir)

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        self.clean()

    def test_prepareTrees(self):
        test_url = "http://repos.fedorapeople.org/repos/pulp/pulp/demo_repos/pulp_unittest/"
        temp_label = "test_prepareTrees"
        repo_dir = os.path.join(self.temp_dir, temp_label)
        info = DistroInfo(repo_url=test_url, repo_dir=repo_dir,
                          distropath=self.temp_dir)
        repoFetch = RepoFetch()
        distro_items = info.prepareTrees(repoFetch)
        self.assertEquals(len(distro_items['files']), 3)

    def test_prepareTrees_no_treeinfo(self):
        test_url = "http://repos.fedorapeople.org/repos/pulp/pulp/demo_repos/repo_resync_a/"
        temp_label = "test_prepareTrees_no_treeinfo"
        repo_dir = os.path.join(self.temp_dir, temp_label)
        info = DistroInfo(repo_url=test_url, repo_dir=repo_dir,
                          distropath=self.temp_dir)
        repoFetch = RepoFetch()
        distro_items = info.prepareTrees(repoFetch)
        self.assertEquals(len(distro_items), 0)

    def test_prepareTrees_bad_url(self):
        test_url = "http://repos.fedorapeople.org/repos/pulp/pulp/BAD_URL_DOESNT_EXIST/"
        temp_label = "test_prepareTrees_bad_url"
        repo_dir = os.path.join(self.temp_dir, temp_label)
        info = DistroInfo(repo_url=test_url, repo_dir=repo_dir,
                          distropath=self.temp_dir)
        repoFetch = RepoFetch()
        distro_items = info.prepareTrees(repoFetch)
        self.assertEquals(len(distro_items), 0)
        
    def test_sync(self):
        test_url = "http://repos.fedorapeople.org/repos/pulp/pulp/demo_repos/pulp_unittest"
        temp_label = "test_sync"
        yum_fetch = YumRepoGrinder(temp_label, test_url, 5)
        sync_report = yum_fetch.fetchYumRepo(self.temp_dir)
        distro_tree_files = glob.glob("%s/%s/images/*" % (self.temp_dir, temp_label))
        print distro_tree_files
        self.assertEquals(len(distro_tree_files), 3)
    
    def test_sync_of_repo_no_treeinfo(self):
        test_url = "http://repos.fedorapeople.org/repos/pulp/pulp/demo_repos/repo_resync_a/"
        temp_label = "test_sync_of_repo_no_treeinfo"
        yum_fetch = YumRepoGrinder(temp_label, test_url, 5)
        sync_report = yum_fetch.fetchYumRepo(self.temp_dir)
        # Ensure that temp files were cleaned up for .treeinfo.part
        self.assertFalse(os.path.exists(os.path.join(self.temp_dir, temp_label, "treeinfo.part")))
        self.assertFalse(os.path.exists(os.path.join(self.temp_dir, temp_label, ".treeinfo.part")))
