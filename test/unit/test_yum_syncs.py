
  #!/usr/bin/python
#
# Copyright (c) 2011 Red Hat, Inc.
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
import logging
import os
import shutil
import sys
import tempfile
import unittest

srcdir = os.path.abspath(os.path.dirname(__file__)) + "/../../src/"
sys.path.insert(0, srcdir)

from grinder import GrinderLog
from grinder import RepoFetch


class TestYumSync(unittest.TestCase):

    def clean(self):
        pass

    def setUp(self):
        # If we want more debug for all tests uncomment below GrinderLog.setup()
        #GrinderLog.setup(False)
        self.clean()

    def tearDown(self):
        self.clean()

    def test_basic_sync(self):
        test_url = "http://repos.fedorapeople.org/repos/pulp/pulp/testing/6Server/i386/"
        temp_label = "temp_label"
        yum_fetch = RepoFetch.YumRepoGrinder(temp_label, test_url, 5)
        temp_dir = tempfile.mkdtemp()
        try:
            sync_report = yum_fetch.fetchYumRepo(temp_dir)
            self.assertEquals(sync_report.errors, 0)
            self.assertTrue(sync_report.successes > 0)
            synced_rpms = glob.glob("%s/%s/*.rpm" % (temp_dir, temp_label))
            self.assertEquals(len(synced_rpms), sync_report.successes)
        finally:
            shutil.rmtree(temp_dir)

    def test_sync_number_old_packages(self):
        test_url = "http://jmatthews.fedorapeople.org/repo_multiple_versions/"
        num_old = 4
        temp_label = "temp_number_old_packages"
        yum_fetch = RepoFetch.YumRepoGrinder(temp_label, test_url, 5, newest=False, 
                remove_old=True, numOldPackages=num_old)
        temp_dir = tempfile.mkdtemp()
        try:
            sync_report = yum_fetch.fetchYumRepo(temp_dir)
            self.assertEquals(sync_report.errors, 0)
            self.assertTrue(sync_report.successes > 0)
            synced_rpms = glob.glob("%s/%s/*.rpm" % (temp_dir, temp_label))
            # Verify we downloaded only what was needed, i.e. we didn't
            # download more older rpms than asked for.
            self.assertEquals(len(synced_rpms), sync_report.successes)
            # Verify # of rpms in synced dir is latest plus num_old
            self.assertEquals(len(synced_rpms), num_old+1)
        finally:
            shutil.rmtree(temp_dir)
