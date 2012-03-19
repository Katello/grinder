
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
import logging
import os
import shutil
import sys
import tempfile
import time
import unittest

from threading import Thread
srcdir = os.path.abspath(os.path.dirname(__file__)) + "/../../src/"
sys.path.insert(0, srcdir)
datadir = os.path.abspath(os.path.dirname(__file__)) + "/../data/"

from grinder import RepoFetch
from grinder.GrinderCallback import ProgressReport

class TestLocalSync(unittest.TestCase):

    def setUp(self):
        # If we want more debug for all tests uncomment below GrinderLog.setup()
        #GrinderLog.setup(False)
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_local_sync(self):
        test_url = "file://%s/%s" % (datadir, "repo_resync_a")
        temp_label = "temp_local_sync"
        yum_fetch = RepoFetch.YumRepoGrinder(temp_label, test_url, 5)
        sync_report = yum_fetch.fetchYumRepo(self.temp_dir)
        self.assertEquals(sync_report.errors, 0)
        self.assertTrue(sync_report.successes > 0)
        synced_rpms = glob.glob("%s/%s/*.rpm" % (self.temp_dir, temp_label))
        self.assertEquals(len(synced_rpms), sync_report.successes)

    def test_local_sync_with_errors(self):
        test_rpm_with_error = os.path.join(datadir, "local_errors", "pulp-test-package-0.3.1-1.fc11.x86_64.rpm")
        orig_stat = os.stat(test_rpm_with_error)
        try:
            os.chmod(test_rpm_with_error, 0000)
            self.assertFalse(os.access(test_rpm_with_error, os.R_OK))
            test_url = "file://%s/%s" % (datadir, "local_errors")
            temp_label = "temp_local_sync_with_errors"
            yum_fetch = RepoFetch.YumRepoGrinder(temp_label, test_url, 5)
            sync_report = yum_fetch.fetchYumRepo(self.temp_dir)
            self.assertEquals(sync_report.errors, 1)
            self.assertTrue(sync_report.successes > 0)
            synced_rpms = glob.glob("%s/%s/*.rpm" % (self.temp_dir, temp_label))
            self.assertEquals(len(synced_rpms), sync_report.successes)
            self.assertEquals(len(synced_rpms), 2)
        finally:
            os.chmod(test_rpm_with_error, orig_stat.st_mode)


