
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
import time
import unittest

from threading import Thread
srcdir = os.path.abspath(os.path.dirname(__file__)) + "/../../src/"
sys.path.insert(0, srcdir)

from grinder import RepoFetch
from grinder.Filter import Filter

class TestFilters(unittest.TestCase):

    def clean(self):
        pass

    def setUp(self):
        # If we want more debug for all tests uncomment below GrinderLog.setup()
        #GrinderLog.setup(False)
        self.clean()

    def tearDown(self):
        self.clean()

    def test_filtered_sync(self):
        test_url = "http://repos.fedorapeople.org/repos/pulp/pulp/v1/testing/6Server/i386/"
        temp_label = "test_filtered_sync"
        yum_fetch = RepoFetch.YumRepoGrinder(temp_label, test_url, 5)
        temp_dir = tempfile.mkdtemp()
        #
        # Verify gofer packages are in repo
        #
        try:
            sync_report = yum_fetch.fetchYumRepo(temp_dir)
            synced_rpms = glob.glob("%s/%s/gofer*.rpm" % (temp_dir, temp_label))
            self.assertTrue(len(synced_rpms) > 0)
        finally:
            shutil.rmtree(temp_dir)
        #
        # Verify when our blacklist filter no gofer packages are synced
        #
        filter_gofer = Filter("blacklist", regex_list=["gofer*"], description="Simple gofer filter")
        yum_fetch = RepoFetch.YumRepoGrinder(temp_label, test_url, 5, filter=filter_gofer)
        temp_dir = tempfile.mkdtemp()
        try:
            sync_report = yum_fetch.fetchYumRepo(temp_dir)
            synced_rpms = glob.glob("%s/%s/gofer*.rpm" % (temp_dir, temp_label))
            self.assertEqual(len(synced_rpms), 0)
        finally:
            shutil.rmtree(temp_dir)

