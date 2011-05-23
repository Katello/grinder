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
import os
import shutil
import sys
import tempfile
import unittest

srcdir = os.path.abspath(os.path.dirname(__file__)) + "/../../src/"
sys.path.insert(0, srcdir)
from grinder import FileFetch


class TestFileSync(unittest.TestCase):

    def clean(self):
        pass

    def setUp(self):
        self.clean()

    def tearDown(self):
        self.clean()

    def test_sync(self):
        test_url = "http://pkilambi.fedorapeople.org/test_file_repo/"
        temp_label = "temp_file_repo"
        file_fetch = FileFetch.FileGrinder(temp_label, test_url, 5)
        temp_dir = tempfile.mkdtemp()
        try:
            sync_report = file_fetch.fetch(temp_dir)
            self.assertEquals(sync_report.errors, 0)
            self.assertTrue(sync_report.successes > 0)
            synced_files = glob.glob("%s/%s/*" % (temp_dir, temp_label))
            self.assertEquals(len(synced_files) - 1, sync_report.successes) # ignore MANIFEST
        finally:
            shutil.rmtree(temp_dir)

  