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
from grinder.GrinderCallback import ProgressReport

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

    def test_sync_with_errors(self):
        global progress
        progress = None
        def progress_callback(report):
            global progress
            progress = report

        test_url = "http://jmatthews.fedorapeople.org/test_repo_files_bad_sizes/"
        temp_label = "test_repo_files_bad_sizes"
        file_fetch = FileFetch.FileGrinder(temp_label, test_url, 5)
        temp_dir = tempfile.mkdtemp()
        try:
            sync_report = file_fetch.fetch(temp_dir, callback=progress_callback)
            self.assertEquals(sync_report.successes, 1) #test_file_1 should be only success
            self.assertEquals(sync_report.errors, 3)
            synced_files = glob.glob("%s/%s/*" % (temp_dir, temp_label))
            self.assertEquals(len(synced_files) - 1, sync_report.successes) # ignore MANIFEST
            print "Progress = <%s>" % (progress)
            self.assertEquals(progress.items_total, 4)
            self.assertEquals(progress.items_left, 0)
            self.assertEquals(progress.size_left, 0)
            self.assertEquals(progress.status, "FINISHED")
            self.assertEquals(progress.num_success, 1)
            self.assertEquals(progress.num_error, 3)
            self.assertEquals(progress.details["file"]["num_success"], 1)
            self.assertEquals(progress.details["file"]["num_error"], 3)
            self.assertEquals(len(progress.error_details), 3)
            for error_detail in progress.error_details:
                self.assertTrue(error_detail["fileName"] in ["test_file_2", "test_file_3", "test_file_4"])
                self.assertTrue(error_detail["error_type"] in ["size_missmatch", "md5_missmatch"])
        finally:
            shutil.rmtree(temp_dir)
  
