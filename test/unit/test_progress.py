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
from grinder.GrinderCallback import ProgressReport
from grinder.ProgressTracker import ProgressTracker

class TestProgress(unittest.TestCase):

    def clean(self):
        pass

    def setUp(self):
        self.clean()

    def tearDown(self):
        self.clean()

    def test_progress_create(self):
        tracker = ProgressTracker()
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 0)
        self.assertEquals(progress["remaining_bytes"], 0)
        self.assertEquals(progress["total_num_items"], 0)
        self.assertEquals(progress["remaining_num_items"], 0)
        self.assertEquals(progress["type_info"], {})

    def test_add_item(self):
        tracker = ProgressTracker()
        progress = tracker.get_progress()

        tracker.add_item("http://test1", 100, "rpm")
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 100)
        self.assertEquals(progress["remaining_bytes"], 100)
        self.assertEquals(progress["total_num_items"], 1)
        self.assertEquals(progress["remaining_num_items"], 1)
        self.assertTrue(progress["type_info"].has_key("rpm"))
        self.assertEquals(progress["type_info"]["rpm"]["total_size_bytes"], 100)
        self.assertEquals(progress["type_info"]["rpm"]["size_left"], 100)
        self.assertEquals(progress["type_info"]["rpm"]["total_count"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["items_left"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["num_success"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["num_error"], 0)

        tracker.item_complete("http://test1", True)
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 100)
        self.assertEquals(progress["remaining_bytes"], 0)
        self.assertEquals(progress["total_num_items"], 1)
        self.assertEquals(progress["remaining_num_items"], 0)
        self.assertTrue(progress["type_info"].has_key("rpm"))
        self.assertEquals(progress["type_info"]["rpm"]["total_size_bytes"], 100)
        self.assertEquals(progress["type_info"]["rpm"]["size_left"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["total_count"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["items_left"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["num_success"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["num_error"], 0)

    def test_update_progress(self):
        tracker = ProgressTracker()
        progress = tracker.get_progress()

        tracker.add_item("http://test1", 100, "rpm")
        tracker.update_progress_download("http://test1", 100, 5)
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 100)
        self.assertEquals(progress["remaining_bytes"], 95)
        self.assertEquals(progress["total_num_items"], 1)
        self.assertEquals(progress["remaining_num_items"], 1)
        self.assertTrue(progress["type_info"].has_key("rpm"))
        self.assertEquals(progress["type_info"]["rpm"]["total_size_bytes"], 100)
        self.assertEquals(progress["type_info"]["rpm"]["size_left"], 95)
        self.assertEquals(progress["type_info"]["rpm"]["total_count"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["items_left"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["num_success"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["num_error"], 0)

        tracker.update_progress_download("http://test1", 100, 90)
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 100)
        self.assertEquals(progress["remaining_bytes"], 10)
        self.assertEquals(progress["total_num_items"], 1)
        self.assertEquals(progress["remaining_num_items"], 1)
        self.assertTrue(progress["type_info"].has_key("rpm"))
        self.assertEquals(progress["type_info"]["rpm"]["total_size_bytes"], 100)
        self.assertEquals(progress["type_info"]["rpm"]["size_left"], 10)
        self.assertEquals(progress["type_info"]["rpm"]["total_count"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["items_left"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["num_success"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["num_error"], 0)

        tracker.update_progress_download("http://test1", 100, 100)
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 100)
        self.assertEquals(progress["remaining_bytes"], 0)
        self.assertEquals(progress["total_num_items"], 1)
        self.assertEquals(progress["remaining_num_items"], 1)
        self.assertTrue(progress["type_info"].has_key("rpm"))
        self.assertEquals(progress["type_info"]["rpm"]["total_size_bytes"], 100)
        self.assertEquals(progress["type_info"]["rpm"]["size_left"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["total_count"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["items_left"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["num_success"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["num_error"], 0)

        tracker.item_complete("http://test1", True)
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 100)
        self.assertEquals(progress["remaining_bytes"], 0)
        self.assertEquals(progress["total_num_items"], 1)
        self.assertEquals(progress["remaining_num_items"], 0)
        self.assertTrue(progress["type_info"].has_key("rpm"))
        self.assertEquals(progress["type_info"]["rpm"]["total_size_bytes"], 100)
        self.assertEquals(progress["type_info"]["rpm"]["size_left"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["total_count"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["items_left"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["num_success"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["num_error"], 0)

    def test_update_progress_with_incomplete_transfer(self):
        tracker = ProgressTracker()
        progress = tracker.get_progress()

        tracker.add_item("http://test1", 100, "rpm")
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 100)
        self.assertEquals(progress["remaining_bytes"], 100)
        self.assertEquals(progress["total_num_items"], 1)
        self.assertEquals(progress["remaining_num_items"], 1)
        self.assertTrue(progress["type_info"].has_key("rpm"))
        self.assertEquals(progress["type_info"]["rpm"]["total_size_bytes"], 100)
        self.assertEquals(progress["type_info"]["rpm"]["size_left"], 100)
        self.assertEquals(progress["type_info"]["rpm"]["total_count"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["items_left"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["num_success"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["num_error"], 0)

        tracker.update_progress_download("http://test1", 100, 5)
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 100)
        self.assertEquals(progress["remaining_bytes"], 95)
        self.assertEquals(progress["total_num_items"], 1)
        self.assertEquals(progress["remaining_num_items"], 1)
        self.assertTrue(progress["type_info"].has_key("rpm"))
        self.assertEquals(progress["type_info"]["rpm"]["total_size_bytes"], 100)
        self.assertEquals(progress["type_info"]["rpm"]["size_left"], 95)
        self.assertEquals(progress["type_info"]["rpm"]["total_count"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["items_left"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["num_success"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["num_error"], 0)

        tracker.item_complete("http://test1", False)
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 100)
        self.assertEquals(progress["remaining_bytes"], 0)
        self.assertEquals(progress["total_num_items"], 1)
        self.assertEquals(progress["remaining_num_items"], 0)
        self.assertTrue(progress["type_info"].has_key("rpm"))
        self.assertEquals(progress["type_info"]["rpm"]["total_size_bytes"], 100)
        self.assertEquals(progress["type_info"]["rpm"]["size_left"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["total_count"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["items_left"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["num_success"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["num_error"], 1)

    def test_basic_sync(self):
        return
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

    def test_progress_modify_size(self):
        tracker = ProgressTracker()
        progress = tracker.get_progress()
        fetchURL = "http://test1"
        tracker.add_item(fetchURL, 100, "rpm")
        # Updating progress with a smaller expected download size, causing total_size_bytes to decrease
        tracker.update_progress_download(fetchURL, 80, 5)
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 80)
        self.assertEquals(progress["remaining_bytes"], 75)
        self.assertEquals(progress["type_info"]["rpm"]["total_size_bytes"], 80)
        self.assertEquals(progress["type_info"]["rpm"]["size_left"], 75)



    def test_progress_after_refetch_of_item(self):
        tracker = ProgressTracker()
        progress = tracker.get_progress()
        fetchURL = "http://test1"
        tracker.add_item(fetchURL, 100, "rpm")
        tracker.update_progress_download(fetchURL, 80, 5)

        tracker.reset_progress(fetchURL)
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 80)
        self.assertEquals(progress["remaining_bytes"], 80)
        self.assertEquals(progress["total_num_items"], 1)
        self.assertEquals(progress["remaining_num_items"], 1)
        self.assertTrue(progress["type_info"].has_key("rpm"))
        self.assertEquals(progress["type_info"]["rpm"]["total_size_bytes"], 80)
        self.assertEquals(progress["type_info"]["rpm"]["size_left"], 80)
        self.assertEquals(progress["type_info"]["rpm"]["total_count"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["items_left"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["num_success"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["num_error"], 0)

        tracker.update_progress_download(fetchURL, 80, 5)
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 80)
        self.assertEquals(progress["remaining_bytes"], 75)


        tracker.item_complete("http://test1", True)
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 80)
        self.assertEquals(progress["remaining_bytes"], 0)
        self.assertEquals(progress["total_num_items"], 1)
        self.assertEquals(progress["remaining_num_items"], 0)
        self.assertTrue(progress["type_info"].has_key("rpm"))
        self.assertEquals(progress["type_info"]["rpm"]["total_size_bytes"], 80)
        self.assertEquals(progress["type_info"]["rpm"]["size_left"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["total_count"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["items_left"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["num_success"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["num_error"], 0)

    def test_update_progress_with_NaN(self):
        tracker = ProgressTracker()
        progress = tracker.get_progress()
        # Simulate a call with 'NaN'
        tracker.add_item("http://test1", 100, "rpm")
        tracker.update_progress_download("http://test1", float("nan"), 5)
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 100)
        self.assertEquals(progress["remaining_bytes"], 100)
        self.assertEquals(progress["total_num_items"], 1)
        self.assertEquals(progress["remaining_num_items"], 1)
        self.assertTrue(progress["type_info"].has_key("rpm"))
        self.assertEquals(progress["type_info"]["rpm"]["total_size_bytes"], 100)
        self.assertEquals(progress["type_info"]["rpm"]["size_left"], 100)
        self.assertEquals(progress["type_info"]["rpm"]["total_count"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["items_left"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["num_success"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["num_error"], 0)

    def test_update_progress_with_bad_data(self):
        tracker = ProgressTracker()
        progress = tracker.get_progress()
        # Simulate a call with bad data
        tracker.add_item("http://test1", "Bad_Value_Non_Integer", "rpm")
        tracker.update_progress_download("http://test1", "Bad_Value", 5)
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 0)
        self.assertEquals(progress["remaining_bytes"], 0)
        self.assertEquals(progress["total_num_items"], 1)
        self.assertEquals(progress["remaining_num_items"], 1)
        self.assertTrue(progress["type_info"].has_key("rpm"))
        self.assertEquals(progress["type_info"]["rpm"]["total_size_bytes"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["size_left"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["total_count"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["items_left"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["num_success"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["num_error"], 0)
        # Now simulate if update is called subsequently with good data
        # Ensure it's working
        tracker.update_progress_download("http://test1", 100, 5)
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 100)
        self.assertEquals(progress["remaining_bytes"], 95)
        self.assertEquals(progress["total_num_items"], 1)
        self.assertEquals(progress["remaining_num_items"], 1)
        self.assertTrue(progress["type_info"].has_key("rpm"))
        self.assertEquals(progress["type_info"]["rpm"]["total_size_bytes"], 100)
        self.assertEquals(progress["type_info"]["rpm"]["size_left"], 95)
        self.assertEquals(progress["type_info"]["rpm"]["total_count"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["items_left"], 1)
        self.assertEquals(progress["type_info"]["rpm"]["num_success"], 0)
        self.assertEquals(progress["type_info"]["rpm"]["num_error"], 0)


