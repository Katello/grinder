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


    def test_add_item(self):
        tracker = ProgressTracker()
        progress = tracker.get_progress()
        self.assertEquals(progress["total_size_bytes"], 0)
        self.assertEquals(progress["remaining_bytes"], 0)
        self.assertEquals(progress["total_num_items"], 0)
        self.assertEquals(progress["remaining_num_items"], 0)
        self.assertEquals(progress["type_info"], {})

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
        self.assertEquals(progress["total_size_bytes"], 0)
        self.assertEquals(progress["remaining_bytes"], 0)
        self.assertEquals(progress["total_num_items"], 0)
        self.assertEquals(progress["remaining_num_items"], 0)
        self.assertEquals(progress["type_info"], {})

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
        self.assertEquals(progress["total_size_bytes"], 0)
        self.assertEquals(progress["remaining_bytes"], 0)
        self.assertEquals(progress["total_num_items"], 0)
        self.assertEquals(progress["remaining_num_items"], 0)
        self.assertEquals(progress["type_info"], {})

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
