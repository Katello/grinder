
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
        yum_fetch = RepoFetch.YumRepoGrinder(temp_label, test_url, parallel=5, newest=False, 
                remove_old=True, numOldPackages=num_old)
        self.assertEquals(yum_fetch.numOldPackages, num_old)
        temp_dir = tempfile.mkdtemp()
        try:
            sync_report = yum_fetch.fetchYumRepo(temp_dir)
            self.assertEquals(sync_report.errors, 0)
            self.assertTrue(sync_report.successes > 0)
            synced_rpms = glob.glob("%s/%s/*.rpm" % (temp_dir, temp_label))
        finally:
            shutil.rmtree(temp_dir)
        # Verify we downloaded only what was needed, i.e. we didn't
        # download more older rpms than asked for.
        self.assertEquals(len(synced_rpms), sync_report.successes)
        # Verify # of rpms in synced dir is latest plus num_old
        self.assertEquals(len(synced_rpms), num_old+1)
    
    def test_remove_existing_packages_that_are_old(self):
        test_url = "http://jmatthews.fedorapeople.org/repo_multiple_versions/"
        temp_label = "temp_number_old_packages"
        num_old = 4
        yum_fetch_a = RepoFetch.YumRepoGrinder(temp_label, test_url, parallel=5)
        yum_fetch_b = RepoFetch.YumRepoGrinder(temp_label, test_url, parallel=5, newest=False, 
                remove_old=True, numOldPackages=num_old)
        self.assertEquals(yum_fetch_b.numOldPackages, num_old)
        temp_dir = tempfile.mkdtemp()
        try:
            # Sync all packages, including old ones
            sync_report = yum_fetch_a.fetchYumRepo(temp_dir)
            self.assertEquals(sync_report.errors, 0)
            self.assertTrue(sync_report.successes > 0)
            synced_rpms = glob.glob("%s/%s/*.rpm" % (temp_dir, temp_label))
            self.assertTrue(len(synced_rpms) > num_old+1)
            # Resync packages with numOldPackages set
            # This will cause the removeOldPackages check at end of sync to run
            sync_report = yum_fetch_b.fetchYumRepo(temp_dir)
            synced_rpms = glob.glob("%s/%s/*.rpm" % (temp_dir, temp_label))
            self.assertEquals(len(synced_rpms), sync_report.successes)
            self.assertEquals(len(synced_rpms), num_old+1)
        finally:
            shutil.rmtree(temp_dir)

    def test_stop_sync(self):
        global progress
        progress = None
        def progress_callback(report):
            print "progress_callback invoked with <%s>" % (report)
            global progress
            progress = report

        class SyncThread(Thread):
            def __init__(self, callback):
                Thread.__init__(self)
                self.test_url = "http://repos.fedorapeople.org/repos/pulp/pulp/demo_repos/test_bandwidth_repo/"
                self.num_threads = 2
                self.max_speed = 1 # 1 KB/sec
                self.callback = callback
                self.temp_dir = tempfile.mkdtemp()
                self.yum_fetch = RepoFetch.YumRepoGrinder(self.temp_dir, self.test_url, self.num_threads,
                                                          max_speed=self.max_speed)
            def run(self):
                try:
                    self.yum_fetch.fetchYumRepo(callback=self.callback)
                finally:
                    shutil.rmtree(self.temp_dir)

            def stop(self, block=False):
                self.yum_fetch.stop(block=block)

        sync_thread = SyncThread(progress_callback)
        print "Starting Sync"
        sync_thread.start()
        # Wait until we are downloading packages
        counter = 0
        while True:
            if hasattr(progress, "step"):
                if progress.step == ProgressReport.DownloadItems:
                    break
            if counter > 30:
                break
            counter = counter + 1
            time.sleep(1)
        print "Now downloading"
        # Now send stop and time how long for us to respond
        start = time.time()
        sync_thread.stop()
        counter = 0
        while True:
            if hasattr(progress, "step"):
                if progress.step != ProgressReport.DownloadItems:
                    break
                if counter > 30:
                    print "Progress is reporting step: %s" % (progress.step)
                    print "Took more than 30 seconds to stop, test failed"
                    self.assertTrue(False)
                counter = counter + 1
                time.sleep(1)
        end = time.time()
        print "Stop took: %s seconds" % (end-start)
        self.assertTrue(end-start < 30)

    def test_purge_orphan_packages(self):
        test_url_a = "http://jmatthews.fedorapeople.org/repo_multiple_versions/"
        test_url_b = "http://jmatthews.fedorapeople.org/repo_resync/"
        temp_label = "temp_purge_orphan"
        yum_fetch_a = RepoFetch.YumRepoGrinder(temp_label, test_url_a, parallel=5)
        yum_fetch_b = RepoFetch.YumRepoGrinder(temp_label, test_url_b, parallel=5)
        temp_dir_a = tempfile.mkdtemp()
        temp_dir_b = tempfile.mkdtemp()
        try:
            # Sync some extra rpms from a different repo
            sync_report_a = yum_fetch_a.fetchYumRepo(temp_dir_a)
            # Simulate orphaned packages by copying extra rpms to a dir, then doing a sync
            if not os.path.exists("%s/%s" % (temp_dir_b, temp_label)):
                os.makedirs("%s/%s" % (temp_dir_b, temp_label))
            for src_file in glob.glob("%s/%s/*.rpm" % (temp_dir_a, temp_label)):
                shutil.copy(src_file, "%s/%s" % (temp_dir_b, temp_label))
            sync_report_b = yum_fetch_b.fetchYumRepo(temp_dir_b)
            self.assertTrue(sync_report_b.successes > 0)
            synced_rpms = glob.glob("%s/%s/*.rpm" % (temp_dir_b, temp_label))
            self.assertEquals(len(synced_rpms), sync_report_b.successes)
        finally:
            shutil.rmtree(temp_dir_a)
            shutil.rmtree(temp_dir_b)
