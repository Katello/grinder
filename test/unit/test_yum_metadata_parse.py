
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

import os
import shutil
import sys
import tempfile
import unittest

srcdir = os.path.abspath(os.path.dirname(__file__)) + "/../../src/"
sys.path.insert(0, srcdir)

from grinder.YumInfo import YumMetadataObj, YumInfo

class TestYumMetadataParse(unittest.TestCase):

    def clean(self):
        pass

    def setUp(self):
        self.clean()

    def tearDown(self):
        self.clean()

    def test_basic_metadata_parse(self):
        test_url = "http://repos.fedorapeople.org/repos/pulp/pulp/testing/6Server/i386/"
        temp_label = "temp_label"
        temp_dir = tempfile.mkdtemp()
        try:
            yum_metadata_obj = YumMetadataObj(temp_label, test_url)
            pkglist = yum_metadata_obj.getDownloadItems(repo_dir=temp_dir)
            self.assertTrue(len(pkglist) > 0)
        finally:
            shutil.rmtree(temp_dir)

    def ignore_test_basic_yum_info_parse(self):
        test_url = "http://repos.fedorapeople.org/repos/pulp/pulp/testing/6Server/i386/"
        temp_label = "temp_label"
        yum_info = None
        temp_dir = os.path.join(tempfile.mkdtemp(), temp_label)
        try:
            yum_info = YumInfo(temp_label, test_url, repo_dir=temp_dir)
            yum_info.setUp()
            self.assertTrue(len(yum_info.rpms) > 0)
        finally:
            shutil.rmtree(temp_dir)


