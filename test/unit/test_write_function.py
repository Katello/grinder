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
import pycurl
import os
import sys
import unittest
from grinder.WriteFunction import WriteFunction
srcdir = os.path.abspath(os.path.dirname(__file__)) + "/../../src/"
sys.path.insert(0, srcdir)


class TestWriteFunction(unittest.TestCase):

    def clean(self):
        pass

    def setUp(self):
        self.clean()

    def tearDown(self):
        self.clean()

    def init_curl(self, url):
        self.curl = curl = pycurl.Curl()
        self.curl.setopt(curl.URL, url)
        
    def test_write_function(self):
        file_path = "/tmp/test.iso"
        if os.path.exists(file_path):
            os.remove(file_path)
        wf = WriteFunction(file_path, 3244032)
        self.init_curl("http://repos.fedorapeople.org/repos/pulp/pulp/demo_repos/test_file_repo/test3.iso")
        self.curl.setopt(pycurl.RESUME_FROM, wf.offset)
        self.curl.setopt(self.curl.BUFFERSIZE, 10240)
        self.curl.setopt(self.curl.WRITEFUNCTION, wf.callback)
        self.curl.perform()
        wf.cleanup()
        print os.stat(file_path).st_size
        assert(os.path.exists("/tmp/test.iso"))
        assert(3244032 == os.stat("/tmp/test.iso").st_size)