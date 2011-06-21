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
import os
import sys
import unittest
import hashlib

srcdir = os.path.abspath(os.path.dirname(__file__)) + "/../../src/"
sys.path.insert(0, srcdir)

from grinder import GrinderUtils

class TestGrinderUtils(unittest.TestCase):

    def clean(self):
        pass

    def setUp(self):
        self.data_dir =os.path.join(os.path.abspath(os.path.dirname(__file__)), "..", "data")
        self.clean()

    def tearDown(self):
        self.clean()

    def testGetKeyFromPEM(self):
        expected_key_file = os.path.join(self.data_dir, "sample_cert.key")
        test_pem_file = os.path.join(self.data_dir, "sample_cert.pem")
        expected_key = open(expected_key_file).read().rstrip()
        test_pem = open(test_pem_file).read()
        key = GrinderUtils.getKeyFromPEM(test_pem)
        print "Found key = <%s>" % (key)
        print "Expected key = <%s>" % (expected_key)
        #print "key == expected key, <%s>" % (key == expected_key)
        self.assertEqual(key, expected_key)
        
    def testGetCertFromPEM(self):
        expected_cert_file = os.path.join(self.data_dir, "sample_cert.cert")
        test_pem_file = os.path.join(self.data_dir, "sample_cert.pem")
        expected_cert = open(expected_cert_file).read().rstrip()
        test_pem = open(test_pem_file).read()
        cert = GrinderUtils.getCertFromPEM(test_pem)
        #print "Found cert = <%s>" % (cert)
        #print "Expected cert = <%s>" % (expected_cert)
        #print "cert == expected cert, <%s>" % (cert == expected_cert)
        self.assertEqual(cert, expected_cert)
        