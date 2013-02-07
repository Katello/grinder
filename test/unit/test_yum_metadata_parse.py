
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

DATA_DIR="../data"

class TestYumMetadataParse(unittest.TestCase):

    def clean(self):
        pass

    def setUp(self):
        self.clean()

    def tearDown(self):
        self.clean()

    def test_basic_metadata_parse(self):
        test_url = "http://repos.fedorapeople.org/repos/pulp/pulp/v1/testing/6Server/i386/"
        temp_label = "temp_label"
        temp_dir = tempfile.mkdtemp()
        try:
            yum_metadata_obj = YumMetadataObj(temp_label, test_url)
            pkglist = yum_metadata_obj.getDownloadItems(repo_dir=temp_dir)
            self.assertTrue(len(pkglist) > 0)
        finally:
            shutil.rmtree(temp_dir)

    def ignore_test_basic_yum_info_parse(self):
        test_url = "http://repos.fedorapeople.org/repos/pulp/pulp/v1/testing/6Server/i386/"
        temp_label = "temp_label"
        yum_info = None
        temp_dir = os.path.join(tempfile.mkdtemp(), temp_label)
        try:
            yum_info = YumInfo(temp_label, test_url, repo_dir=temp_dir)
            yum_info.setUp()
            self.assertTrue(len(yum_info.rpms) > 0)
        finally:
            shutil.rmtree(temp_dir)


    def test_xml_base_attribute_of_package_metadata(self):
        repo_dir = os.path.abspath(os.path.join(DATA_DIR, "repo_separate_pkg_dir"))
        test_url = "file://%s" % (repo_dir)
        temp_label = "temp_label"
        temp_dir = tempfile.mkdtemp()
        try:
            yum_metadata_obj = YumMetadataObj(temp_label, test_url)
            pkglist = yum_metadata_obj.getDownloadItems(repo_dir=temp_dir)
            self.assertTrue(pkglist.has_key("rpms"))
            self.assertEquals(len(pkglist["rpms"]), 2)
            pkg = pkglist["rpms"][0]
            print "pkg = %s" % (pkg)
            self.assertTrue(pkg.has_key("repodata"))
            self.assertTrue(pkg["repodata"].has_key("primary"))
            primary_xml_snippet = pkg["repodata"]["primary"]
            loc_start = primary_xml_snippet.find("<location")
            loc_end = primary_xml_snippet.find("/>", loc_start) + 2
            location = primary_xml_snippet[loc_start:loc_end]
            self.assertTrue(location)
            xml_base_index = location.find("xml:base")
            self.assertEquals(xml_base_index, -1)
        finally:
            shutil.rmtree(temp_dir)

    def test_rpm_changelog_files_data(self):
        repo_dir = os.path.abspath(os.path.join(DATA_DIR, "repo_separate_pkg_dir"))
        test_url = "file://%s" % (repo_dir)
        temp_label = "temp_label"
        temp_dir = tempfile.mkdtemp()
        try:
            yum_metadata_obj = YumMetadataObj(temp_label, test_url)
            pkglist = yum_metadata_obj.getDownloadItems(repo_dir=temp_dir)
            self.assertTrue(pkglist.has_key("rpms"))
            self.assertEquals(len(pkglist["rpms"]), 2)
            pkg = pkglist["rpms"][0]
            self.assertTrue(pkg.has_key("changelog"))
            self.assertTrue(pkg.has_key("filelist"))
            self.assertTrue(pkg.has_key("files"))
            self.assertEquals(len(pkg['changelog']), 2)
            self.assertEquals(len(pkg['filelist']), 1)
            self.assertEquals(len(pkg['files']), 1)
        finally:
            shutil.rmtree(temp_dir)

    def test_change_location_tag(self):
        yum_metadata_obj = YumMetadataObj("test_label", "unused_url")
        orig_xml = """
  <package type="rpm">
  <name>pulp-test-package</name>
  <arch>x86_64</arch>
  <version epoch="0" ver="0.3.1" rel="1.fc11"/>
  <checksum type="sha256" pkgid="YES">6bce3f26e1fc0fc52ac996f39c0d0e14fc26fb8077081d5b4dbfb6431b08aa9f</checksum>
  <summary>Test package</summary>
  <description>Test package.  Nothing to see here.</description>
  <packager></packager>
  <url>https://fedorahosted.org/pulp/</url>
  <time file="1355411601" build="1273087488"/>
  <size package="2216" installed="5" archive="268"/>
<location xml:base="file:///git/grinder/test/data/repo_separate_pkg_dir/Packages" href="pulp-test-package-0.3.1-1.fc11.x86_64.rpm"/>
  <format>
    <rpm:license>MIT</rpm:license>
    <rpm:vendor/>
    <rpm:group>Development/Libraries</rpm:group>
    <rpm:buildhost>gibson</rpm:buildhost>
    <rpm:sourcerpm>pulp-test-package-0.3.1-1.fc11.src.rpm</rpm:sourcerpm>
    <rpm:header-range start="280" end="2092"/>
    <rpm:provides>
      <rpm:entry name="config(pulp-test-package)" flags="EQ" epoch="0" ver="0.3.1" rel="1.fc11"/>
      <rpm:entry name="pulp-test-package" flags="EQ" epoch="0" ver="0.3.1" rel="1.fc11"/>
      <rpm:entry name="pulp-test-package(x86-64)" flags="EQ" epoch="0" ver="0.3.1" rel="1.fc11"/>
    </rpm:provides>
    <file>/etc/pulp-test-file.txt</file>
  </format>
</package>
        """

        expected_xml = """
  <package type="rpm">
  <name>pulp-test-package</name>
  <arch>x86_64</arch>
  <version epoch="0" ver="0.3.1" rel="1.fc11"/>
  <checksum type="sha256" pkgid="YES">6bce3f26e1fc0fc52ac996f39c0d0e14fc26fb8077081d5b4dbfb6431b08aa9f</checksum>
  <summary>Test package</summary>
  <description>Test package.  Nothing to see here.</description>
  <packager></packager>
  <url>https://fedorahosted.org/pulp/</url>
  <time file="1355411601" build="1273087488"/>
  <size package="2216" installed="5" archive="268"/>
<location href="pulp-test-package-0.3.1-1.fc11.x86_64.rpm"/>
  <format>
    <rpm:license>MIT</rpm:license>
    <rpm:vendor/>
    <rpm:group>Development/Libraries</rpm:group>
    <rpm:buildhost>gibson</rpm:buildhost>
    <rpm:sourcerpm>pulp-test-package-0.3.1-1.fc11.src.rpm</rpm:sourcerpm>
    <rpm:header-range start="280" end="2092"/>
    <rpm:provides>
      <rpm:entry name="config(pulp-test-package)" flags="EQ" epoch="0" ver="0.3.1" rel="1.fc11"/>
      <rpm:entry name="pulp-test-package" flags="EQ" epoch="0" ver="0.3.1" rel="1.fc11"/>
      <rpm:entry name="pulp-test-package(x86-64)" flags="EQ" epoch="0" ver="0.3.1" rel="1.fc11"/>
    </rpm:provides>
    <file>/etc/pulp-test-file.txt</file>
  </format>
</package>
        """
        relpath = "./Packages/pulp-test-package-0.3.1-1.fc11.x86_64.rpm"
        mod_xml = yum_metadata_obj.change_location_tag(orig_xml, relpath)
        print "Found:\n%s" % (mod_xml)
        print "Expected:\n%s" % (expected_xml)
        for index in range(0, len(mod_xml)):
            if mod_xml[index] != expected_xml[index]:
                print "Problem at index '%s' with char '%s' != '%s'" % (index, mod_xml[index], expected_xml[index])
        self.assertEquals(mod_xml, expected_xml)
