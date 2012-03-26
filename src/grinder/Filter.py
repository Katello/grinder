# This software is licensed to you under the GNU General Public
# License as published by the Free Software Foundation; either version
# 2 of the License (GPLv2) or (at your option) any later version.
# There is NO WARRANTY for this software, express or implied,
# including the implied warranties of MERCHANTABILITY,
# NON-INFRINGEMENT, or FITNESS FOR A PARTICULAR PURPOSE. You should
# have received a copy of GPLv2 along with this software; if not, see
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.

# Filters for syncing remote package repos
# Grabbed from Pulp by John Morris <john@zultron.com>
#
# This is the main feature I wanted Pulp for; my use case is downloading
# a limited set of packages (whitelist) from a repo without having
# to sync the whole thing, but still retain yum's smarts for grabbing new
# versions and removing old ones


import re
import logging

LOG = logging.getLogger("grinder.Filter")

class Filter(object):
    """
    Class represents a 'blacklist' or 'whitelist' filter type that can be
    applied when syncing a local repository

    regex_list is a list of regex strings to be applied to package 'filename'
    (see below); if any regex matches, the Filter.test() will be true for
    whitelists or false for blacklists

    use the set_regex_list method to change the regex list after object
    creation; this ensures that the regexes are compiled

    (actually, using 'filename' seems hackish, but it's easy to do from
    the command line and with simple regexes)
    (more hackish still, because the closest yum.packages.PackageObject 
    appears to have to a 'filename' is its '__str__()', used instead
    of some actual RPM filename)
    """
    def __init__(self, type, regex_list=None, description=None):
        self.description = description
        self.type = type
        self.set_regex_list(regex_list)

    def __str__(self):
        return "%s, %s filter with: %s" % (self.description, self.type, self.regex_list)

    def set_regex_list(self,regex_list):
        """
        Set the list of regexes & list of compiled regexes
        """
        self.regex_list = []
        self.regex_obj_list = []
        if not regex_list:
            return
        for regex in regex_list:
            self.regex_list.append(regex)
            self.regex_obj_list.append(re.compile(regex))

    def iswhitelist(self):
        """
        return true if self is a whitelist
        """
        return self.type == "whitelist"

    def isblacklist(self):
        """
        return true if self is a blacklist
        """
        return self.type == "blacklist"

    def test(self, pkg):
        """
        return pkg if pkg passes through the filter, else None

        pkg is a yum package object
        """

        # the string we match against
        pkg_filename = str(pkg)

        # compare pkg to each regex & break if there's a match
        match_result = None
        for regex_obj in self.regex_obj_list:
            if regex_obj.match(pkg_filename):
                match_result = regex_obj.pattern
                break

        # return result based on match and filter type
        if self.iswhitelist():
            if match_result:
                LOG.debug ("package %s:  passed whitelist, matched %s" %
                           (pkg_filename, match_result))
                return pkg
            else:
                LOG.debug ("package %s:  blocked by whitelist" % pkg_filename)
                return None
        else:
            if match_result:
                LOG.debug ("package %s:  blocked by blacklist, matched %s" %
                           (pkg_filename, match_result))
                return None
            else:
                LOG.debug ("package %s:  passed blacklist" % pkg_filename)
                return pkg
