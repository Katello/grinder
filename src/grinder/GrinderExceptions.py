#
# Copyright (c) 2011 Red Hat, Inc.
#
# This software is licensed to you under the GNU General Public
# License as published by the Free Software Foundation; either version
# 2 of the License (GPLv2) or (at your option) any later version.
# There is NO WARRANTY for this software, express or implied,
# including the implied warranties of MERCHANTABILITY,
# NON-INFRINGEMENT, or FITNESS FOR A PARTICULAR PURPOSE. You should
# have received a copy of GPLv2 along with this software; if not, see
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.
#

class GrinderException(Exception):
    pass

class NoChannelLabelException(GrinderException):
    def __init__(self):
        return
    def __str__(self):
        return "No channel label was specified"

class BadCertificateException(GrinderException):
    def __init__(self):
        return
    def __str__(self):
        return "Unable to read certificate"

class BadSystemIdException(GrinderException):
    def __init__(self):
        return
    def __str__(self):
        return "Unable to authenticate systemid, please ensure your system is registered to RHN"

class CantActivateException(GrinderException):
    def __init__(self):
        return
    def __str__(self):
        return "Your system can not be activated to synch content from RHN Hosted"

class SystemNotActivatedException(GrinderException):
    def __init__(self):
        return
    def __str__(self):
        return "Your system is not activated to sync content from RHN Hosted. Activate system with command: grinder -u username -p password"

class GetRequestException(Exception):
    def __init__(self, url, code):
        self.url = url
        self.code = code
    def __str__(self):
        return "%s : %s" % (self.url, self.code)

