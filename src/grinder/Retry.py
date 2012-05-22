#
# Copyright (c) 2012 Red Hat, Inc.
#
# Module to fetch content from yum repos
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
import time
import logging

LOG = logging.getLogger("grinder.Retry")

class Retry(object):
    """
    Decorator to add retry logic
    """
    def __init__(self, retries=5, delay=5):
        """
        @param retries number of attempts before stopping and raising the last exception
        @type retries int

        @param delay initial number of seconds to wait before retry
        @type delay int
        """
        self.retries = retries
        self.delay = delay

    def __call__(self, f):
        def wrapped(*args):
            attempt = 0
            while True:
                try:
                    return f(*args)
                except Exception, e:
                    attempt += 1
                    if attempt > self.retries:
                        raise e
                    time_to_sleep = self.delay * attempt
                    LOG.error("Attempt %s: Caught exception: %s. Will retry in %s seconds." % (attempt, e, time_to_sleep))
                    time.sleep(time_to_sleep)
        return wrapped
