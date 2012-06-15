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
GRINDER_DEFAULT_NUM_RETRIES = 5
GRINDER_DEFAULT_SECONDS_DELAY = 5

class Retry(object):
    """
    Decorator to add retry logic
    """
    def __init__(self, retries=None, delay=None):
        """
        @param retries number of attempts before stopping and raising the last exception
        @type retries int

        @param delay initial number of seconds to wait before retry
        @type delay int
        """
        self.retries = retries
        self.delay = delay

    def get_num_retries(self, *args):
        caller_self = None
        if len(args) >= 1:
            caller_self = args[0]
        # 1st priority get retries/delay from decorator invocation
        # 2nd priority see if the invoking object, or the first param to the object has num_retries or retry_delay set
        # 3rd default to global defaults
        # ...remember num_retries=0 is valid, so test for None....not just true/false
        num_retries = self.retries
        if num_retries is not None and caller_self:
            if hasattr(caller_self, "num_retries"):
                num_retries = getattr(caller_self, "num_retries")
        if num_retries is not None:
            num_retries = GRINDER_DEFAULT_NUM_RETRIES
        return num_retries

    def get_retry_delay(self, *args):
        caller_self = None
        if len(args) >= 1:
            caller_self = args[0]
        # 1st priority get retries/delay from decorator invocation
        # 2nd priority see if the invoking object, or the first param to the object has num_retries or retry_delay set
        # 3rd default to global defaults
        retry_delay = self.delay
        if retry_delay is not None and caller_self:
            if hasattr(caller_self, "retry_delay"):
                retry_delay = getattr(caller_self, "retry_delay")
        if retry_delay is not None:
            retry_delay = GRINDER_DEFAULT_SECONDS_DELAY
        return retry_delay

    def __call__(self, f):
        def wrapped(*args):
            retries = self.get_num_retries(args)
            delay = self.get_retry_delay(args)
            attempt = 0
            while True:
                try:
                    return f(*args)
                except Exception, e:
                    attempt += 1
                    if attempt > retries:
                        raise e
                    time_to_sleep = delay * attempt
                    LOG.error("Attempt %s: Caught exception: %s. Will retry in %s seconds." % (attempt, e, time_to_sleep))
                    time.sleep(time_to_sleep)
        return wrapped
