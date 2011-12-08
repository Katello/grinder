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
import os
import fcntl
import logging
LOG = logging.getLogger("grinder.GrinderLock")

class GrinderLock(object):
    """
     Class to acquire, release and validate a pid lock file
    """
    def __init__(self, lockfile, pid=None):
        self.lockfile = lockfile
        self.pid = pid or os.getpid()

    def acquire(self):
        """
        Acquire a lock and write the pid to lockfile
        """
        existing_pid = self.readlock() 
        if existing_pid:
            if self.isvalid(existing_pid):
                # NO-OP
                LOG.debug("A lock with pid [%s] already exists at [%s]" % (self.pid, self.lockfile))
                return
            else:
                # process is not valid, clean it up to acquire another lock
                self.release()
        self.lockfd = os.open(self.lockfile, os.O_RDWR|os.O_CREAT|os.O_SYNC)
        try:
            fcntl.flock(self.lockfd, fcntl.LOCK_EX|fcntl.LOCK_NB)
        except Exception, e:
            LOG.debug("A lock already exists on this file descriptor with pid %s" % self.pid)
            raise 
        fcntl.fcntl(self.lockfd, fcntl.F_SETFD, 1) 
        # write the pid to the lock file
        if not self.readlock() or (self.readlock() and self.readlock == str(self.pid)):
            os.ftruncate(self.lockfd, 0)
            os.write(self.lockfd, str(self.pid) + '\n')

    def release(self):
        """
        Release a lock file
        """
        try:
            if hasattr(self, 'lockfd'):
                try:
                    fcntl.flock(self.lockfd, fcntl.LOCK_UN)
                finally:
                    os.close(self.lockfd)
            if os.path.exists(self.lockfile):
                os.unlink(self.lockfile)
        except Exception,e:
            LOG.error("unable to release lock due to error %s" % str(e))

    def readlock(self):
        """
        read the existing lock file and return back a pid
        """
        if not os.path.exists(self.lockfile):
            return None
        try:
            try:
                fd = open(self.lockfile)
                pid = fd.read().rstrip()
                return pid
            finally:
                fd.close()
        except:
            return None

    def isvalid(self, pid=None):
        """
         validate if an process id is alive
        """
        return os.path.exists("/proc/%s" % (pid or self.pid))

if __name__ == '__main__':
    L = GrinderLock('/tmp/test.lock')
    print "lock acquired ", L.acquire()
    print "lock file has pid %s" % L.readlock()
    print "Is process valid : %s" % L.isvalid()
    print "lock released ", L.release()

