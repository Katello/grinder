#
# Copyright (c) 2011 Red Hat, Inc.
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

import os
import shutil
import errno
import logging
from random import random
from logging import getLogger

log = getLogger(__name__)


class TmpDir:
    """
    Smart temporary directory.
    @cvar ROOT: The root directory.
    @type ROOT: str
    @ivar tmp: The random subdir.
    @type tmp: str
    """
    
    ROOT = '/tmp/grinder'
    INFO = '.info'
    
    @classmethod
    def clean(cls):
        """
        Delete directories for processes that are
        no longer running.
        @return: The number of directories deleted.
        @rtype: int
        """
        if not os.path.exists(cls.ROOT):
            return 0
        deleted = 0
        for fn in os.listdir(cls.ROOT):
            path = os.path.join(cls.ROOT, fn)
            if os.path.isfile(path):
                continue
            pid = int(fn)
            if cls.running(pid):
                continue
            cls.rmdir(path)
            deleted += 1
        return deleted
    
    @classmethod
    def running(cls, pid):
        """
        Get whether a process is running by pid.
        @param pid: A process id.
        @type pid: int
        @return: True if pid is valid.
        @rtype: bool
        """
        valid = True
        try:
            os.kill(pid, 0)
        except OSError, e:
            if e.errno == errno.ESRCH:
                valid = False
            elif e.errno == errno.EPERM:
                pass
            else:
                raise
        return valid
    
    @classmethod
    def rmdir(cls, path):
        try:
            shutil.rmtree(path)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise
    
    @classmethod
    def mkdir(cls, path, mode=None):
        try:
            os.makedirs(path)
            if mode:
                os.chmod(path, 0777)
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise

    @classmethod
    def random(cls):
        n = int(random()*0xFFFFFFFFF)
        s = hex(n).upper()
        return s[2:]

    def __init__(self):
        self.mkdir(self.ROOT, 0777)
        self.subdir = self.random()
        
    def path(self):
        """
        Get the fully qualified directory path.
        @return: The path.
        @rtype: str
        """
        pid = os.getpid()
        return os.path.join(self.ROOT, str(pid), self.subdir)
    
    def create(self, info=None):
        """
        Create the directory tree and write information
        when specified.
        @param info: Optional info written to <dir>/.info.
        @type info: str
        @return: The path created.
        @rtype: str
        @raise OSError: On failure.
        """
        path = self.path()
        self.mkdir(path)
        self.write(path, info)
        return path

    def delete(self):
        """
        Delete the leaf directory for the label.
        The <pid> directory is not deleted as it may be in use
        by other threads.  It can be cleaned up by clean().
        """
        self.rmdir(self.path())

    def write(self, path, info):
        if not info:
            return
        fpath = os.path.join(path, self.INFO)
        f = open(fpath, 'w')
        try:
            f.write(info)
        finally:
            f.close()

    def __str__(self):
        return self.path()
    
    def __del__(self):
        self.delete()
