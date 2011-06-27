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

"""
Active Object classes.
"""

import sys
import atexit
import cPickle as pickle
import traceback as tb
from subprocess import Popen, PIPE
    

class Method:
    """
    Remote method stub.
    @ivar name: The method name.
    @type name: str
    @ivar object: An active object.
    @type object: L{ActiveObject}
    """
    
    def __init__(self, name, object):
        """
        @param name: The method name.
        @type name: str
        @param object: An active object.
        @type object: L{ActiveObject}
        """
        self.name = name
        self.object = object
    
    def __call__(self, *args, **kwargs):
        """
        Method invocation using the active object.
        @param args: The argument list.
        @type args: list
        @param kwargs: The kwargs argument dict.
        @type kwargs: dict
        """
        return self.object(self, *args, **kwargs)
    

class ActiveObject:
    """
    An remote (active) object.
    Methods invoked in a child process.
    @ivar object: The wrapped object.
    @type object: object
    @ivar __child: The child process.
    @type __child: Process
    """

    def __init__(self, object):
        """
        @param object: A I{real} object whos methods are invoked
            in the child process.
        @type object: object
        """
        self.object = object
        atexit.register(self.__kill)
        self.__child = None
        self.__spawn()
        
    def __call(self, method, *args, **kwargs):
        """
        Method invocation.
        The active object, method name and arguments are picked and
        sent to the child on the stdin pipe.  Then, the result is read
        on the child stdout pipe. A code=0 indicates that the retval is
        a normal returned value.  A code=1 indicates that the retval is
        a rasied exception which is raised.
        @param method: The method name.
        @type method: str
        @param args: The argument list.
        @type args: list
        @param kwargs: The kwargs argument dict.
        @type kwargs: dict
        """
        p = self.__child
        call = (self.object, method, args, kwargs)
        pickle.dump(call, p.stdin)
        p.stdin.flush()
        code, retval, state = pickle.load(p.stdout)
        if code == 0:
            setstate(self.object, state)
            return retval
        else:
            raise Exception(retval)
    
    def __spawn(self):
        """
        Spawn the child process.
        """
        self.__child = Popen(
            (sys.executable, '-m', __name__),
            close_fds=True,
            stdin=PIPE,
            stdout=PIPE)
        
    def __kill(self):
        """
        Kill the child process.
        """
        if self.__child:
            self.__child.kill()
            self.__child.wait()
            self.__child = None
    
    def __call__(self, method, *args, **kwargs):
        """
        Method invocation.
        An IOError indictes a broken pipe(s) between the parent and
        child process.  This usually indicates that the child has terminated.
        For robustness, we respawn the child and try again.
        @param args: The argument list.
        @type args: list
        @param kwargs: The kwargs argument dict.
        @type kwargs: dict
        """
        retry = 3
        while True:
            try:
                return self.__call(method.name, *args, **kwargs)
            except IOError, e:
                if retry:
                    self.__kill()
                    self.__spawn()
                    retry -= 1
                else:
                    raise e
    
    def __getattr__(self, name):
        """
        @return: A method stub.
        @rtype: L{Method}
        """
        if name.startswith('__') and name.endswith('__'):
            return self.__dict__[name]
        return Method(name, self)
    
    def __del__(self):
        """
        Clean up the child process.
        """
        self.__kill()    

    
def process():
    """
    Reads and processes RMI requests.
     Input: (object, method, *args, **kwargs)
    Output: (status, retval, state)
        status (0=return value, 1=exception).
        retval (returned value | exception )
    """
    code = 0
    state = {}
    try:
        call = pickle.load(sys.stdin)
        object = call[0]
        method = getattr(object, call[1])
        args = call[2]
        kwargs = call[3]
        retval = method(*args, **kwargs)
        state = getstate(object)
    except:
        code = 1
        info = sys.exc_info()
        retval = '\n'.join(tb.format_exception(*info))
    result = (code, retval, state)
    pickle.dump(result, sys.stdout)
    sys.stdout.flush()
    
def getstate(object):
    M = '__getstate__'
    if hasattr(object, M):
        m = getattr(object, M)
        return m()
    else:
        return object.__dict__
    
def setstate(object, state):
    M = '__setstate__'
    if hasattr(object, M):
        m = getattr(object, M)
        m(state)
    else:
        object.__dict__.update(state)

def main():
    while True:
        process()
    
if __name__ == '__main__':
    main()