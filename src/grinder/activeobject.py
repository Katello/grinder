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
Protocol:
 Codes:
     0 = Normal Return
         retval = returned value
     1 = Raised Exception
         retval = raised exception
     2 = Logging Request
         retval = log-record
     3 = Parent Method (back) Invocation
         (child->parent)
 Record:
   call  = (object, method, *args, **kwargs)
   log   = (logger, level, msg, args)
   reply = (code, retval, state)
"""

import os
import sys
import errno
import logging
import inspect
import cPickle as pickle
import traceback as tb
from subprocess import Popen, PIPE
from threading import RLock
from signal import SIGTERM


# codes
RETURN = 0
EXCEPTION = 1
LOG = 2
PMETHOD = 3


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

    def abort(self):
        """
        Abort (kill) the active object child process.
        """
        self.object._ActiveObject__kill()

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
    @ivar __mutex: Mutex to ensure serial RMI.
    @type __mutex: RLock
    """

    def __init__(self, object, *pmethods):
        """
        @param object: A I{real} object whos methods are invoked
            in the child process.
        @type object: object
        """
        self.object = object
        self.__pmethods = pmethods
        self.__child = None
        self.__mutex = RLock()
        self.__spawn()
        
    def __rmi(self, method, args, kwargs):
        """
        Remote Method invocation.
        The active object, method name and arguments are pickled and
        sent to the child on the stdin pipe.  Then, the result is read
        on the child stdout pipe.  See: Protocol.
        @param method: The method name.
        @type method: str
        @param args: The argument list.
        @type args: list
        @param kwargs: The kwargs argument dict.
        @type kwargs: dict
        """
        p = self.__child
        call = ((self.object, self.__pmethods), method, args, kwargs)
        pickle.dump(call, p.stdin)
        p.stdin.flush()
        while True:
            packet = pickle.load(p.stdout)
            code = packet[0]
            if code == RETURN:
                retval = packet[1]
                state = packet[2]
                setstate(self.object, state)
                return retval
            if code == EXCEPTION:
                ex = packet[1]
                if isinstance(ex, Exception):
                    raise ex
                raise Exception(ex)
            if code == LOG:
                lr = packet[1]
                self.__logchild(*lr)
                continue
            if code == PMETHOD:
                call = packet[1]
                self.__pmethod(*call)
                continue
                
    def __logchild(self, name, level, fmt, args):
        """
        Perform child logging request
        @param name: The logger name.
        @type name: str
        @param level: The logging level.
        @type level: str
        @param fmt: The log message
        @type fmt: str
        @param args: The argument list
        @type args: list
        """
        log = logging.getLogger(name)
        method = getattr(log, level)
        method(fmt, *args)
        
    def __pmethod(self, method, args, kwargs):
        """
        Invoke a parent (back) method.
        This is a method invoked on the parent from the object in
        the child.  Mainly supports callbacks.
        @param method: A method name.
        @type method: str
        @param args: The method arglist.
        @type args: list
        @type kwargs: The keyword arguments.
        @type kwargs: dict
        @return: None
        """
        method = getattr(self.object, method)
        method(*args, **kwargs)

    def __spawn(self):
        """
        Spawn the child process.
        """
        args = [
            sys.executable,
            __file__,]
        args.extend(sys.path)
        self.__child = Popen(args, close_fds=True, stdin=PIPE, stdout=PIPE)
        
    def __respawn(self):
        """
        Respawn the child process.
        """
        self.__kill()
        self.__spawn()

    def __kill(self):
        """
        Kill the child process and close pipes.
        Does not use Popen.kill() for python 2.4 compat.
        Need to call Popen.wait() so Popen object is not placed in
        the subprocess._active in Popen.__del__().
        """
        if not self.__child:
            return
        p = self.__child
        self.__child = None
        kill(p.pid)
        self.__lock()
        try:
            p.stdin.close()
            p.stdout.close()
            p.wait()
        finally:
            self.__unlock()

    def __lock(self):
        """
        Lock the object.
        """
        self.__mutex.acquire()

    def __unlock(self):
        """
        Unlock the object.
        """
        self.__mutex.release()

    def __call(self, method, args, kwargs):
        """
        Method invocation.
        An IOError indictes a broken pipe(s) between the parent and
        child process.  This usually indicates that the child has terminated.
        For robustness, we respawn the child and try again.  An EOFError|ValueError
        and a __child = None, indicates the child was killed through the
        parent __kill().
        @param args: The argument list.
        @type args: list
        @param kwargs: The kwargs argument dict.
        @type kwargs: dict
        """
        retry = 3
        while True:
            try:
                return self.__rmi(method.name, args, kwargs)
            except (EOFError, ValueError):
                if not self.__child:
                    break # killed/aborted
            except IOError:
                if retry:
                    self.__respawn()
                    retry -= 1
                else:
                    raise
    
    def __call__(self, method, *args, **kwargs):
        """
        Method invocation.
        Mutexed to ensure serial access to the child.
        @param args: The argument list.
        @type args: list
        @param kwargs: The kwargs argument dict.
        @type kwargs: dict
        """
        self.__lock()
        try:
            if not self.__child:
                self.__spawn()
            return self.__call(method, args, kwargs)
        finally:
            self.__unlock()

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
        
        
class ParentMethod:
    """
    A method that is invoked in the parent process.
    @ivar name: The method name.
    @type name: str
    """
    
    def __init__(self, name):
        """
        @param name: The method name.
        @type name: str
        """
        self.name = name
    
    def __call__(self, *args, **kwargs):
        """
        Send the PMETHOD record to the parent process.
        """
        call = (self.name, args, kwargs)
        pmethod = (3, call)
        pickle.dump(pmethod, sys.stdout)
    
    
class Logger:
    """
    The remote logging proxy.
    @ivar name: The logger name.
    @type name: str
    """
    
    def __init__(self, name):
        """
        @param name: The logger name.
        @type name: str
        """
        self.name = name

    class Method:
        
        def __init__(self, logger, name):
            """
            The logging method stub.
            @param logger: The logger object.
            @type logger: L{Logger}
            @param name: The method name (level).
            @type name: str
            """
            self.logger = logger
            self.name = name
        
        def __call__(self, msg, *args, **kwargs):
            """
            Invoke the logging call.
            @param msg: The message to log.
            @type msg: object
            @param args: The argument list
            @type args: tuple
            @param kwargs: The keyword args.
            @type kwargs: dict
            """
            msg, args = \
                self.__processArgs(msg, args)
            msg, kwargs = \
                self.__processKeywords(msg, kwargs)  
            self.__send(msg, args)
            
        def __processArgs(self, msg, args):
            """
            Process the arguments.
            When (msg) is an exception, replace it with formatted
            message and concattenated traceback.
            @param msg: The message argument.
            @type msg: object
            @param args: The argument list.
            @type args: tuple
            @return: The processed (msg, args)
            @rtype: tuple 
            """
            arglist = []
            if isinstance(msg, Exception):
                msg = '\n'.join((str(msg), trace()))
            for arg in args:
                if isinstance(arg, Exception):
                    arg = '\n'.join((str(arg), trace()))
                arglist.append(arg)
            return (msg, arglist)
            
        def __processKeywords(self, msg, keywords):
            """
            Process the keyword arguments.
            When 'exc_info' is True, append the traceback information
            to the returned message.
            @param msg: The message argument.
            @type msg: object
            @param keywords: The keyword arguments.
            @type keywords: dict
            @return: The processed (msg, kwargs)
            @rtype: tuple 
            """
            exflag = keywords.pop('exc_info', 0)
            if exflag:
                msg = '\n'.join((msg, trace()))
            return (msg, keywords)
            
        def __send(self, msg, args):
            """
            Send the logging request to the parent.
            @type msg: object
            @param args: The argument list.
            @type args: tuple
            """
            lr = (self.logger.name,
                  self.name, 
                  msg,
                  args)
            pickle.dump((LOG, lr, {}), sys.stdout)
            sys.stdout.flush()
            
    def __getattr__(self, name):
        """
        @return: The method stub.
        @rtype: L{Logger.Method}
        """
        return self.Method(self, name)

    
def process():
    """
    Reads and processes RMI requests.
     Input: (object, method, args, kwargs)
    Output: (code, retval, state)
    See: Protocol.
    """
    code = RETURN
    state = {}
    try:
        call = pickle.load(sys.stdin)
        object, pmethods = call[0]
        setpmethods(object, pmethods)
        method = getattr(object, call[1])
        args = call[2]
        kwargs = call[3]
        retval = method(*args, **kwargs)
        state = getstate(object)
    except Exception, e:
        code = EXCEPTION
        retval = e
    result = (code, retval, state)
    pickle.dump(result, sys.stdout)
    sys.stdout.flush()
    
def getstate(object):
    key = '__getstate__'
    if hasattr(object, key):
        method = getattr(object, key)
        return cleaned(method())
    else:
        return cleaned(object.__dict__)
    
def cleaned(state):
    d = {}
    for k,v in state.items():
        if not isinstance(v, ParentMethod):
            d[k] = v
    return d
    
def setstate(object, state):
    key = '__setstate__'
    if hasattr(object, key):
        method = getattr(object, key)
        method(state)
    else:
        object.__dict__.update(state)
        
def trace():
    info = sys.exc_info()
    return '\n'.join(tb.format_exception(*info))

def kill(pid, sig=SIGTERM):
    try:
        os.kill(pid, sig)
    except OSError, e:
        if e.errno != errno.ESRCH:
            raise e

def setpmethods(object, names):
    for name in names:
        pmethod = ParentMethod(name)
        setattr(object, name, pmethod)

def addpath(path):
    for p in path:
        if p in sys.path:
            continue
        sys.path.append(p)

def main():
    addpath(sys.argv[1:])
    logging.getLogger = Logger
    while True:
        try:
            process()
        except IOError, ioe:
            # This is added to silence the exceptions printed when a process ends unexpectedly
            if ioe.errno == 32:
                break
            raise
if __name__ == '__main__':
    main()
