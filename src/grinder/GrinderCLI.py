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
import os
import sys
import optparse
import signal
import socket
import traceback
import logging
from optparse import OptionParser
from grinder import GrinderLog
from grinder.RepoFetch import YumRepoGrinder
from grinder.RHNSync import RHNSync
from grinder.GrinderExceptions import *
from grinder.FileFetch import FileGrinder
from grinder.Filter import Filter

LOG = logging.getLogger("grinder.GrinderCLI")

class CliDriver(object):
    """ Base class for all sub-commands. """
    def __init__(self, name="cli", usage=None, shortdesc=None,
            description=None):
        self.shortdesc = shortdesc
        if shortdesc is not None and description is None:
            description = shortdesc
        self.parser = OptionParser(usage=usage, description=description)
        self._add_common_options()
        self.name = name
        self.killcount = 0

    def _add_common_options(self):
        """ Add options that apply to all sub-commands. """
        self.parser.add_option("--debug", dest="debug",
                action="store_true", help="enable debug logging")

    def _do_command(self):
        """ implement this in sub classes"""
        pass

    def stop(self):
        pass

    def main(self):

        (self.options, self.args) = self.parser.parse_args()
        GrinderLog.setup(self.options.debug)
        self.args = self.args[1:]
        # do the work, catch most common errors here:
        self._do_command()

class RHNDriver(CliDriver):
    def __init__(self):
        usage = "usage: %prog rhn [OPTIONS]"
        shortdesc = "Fetches content from a rhn source."
        desc = "rhn"
        CliDriver.__init__(self, "rhn", usage, shortdesc, desc)
        #GrinderLog.setup(self.debug)
        self.rhnSync = RHNSync()

        self.parser.add_option('-a', '--all', action='store_true', 
                help='Fetch ALL packages from a channel, not just latest')
        self.parser.add_option('-b', '--basepath', action='store', 
                help='Path RPMs are stored')
        self.parser.add_option('-c', '--certfile', action='store', 
                help='Entitlement Certificate')
        self.parser.add_option('-C', '--config', action='store', 
                help='Configuration file')
        self.parser.add_option('-k', '--kickstarts', action='store_true', 
                help='Sync all kickstart trees for channels specified')
        self.parser.add_option('-K', '--skippackages', action='store_true', 
                help='Skip sync of packages', default=False)
        self.parser.add_option('-L', '--listchannels', action='store_true', 
                help='List all channels we have access to synchronize')
        self.parser.add_option('-p', '--password', action='store',
                help='RHN Password')
        self.parser.add_option('-P', '--parallel', action='store',
                help='Number of threads to fetch in parallel.')
        self.parser.add_option('-r', '--removeold', action='store_true', 
                help='Remove older rpms')
        self.parser.add_option('-s', '--systemid', action='store', help='System ID')
        self.parser.add_option('-u', '--username', action='store', help='RHN User Account')
        self.parser.add_option('-U', '--url', action='store', help='Red Hat Server URL')
        self.parser.add_option("--limit", dest="limit",
                          help="Limit bandwidth in KB/sec", default=None)

    def _validate_options(self):
        if self.options.all and self.options.removeold:
            systemExit(1, "Conflicting options specified 'all' and 'removeold'.")
        if self.options.config:
            if not self.rhnSync.loadConfig(self.options.config):
                systemExit(1, "Unable to parse config file: %s" % (self.options.config))
        if self.options.all:
            self.rhnSync.setFetchAllPackages(self.options.all)
            self.rhnSync.setRemoveOldPackages(False)
        if self.options.basepath:
            self.rhnSync.setBasePath(self.options.basepath)
        if self.options.url:
            self.rhnSync.setURL(self.options.url)
        if self.options.username:
            self.rhnSync.setUsername(self.options.username)
        if self.options.password:
            self.rhnSync.setPassword(self.options.password)
        if self.options.certfile:
            cert = open(self.options.certfile, 'r').read()
            self.rhnSync.setCert(cert)
        if self.options.systemid:
            sysid = open(self.options.systemid, 'r').read()
            self.rhnSync.setSystemId(sysid)
        if self.options.parallel:
            self.rhnSync.setParallel(self.options.parallel)
        if self.options.debug:
            self.rhnSync.setVerbose(self.options.debug)
        if self.options.removeold:
            self.rhnSync.setRemoveOldPackages(self.options.removeold)
            self.rhnSync.setFetchAllPackages(False)

    def _do_command(self):
        """
        Executes the command.
        """
        self._validate_options()
        try:
            self.rhnSync.activate()
        except SystemNotActivatedException, e:
            LOG.debug("%s" % (traceback.format_exc()))
            systemExit(1, "System is not activated. Please pass in a valid username/password")
        except CantActivateException, e:
            LOG.debug("%s" % (traceback.format_exc()))
            systemExit(1, "Unable to activate system.")
        except BadSystemIdException, e:
            LOG.debug("%s" % (traceback.format_exc()))
            systemExit(1, "Problem with systemid")
        except BadCertificateException, e:
            LOG.debug("%s" % (traceback.format_exc()))
            systemExit(1, "Problem with satellite certificate")
        except socket.gaierror, e:
            LOG.debug("%s" % (traceback.format_exc()))
            systemExit(1, "Error communicating to: %s\n%s" % (self.rhnSync.getURL(), e))
        except Exception, e:
            LOG.critical("%s" % (traceback.format_exc()))
            systemExit(1, "Unknown error from checking/attempting activation.")
            
        if self.options.listchannels:
            self.rhnSync.displayListOfChannels()
        else:
            # Check command line args for bad channel labels
            badChannels = self.rhnSync.checkChannels(self.args)
            if len(badChannels) > 0:
                LOG.critical("Bad channel labels: %s" % (badChannels))
                systemExit(1, "Please correct the channel labels you entered, then re-run")
            channels = self.rhnSync.getChannelSyncList()
            # Check config file for bad channel labels
            badChannels = self.rhnSync.checkChannels([x['label'] for x in channels])
            if len(badChannels) > 0:
                LOG.critical("Bad channel labels: %s" % (badChannels))
                systemExit(1, "Please correct the channel labels in: %s, then re-run" % (self.options.config))
            basePath = self.rhnSync.getBasePath()
            if not basePath:
                basePath = "./"
            for c in self.args:
                channels.append({'label':c, 'relpath':os.path.join(basePath,c)})
            report = {}
            for info in channels:
                label = info['label']
                savePath = info['relpath']
                report[label] = {}
                if not self.options.skippackages:
                    report[label]["packages"] = self.rhnSync.syncPackages(label, 
                            savePath, self.rhnSync.getVerbose())
                if self.options.kickstarts:
                    report[label]["kickstarts"] = self.rhnSync.syncKickstarts(label, 
                            savePath, self.rhnSync.getVerbose())
            for r in report:
                if report[r].has_key("packages"):
                    print "%s packages = %s" % (r, report[r]["packages"])
                if report[r].has_key("kickstarts"):
                    print "%s kickstarts = %s" % (r, report[r]["kickstarts"])

    def stop(self):
        self.rhnSync.stop()


class RepoDriver(CliDriver):
    parallel = 5
    def __init__(self):
        usage = "usage: %prog yum [OPTIONS]"
        shortdesc = "Fetches content from a yum source."
        desc = "yum"
        CliDriver.__init__(self, "yum", usage, shortdesc, desc)
        #GrinderLog.setup(self.debug)

        self.parser.add_option("--label", dest="label",
                          help="Label for the content fetched from repository URL")
        self.parser.add_option("--limit", dest="limit",
                          help="Limit bandwidth in KB/sec per thread", default=None)
        self.parser.add_option('-U', "--url", dest="url",
                          help="URL to the repository whose content to fetch")
        self.parser.add_option("--cacert", dest="cacert", default=None,
                          help="Path location to CA Certificate.")
        self.parser.add_option("--cert", dest="clicert", default=None,
                          help="Path location to Client SSl Certificate.")
        self.parser.add_option("--key", dest="clikey", default=None,
                          help="Path location to Client Certificate Key.")
        self.parser.add_option("--nosslverify", action="store_true", dest="nosslverify",
                          help="disable ssl verify of server cert")
        self.parser.add_option('-P', "--parallel", dest="parallel",
                          help="Thread count to fetch the bits in parallel. Defaults to 5")
        self.parser.add_option('-b', '--basepath', dest="basepath",
                          help="Directory path to store the fetched content.Defaults to current working directory")
        self.parser.add_option('--proxy_url', dest="proxy_url",
                          help="proxy url, example 'http://172.31.1.1'", default=None)
        self.parser.add_option('--proxy_port', dest="proxy_port",
                          help="proxy port, default is 3128", default='3128')
        self.parser.add_option('--proxy_user', dest="proxy_user",
                          help="proxy username, if auth is required", default=None)
        self.parser.add_option('--proxy_pass', dest="proxy_pass",
                          help="proxy password, if auth is required", default=None)
        self.parser.add_option('--skip_verify_size', action="store_true",
                          help="skip verify size of existing packages")
        self.parser.add_option('--skip_verify_checksum', action="store_true",
                          help="skip verify checksum of existing packages")
        self.parser.add_option('--filter', action="store",
                          help="add a filter, either whitelist or blacklist")
        self.parser.add_option('--filter_regex', action="append",
                          help="add a filter regex; may be use multiple times")

    def _validate_options(self):
        if not self.options.label:
            print("repo label is required. Try --help.")
            sys.exit(-1)

        if not self.options.url:
            print("No Url specified to fetch content. Try --help")
            sys.exit(-1)

        if self.options.parallel:
            self.parallel = self.options.parallel

        if self.options.filter:
            if ((self.options.filter != "whitelist") and 
                (self.options.filter != "blacklist")):
                print("--filter=<type> should be either " +
                      "'whitelist' or 'blacklist'")
                sys.exit(-1)
            if not self.options.filter_regex:
                print("please provide a --filter_regex when using --filter")
                sys.exit(-1)
            LOG.debug("--filter=%s --filter_regex=%s" % 
                      (self.options.filter, 
                       self.options.filter_regex))

    def _do_command(self):
        """
        Executes the command.
        """
        self._validate_options()
        sslverify=1
        if self.options.nosslverify:
            sslverify=0
        limit = None
        if self.options.limit:
            limit = int(self.options.limit)
        verify_options={}
        if self.options.skip_verify_size:
            verify_options["size"] = False
        if self.options.skip_verify_checksum:
            verify_options["checksum"] = False
        if self.options.filter:
            self.options.filter = Filter(self.options.filter, 
                                         regex_list=self.options.filter_regex)
        self.yfetch = YumRepoGrinder(
            self.options.label, self.options.url,
            self.parallel, cacert=self.options.cacert,
            clicert=self.options.clicert,
            clikey=self.options.clikey,
            proxy_url=self.options.proxy_url, 
            proxy_port=self.options.proxy_port,
            proxy_user=self.options.proxy_user,
            proxy_pass=self.options.proxy_pass,
            sslverify=sslverify, max_speed=limit,
            filter=self.options.filter)
        if self.options.basepath:
            self.yfetch.fetchYumRepo(self.options.basepath, verify_options=verify_options)
        else:
            self.yfetch.fetchYumRepo(verify_options=verify_options)

    def stop(self):
        self.yfetch.stop()

class FileDriver(CliDriver):
    parallel = 5
    def __init__(self):
        usage = "usage: %prog file [OPTIONS]"
        shortdesc = "Fetches content from a file source."
        desc = "file"
        CliDriver.__init__(self, "file", usage, shortdesc, desc)
        #GrinderLog.setup(self.debug)

        self.parser.add_option("--label", dest="label",
                          help="Label for the content fetched from repository URL")
        self.parser.add_option("--limit", dest="limit",
                          help="Limit bandwidth in KB/sec per thread", default=None)
        self.parser.add_option('-U', "--url", dest="url",
                          help="URL to the repository whose content to fetch")
        self.parser.add_option("--cacert", dest="cacert", default=None,
                          help="Path location to CA Certificate.")
        self.parser.add_option("--cert", dest="clicert", default=None,
                          help="Path location to Client SSl Certificate.")
        self.parser.add_option("--key", dest="clikey", default=None,
                          help="Path location to Client Certificate Key.")
        self.parser.add_option("--nosslverify", action="store_true", dest="nosslverify",
                          help="disable ssl verify of server cert")
        self.parser.add_option('-P', "--parallel", dest="parallel",
                          help="Thread count to fetch the bits in parallel. Defaults to 5")
        self.parser.add_option('-b', '--basepath', dest="basepath",
                          help="Directory path to store the fetched content.Defaults to current working directory")
        self.parser.add_option('--proxy_url', dest="proxy_url",
                          help="proxy url, example 'http://172.31.1.1'", default=None)
        self.parser.add_option('--proxy_port', dest="proxy_port",
                          help="proxy port, default is 3128", default='3128')
        self.parser.add_option('--proxy_user', dest="proxy_user",
                          help="proxy username, if auth is required", default=None)
        self.parser.add_option('--proxy_pass', dest="proxy_pass",
                          help="proxy password, if auth is required", default=None)
        self.parser.add_option('--skip_verify_size', action="store_true",
                          help="skip verify size of existing packages")
        self.parser.add_option('--skip_verify_checksum', action="store_true",
                          help="skip verify checksum of existing packages")

    def _validate_options(self):
        if not self.options.label:
            print("repo label is required. Try --help.")
            sys.exit(-1)

        if not self.options.url:
            print("No Url specified to fetch content. Try --help")
            sys.exit(-1)

        if self.options.parallel:
            self.parallel = self.options.parallel

    def _do_command(self):
        """
        Executes the command.
        """
        self._validate_options()
        sslverify=1
        if self.options.nosslverify:
            sslverify=0
        limit = None
        if self.options.limit:
            limit = int(self.options.limit)

        verify_options={}
        if self.options.skip_verify_size:
            verify_options["size"] = False
        if self.options.skip_verify_checksum:
            verify_options["checksum"] = False
        self.file_fetch = FileGrinder(self.options.label, self.options.url, \
                                self.parallel, cacert=self.options.cacert, \
                                clicert=self.options.clicert, clikey=self.options.clikey, \
                                proxy_url=self.options.proxy_url,
                                proxy_port=self.options.proxy_port, \
                                proxy_user=self.options.proxy_user, \
                                proxy_pass=self.options.proxy_pass,
                                sslverify=sslverify, max_speed=limit)
        if self.options.basepath:
            self.file_fetch.fetch(self.options.basepath)
        else:
            self.file_fetch.fetch()

    def stop(self):
        self.file_fetch.stop()

# this is similar to how rho does parsing
class CLI:
    def __init__(self):

        self.cli_commands = {}
        for clazz in [ RepoDriver, RHNDriver, FileDriver]:
            cmd = clazz()
            # ignore the base class
            if cmd.name != "cli":
                self.cli_commands[cmd.name] = cmd 


    def _add_command(self, cmd):
        self.cli_commands[cmd.name] = cmd

    def _usage(self):
        print "\nUsage: %s [options] MODULENAME --help\n" % os.path.basename(sys
.argv[0])
        print "Supported modules:\n"

        # want the output sorted
        items = self.cli_commands.items()
        items.sort()
        for (name, cmd) in items:
            print("\t%-14s %-25s" % (name, cmd.shortdesc))
        print("")

    def _find_best_match(self, args):
        """
        Returns the subcommand class that best matches the subcommand specified
        in the argument list. For example, if you have two commands that start
        with auth, 'auth show' and 'auth'. Passing in auth show will match
        'auth show' not auth. If there is no 'auth show', it tries to find
        'auth'.

        This function ignores the arguments which begin with --
        """
        possiblecmd = []
        for arg in args[1:]:
            if not arg.startswith("-"):
                possiblecmd.append(arg)

        if not possiblecmd:
            return None

        cmd = None
        key = " ".join(possiblecmd)
        if self.cli_commands.has_key(" ".join(possiblecmd)):
            cmd = self.cli_commands[key]

        i = -1
        while cmd == None:
            key = " ".join(possiblecmd[:i])
            if key is None or key == "":
                break

            if self.cli_commands.has_key(key):
                cmd = self.cli_commands[key]
            i -= 1

        return cmd

    def main(self):
        global cmd
        if len(sys.argv) < 2 or not self._find_best_match(sys.argv):
            self._usage()
            sys.exit(0)

        cmd = self._find_best_match(sys.argv)
        if not cmd:
            self._usage()
            sys.exit(0)
        cmd.main()

def handleKeyboardInterrupt(signalNumer, frame):
    if (cmd.killcount > 0):
        LOG.error("force quitting.")
        sys.exit()
    if (cmd.killcount == 0):
        cmd.killcount = 1
        msg = "SIGINT caught, will finish currently downloading" + \
              " packages and exit. Press CTRL+C again to force quit"
        LOG.error(msg)
        cmd.stop()

signal.signal(signal.SIGINT, handleKeyboardInterrupt)

def systemExit(code, msgs=None):
    "Exit with a code and optional message(s). Saved a few lines of code."
    if msgs:
        if type(msgs) not in [type([]), type(())]:
            msgs = (msgs, )
        for msg in msgs:
            sys.stderr.write(str(msg)+'\n')
    sys.exit(code)

if __name__ == "__main__":
    try:
        sys.exit(abs(CLI().main() or 0))
    except KeyboardInterrupt:
        systemExit(0, "\nUser interrupted process.")
