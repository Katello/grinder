%{!?python_sitelib: %define python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}

Name: grinder
Version: 0.0.63
Release: 1%{?dist}
Summary: A tool synching content

Group: Development/Tools
License: GPLv2
URL: http://github.com/mccun934/grinder
Source0: http://mmccune.fedorapeople.org/grinder/grinder-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildArch: noarch
BuildRequires: python-setuptools
Requires:      createrepo, python >= 2.4
Requires:      PyYAML
Requires:      python-pycurl
Requires:      python-hashlib
%description
A tool for synching content from the Red Hat Network.

%prep
%setup -q -n grinder-%{version}


%build
%{__python} setup.py build


%install
rm -rf $RPM_BUILD_ROOT
%{__python} setup.py install -O1 --skip-build --root $RPM_BUILD_ROOT
rm -f $RPM_BUILD_ROOT%{python_sitelib}/*egg-info/requires.txt

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%doc README COPYING
%{_bindir}/grinder
%dir %{python_sitelib}/grinder
%{python_sitelib}/grinder/*
%{python_sitelib}/grinder-*.egg-info
%config(noreplace) %{_sysconfdir}/grinder/grinder.yml


%changelog
* Fri Sep 24 2010 John Matthews <jmatthew@redhat.com> 0.0.63-1
- 608672 - clearly state error when systemid/certifcate are unable to be read
  (jmatthew@redhat.com)

* Wed Sep 22 2010 John Matthews <jmatthew@redhat.com> 0.0.62-1
- fix for rhn sync (jmatthew@redhat.com)
- In certain case such as CDN, dotted files are not allowed. Try a treeinfo if
  .treeinfo fails before quitting (pkilambi@redhat.com)

* Fri Sep 17 2010 Pradeep Kilambi <pkilambi@redhat.com> 0.0.61-1
- removes unused files key (pkilambi@redhat.com)
- Adding support to be able to sync down trees associated to the product repo
  (pkilambi@redhat.com)
- include checksum in package store path (pkilambi@redhat.com)

* Tue Sep 07 2010 Pradeep Kilambi <pkilambi@redhat.com> 0.0.60-1
- Somtimes the package path could have directories, created the dirs before
  creating symlinks (pkilambi@redhat.com)

* Thu Sep 02 2010 Pradeep Kilambi <pkilambi@redhat.com> 0.0.59-1
- Adding central package location support for grinder. Packages are synced to
  packages_location and symlined to repo directory. Default is repo directory
  unless packages_location is passed. DRPMS will be stored in individual repos
  as usual (pkilambi@redhat.com)
- purge orphaned packages that are not part of updated repodata
  (pkilambi@redhat.com)

* Tue Aug 24 2010 John Matthews <jmatthew@redhat.com> 0.0.58-1
- Adding a progress callback (jmatthew@redhat.com)
- RHN now expects the updateinfo fetch to use <checksum>-updateinfo.xml.gz as
  the request file, so we ask for repomd.xml, get the checksum for updateinfo
  and construct the request name to match RHN (pkilambi@redhat.com)

* Tue Aug 03 2010 Pradeep Kilambi <pkilambi@redhat.com> 0.0.57-1
- 620791 - exclude epoch from the filename stored on disk (pkilambi@redhat.com)

* Mon Aug 02 2010 Pradeep Kilambi <pkilambi@redhat.com> 0.0.56-1
- exposing the newest package download flag to YumRepoGrinder class
  (pkilambi@redhat.com)

* Mon Aug 02 2010 John Matthews <jmatthew@redhat.com> 0.0.55-1
- add proxy basic http user authentication to package fetch
  (jmatthew@redhat.com)

* Fri Jul 30 2010 Jay Dobies <jason.dobies@redhat.com> 0.0.54-1
- 

* Fri Jul 30 2010 Jay Dobies <jason.dobies@redhat.com> 0.0.53-1
- Adding http proxy support to grinder's yum repo fetches - user/password auth
  is not implemented for the pkg dowload yet (jmatthew@redhat.com)
- moving code authors to a separate AUTHORS file (pkilambi@redhat.com)
- 602243 - fixing the drpm path (pkilambi@redhat.com)
- 570887 - grinder, running grinder with -a option gives conflicting options
  specified error. If --all is specified on command line, disable removeold If
  --removeold is specified on command line, disable fetchall
  (jmatthew@redhat.com)
- 573138 - traceback when using a bad URL (jmatthew@redhat.com)
- adding a debug line to display what basepath is set to for yum fetches
  (jmatthew@redhat.com)

* Wed May 26 2010 Pradeep Kilambi <pkilambi@redhat.com> 0.0.52-1
- copy repofiles to repodata dir instead of move so packagesack can use the
  primary (pkilambi@redhat.com)

* Wed May 26 2010 John Matthews <jmatthew@redhat.com> 0.0.51-1
- fix for activation, we dropped called to "activate" when porting to new CLI
  (jmatthew@redhat.com)

* Tue May 25 2010 Pradeep Kilambi <pkilambi@redhat.com> 0.0.50-1
- set the primary_db to not retrieved so repo can refecth it for metadata dir
- 594496 - fix typos in help messages
- 572597 - updating grinder man page to include new options for yum
- 592316: making cli options for yuma nd rhn look close

* Fri May 21 2010 John Matthews <jmatthew@redhat.com> 0.0.49-1
- fix 'fetch' call to pass in hashType, this prob showed up during a long sync
  when auth data became stale we would refresh auth data, then re-call fetch.
  The call to fetch was missing hashType (jmatthew@redhat.com)
- Grinder: before fetching the repodata convert the url to ascii so urlgrabber
  doesnt freakout (pkilambi@redhat.com)
- logging info change, as per QE request (jmatthew@redhat.com)
- added web install requirement (jconnor@redhat.com)
- changed package_dir argument from 'grinder': 'src/grinder' to '': 'src' which
  tells disutils that the packages found by find_packages are under src/
  (jconnor@redhat.com)
- moving grinder tests from 'tests' to 'test', makes setup.py happy for develop
  install (jmatthew@redhat.com)
- Change --debug to be a True/False only, removed unused 'logging level'
  ability. (jmatthew@redhat.com)

* Wed May 19 2010 John Matthews <jmatthew@redhat.com> 0.0.48-1
- Adding extra log output to help QE in automation testing
  (jmatthew@redhat.com)

* Wed May 19 2010 Mike McCune <mmccune@redhat.com> 0.0.46-1
- import into pulp

* Tue May 18 2010 Pradeep Kilambi <pkilambi@redhat.com> 0.0.44-1
- 593304 - Minor issue, visible python errors at the end of a kickstart sync
  (jwmatthews@gmail.com)
- adding a prefix of "grinder." to our logger instances (jwmatthews@gmail.com)
- 593074 - set the relative path based on primary xml (pkilambi@redhat.com)

* Mon May 17 2010 Pradeep Kilambi <pkilambi@redhat.com> 0.0.43-1
- 

* Fri May 14 2010 John Matthews <jwmatthews@gmail.com> 0.0.42-1
- Updates for Package/Kickstart fetch to work with changes in BaseFetch Note:
  RHN comm to https is currently broken, http is working (jwmatthews@gmail.com)
- Refactor BaseFtech to use pycurl so RHN and yum fetch use the same logic to
  fetch and validate downloads (pkilambi@redhat.com)
- refactor, remove rhncomm from BaseFetch (jwmatthews@gmail.com)
- Fix for kickstarts, need to keep filename same as what RHN uses (don't use
  epoch in filename) (jwmatthews@gmail.com)

* Thu May 13 2010 Pradeep Kilambi <pkilambi@redhat.com> 0.0.40-1
- Adding python-hashlib dependency to grinder (pkilambi@redhat.com)
- Adding validation for drpms fetch

* Wed May 12 2010 Pradeep Kilambi <pkilambi@redhat.com> 0.0.38-1
- log tracebacks for debug purposes (pkilambi@redhat.com)
- RepoFecth now validates existing packages and only fetches new ones. Added a
  new utils module for common calls (pkilambi@redhat.com)
- fix typo for 'packages' instead of 'kickstarts' (jwmatthews@gmail.com)
- bz591120 - running grinder with -k and -K results in error
  (jwmatthews@gmail.com)
- move 'removeold' functionality to BaseSync, add in CLI option for 'removeold'
  (jwmatthews@gmail.com)

* Mon May 10 2010 John Matthews <jwmatthews@gmail.com> 0.0.37-1
- fix for basePath being used when set in config file and cleanup of unused
  "main" method (jwmatthews@gmail.com)

* Thu May 06 2010 John Matthews <jwmatthews@gmail.com> 0.0.36-1
- add createRepo/updateRepo calls to syncPackages() (jwmatthews@gmail.com)

* Thu May 06 2010 Pradeep Kilambi <pkilambi@redhat.com> 0.0.35-1
- Adding support to fetch content by passing in ssl ca and content certs via
  yum for metadata and pycurl to fetch the bits (pkilambi@redhat.com)

* Wed May 05 2010 Mike McCune <mmccune@redhat.com> 0.0.33-1
- copy repomd.xml to the repodata directory (pkilambi@redhat.com)
- update for kickstart syncs (jwmatthews@gmail.com)
- add check for systemid and ensure we cleanup test cert/systemid
  (jwmatthews@gmail.com)
- adding unittests for RHNSync parsing configfile and reading options from
  command line (jwmatthews@gmail.com)
- rename RHNContent to RHNFetch (jwmatthews@gmail.com)
- adding 'rhn' operation to GrinderCLI  - options are initialized in order of
  Defaults, Config File, CLI  - basic package syncing has been tested  - needs
  exhaustive testing with different option combinations (jwmatthews@gmail.com)
- minor fixes after testing presto stuff (pkilambi@redhat.com)
- Support to sync down delta rpms metadata and corresponding binaries for a
  given repo if available. (pkilambi@redhat.com)
- Fetch the repodata generically so we can support presto metadata if available
  (pkilambi@redhat.com)
- some useful logging info on fetch (pkilambi@redhat.com)
- including logrotate in logger class (pkilambi@redhat.com)
- new Grinder CLI architecture with yum repo sync cli integrated and functional
  (pkilambi@redhat.com)
- clean up (pkilambi@redhat.com)
- Adding a module to support content fetch from a yum repo url. CLI integration
  follows (pkilambi@redhat.com)

* Thu Apr 08 2010 John Matthews <jwmatthews@gmail.com> 0.0.32-1
- fixing typeError in log statement cauusing createrepo to fail
  (pkilambi@redhat.com)

* Wed Apr 07 2010 John Matthews <jwmatthews@gmail.com> 0.0.31-1
- 580082 - grinder -b /tmp/syncdir is not syncing channel to specified
  basepath. (jwmatthews@gmail.com)

* Tue Apr 06 2010 John Matthews <jwmatthews@gmail.com> 0.0.29-1
- wip for kickstart fetching (jwmatthews@gmail.com)
- Refactor ParallelFetch/PackageFetch code to get ready for Kickstart fetching
  (jwmatthews@gmail.com)
- add fetch of metadata for kickstarts (jwmatthews@gmail.com)
- add method for returning filtered channel labels (jwmatthews@gmail.com)
- bz572639 - add debug output for removeold and numOldPkgsKeep
  (jwmatthews@gmail.com)
- corrected typo (jconnor@satellite.localdomain)

* Mon Mar 29 2010 John Matthews <jwmatthews@gmail.com> 0.0.28-1
- small typo change (jwmatthews@gmail.com)

* Fri Mar 26 2010 Mike McCune <mmccune@redhat.com> 0.0.27-1
- fixing condition when channel has no comps or update data
  (mmccune@redhat.com)
- Support for updateinfo.xml fetch and munge with existing createrepo data.
  This is to make the errata data work in conjunction with yum security plugin
  (pkilambi@redhat.com)

* Tue Mar 23 2010 Mike McCune <mmccune@redhat.com> 0.0.25-1
- adding SyncReport to show # downloads, errors, etc.. (mmccune@redhat.com)
- add fetching of comps.xml to support yum "group" operations
  (jwmatthews@gmail.com)

* Mon Mar 22 2010 Mike McCune <mmccune@redhat.com> 0.0.21-1
- 572663 - grinder command line arg "-P one" should throw non int exception for
  parallel (jwmatthews@gmail.com)
- 572657 - please remove username password from grinder config
  (jwmatthews@gmail.com)

* Thu Mar 11 2010 Mike McCune <mmccune@redhat.com> 0.0.20-1
- 572565 - Running grinder gives a Unable to parse config file message
  (jwmatthews@gmail.com)
- updating comment in config for how many previous packages to store
  (jwmatthews@gmail.com)
- typo fix (jwmatthews@gmail.com)
- Keep a configurable number of old packages & bz572327 fix bz572327 Running
  grinder for a specific channel syncs that channel and the channels specified
  in the config (jwmatthews@gmail.com)

* Wed Mar 10 2010 Mike McCune <mmccune@redhat.com> 0.0.18-1
- fixing spacing (mmccune@redhat.com)
- 571452 - ParallelFetch create channel directory should be silent if the
  directory already exists (jwmatthews@gmail.com)

* Thu Mar 04 2010 Mike McCune <mmccune@redhat.com> 0.0.17-1
- add log statement to show if/where removeold package is working from
  (jmatthews@virtguest-rhq-server.localdomain)
- add option to remove old RPMs from disk (jmatthews@virtguest-rhq-
  server.localdomain)

* Wed Mar 03 2010 Mike McCune <mmccune@redhat.com> 0.0.16-1
- update dir name for /etc/grinder (jmatthews@virtguest-rhq-server.localdomain)
- add PyYAML to grinder.spec (jmatthews@virtguest-rhq-server.localdomain)
- add yaml configuration file to setuptools (jmatthews@virtguest-rhq-
  server.localdomain)
- adding yaml configuration file/parsing to grinder (jmatthews@virtguest-rhq-
  server.localdomain)
- fixing paths and moving a bit forward (mmccune@redhat.com)

* Tue Mar 02 2010 Mike McCune <mmccune@redhat.com> 0.0.14-1
- 569963 - Adding dependency on createrepo (skarmark@redhat.com)
- adding test hook (mmccune)
- Adding error handling for a system trying to run grinder without activating
  (skarmark@redhat.com)

* Fri Feb 26 2010 Mike McCune <mmccune@redhat.com> 0.0.11-1
- Initial creation of RPM/specfile 

