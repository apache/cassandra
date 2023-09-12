#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

%define __jar_repack %{nil}
# Turn off the brp-python-bytecompile script and mangling shebangs for Python scripts
%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g' -e 's!/usr/lib[^[:space:]]*/brp-mangle-shebangs[[:space:]].*$!!g')

# rpmbuild should not barf when it spots we ship
# binary executable files in our 'noarch' package
%define _binaries_in_noarch_packages_terminate_build   0

%define __python /usr/bin/python3

%global username cassandra

# input of ~alphaN, ~betaN, ~rcN package versions need to retain upstream '-alphaN, etc' version for sources
%define upstream_version %(echo %{version} | sed -r 's/~/-/g')
%define relname apache-cassandra-%{upstream_version}

# default DIST_DIR to build
%global _get_dist_dir %(echo "${DIST_DIR:-build}")

Name:          cassandra
Version:       %{version}
Release:       %{revision}
Summary:       Cassandra is a highly scalable, eventually consistent, distributed, structured key-value store.

Group:         Development/Libraries
License:       Apache Software License 2.0
URL:           http://cassandra.apache.org/
Source0:       %{relname}-src.tar.gz
BuildRoot:     %{_tmppath}/%{relname}root-%(%{__id_u} -n)

BuildRequires: ant >= 1.9
BuildRequires: ant-junit >= 1.9

Requires:      (java-11-headless or java-17-headless)
Requires:      python(abi) >= 3.6
Requires:      procps-ng >= 3.3
Requires(pre): user(cassandra)
Requires(pre): group(cassandra)
Requires(pre): shadow-utils
Provides:      user(cassandra)
Provides:      group(cassandra)

BuildArch:     noarch

# Don't examine the .so files we bundle for dependencies
AutoReqProv:   no

%description
Cassandra is a distributed (peer-to-peer) system for the management and storage of structured data.

%prep
%setup -q -n %{relname}-src

%build
export LANG=en_US.UTF-8
export JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF-8"
ant jar -Dversion=%{upstream_version} -Dno-checkstyle=true -Drat.skip=true -Dant.gen-doc.skip=true

%install
%{__rm} -rf %{buildroot}
mkdir -p %{buildroot}/%{_sysconfdir}/%{username}
mkdir -p %{buildroot}/usr/share/%{username}
mkdir -p %{buildroot}/usr/share/%{username}/lib
mkdir -p %{buildroot}/%{_sysconfdir}/%{username}/default.conf
mkdir -p %{buildroot}/%{_sysconfdir}/rc.d/init.d
mkdir -p %{buildroot}/%{_sysconfdir}/security/limits.d
mkdir -p %{buildroot}/%{_sysconfdir}/default
mkdir -p %{buildroot}/usr/sbin
mkdir -p %{buildroot}/usr/bin
mkdir -p %{buildroot}/var/lib/%{username}/commitlog
mkdir -p %{buildroot}/var/lib/%{username}/data
mkdir -p %{buildroot}/var/lib/%{username}/saved_caches
mkdir -p %{buildroot}/var/lib/%{username}/hints
mkdir -p %{buildroot}/var/run/%{username}
mkdir -p %{buildroot}/var/log/%{username}
( cd pylib && %{__python} setup.py install --no-compile --root %{buildroot}; )

# patches for data and log paths
patch -p1 < debian/patches/cassandra_yaml_dirs.diff
patch -p1 < debian/patches/cassandra_logdir_fix.diff
# uncomment hints_directory path
sed -i 's/^# hints_directory:/hints_directory:/' conf/cassandra.yaml

# remove other files not being installed
rm -f bin/stop-server
rm -f bin/*.orig
rm -f bin/cassandra.in.sh
rm -f lib/sigar-bin/*winnt*  # strip segfaults on dll..
rm -f tools/bin/cassandra.in.sh

# copy default configs
cp -pr conf/* %{buildroot}/%{_sysconfdir}/%{username}/default.conf/

# step on default config with our redhat one
cp -p redhat/%{username}.in.sh %{buildroot}/usr/share/%{username}/%{username}.in.sh
cp -p redhat/%{username} %{buildroot}/%{_sysconfdir}/rc.d/init.d/%{username}
cp -p redhat/%{username}.conf %{buildroot}/%{_sysconfdir}/security/limits.d/
cp -p redhat/default %{buildroot}/%{_sysconfdir}/default/%{username}

# copy cassandra bundled libs
cp -pr lib/* %{buildroot}/usr/share/%{username}/lib/

# copy stress jar
cp -p %{_get_dist_dir}/tools/lib/stress.jar %{buildroot}/usr/share/%{username}/

# copy fqltool jar
cp -p %{_get_dist_dir}/tools/lib/fqltool.jar %{buildroot}/usr/share/%{username}/

# copy binaries
mv bin/cassandra %{buildroot}/usr/sbin/
cp -p bin/* %{buildroot}/usr/bin/
cp -p tools/bin/* %{buildroot}/usr/bin/

# copy cassandra jar
cp %{_get_dist_dir}/apache-cassandra-%{upstream_version}.jar %{buildroot}/usr/share/%{username}/

%clean
%{__rm} -rf %{buildroot}

%pre
getent group %{username} >/dev/null || groupadd -r %{username}
getent passwd %{username} >/dev/null || \
useradd -d /var/lib/%{username} -g %{username} -M -r %{username}
exit 0

%files
%defattr(0644,root,root,0755)
%doc CHANGES.txt LICENSE.txt README.asc NEWS.txt NOTICE.txt CASSANDRA-14092.txt
%attr(755,root,root) %{_bindir}/auditlogviewer
%attr(755,root,root) %{_bindir}/jmxtool
%attr(755,root,root) %{_bindir}/cassandra-stress
%attr(755,root,root) %{_bindir}/cqlsh
%attr(755,root,root) %{_bindir}/cqlsh.py
%attr(755,root,root) %{_bindir}/debug-cql
%attr(755,root,root) %{_bindir}/fqltool
%attr(755,root,root) %{_bindir}/generatetokens
%attr(755,root,root) %{_bindir}/nodetool
%attr(755,root,root) %{_bindir}/sstableloader
%attr(755,root,root) %{_bindir}/sstablescrub
%attr(755,root,root) %{_bindir}/sstableupgrade
%attr(755,root,root) %{_bindir}/sstableutil
%attr(755,root,root) %{_bindir}/sstableverify
%attr(755,root,root) %{_sbindir}/cassandra
%attr(755,root,root) /%{_sysconfdir}/rc.d/init.d/%{username}
%{_sysconfdir}/default/%{username}
%{_sysconfdir}/security/limits.d/%{username}.conf
/usr/share/%{username}*
%config(noreplace) /%{_sysconfdir}/%{username}
%attr(750,%{username},%{username}) %config(noreplace) /var/lib/%{username}/*
%attr(750,%{username},%{username}) /var/log/%{username}*
%attr(750,%{username},%{username}) /var/run/%{username}*
%{python_sitelib}/cqlshlib/
%{python_sitelib}/cassandra_pylib*.egg-info

%post
alternatives --install /%{_sysconfdir}/%{username}/conf %{username} /%{_sysconfdir}/%{username}/default.conf/ 0
exit 0

%preun
# only delete alternative on removal, not upgrade
if [ "$1" = "0" ]; then
    alternatives --remove %{username} /%{_sysconfdir}/%{username}/default.conf/
fi
exit 0


%package tools
Summary:       Extra tools for Cassandra. Cassandra is a highly scalable, eventually consistent, distributed, structured key-value store.
Group:         Development/Libraries
Requires:      cassandra = %{version}-%{revision}

%description tools
Cassandra is a distributed (peer-to-peer) system for the management and storage of structured data.
.
This package contains extra tools for working with Cassandra clusters.

%files tools
%attr(755,root,root) %{_bindir}/sstabledump
%attr(755,root,root) %{_bindir}/compaction-stress
%attr(755,root,root) %{_bindir}/sstableexpiredblockers
%attr(755,root,root) %{_bindir}/sstablelevelreset
%attr(755,root,root) %{_bindir}/sstablemetadata
%attr(755,root,root) %{_bindir}/sstableofflinerelevel
%attr(755,root,root) %{_bindir}/sstablerepairedset
%attr(755,root,root) %{_bindir}/sstablesplit
%attr(755,root,root) %{_bindir}/sstablepartitions
%attr(755,root,root) %{_bindir}/auditlogviewer
%attr(755,root,root) %{_bindir}/jmxtool
%attr(755,root,root) %{_bindir}/fqltool
%attr(755,root,root) %{_bindir}/generatetokens
%attr(755,root,root) %{_bindir}/hash_password


%changelog
# packaging changes, not software changes
* Thu May 04 2023 Mick Semb Wever <mck@apache.org>
- 5.0
- RPM packaging brought in-tree. CASSANDRA-18133
* Mon Dec 05 2016 Michael Shuler <mshuler@apache.org>
- 2.1.17, 2.2.9, 3.0.11, 3.10
- Reintroduce RPM packaging
