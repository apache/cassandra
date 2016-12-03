# Apache Cassandra rpmbuild

### Requirements:
- The build system needs to have Apache Cassandra `ant artifacts` build dependencies installed.
- Since Apache Cassandra depends on Python 2.7, the earliest version supported is RHEL/CentOS 7.0.

### Step 1:
- Build and copy sources to build tree:
```
ant artifacts
cp build/apache-cassandra-*-src.tar.gz $RPM_BUILD_DIR/SOURCES/
```

### Step 2:
- Since there is no version specified in the SPEC file, one needs to be passed at `rpmbuild` time (example with 4.0):
```
rpmbuild --define="version 4.0" -ba redhat/cassandra.spec
```

- RPM files can be found in their respective build tree directories:
```
ls -l $RPM_BUILD_DIR/{SRPMS,RPMS}/
```

### Hint:
- Don't build packages as root..
```
# this makes your RPM_BUILD_DIR = ~/rpmbuild
mkdir -p ~/rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
echo '%_topdir %(echo $HOME)/rpmbuild' > ~/.rpmmacros
```
