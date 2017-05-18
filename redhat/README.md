# Apache Cassandra rpmbuild

### Requirements:
- The build system needs to have Apache Cassandra `ant artifacts` build dependencies installed.
- Since Apache Cassandra depends on Python 2.7, the earliest version supported is RHEL/CentOS 7.0.

### Step 1:
- Build and copy sources to build tree:
```
ant artifacts -Drelease=true
```

### Step 2:
- Since there is no version specified in the SPEC file, one needs to be passed at `rpmbuild` time (example with 4.0):
```
mkdir -p build/rpmbuild/{BUILD,RPMS,SPECS,SRPMS}
rpmbuild --define="version 4.0" \
    --define="revision $(date +"%Y%m%d")git$(git rev-parse --short HEAD)%{?dist}" \
    --define "_topdir $(pwd)/build/rpmbuild" \
    --define "_sourcedir $(pwd)/build" \
    -ba redhat/cassandra.spec
```

Use revision value in the example above for git based snapshots. Change to `--define="revision 1"` for non-snapshot releases.

- RPM files can be found in their respective build tree directories:
```
ls -l build/rpmbuild/{SRPMS,RPMS}/
```

### Hint:
- Don't build packages as root..
