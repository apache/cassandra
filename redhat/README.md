<!--
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
-->

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
