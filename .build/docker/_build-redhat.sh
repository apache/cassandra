#!/bin/bash
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

################################
#
# Prep
#
################################

# variables, w/ defaults, w/ checks
[ "x${CASSANDRA_DIR}" != "x" ] || CASSANDRA_DIR="$(readlink -f $(dirname "$0")/..)"
[ "x${DIST_DIR}" != "x" ] || DIST_DIR="${CASSANDRA_DIR}/build"
[ "x${RPM_BUILD_DIR}" != "x" ] || RPM_BUILD_DIR="$(mktemp -d /tmp/rpmbuild.XXXXXX)"

# pre-conditions
command -v ant >/dev/null 2>&1 || { echo >&2 "ant needs to be installed"; exit 1; }
command -v git >/dev/null 2>&1 || { echo >&2 "git needs to be installed"; exit 1; }
command -v rpmbuild >/dev/null 2>&1 || { echo >&2 "rpm-build needs to be installed"; exit 1; }
[ -d "${CASSANDRA_DIR}" ] || { echo >&2 "Directory ${CASSANDRA_DIR} must exist"; exit 1; }
[ -f "${CASSANDRA_DIR}/build.xml" ] || { echo >&2 "${CASSANDRA_DIR}/build.xml must exist"; exit 1; }
[ -d "${DIST_DIR}" ] || mkdir -p "${DIST_DIR}"
[ -d "${RPM_BUILD_DIR}/SOURCES" ] || mkdir -p ${RPM_BUILD_DIR}/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}


if [ "$1" == "-h" ]; then
   echo "$0 [-h] [dist_type]"
   echo "dist types are [rpm, noboolean] and rpm is default"
   exit 1
fi

RPM_DIST=$1
[ "x${RPM_DIST}" != "x" ] || RPM_DIST="rpm"

if [ "${RPM_DIST}" == "rpm" ]; then
   RPM_SPEC="redhat/cassandra.spec"
elif [ "${RPM_DIST}" == "noboolean" ]; then # noboolean
   RPM_SPEC="redhat/noboolean/cassandra.spec"
else
   echo >&2 "Only rpm and noboolean are valid dist_type arguments. Got ${RPM_DIST}"
   exit 1
fi

################################
#
# Main
#
################################

set -e

# note, this edits files in your working cassandra directory
pushd $CASSANDRA_DIR >/dev/null

# Used version for build will always depend on the git referenced used for checkout above
# Branches will always be created as snapshots, while tags are releases
tag=`git describe --tags --exact-match 2> /dev/null || true`
branch=`git symbolic-ref -q --short HEAD 2> /dev/null || true`

is_tag=false
git_version=''

# Parse version from build.xml so we can verify version against release tags and use the build.xml version
# for any branches. Truncate from snapshot suffix if needed.
buildxml_version=`grep 'property\s*name="base.version"' build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`
regx_snapshot="([0-9.]+)-SNAPSHOT$"
if [[ $buildxml_version =~ $regx_snapshot ]]; then
   buildxml_version=${BASH_REMATCH[1]}
fi

if [ "$tag" ]; then
   is_tag=true
   # Official release
   regx_tag="cassandra-(([0-9.]+)(-(alpha|beta|rc)[0-9]+)?)$"
   # Tentative release
   regx_tag_tentative="(([0-9.]+)(-(alpha|beta|rc)[0-9]+)?)-tentative$"
   if [[ $tag =~ $regx_tag ]] || [[ $tag =~ $regx_tag_tentative ]]; then
      git_version=${BASH_REMATCH[1]}
   else
      echo "Error: could not recognize version from tag $tag">&2
      exit 2
   fi
   if [ $buildxml_version != $git_version ]; then
      echo "Error: build.xml version ($buildxml_version) not matching git tag derived version ($git_version)">&2
      exit 4
   fi
   CASSANDRA_VERSION=$git_version
   CASSANDRA_REVISION='1'
else
   # This could be either trunk or any dev branch or SHA, so we won't be able to get the version
   # from the branch name. In this case, fall back to version specified in build.xml.
   CASSANDRA_VERSION="${buildxml_version}"
   dt=`date +"%Y%m%d"`
   ref=`git rev-parse --short HEAD`
   CASSANDRA_REVISION="${dt}git${ref}"
fi

# Artifact will only be used internally for build process and won't be found with snapshot suffix
ant artifacts -Drelease=true -Dant.gen-doc.skip=true -Djavadoc.skip=true -Dcheck.skip=true
cp ${DIST_DIR}/apache-cassandra-*-src.tar.gz ${RPM_BUILD_DIR}/SOURCES/

# if CASSANDRA_VERSION is -alphaN, -betaN, -rcN, then rpmbuild fails on the '-' char; replace with '~'
CASSANDRA_VERSION=${CASSANDRA_VERSION/-/\~}

command -v python >/dev/null 2>&1 || alias python=/usr/bin/python3
rpmbuild --define="version ${CASSANDRA_VERSION}" --define="revision ${CASSANDRA_REVISION}" --define="_topdir ${RPM_BUILD_DIR}" -ba ${RPM_SPEC}
cp ${RPM_BUILD_DIR}/SRPMS/*.rpm ${RPM_BUILD_DIR}/RPMS/noarch/*.rpm ${DIST_DIR}

popd >/dev/null
