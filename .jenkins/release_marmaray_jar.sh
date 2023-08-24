#!/bin/bash
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

# when publishing JAVA8 make sure to change the version so it won't conflict with JAVA11 builds

set -ex

echo "BRANCH: $BRANCH"
echo "RELEASE: $RELEASE"

wget http://artifactory.uber.internal:4587/artifactory/libs-release-local/oracle/server-jdk-linux-x64/1.8.0_111/server-jdk-linux-x64-1.8.0_111.tar.gz
tar xzvf server-jdk-linux-x64-1.8.0_111.tar.gz
JAVA_HOME="$(readlink -f jdk1.8.0_111)"
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"

RELEASE_URL="http://artifactory.uber.internal:4587/artifactory/libs-release-local/"
SNAPSHOT_URL="http://artifactory.uber.internal:4587/artifactory/cassandra-snapshots/"

# set ant
wget http://artifactory.uber.internal:4587/artifactory/libs-release-local/org/apache/ant/apache-ant-1.10.12-bin.tar.gz
tar xzvf apache-ant-1.10.12-bin.tar.gz
ANT_HOME="$(readlink -f apache-ant-1.10.12)"
export ANT_HOME
export PATH="$ANT_HOME/bin:$PATH"

ant realclean

./build-shaded-cassandra-marmaray-jar.sh

if [ "$RELEASE" = "true" ]; then
    ant publish-shaded -Drelease=set \
    -Dmaven-repository-url="$RELEASE_URL" \
    -Dmaven-repository-id=central \
    -Dartifact.remoteRepository.central="$RELEASE_URL"
else
    ant publish-shaded \
    -Dmaven-repository-url="$SNAPSHOT_URL" \
    -Dmaven-repository-id=snapshots \
    -Dartifact.remoteRepository.central="$RELEASE_URL"
fi
