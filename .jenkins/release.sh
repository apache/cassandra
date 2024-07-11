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

set -ex

echo "RELEASE: $RELEASE"

RELEASE_URL="http://artifactory.uber.internal:4587/artifactory/libs-release-local/"
SNAPSHOT_URL="http://artifactory.uber.internal:4587/artifactory/cassandra-snapshots/"

# set ant
wget http://artifactory.uber.internal:4587/artifactory/libs-release-local/org/apache/ant/apache-ant-1.10.12-bin.tar.gz
tar xzvf apache-ant-1.10.12-bin.tar.gz
ANT_HOME="$(readlink -f apache-ant-1.10.12)"
export ANT_HOME

# update PATH
export PATH="$ANT_HOME/bin:$JAVA_HOME/bin:$PATH"

# use java 11
export CASSANDRA_USE_JDK11=true

# generic-udj is migrated to Debian 12, which is using Java 17 by default. We want to switch to Java 11
JAVA_HOME="$(readlink -f $HOME/java_home/jdk_11)"
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"
java --version

ant realclean
ant build

if [ "$RELEASE" = "true" ]; then
    ant publish -Drelease=set \
    -Dmaven-repository-url="$RELEASE_URL" \
    -Dmaven-repository-id=central \
    -Dartifact.remoteRepository.central="$RELEASE_URL"
else
    ant publish \
    -Dmaven-repository-url="$SNAPSHOT_URL" \
    -Dmaven-repository-id=snapshots \
    -Dartifact.remoteRepository.central="$RELEASE_URL"
fi
