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

export PROJECT_DIR=$(git rev-parse --show-toplevel)

j11Setup() {
  if ! java -version 2>&1 | grep 'version "11\.'; then
    # use java 11
    export CASSANDRA_USE_JDK11=true
    # generic-udj is migrated to Debian 12, which is using Java 17 by default. We want to switch to Java 11
    JAVA_HOME="$(readlink -f $HOME/java_home/jdk_11)"
    export JAVA_HOME
    export PATH="$JAVA_HOME/bin:$PATH"
    java --version
  fi
}

j8Setup() {
  if ! java -version 2>&1 | grep 'version "1.8\.'; then
    curl http://artifactory.uber.internal:4587/artifactory/libs-release-local/oracle/server-jdk-linux-x64/1.8.0_111/server-jdk-linux-x64-1.8.0_111.tar.gz -o server-jdk-linux-x64-1.8.0_111.tar.gz
    tar xzvf server-jdk-linux-x64-1.8.0_111.tar.gz
    JAVA_HOME="$(readlink -f jdk1.8.0_111)"
    export JAVA_HOME
    export PATH="$JAVA_HOME/bin:$PATH"
    java --version
  fi
}

antSetup() {
  if ! command -v ant >/dev/null 2>&1; then
    wget http://artifactory.uber.internal:4587/artifactory/libs-release-local/org/apache/ant/apache-ant-1.10.12-bin.tar.gz
    tar xzvf apache-ant-1.10.12-bin.tar.gz
    ANT_HOME="$(readlink -f apache-ant-1.10.12)"
    export ANT_HOME
    export PATH="$ANT_HOME/bin:$JAVA_HOME/bin:$PATH"
  fi
}
