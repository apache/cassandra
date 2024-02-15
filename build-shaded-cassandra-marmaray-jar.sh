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

# This script is used to generate cassandra-all jar which is used by Marmaray team with netty jar shaded
# This script is using JAVA 8

set -xe

ARTIFACT_NAME=cassandra-all-shaded-local
REPO_DIR=~/.m2/repository
CASSANDRA_VERSION=$(cat build.xml | grep 'property name="base.version"' | awk -F "\"" '{print $4}')
SHADED_LOCAL_VERSION=$(cat relocate-shaded-cassandra-marmaray-dependencies.pom | grep "shaded-local.version>" | awk -F "\>|\<" '{print $3}')
SHADED_OUTPUT_VERSION=$(cat relocate-shaded-cassandra-marmaray-dependencies.pom | grep -m 1 "<version>" | awk -F "\>|\<" '{print $3}')

echo $CASSANDRA_VERSION
echo SHADED_LOCAL_VERSION

ant clean
ant shaded-jar -Dno-checkstyle=true

# Install the version that will be shaded
mvn install:install-file               \
   -Dfile=./build/shaded-${CASSANDRA_VERSION}.jar \
   -DgroupId=org.apache.cassandra      \
   -DartifactId=${ARTIFACT_NAME} \
   -Dversion=${SHADED_LOCAL_VERSION}          \
   -Dpackaging=jar                     \
   -DgeneratePom=true                  \
   -DlocalRepositoryPath=${REPO_DIR}

# Create shaded artifact
mvn -f relocate-shaded-cassandra-marmaray-dependencies.pom package -DskipTests -nsu

set +xe
