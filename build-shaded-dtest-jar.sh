#!/bin/bash

set -xe

ARTIFACT_NAME=cassandra-dtest
REPO_DIR=~/.m2/repository
CASSANDRA_VERSION=$(cat build.xml | grep 'property name="base.version"' | awk -F "\"" '{print $4}')
DTEST_VERSION=$(cat relocate-dependencies.pom | grep "dtest-local.version>" | awk -F "\>|\<" '{print $3}')
SHADED_DTEST_VERSION=$(cat relocate-dependencies.pom | grep -m 1 "<version>" | awk -F "\>|\<" '{print $3}')

echo $CASSANDRA_VERSION
echo $DTEST_VERSION

ant clean
ant dtest-jar

# Install the version that will be shaded
mvn install:install-file               \
   -Dfile=./build/dtest-${CASSANDRA_VERSION}.jar \
   -DgroupId=org.apache.cassandra      \
   -DartifactId=${ARTIFACT_NAME}-local \
   -Dversion=${DTEST_VERSION}          \
   -Dpackaging=jar                     \
   -DgeneratePom=true                  \
   -DlocalRepositoryPath=${REPO_DIR}

# Create shaded artifact
mvn -f relocate-dependencies.pom package -DskipTests -nsu

# Deploy shaded artifact
mvn install:install-file                 \
   -Dfile=./target/${ARTIFACT_NAME}-shaded-${SHADED_DTEST_VERSION}.jar \
   -DgroupId=org.apache.cassandra        \
   -DartifactId=${ARTIFACT_NAME}-shaded  \
   -Dversion=${DTEST_VERSION}            \
   -Dpackaging=jar                       \
   -DgeneratePom=true                    \
   -DlocalRepositoryPath=${REPO_DIR}

set +xe
