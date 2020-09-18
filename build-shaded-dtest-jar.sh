#!/bin/bash

set -xe

# Cassandra version that is specified in build.xml
CASSANDRA_VERSION=$1
# In-JVM dtest version that is specified in relocate-dependencies.pom
DTEST_VERSION=$2
ARTIFACT_NAME=cassandra-dtest
REPO_DIR=~/.m2/repository

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
   -Dfile=./target/${ARTIFACT_NAME}-shaded-${DTEST_VERSION}.jar \
   -DgroupId=org.apache.cassandra        \
   -DartifactId=${ARTIFACT_NAME}-shaded  \
   -Dversion=${DTEST_VERSION}            \
   -Dpackaging=jar                       \
   -DgeneratePom=true                    \
   -DlocalRepositoryPath=${REPO_DIR}

set +xe
