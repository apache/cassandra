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
#
# Dockerfile for running Cassandra unit tests
# Uses official Ubuntu image as base
FROM ubuntu:24.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install OpenJDK 11 and required build tools
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    ant \
    ant-optional \
    git \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /cassandra

# Set environment variables for Cassandra build
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV ANT_HOME=/usr/share/ant
ENV PATH="${ANT_HOME}/bin:${PATH}"

# Default command - run the full test suite
CMD [".build/run-tests.sh", "-a", "test"]
