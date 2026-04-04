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

# Build in-tree HTML documentation using the cassandra-website Docker image
# This allows building docs without installing Antora locally

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DOC_DIR="${REPO_ROOT}/doc"
DOCKER_IMAGE="apache/cassandra-website:latest"

echo "Building in-tree HTML documentation using Docker..."
echo "Docker image: ${DOCKER_IMAGE}"

# Run the full doc build in Docker (generates asciidoc + builds HTML)
# Use --entrypoint to bypass the cassandra-website entrypoint and ant gen-doc directly
echo "Running documentation build in Docker..."
docker run --rm \
  --entrypoint /bin/bash \
  -v "${REPO_ROOT}:/cassandra" \
  -w /cassandra/ \
  "${DOCKER_IMAGE}" \
  -c "ant gen-doc"

echo "Documentation built in ${REPO_ROOT}/build/html/"
