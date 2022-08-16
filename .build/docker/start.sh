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

set -e

function display_help {
  echo
  echo "Use 'docker-exec -it cassandra-test <CMD>' to execute commands on this container."
  echo "ie. docker exec -it cassandra-test nodetool status"
  echo "ie. docker exec -it cassandra-test cqlsh"
}

if ! docker info > /dev/null 2>&1; then
  echo "Docker is not running, please retry after starting docker."
  exit 1
fi

# Check if local image exists
if [[ "$(docker images -q apache/cassandra-test 2> /dev/null)" == "" ]]; then
  echo "cassandra-test image does not exist, please create it with 'ant docker-build'."
  exit 1
fi

# Check if container is already running
if [ $( docker ps -a -f name=cassandra-test | wc -l ) -eq 2 ]; then
  echo "cassandra-test container is already running, please stop it with 'ant docker-stop'."
  display_help
  exit 1
fi

## Create network if does not exist
docker network create cassandra-test 2>/dev/null || true

CONTAINER_ID=`docker run --rm --name cassandra-test --network cassandra-test -d apache/cassandra-test`

until [ "$( docker container inspect -f '{{.State.Health.Status}}' $CONTAINER_ID )" != "starting" ]
do
  echo "Waiting for cassandra-test container $CONTAINER_ID to start."
  sleep 5
done

if [ "$( docker container inspect -f '{{.State.Health.Status}}' $CONTAINER_ID )" != "healthy" ]; then
  echo "Cassandra test container $CONTAINER_ID started but is unhealty."
  exit 1
fi

echo "cassandra-test container started with id $CONTAINER_ID"
display_help
