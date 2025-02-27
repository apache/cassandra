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
# If x is not provided, it will be set to a random integer between 100 and 150.

# 1. Capture or generate x
x="$1"
if [ -z "$x" ]; then
  # Generate a random integer between 100 and 150
  x=$((500 + RANDOM % 51))
fi

echo "Using x = $x"

# 2. Define the YAML file path
CONFIG_FILE="test/conf/cassandra.yaml"

# 3. Determine correct sed in-place flag based on OS
#    - macOS (Darwin) needs `-i ''`
#    - Linux typically just needs `-i`
if [[ "$(uname)" == "Darwin" ]]; then
  SED_INPLACE=(-i '')
else
  SED_INPLACE=(-i)
fi

# Capture the output into an array
available_ports=()
while IFS= read -r line; do
  available_ports+=("$line")
done < <(./.jenkins/get_available_ports.sh)
available_ports_index=0

# 4. List of parameters to update
PARAMS=("storage_port" "ssl_storage_port" "native_transport_port")

# We will store the new storage_port value to update the seeds line afterwards
new_storage_port=""

# 5. Update each parameter
for param in "${PARAMS[@]}"; do
  old_value=$(grep -E "^${param}:" "${CONFIG_FILE}" | awk '{print $2}')
  if [ -z "$old_value" ]; then
    echo "Warning: Could not find '$param' in $CONFIG_FILE. Skipping..."
    continue
  fi

  # If results[results_index] exists (non-empty), use it; otherwise use fallback
  if [ -n "${available_ports[available_ports_index]}" ]; then
    new_value="${available_ports[available_ports_index]}"
    # Move to the next index for future use
    (( available_ports_index++ ))
    echo "Using available_port provided by buildkite${new_value}"
  else
    # Fall back to random
    # old value to old value + 1000 is reserved for jvm dtests
    new_value=$((old_value + 15000 + x * 50 - RANDOM % 50))
  fi

  # Use the OS-specific in-place sed argument
  sed "${SED_INPLACE[@]}" "s|^${param}:.*|${param}: ${new_value}|" "${CONFIG_FILE}"

  # If this parameter is storage_port, remember the new value
  if [ "${param}" == "storage_port" ]; then
    new_storage_port=${new_value}
  fi
done

if [ -n "$new_storage_port" ]; then
  sed "${SED_INPLACE[@]}" "s|seeds: \"127.0.0.1:[0-9]*\"|seeds: \"127.0.0.1:${new_storage_port}\"|" "${CONFIG_FILE}"
fi

# check ports updated
git diff

echo "Ports updated successfully!"
