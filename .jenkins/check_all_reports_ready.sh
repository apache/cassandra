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

# Input BASE_NAMES as a space-separated string
BASE_NAMES="$1"

# Define EXPECTED_BASE_NAMES by looping through 1 to 20 and adding additional jacoco-utest- names
EXPECTED_BASE_NAMES=""

NUM_JVM_DTEST_GROUPS=20
# Loop to generate jacoco-dtest-group{index}
for i in $(seq 1 $NUM_JVM_DTEST_GROUPS); do
    EXPECTED_BASE_NAMES="$EXPECTED_BASE_NAMES jacoco-dtest-group$i"
done

# Add additional jacoco-utest- names
EXPECTED_BASE_NAMES="$EXPECTED_BASE_NAMES jacoco-utest-long-test jacoco-utest-stress-test jacoco-utest-test jacoco-utest-test-cdc jacoco-utest-test-compression jacoco-utest-test-memory"

# Initialize a variable to track missing names
missing_names=""

# Loop through each expected base name
for expected_name in $EXPECTED_BASE_NAMES; do
    # Check if the expected name is in BASE_NAMES
    if [[ ! " $BASE_NAMES " =~ " $expected_name " ]]; then
        # If a name is missing, add it to missing_names
        missing_names="$missing_names $expected_name"
    fi
done

# If there are missing names, print them; otherwise, print "true"
if [ -n "$missing_names" ]; then
    echo "false"
    echo "Missing names: $missing_names"
else
    echo "true"
fi

