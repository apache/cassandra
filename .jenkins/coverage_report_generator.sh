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

echo "PHAB Diff ID: $PHAB_DIFF_ID"
echo "Test PHAB Diff ID: $TEST_PHAB_DIFF_ID"

FILE_PHAB_COMMENT="build/comment/phabricator-comment-utest-jvmdtest"
DIFF_COVERAGE_REPORT_FILE="build/jacoco/diffCoverage/report.csv"
COVERAGE_THRESHOLD=0.8

# env setup (Java11, ant)
source "./.jenkins/env_setup.sh"
j11Setup
antSetup

# python setup
python3 --version
virtualenv --python=python3 venv
source venv/bin/activate
pip3 install --upgrade setuptools
pip3 install --no-cache-dir -r .jenkins/code_coverage/requirements.txt

if [ -z "$TEST_PHAB_DIFF_ID" ]; then
  DIFF_ID=$PHAB_DIFF_ID
else
  DIFF_ID=$TEST_PHAB_DIFF_ID
fi
echo "Diff ID to collect coverage report: $DIFF_ID"

python3 .jenkins/code_coverage/coverage_report_generator.py --diff_id $DIFF_ID

if [ -f /tmp/buildkite_build_ids.json ]; then
    output=$(cat /tmp/buildkite_build_ids.json)
    echo "JSON content: $output"
    BUILDKITE_BUILD_IDS=($(echo "$output" | jq -r '.[]'))
else
    echo "JSON file not found!"
    exit 1
fi

echo "Buildkite build ids: ${BUILDKITE_BUILD_IDS[@]}"

# Record the existing files in build/jacoco/ before the loop downloading starts
existing_files=$(ls build/jacoco/*.exec 2>/dev/null)

for build_phid in "${BUILDKITE_BUILD_IDS[@]}"; do
    echo "Downloading artifact for build PHID: $build_phid"
    buildkite-agent artifact download "build/jacoco/*.exec" . --build "$build_phid"
done

# List the files in build/jacoco/ after all downloads
new_files=$(ls build/jacoco/*.exec 2>/dev/null)

# Compare the existing and new files to identify newly downloaded files
newly_downloaded_files=$(comm -13 <(echo "$existing_files" | sort) <(echo "$new_files" | sort))

# create build/jacoco file
ant jacoco-init
BASE_NAMES=""
for file in build/jacoco/*.exec; do
    base_name=$(basename "$file" .exec)
    BASE_NAMES="$BASE_NAMES $base_name"
done

# Capture the result of the check_base_names.sh script
result=$(./.jenkins/check_all_reports_ready.sh "$BASE_NAMES")
echo "$result"

# Check if the result is "true"
if [ "$result" == "true" ]; then
    echo "All expected coverage reports are present! Proceeding with the next action."
    ant build
    ant jacoco-report

    # diff coverage
    git diff $(git merge-base HEAD origin/master) -- src/java/org/apache/cassandra/ > test.diff
    pushd .jenkins/code_coverage
    ./gradlew test diffCoverage
    popd

    ls build/jacoco

    # Phabricator comment
    if [[ ! -f "build/comment" ]]; then
        mkdir "build/comment"
    fi

    py_command="python3 .jenkins/code_coverage/parse_jacoco_report.py --path build/jacoco/report.csv --test utest+jvmdtest"
    if [ -f "$DIFF_COVERAGE_REPORT_FILE" ]; then
        py_command+=" --diff_path $DIFF_COVERAGE_REPORT_FILE --threshold $COVERAGE_THRESHOLD"
    fi

    comment=$($py_command)
    echo "$comment" >> "$FILE_PHAB_COMMENT"
    echo "" >> "$FILE_PHAB_COMMENT"
    BASE_NAMES=$(echo $BASE_NAMES | tr ' ' '\n' | sort | xargs)
    echo "jacoco.exec files found for: **$BASE_NAMES**" >> "$FILE_PHAB_COMMENT"
else
    echo "Not all expected coverage reports are present. Deleting downloaded files and exit..."
    if [ -z "$newly_downloaded_files" ]; then
        echo "No new files were downloaded"
    else
        echo "Newly downloaded files:"
        echo "$newly_downloaded_files"

        # Delete the newly downloaded files
        echo "Deleting newly downloaded files..."
        while IFS= read -r file; do
            rm -f "$file"
            echo "Deleted $file"
        done <<< "$newly_downloaded_files"
    fi
fi

# deactivate virtualenv
deactivate
