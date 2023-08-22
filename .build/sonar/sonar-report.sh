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

REPORT_FILE="${1:-sonar-report.json}"
SEVERITIES="${2:-BLOCKER,CRITICAL,MAJOR}"
SCOPE=${3:-MAIN}

if [ -z "$SONAR_HOST_URL" ]; then
  SONAR_HOST_URL="http://127.0.0.1:9000"
fi

if [ -z "$SONAR_PROJECT_KEY" ]; then
  echo "SONAR_PROJECT_KEY is not set"
  exit 1
fi

rm -f "$REPORT_FILE"

# Maximum page size is 500, maximum number of returned issues is 10000
for i in $(seq 1 20); do
  if [ -n "$SONAR_CASSANDRA_TOKEN" ]; then
    curl -sS --header "Authorization: Bearer ${SONAR_CASSANDRA_TOKEN}" "$SONAR_HOST_URL/api/issues/search?s=SEVERITY&asc=false&scopes=${SCOPE}&severities=${SEVERITIES}&components=${SONAR_PROJECT_KEY}&ps=500&p=$i" -o "${REPORT_FILE}.page"
  else
    curl -sS "$SONAR_HOST_URL/api/issues/search?s=SEVERITY&asc=false&scopes=${SCOPE}&severities=${SEVERITIES}&components=${SONAR_PROJECT_KEY}&ps=500&p=$i" -o "${REPORT_FILE}.page"
  fi
  if [ ! -s "${REPORT_FILE}.page" ]; then
    echo "Failed to fetch SonarQube report"
    rm -f "${REPORT_FILE}.page"
    rm -f "${REPORT_FILE}.page.array"
    rm -f "${REPORT_FILE}.array"
    exit 1
  fi
  cnt="$(jq '.issues | length' "${REPORT_FILE}.page")"
  if [ "${cnt}" == "0" ] || [ "${cnt}" == "" ]; then
    break
  fi
  jq -r '.issues' "${REPORT_FILE}.page" > "${REPORT_FILE}.page.array"

  if [ -f "${REPORT_FILE}" ]; then
    jq -r -s '.[0] + .[1]' "${REPORT_FILE}" "${REPORT_FILE}.page.array" > "${REPORT_FILE}.array"
    mv -f "${REPORT_FILE}.array" "${REPORT_FILE}"
  else
    mv -f "${REPORT_FILE}.page.array" "${REPORT_FILE}"
  fi
done

rm -f "${REPORT_FILE}.page"
rm -f "${REPORT_FILE}.page.array"
