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

#!/bin/sh

set -e

if [ -z "$SONAR_HOST_URL" ]; then
  SONAR_HOST_URL="http://127.0.0.1:9000"
fi

if [ -z "$SONAR_PROJECT_KEY" ]; then
  echo "SONAR_PROJECT_KEY is not set"
  exit 1
fi

creds="${1:-'admin:password'}"

quality_profile_name="cassandra"
quality_profile_config="${2:-.build/sonar/sonar-quality-profile.xml}"

quality_gate_name="cassandra"
quality_gate_config="${3:-.build/sonar/sonar-quality-gate.json}"


curl_cmd() {
  curl -sS -u "$creds" "$@"
}

# Create project
curl_cmd -X POST "${SONAR_HOST_URL}/api/projects/create?name=${SONAR_PROJECT_KEY}&mainBranch=trunk&newCodeDefinitionType=PREVIOUS_VERSION&project=${SONAR_PROJECT_KEY}&visibility=public" 1>&2

# Setup quality profile
curl_cmd -X POST "${SONAR_HOST_URL}/api/qualityprofiles/restore" --form "backup=@${quality_profile_config}" 1>&2
curl_cmd -X POST "${SONAR_HOST_URL}/api/qualityprofiles/add_project?language=java&project=${SONAR_PROJECT_KEY}&qualityProfile=${quality_profile_name}" 1>&2

# Setup quality gate
curl_cmd -X POST "${SONAR_HOST_URL}/api/qualitygates/destroy?name=${quality_gate_name}" 1>&2 2>/dev/null || true
curl_cmd -X POST "${SONAR_HOST_URL}/api/qualitygates/create?name=${quality_gate_name}" 1>&2
for cond_id in $(curl_cmd -X GET "${SONAR_HOST_URL}/api/qualitygates/show?name=${quality_gate_name}" | jq -r '.conditions[].id')
do
  curl_cmd -X POST "${SONAR_HOST_URL}/api/qualitygates/delete_condition?id=${cond_id}" 1>&2
done

for cond in $(jq -r '.conditions[] | "metric=\(.metric)&op=\(.op)&error=\(.error)"' "${quality_gate_config}")
do
  curl_cmd -X POST "${SONAR_HOST_URL}/api/qualitygates/create_condition?gateName=${quality_gate_name}&${cond}" 1>&2
done

curl_cmd -X POST "${SONAR_HOST_URL}/api/qualitygates/select?gateName=${quality_gate_name}&projectKey=${SONAR_PROJECT_KEY}" 1>&2

# Setup access token

# Drop the existing access token so that we can create it once again - we need to recreate a token because it is
# impossible to read the existing token
curl_cmd -X POST "${SONAR_HOST_URL}/api/user_tokens/revoke?name=cassandra&projectKey=${SONAR_PROJECT_KEY}" 1>&2 2>/dev/null || true

# Create the access token for the project analysis
curl_cmd -X POST "${SONAR_HOST_URL}/api/user_tokens/generate?name=cassandra&type=PROJECT_ANALYSIS_TOKEN&projectKey=${SONAR_PROJECT_KEY}" | jq -r '.token'
