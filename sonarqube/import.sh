#! /bin/bash

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

ARTIFACTORY_BASE_URL="http://artifactory.uber.internal:4587/artifactory/java"

#To download required artifacts from uber artifactory
import(){
  echo "$1"

  IFS=':' read -ra COORDS <<< "$1"

  LEN_COORDS=${#COORDS[@]}

  if [[ $LEN_COORDS -lt 5 ]] || [[ $LEN_COORDS -gt 6 ]]; then
      echo "Incorrect maven coordinates : $1"
      exit 1
  fi

  GROUP="$(echo "${COORDS[1]}" | sed 's/\./\//g')"
  ID="${COORDS[2]}"
  TYPE="${COORDS[3]}"
  VERSION="${COORDS[4]}"
  ARTIFACT_NAME="$ID-$VERSION"

  if [ "$LEN_COORDS" -eq 6 ]; then
    ARTIFACT_NAME="${ARTIFACT_NAME}-${COORDS[5]}"
  fi

  local URL="$ARTIFACTORY_BASE_URL/$GROUP/$ID/$VERSION/$ARTIFACT_NAME.$TYPE"

  local curl_flags=(--fail --connect-timeout 5 --progress-bar -fSL --netrc-optional --output "$1")
  STATUS_CODE="$(curl "${curl_flags[@]}" "$URL" | tail -1)"
  echo "$STATUS_CODE"
}