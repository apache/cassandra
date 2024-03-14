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

importSonarW() {
  PROJECT_ROOT=${PROJECT_ROOT:-$(git rev-parse --show-toplevel)}

  source "${PROJECT_ROOT}/sonarqube/import.sh"

  # Export sonar scanner maven artifact name for both linux and macos
  SONAR_SCANNER_VERSION="4.6.0.2311.3"
  export SONAR_SCANNER_LINUX="mvn:com.uber.sonarqube:sonar-scanner-linux:zip:${SONAR_SCANNER_VERSION}"
  export SONAR_SCANNER_MACOS="mvn:com.uber.sonarqube:sonar-scanner-macos:zip:${SONAR_SCANNER_VERSION}"

  # Download sonar-wrapper-pex for given version and export the executable sonar-wrapper-pex
  SONARW_PEX_VERSION="1.5.8"
  SONARW_ZIP_URL="mvn:com.uber.sonarqube:sonar-wrapper:zip:${SONARW_PEX_VERSION}"
  SONAR_WRAPPER_ZIP="$(import "$SONARW_ZIP_URL")"
  SONAR_WRAPPER_ZIP_DIR=$(dirname "$SONAR_WRAPPER_ZIP")
  export SONAR_WRAPPER_PEX="$SONAR_WRAPPER_ZIP_DIR/sonar-wrapper.pex"

  if [ ! -f "$SONAR_WRAPPER_PEX" ]; then
    unzip -d "$SONAR_WRAPPER_ZIP_DIR" "$SONAR_WRAPPER_ZIP"
  fi

  if [[ ! -x "$SONAR_WRAPPER_PEX" ]]; then
    chmod +x "$SONAR_WRAPPER_PEX"
  fi
}