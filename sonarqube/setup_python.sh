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

set -x

source "$PROJECT_ROOT/sonarqube/import.sh"

function generate_env_props() {
  local build_dir="$PROJECT_ROOT/build"
  # Make the build dir if not already
  mkdir -p "$build_dir" || true

  local python_env_properties="$build_dir/.env_python_properties"
  local python_env_path="$build_dir/python_env"

  echo "PATH=$PYTHON_BINARY_HOME/bin:$PATH" > "$python_env_properties"

  export PATH=$PYTHON_BINARY_HOME/bin:$PATH
  echo "export PATH=$PYTHON_BINARY_HOME/bin:\$PATH" > "$python_env_path"
}

function download_and_extract_python() {
  local python_binary
  python_binary="$(import "$PYTHON_BINARY_VERSION")"
  unzip -q "$python_binary" -d "$HOME"
}

PYTHON_MAJOR_VERSION=$(echo "$PYTHON_SDK_NUMBER" | cut -d'.' -f 1)
PYTHON_BINARY_HOME="$HOME/python_home/$PYTHON_SDK_NUMBER"

# Only download if we already don't have the python binary installed
if [[ ! -f "$PYTHON_BINARY_HOME/bin/python${PYTHON_MAJOR_VERSION}" ]]; then
  download_and_extract_python
else
  echo "Python-$PYTHON_SDK_NUMBER already installed in $PYTHON_BINARY_HOME. Using this version instead."
fi

generate_env_props