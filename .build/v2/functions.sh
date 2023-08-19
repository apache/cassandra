#!/usr/bin/env bash
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

# We parse out .yaml files so rely on some mature, well-proven MIT code for that.
# Usage:
#   yq '.a.b[0].c' file.yaml
if [[ $(which yq > /dev/null) -ne 0 ]]; then
    echo "yq is required to use ci functions. See https://github.com/mikefarah/yq#install"
    exit 1
fi

# $1 int: count expected
# $2 int: count actual
confirm_argument_count() {
    if [ "$#" -ne 2 ]; then
        echo "Must pass 2 arguments to ${FUNCNAME[0]}. Error in ${FUNCNAME[1]}"
        exit 1
    fi
    assert_equals $1 $2 "Unexpected number of arguments passed in to ${FUNCNAME[1]}"
}

# Confirm that a given variable exists
# $2: Message to print on error
# $1: Variable to check for definition
# return: string echo of input variable
check_argument() {
    if [ $# != 2 ]; then
        echo "Invalid call to check_argument in ${FUNCNAME[1]}. Expect message and variable to check."
        exit 1
    fi

    if [ -z "$1" ]; then
        echo "Must provide $2"
        exit 1
    fi

    echo "$1"
}

# Confirm a given file exists on disk; error out if it doesn't
# $1: Path to check
confirm_file_exists() {
    local file=$(check_argument "$1" "file name to check")
    if [ ! -f "$file" ]; then
        echo "File does not exist: ${file}. Cannot proceed (caller: ${FUNCNAME[1]})"
        exit
    fi
}

# Confirm a given directory exists on disk; error out if it doesn't
# $1: Path to check
confirm_directory_exists() {
    local dir=$(check_argument "$1" "directory name to check")
    if [ ! -d "$dir" ]; then
        echo "Directory does not exist: ${dir}. Cannot proceed (caller: ${FUNCNAME[1]})"
        exit
    fi
}

confirm_directory_does_not_exist() {
    local dir=$(check_argument "$1" "directory name to check")
    if [ -d "$dir" ]; then
        echo "Directory already exists: ${dir}. Cannot proceed (caller: ${FUNCNAME[1]})"
        exit
    fi
}

debug_log() {
    if [ -n "$DEBUG" ]; then
        echo "DEBUG: $1"
    fi
}

# For a given environment variable, we assign based on the following fallthrough:
#
# Flow:
#   1: Set in yaml override takes precedence
#   2: Then set in the yaml
#   3: Then finally the local env
init_env_var() {
    local to_init=$(check_argument "$1" "the env var to set")
    local yaml_member_path=$(check_argument "$2" "the optional yaml path to pull from if not set in local env")

    local value=retrieve_param_from_yaml "$yaml_member_path"
    if [[ -z "${value}" ]]; then
        if [[ -z "${!to_init}" ]]; then
            echo "Cannot find ${to_init} in basic yaml [$DEFAULT_YAML], yaml override [$YAML_OVERRIDES], or env: $(env). Aborting."
            exit 1
        fi
        value="${!to_init}"
    fi

    declare -g "$to_init=${value}"
}

# Will pull from the yaml override if present, defaults to base $DEFAULT_YAML if no override file is supplied.
# This method does not allow for not finding the value inside either of the .yaml files. If it's not found it'll exit out.
#
# return: value if found, empty string if none
retrieve_param_from_yaml_with_override() {
    local member=$(check_argument "$1" "the path to the member inside the .yaml to retrieve")

    result=yq "$member" "$DEFAULT_YAML"
    if [[ -n "${YAML_OVERRIDES}" ]]; then
        override=yq "$member" "$YAML_OVERRIDES"
        if [[ -n "$override" ]]; then
            result="$override"
        fi
    fi

    if [[ -z "$result" ]]; then echo ""; fi
    echo "$result"
}

retrieve_array_from_yaml() {
    local key=$(check_argument "$1" "the key pointing to the array in the .yaml file")
    local target=$(check_argument "$2" "the variable name of the global array in which to store the .yaml results")
    mapfile -t temp_array < <(yq e ".${key}[]" "this.yaml")
    declare -ag "${target}=(\"${temp_array[@]}\")"
}

retrieve_value_from_xml() {
    local file=$(check_argument "$1" "the xml file to retrieve the value from")
    local property_name=$(check_argument "$2" "the property name to extract")

    if [ ! -f "$file" ]; then
        echo "Cannot find xml file to retrieve value from: $file. Aborting." && exit 1
    fi
    grep "name=\"${property_name}\"" "$file" | awk -F'"' '{print $4}'
}

cd_with_check() {
    local dest=$(check_argument "$1" "the path to change directory to")
    if [ ! -d "${dest}" ]; then
        echo "${dest} not found; cannot change directory to it."
        exit 2
    fi
    cd "${dest}" || echo "Failed to cd to ${dest}: ${?}. Aborting." && exit 2
}

check_command() {
    local to_check=$(check_argument "$1" "the command to check the environment for")
    if ! type $to_check &> /dev/null; then
        echo "Error: $to_check is not found in the environment." && exit 1
    fi
}
