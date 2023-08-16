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
