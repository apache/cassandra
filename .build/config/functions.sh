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

##############################################################################
# Helper functions for use in our build scripting
##############################################################################

# Confirm that a given variable exists
# $1: Message to print on error
# $2: Variable to check for definition
check_argument() {
    if [ $# != 2 ]; then
        echo "Invalid call to check_argument. Expect message and variable to check."
        exit
    fi

    if [ -z "$2" ]; then
        echo "$1"
        exit
    fi
}

# Confirm a given file exists on disk
# $1: Path to check
confirm_file_exists() {
    check_argument "${FUNCNAME[0]} requires one argument", "$1"
    if [ ! -f "$1" ]; then
        echo "File does not exist: $1. Cannot proceed."
        exit
    fi
}

debug_log() {
    if [ -n "$DEBUG" ]; then
        echo "DEBUG: $1"
    fi
}
