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

# Borrowed from https://github.com/torokmark/assert.sh/blob/main/assert.sh

#####################################################################
##
## title: Assert Extension
##
## description:
## Assert extension of shell (bash, ...)
##   with the common assert functions
## Function list based on:
##   http://junit.sourceforge.net/javadoc/org/junit/Assert.html
## Log methods : inspired by
##	- https://natelandau.com/bash-scripting-utilities/
## author: Mark Torok
##
## date: 07. Dec. 2016
##
## license: MIT
##
#####################################################################

. ci_functions.sh

if command -v tput &>/dev/null && tty -s; then
    RED=$(tput setaf 1)
    GREEN=$(tput setaf 2)
    MAGENTA=$(tput setaf 5)
    NORMAL=$(tput sgr0)
    BOLD=$(tput bold)
else
    RED=$(echo -en "\e[31m")
    GREEN=$(echo -en "\e[32m")
    MAGENTA=$(echo -en "\e[35m")
    NORMAL=$(echo -en "\e[00m")
    BOLD=$(echo -en "\e[01m")
fi

log_header() {
    printf "\n${BOLD}${MAGENTA}==========  %s  ==========${NORMAL}\n" "$@" >&2
}

log_success() {
    printf "${GREEN}✔ %s${NORMAL}\n" "$@" >&2
}

log_failure() {
    printf "${RED}✖ %s${NORMAL}\n" "$@" >&2
}

assert_equals() {
    local expected="$1"
    local actual="$2"
    local msg="${3-}"

    if [ "$expected" == "$actual" ]; then
        return 0
    else
        [ "${#msg}" -gt 0 ] && log_failure "Expected [$expected] != actual: [$actual]. $msg" || true
        return 1
    fi
}

assert_not_equals() {
    local expected="$1"
    local actual="$2"
    local msg="${3-}"

    if [ ! "$expected" == "$actual" ]; then
        return 0
    else
        [ "${#msg}" -gt 0 ] && log_failure "$expected != $actual :: $msg" || true
        return 1
    fi
}

assert_true() {
    local actual="$1"
    local msg="${2-}"

    assert_equals true "$actual" "$msg"
    return "$?"
}

assert_false() {
    local actual="$1"
    local msg="${2-}"

    assert_equals false "$actual" "$msg"
    return "$?"
}

assert_array_equals() {
    declare -a expected=("${!1-}")
    # echo "AAE ${expected[@]}"

    declare -a actual=("${!2}")
    # echo "AAE ${actual[@]}"

    local msg="${3-}"

    local return_code=0
    if [ ! "${#expected[@]}" == "${#actual[@]}" ]; then
        return_code=1
    fi

    local i
    for ((i = 1; i < ${#expected[@]} + 1; i += 1)); do
        if [ ! "${expected[$i - 1]}" == "${actual[$i - 1]}" ]; then
            return_code=1
            break
        fi
    done

    if [ "$return_code" == 1 ]; then
        [ "${#msg}" -gt 0 ] && log_failure "(${expected[*]}) != (${actual[*]}) :: $msg" || true
    fi

    return "$return_code"
}

assert_array_not_equals() {
    declare -a expected=("${!1-}")
    declare -a actual=("${!2}")

    local msg="${3-}"

    local return_code=1
    if [ ! "${#expected[@]}" == "${#actual[@]}" ]; then
        return_code=0
    fi

    local i
    for ((i = 1; i < ${#expected[@]} + 1; i += 1)); do
        if [ ! "${expected[$i - 1]}" == "${actual[$i - 1]}" ]; then
            return_code=0
            break
        fi
    done

    if [ "$return_code" == 1 ]; then
        [ "${#msg}" -gt 0 ] && log_failure "(${expected[*]}) == (${actual[*]}) :: $msg" || true
    fi

    return "$return_code"
}

assert_empty() {
    local actual=$1
    local msg="${2-}"

    assert_equals "" "$actual" "$msg"
    return "$?"
}

assert_not_empty() {
    local actual=$1
    local msg="${2-}"

    assert_not_equals "" "$actual" "$msg"
    return "$?"
}

assert_contain() {
    local haystack="$1"
    local needle="${2-}"
    local msg="${3-}"

    if [ -z "${needle:+x}" ]; then
        return 0
    fi

    if [ -z "${haystack##*$needle*}" ]; then
        return 0
    else
        [ "${#msg}" -gt 0 ] && log_failure "$haystack doesn't contain $needle :: $msg" || true
        return 1
    fi
}

assert_not_contain() {
    local haystack="$1"
    local needle="${2-}"
    local msg="${3-}"

    if [ -z "${needle:+x}" ]; then
        return 0
    fi

    if [ "${haystack##*$needle*}" ]; then
        return 0
    else
        [ "${#msg}" -gt 0 ] && log_failure "$haystack contains $needle :: $msg" || true
        return 1
    fi
}

assert_gt() {
    local first="$1"
    local second="$2"
    local msg="${3-}"

    if [[ "$first" -gt "$second" ]]; then
        return 0
    else
        [ "${#msg}" -gt 0 ] && log_failure "$first > $second :: $msg" || true
        return 1
    fi
}

assert_ge() {
    local first="$1"
    local second="$2"
    local msg="${3-}"

    if [[ "$first" -ge "$second" ]]; then
        return 0
    else
        [ "${#msg}" -gt 0 ] && log_failure "$first >= $second :: $msg" || true
        return 1
    fi
}

assert_lt() {
    local first="$1"
    local second="$2"
    local msg="${3-}"

    if [[ "$first" -lt "$second" ]]; then
        return 0
    else
        [ "${#msg}" -gt 0 ] && log_failure "$first < $second :: $msg" || true
        return 1
    fi
}

assert_le() {
    local first="$1"
    local second="$2"
    local msg="${3-}"

    if [[ "$first" -le "$second" ]]; then
        return 0
    else
        [ "${#msg}" -gt 0 ] && log_failure "$first <= $second :: $msg" || true
        return 1
    fi
}
