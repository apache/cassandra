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

export TEXT_RED="0;31"
export TEXT_GREEN="0;32"
export TEXT_LIGHTGREEN="1;32"
export TEXT_BROWN="0;33"
export TEXT_YELLOW="1;33"
export TEXT_BLUE="0;34"
export TEXT_LIGHTBLUE="1;34"
export TEXT_PURPLE="0;35"
export TEXT_LIGHTPURPLE="1;35"
export TEXT_CYAN="0;36"
export TEXT_LIGHTCYAN="1;36"
export TEXT_LIGHTGRAY="0;37"
export TEXT_WHITE="1;37"
export TEXT_DARKGRAY="1;30"
export TEXT_LIGHTRED="1;31"

export SILENCE_LOGGING=false
export LOG_TO_FILE="${LOG_TO_FILE:-false}"

disable_logging() {
  export SILENCE_LOGGING=true
}

enable_logging() {
  export SILENCE_LOGGING=false
}

echo_color() {
  if [[ $LOG_TO_FILE == "true" ]]; then
    echo "$1"
  elif [[ $SILENCE_LOGGING != "true" ]]; then
    echo -e "\033[1;${2}m${1}\033[0m"
  fi
}

log_header() {
  if [[ $SILENCE_LOGGING != "true" ]]; then
    log_separator
    echo_color "$1" $TEXT_GREEN
    log_separator
  fi
}

log_progress() {
  if [[ $SILENCE_LOGGING != "true" ]]; then
    echo_color "$1" $TEXT_LIGHTCYAN
  fi
}

log_info() {
  if [[ $SILENCE_LOGGING != "true" ]]; then
    echo_color "$1" $TEXT_LIGHTGRAY
  fi
}

log_quiet() {
  if [[ $SILENCE_LOGGING != "true" ]]; then
    echo_color "$1" $TEXT_DARKGRAY
  fi
}

# For transient always-on debugging
log_transient() {
  if [[ $SILENCE_LOGGING != "true" ]]; then
    echo_color "[TRANSIENT]: $1" $TEXT_BROWN
  fi
}

# For durable user-selectable debugging
log_debug() {
  if [[ "$SILENCE_LOGGING" = "true" ]]; then
    return
  fi

  if [[ "${DEBUG:-false}" == true || "${DEBUG_LOGGING:-false}" == true ]]; then
    echo_color "[DEBUG] $1" $TEXT_PURPLE
  fi
}

log_quiet() {
  if [[ $SILENCE_LOGGING != "true" ]]; then
    echo_color "$1" $TEXT_LIGHTGRAY
  fi
}

log_todo() {
  if [[ $SILENCE_LOGGING != "true" ]]; then
    echo_color "TODO: $1" $TEXT_LIGHTPURPLE
  fi
}

log_warning() {
  if [[ $SILENCE_LOGGING != "true" ]]; then
    echo_color "WARNING: $1" $TEXT_YELLOW
  fi
}

log_error() {
  if [[ $SILENCE_LOGGING != "true" ]]; then
    echo_color "ERROR: $1" $TEXT_RED
  fi
}

log_separator() {
  if [[ $SILENCE_LOGGING != "true" ]]; then
    echo_color "--------------------------------------------" $TEXT_GREEN
  fi
}