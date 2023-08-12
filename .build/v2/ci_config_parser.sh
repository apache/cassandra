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

. ci_functions.sh

# This script relies on yq: https://github.com/mikefarah/yq
# License: MIT: https://github.com/jmckenzie-dev/yq/blob/master/LICENSE
# Plain binary install: wget https://github.com/mikefarah/yq/releases/download/${VERSION}/${BINARY} -O /usr/bin/yq &&\ chmod +x /usr/bin/yq
# Brew install: "brew install yq"

# Text array of all known pipelines found in processed config file
export pipelines=()

# Text array of all known jobs found in the jobs config
export pipeline_jobs=()

# The keys for various properties as defined in the test jobs.yaml file
KEY_PARENT="parent"
KEY_CMD="cmd"
KEY_ENV="env"
KEY_TESTLIST_CMD="testlist"
KEY_MEM="memory"
KEY_CPU="cpu"
KEY_STORAGE="storage"

# Params for the currently processing job; reference these from within your build pipeline for a given job
clear_job_params() {
    export JOB_CMD=""
    export JOB_ENV=""
    export JOB_TESTLIST_CMD=""
    export JOB_MEM=""
    export JOB_CPU=""
    export JOB_STORAGE=""
}

# Do an initial call so the variables are all cleared out
clear_job_params

# See cassandra_ci.yaml for format reference
populate_pipelines() {
    check_argument "${FUNCNAME[0]} requires a primary argument of a filename containing pipeline names", "$1"
    debug_log "Populating pipelines from file: $1"
    while read -r line; do
        debug_log "Processing line: $line"
        if [[ "$line" =~ "pipeline:" ]]; then
            debug_log "Matches pipeline:; processing"
            IFS=: read -r key value <<< "$line"
            pipelines+=("$value")
        else
            debug_log "Skipping line: $line"
        fi
    done < "$1"
}

# Name of contained jobs for given pipeline are stored in pipeline_jobs
# $1: filename containing pipelines; see cassandra_ci.yaml
# $2: name of pipeline to parse params into current_pipeline array
populate_jobs() {
    check_argument "${FUNCNAME[0]} requires a primary argument of a filename with pipelines defined", "$1"
    check_argument "${FUNCNAME[0]} requires a secondary argument of a pipeline name to parse into pipeline_jobs", "$2"
    local _file_name=$1
    confirm_file_exists "$_file_name"
    local _target_pipeline=$2

    # Clear out the dict if it already exists
    local found_pipeline=0

    debug_log "Populating jobs from file: $_file_name"
    while read -r line; do
        debug_log "Processing line: $line"
        # Skip whitespace only lines
        if [[ -z "${line// }" ]]; then
            debug_log "Skipping whitespace only line"
        elif [[ "$line" = "pipeline:$_target_pipeline" ]]; then
            debug_log "Found pipeline; entering pipeline processing"
            found_pipeline=1
        # if we're processing a pipeline and see the start of another, we're done.
        elif [[ $found_pipeline == 1 && "$line" =~ "pipeline:" ]]; then
            debug_log "Terminating processing upon finding subsequent pipeline"
            return 0
        elif [ $found_pipeline == 1 ]; then
            debug_log "Adding to pipeline_jobs: $line"
            pipeline_jobs+=("$line")
        fi
    done < "$_file_name"
    if [ $found_pipeline == 0 ]; then
        echo "Failed to find pipeline: ${_target_pipeline} in file: ${_file_name}"
        exit 1
    fi
    return 0
}

# Parameters for active job are stored in current_job_params
# Before calling this in a script, ensure you run "clear_job_params" first so no vestigial params persist
# $1 filename of config file with job
# $2 job name to parse params from into global exports
parse_job_params() {
    check_argument "${FUNCNAME[0]} requires a primary argument of a filename with jobs defined", "$1"
    check_argument "${FUNCNAME[0]} requires a secondary argument of a job name to parse into current_job_params", "$2"
    local _file_name=$1
    confirm_file_exists $_file_name
    local _target_job=$2

    # We can't clear_job_params here as we rely on recursion of parent parsing to compose final values
    local found_job=0

    while read -r line; do
        debug_log "processing line [$line] for [job:$_target_job]"
        if [[ "$line" == "job:$_target_job" ]]; then
            debug_log "Found; flipping into processing mode"
            found_job=1
        # if we're processing a pipeline and see the start of another, we're done.
        elif [[ $found_job == 1 && $line == *"job:"* ]]; then
            debug_log "Hit terminal condition; stopping processing."
            return 0
        elif [ $found_job == 1 ]; then
            IFS=: read -r key value <<< "$line"
            debug_log "Processing k/v pair for job. key: $key value: $value"
            # Recurse up to the parent job
            if [[ "$key" = "$KEY_PARENT" ]]; then
                debug_log "Found parent key. Recursing with new job: $value"
                debug_log "------------------------ Recursing"
                parse_job_params "$_file_name" "$value"
                debug_log "------------------------ Done recursing"
            # In the special case of it being an env definition, we need to append the new value instead of overwrite
            elif [[ "$key" = "$KEY_ENV" ]]; then
                debug_log "Combining env. Env prior: [${JOB_ENV}]"
                NEW_ENV="${JOB_ENV} $value"
                JOB_ENV=${NEW_ENV}
                debug_log "Combined env. Env post: [${JOB_ENV}]"
            else
                case $key in
                    "$KEY_CMD")
                        JOB_CMD=$value
                        debug_log "Set job command to: $JOB_TESTLIST_CMD";;
                    "$KEY_TESTLIST_CMD")
                        JOB_TESTLIST_CMD=$value
                        debug_log "Set testlist command to: $JOB_TESTLIST_CMD";;
                    "$KEY_MEM")
                        JOB_MEM=$value
                        debug_log "Set mem to: $JOB_TESTLIST_CMD";;
                    "$KEY_CPU")
                        JOB_CPU=$value
                        debug_log "Set cpu to: $JOB_TESTLIST_CMD";;
                    "$KEY_STORAGE")
                        JOB_STORAGE=$value
                        debug_log "Set storage to: $JOB_TESTLIST_CMD";;
                esac
            fi
        fi
    done < "$_file_name"
    if [ $found_job == 0 ]; then
        echo "Failed to find job: ${_target_job} in file: ${_file_name}"
        exit 1
    fi
    return 0
}

