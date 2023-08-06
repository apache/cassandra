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

. ../ci_config_parser.sh
. ../assert.sh
. ../functions.sh

# For a given job, parses things and ensures that the results match what we expect.
_test_job_parse() {
    TEST_JOB=$1
    mem=$2
    cpu=$3
    storage=$4
    env=$5
    testlist_cmd=$6
    cmd=$7

    debug_log "Parsing params for job: $TEST_JOB"
    parse_job_params "test_jobs.cfg" $TEST_JOB
    failed=0
    if ! assert_equals "$mem" "$JOB_MEM" "Got unexpected memory on job parse for $TEST_JOB"; then failed=1; fi
    if ! assert_equals "$cpu" "$JOB_CPU" "Got unexpected cpu on job parse for $TEST_JOB"; then failed=1; fi
    if ! assert_equals "$storage" "$JOB_STORAGE" "Got unexpected storage on job parse for $TEST_JOB"; then failed=1; fi
    if ! assert_equals "$env" "$JOB_ENV" "Got unexpected env on job parse for $TEST_JOB"; then failed=1; fi
    if ! assert_equals "${testlist_cmd}" "${JOB_TESTLIST_CMD}" "Got unexpected testlist_cmd on job parse for $TEST_JOB"; then failed=1; fi
    if ! assert_equals "${cmd}" "${JOB_CMD}" "Got unexpected cmd on job parse for $TEST_JOB"; then failed=1; fi
    if [ $failed == 0 ]; then
        log_success "Job param parsing for $TEST_JOB completed successfully"
    else
        log_failure "Failed to parse all params for $TEST_JOB successfully"
        all_succeeded=0
    fi
    debug_log "Completed job param parsing"
}

_main() {
    all_succeeded=1

    # Confirm pipeline name parsing works as expected
    log_header "Testing pipeline name parsing"
    populate_pipelines test_pipelines.cfg
    if ! assert_equals 2 ${#pipelines[@]} "Mismatch in size of pipelines"; then
        echo "Current contents of pipelines:"
        printf '[%s]\n' "${pipelines[@]}"
        echo ""
        all_succeeded=0
    else
        log_success "Pipeline parsing completed successfully"
    fi

    #-----------------------------------------------------------------------------
    # Confirm job names from a given pipeline parsing works correctly
    clear_job_params
    log_header "Testing job name parsing"
    TEST_PIPELINE="test_one"
    populate_jobs "test_pipelines.cfg" $TEST_PIPELINE
    if ! assert_equals 4 ${#pipeline_jobs[@]} "Mismatch in number of jobs in pipeline $TEST_PIPELINE"; then
        echo "Current contents of pipeline_jobs:"
        printf '[%s]\n' "${pipeline_jobs[@]}"
        echo ""
        all_succeeded=0
    else
        log_success "Job parsing from pipeline $TEST_PIPELINE completed successfully"
    fi

    #-----------------------------------------------------------------------------
    # Test base case of job parsing; no inheritance
    clear_job_params
    log_header "Testing base job parsing with no inheritance"
    _test_job_parse "base" 10 10 10 \
        " ENV_ONE=TEN" \
        "echo \"TEN; base testlist command\"" \
        "echo \"TEN; base job command\""

    #-----------------------------------------------------------------------------
    # Test single inheritance case
    clear_job_params
    log_header "Testing job parsing with single inheritance"
    _test_job_parse "job1" 1 1 1 \
        " ENV_ONE=TEN ENV_ONE=ONE" \
        "echo \"find test_one tests\"" \
        "echo \"execute job1 cmd\""

    #-----------------------------------------------------------------------------
    # Confirm if we're a couple layers deep inheriting params it still works
    clear_job_params
    log_header "Testing job parsing multiple levels deep on inheritance"
    _test_job_parse "job3" 3 3 3 \
        " ENV_ONE=TEN ENV_ONE=ONE ENV_ONE=TWO ENV_TWO=TWO ENV_ONE=THREE ENV_TWO=THREE ENV_THREE=THREE" \
        "echo \"find test_three tests\"" \
        "echo \"execute job1 cmd\""

    log_header "Config Parsing Test Suite Results"
    if [ $all_succeeded == 1 ]; then
        log_success "All tests passed successfully"
    else
        log_failure "Some tests failed. Check logs for details."
    fi
}

_main