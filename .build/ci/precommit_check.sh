#!/bin/bash
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


source "logging.sh"

skip_mypy=(
    "./logging_helper.py"
)

failed=0
log_progress "Linting ci_parser..."
for i in `find . -maxdepth 1 -name "*.py"`; do
    log_progress "Checking $i..."
    flake8 "$i"
    if [[ $? != 0 ]]; then
        failed=1
    fi

    if [[ ! " ${skip_mypy[*]} " =~ ${i} ]]; then
        mypy --ignore-missing-imports "$i"
        if [[ $? != 0 ]]; then
            failed=1
        fi
    fi
done


if [[ $failed -eq 1 ]]; then
    log_error "Failed linting. See above errors; don't merge until clean."
    exit 1
else
    log_progress "All scripts passed checks"
    exit 0
fi
