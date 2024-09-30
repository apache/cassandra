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
# How we get the buildkite build id for a given diff
# 1. differential.diff.search (id) diff id -> revisionPHID
# 2. harbormaster.buildable.search (containerPHID) revisionPHID -> buildablePHID
# 3. harbormaster.build.search (buildables) buildablePHID -> list of buildPHID + name
# 4. harbormaster.target.search (buildPHIDs) buildPHID -> buildTargetPHID
# 5. harbormaster.log.search (buildTargetPHID) buildTargetPHID -> filePHID
# 6. file.download filePHID -> base64-bytes -> BUILDKITE_BUILD_ID
#
# Partial of the script is modified from tooling/ci/kameleon/refactorings/stale_feature_flag_cleanup/stale_feature_flag_cleanup/common/phab/utils.py

import json
import subprocess
from typing import List, Set, Dict, Any
import logging
import requests
import argparse
import base64

parser = argparse.ArgumentParser(
    description='Download coverage reports from artifacts for all builds under the diff revision.')
parser.add_argument("--diff_id", required=True,
                    help='Phabricator Differential Diff ID (should be an integer)')


def _api_conduit_call(end_point):
    call_command = [
        "arc",
        "call-conduit",
        "--conduit-uri",
        "https://code.uberinternal.com/api",
    ]
    call_command.append(end_point)
    return call_command


def api_request(end_point, params, callback_func):
    call_command = _api_conduit_call(end_point)
    p = subprocess.Popen(
        call_command, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    response, _ = p.communicate(input=json.dumps(params).encode())
    if not response:
        raise Exception(
            "Unsuccessful api call to {} with params:\n{}".format(
                end_point, params)
        )

    print(f"Response: {json.loads(response)}")
    return callback_func(json.loads(response))


def get_revision_phid_from_diff_id(diff_id):
    request_params = {"constraints": {"ids": diff_id}}

    def process_result(res):
        return [obj["fields"]["revisionPHID"] for obj in res["response"]["data"]]
    return api_request("differential.diff.search", request_params, process_result)


def get_newest_buildable_phid_from_revision_phid(revision_phid):
    request_params = {
        "constraints": {
            "containerPHIDs": revision_phid,
            "manual": False,  # exclude manual builds (re-generate the report)
        },
        "order": "newest",
    }

    def process_result(res):
        # one revision_phid has multiple buildable_phid, we take only the newest one
        buildable_phids = [obj["phid"] for obj in res["response"]["data"]]
        return [buildable_phids[0]]
    return api_request("harbormaster.buildable.search", request_params, process_result)


def get_build_phids_from_buildable_phid(buildable_phid):
    request_params = {"constraints": {"buildables": buildable_phid}}

    def process_result(res):
        # one buildable_phid has multiple build_phids
        # we only want the build_phids for utest/jvm-dtest
        return [obj["phid"] for obj in res["response"]["data"] if "utest" in obj["fields"]["name"] or "jvm" in obj["fields"]["name"]]
    return api_request("harbormaster.build.search", request_params, process_result)


def get_build_target_phids_from_build_phids(build_phids):
    request_params = {"constraints": {"buildPHIDs": build_phids}}

    def process_result(res):
        # one build_phid has one build_target_phid
        return [obj["phid"] for obj in res["response"]["data"]]
    return api_request("harbormaster.target.search", request_params, process_result)


def get_file_phids_from_build_target_phids(build_target_phids):
    request_params = {"constraints": {"buildTargetPHIDs": build_target_phids}}

    def process_result(res):
        # one build_target_phid may have multiple file_phids
        return [obj["fields"]["filePHID"] for obj in res["response"]["data"]]
    return api_request("harbormaster.log.search", request_params, process_result)


def download_files_from_file_phids(file_phids):
    raw_files = []
    for file_phid in file_phids:
        request_params = {"phid": file_phid}

        def process_result(res):
            return res["response"]
        raw_files.append(api_request(
            "file.download", request_params, process_result))
    return raw_files


def convert_base64_to_json_str(raw_files):
    return [base64.b64decode(raw_file).decode('utf-8') for raw_file in raw_files]


def try_get_buildkite_build_id(json_strs):
    buildkite_build_ids = []
    for json_str in json_strs:
        try:
            json_obj = json.loads(json_str)
            buildkite_build_ids.append(json_obj["id"])
        except Exception as e:
            # Usually we see 2 log files for each build target, one with request metadata and another with file metadata.
            # We read the buildkite build id from the file metadata.
            continue
    return buildkite_build_ids


def main(args):
    print("MAIN start")
    revision_phid = get_revision_phid_from_diff_id([int(args.diff_id)])
    print(f"Revision PHID: {revision_phid}")
    buildable_phid = get_newest_buildable_phid_from_revision_phid(
        revision_phid)
    print(f"Newest Buildable PHID: {buildable_phid}")
    build_phids = get_build_phids_from_buildable_phid(buildable_phid)
    print(f"Build PHIDs: {build_phids}")
    print(f"length of Build PHIDs: {len(build_phids)}")
    build_target_phids = get_build_target_phids_from_build_phids(build_phids)
    print(f"Build Target PHIDs: {build_target_phids}")
    file_phids = get_file_phids_from_build_target_phids(build_target_phids)
    print(f"File PHIDs: {file_phids}")
    raw_files = download_files_from_file_phids(file_phids)
    json_strs = convert_base64_to_json_str(raw_files)
    buildkite_build_ids = try_get_buildkite_build_id(json_strs)
    print(f"Buildkite Build IDs: {buildkite_build_ids}")
    print(f"length of Buildkite Build IDs: {len(buildkite_build_ids)}")

    with open("/tmp/buildkite_build_ids.json", "w") as f:
        json.dump(buildkite_build_ids, f)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
