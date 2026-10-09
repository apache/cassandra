#!/usr/bin/env python3
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

"""Skip MD5 image tags only when both registries have both required platforms."""

import argparse
import json
import subprocess
import sys


REGISTRIES = ("docker.io", "apache.jfrog.io/cassan-docker")
PLATFORMS = {("linux", "amd64"), ("linux", "arm64")}


def complete_image(reference):
    try:
        result = subprocess.run(["docker", "buildx", "imagetools", "inspect", "--raw", reference],
                                capture_output=True, text=True, check=False, timeout=120)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Timed out inspecting {reference}") from None
    if result.returncode:
        error = result.stderr.lower().strip()
        missing = f"{reference.lower()}: not found"
        if "manifest unknown" in error or error in (missing, f"error: {missing}"):
            return False
        # Do not mistake an authentication or transport failure for a missing tag,
        # or include potentially sensitive provider diagnostics in our exception.
        raise RuntimeError(f"Cannot inspect {reference}; check registry access and availability")
    manifest = json.loads(result.stdout)
    platforms = {(item.get("platform", {}).get("os"), item.get("platform", {}).get("architecture"))
                 for item in manifest.get("manifests", [])}
    return PLATFORMS <= platforms


def missing_images(matrix):
    images = []
    for image in matrix["include"]:
        # Inspect both registries even when the first is missing, so errors in
        # the second registry stop the run before a costly build starts.
        complete = [complete_image(f"{registry}/apache/cassandra-{image['name']}:{image['tag']}")
                    for registry in REGISTRIES]
        if all(complete):
            print(f"Skipping {image['name']}:{image['tag']}: both registries have both platforms", file=sys.stderr)
        else:
            images.append(image)
    return {"include": images}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", metavar="IMAGE", help="Require both platforms in one published image")
    args = parser.parse_args()
    try:
        if args.verify:
            if not complete_image(args.verify):
                raise RuntimeError(f"Missing AMD64 or ARM64 image: {args.verify}")
        else:
            print(json.dumps(missing_images(json.load(sys.stdin)), separators=(",", ":")))
    except (RuntimeError, ValueError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
