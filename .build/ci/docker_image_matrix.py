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

"""Select Dockerfiles and the MD5 tags consumed by the in-tree build scripts."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


def git_paths(root, *args):
    output = subprocess.check_output(["git", *args], cwd=root)
    return {Path(path.decode("utf-8")) for path in output.split(b"\0") if path}


def image_matrix(root, before=None):
    paths = git_paths(root, "ls-files", "-z", "--", ".build/docker")
    # A new branch has no previous commit; a manual run also selects all images.
    if before and before != "0" * 40:
        base = subprocess.run(["git", "rev-parse", "--verify", "--quiet", f"{before}^{{commit}}"],
                              cwd=root, stdout=subprocess.DEVNULL)
        if base.returncode == 1:
            # A force push can leave the previous commit outside fetched history.
            print(f"Warning: Previous commit {before} is unavailable; selecting all Dockerfiles.",
                  file=sys.stderr)
        else:
            base.check_returncode()
            paths &= git_paths(root, "diff", "--name-only", "-z", "--no-renames",
                               "--diff-filter=AMT", before, "HEAD", "--", ".build/docker")

    images = []
    for path in sorted(paths):
        if path.parent != Path(".build/docker") or path.suffix != ".docker":
            continue
        if not re.fullmatch(r"[a-z0-9]+(?:[._-][a-z0-9]+)*", path.stem):
            raise ValueError(f"Invalid Docker image name: {path.stem}")
        source = root / path
        if source.is_symlink() or not source.is_file():
            raise ValueError(f"Dockerfile must be a regular file: {path}")
        images.append({
            "name": path.stem,
            "dockerfile": path.as_posix(),
            "tag": hashlib.md5(source.read_bytes()).hexdigest(),
        })
    return {"include": images}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--before", help="Commit before the push (compare with HEAD)")
    selection.add_argument("--all", action="store_true", help="Select all tracked Dockerfiles")
    parser.add_argument("--image", help="Select one Dockerfile basename without .docker")
    args = parser.parse_args()
    if args.before and not re.fullmatch(r"[0-9a-f]{40}", args.before):
        parser.error("--before must be a full Git commit SHA")
    root = Path(__file__).resolve().parents[2]
    matrix = image_matrix(root, args.before)
    if args.image:
        matrix["include"] = [image for image in matrix["include"] if image["name"] == args.image]
        if not matrix["include"]:
            parser.error(f"No selected Dockerfile named {args.image}.docker")
    print(json.dumps(matrix, separators=(",", ":")))


if __name__ == "__main__":
    main()
