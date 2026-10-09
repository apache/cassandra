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

"""Registry responses must distinguish a missing tag from an unavailable registry."""

import importlib.util
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch


MODULE_PATH = Path(__file__).resolve().parents[2] / "ci/docker_image_registry.py"
SPEC = importlib.util.spec_from_file_location("docker_image_registry", MODULE_PATH)
REGISTRY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REGISTRY)


def response(platforms):
    manifest = {"manifests": [{"platform": {"os": os, "architecture": arch}} for os, arch in platforms]}
    return subprocess.CompletedProcess([], 0, json.dumps(manifest), "")


class DockerImageRegistryTest(unittest.TestCase):
    def setUp(self):
        self.image = {"name": "ubuntu-test", "tag": "900150983cd24fb0d6963f7d28e17f72",
                      "dockerfile": ".build/docker/ubuntu-test.docker"}
        self.matrix = {"include": [self.image]}
        self.reference = f"docker.io/apache/cassandra-ubuntu-test:{self.image['tag']}"
        self.complete = response([("linux", "amd64"), ("linux", "arm64"), ("unknown", "unknown")])

    @patch.object(REGISTRY.subprocess, "run")
    def test_existing_tag_is_skipped_only_when_both_registries_are_complete(self, run):
        run.return_value = self.complete
        self.assertEqual({"include": []}, REGISTRY.missing_images(self.matrix))
        self.assertEqual([self.reference, f"apache.jfrog.io/cassan-docker/apache/cassandra-ubuntu-test:{self.image['tag']}"],
                         [call.args[0][-1] for call in run.call_args_list])

    @patch.object(REGISTRY.subprocess, "run")
    def test_missing_or_incomplete_mirror_needs_publication(self, run):
        for mirror in (response([("linux", "amd64")]),
                       response([("linux", "amd64"), ("windows", "arm64")]),
                       subprocess.CompletedProcess([], 1, "", "ERROR: manifest unknown")):
            with self.subTest(mirror=mirror):
                run.side_effect = [self.complete, mirror]
                self.assertEqual(self.matrix, REGISTRY.missing_images(self.matrix))

    @patch.object(REGISTRY.subprocess, "run")
    def test_missing_tag_diagnostics(self, run):
        for error in ("ERROR: manifest unknown", f"ERROR: {self.reference}: not found"):
            with self.subTest(error=error):
                run.return_value = subprocess.CompletedProcess([], 1, "", error)
                self.assertFalse(REGISTRY.complete_image(self.reference))

    @patch.object(REGISTRY.subprocess, "run")
    def test_registry_errors_fail_without_disclosing_provider_output(self, run):
        for error in ("401 Unauthorized", "403 Forbidden", "429 Too Many Requests", "TLS handshake timeout",
                      "dial tcp: no such host", "500 Internal Server Error", "credential helper not found",
                      f"rejecting 100000000 byte manifest for {self.reference}: not found"):
            with self.subTest(error=error):
                run.return_value = subprocess.CompletedProcess([], 1, "", error + " private-provider-detail")
                with self.assertRaises(RuntimeError) as caught:
                    REGISTRY.complete_image(self.reference)
                self.assertNotIn("private-provider-detail", str(caught.exception))

    @patch.object(REGISTRY.subprocess, "run")
    def test_inspection_timeout_fails_without_provider_output(self, run):
        run.side_effect = subprocess.TimeoutExpired("docker", 120, output="private-provider-detail")
        with self.assertRaisesRegex(RuntimeError, "Timed out inspecting") as caught:
            REGISTRY.complete_image(self.reference)
        self.assertNotIn("private-provider-detail", str(caught.exception))

    @patch.object(REGISTRY.subprocess, "run")
    def test_both_registries_checked_even_when_first_tag_is_missing(self, run):
        run.side_effect = [subprocess.CompletedProcess([], 1, "", "manifest unknown"),
                           subprocess.CompletedProcess([], 1, "", "403 Forbidden")]
        with self.assertRaises(RuntimeError):
            REGISTRY.missing_images(self.matrix)
        self.assertEqual(2, run.call_count)

    @patch.object(REGISTRY.subprocess, "run")
    def test_single_platform_manifest_is_not_complete(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, '{"schemaVersion":2,"config":{}}', "")
        self.assertFalse(REGISTRY.complete_image(self.reference))

    @patch.object(REGISTRY.subprocess, "run")
    def test_invalid_manifest_is_an_error(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, "not JSON", "")
        with self.assertRaises(ValueError):
            REGISTRY.complete_image(self.reference)

    @patch.object(REGISTRY.subprocess, "run")
    def test_next_branch_run_skips_image_published_by_previous_run(self, run):
        missing = subprocess.CompletedProcess([], 1, "", "manifest unknown")
        run.side_effect = [missing, missing, self.complete, self.complete]
        self.assertEqual(self.matrix, REGISTRY.missing_images(self.matrix))
        self.assertEqual({"include": []}, REGISTRY.missing_images(self.matrix))


if __name__ == "__main__":
    unittest.main()
