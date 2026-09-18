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

"""Exercise image selection against real, temporary Git histories."""

import importlib.util
from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


MODULE_PATH = Path(__file__).resolve().parents[2] / "ci/docker_image_matrix.py"
SPEC = importlib.util.spec_from_file_location("docker_image_matrix", MODULE_PATH)
MATRIX = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MATRIX)


class DockerImageMatrixTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.git("init", "-q")
        self.git("config", "user.name", "Image selection test")
        self.git("config", "user.email", "test@example.invalid")
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "core.hooksPath", "/dev/null")
        self.write(".build/docker/first.docker", "abc")
        self.write(".build/docker/second.docker", "unchanged")
        self.before = self.commit()

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root, stderr=subprocess.PIPE).decode().strip()

    def write(self, name, contents):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")

    def commit(self):
        self.git("add", ".")
        self.git("commit", "-qm", "Test fixture")
        return self.git("rev-parse", "HEAD")

    def images(self, before=None):
        return MATRIX.image_matrix(self.root, before)["include"]

    def test_manual_run_selects_all_tracked_images_with_consumer_tags(self):
        self.write(".build/docker/untracked.docker", "ignore me")
        images = self.images()
        self.assertEqual(["first", "second"], [image["name"] for image in images])
        self.assertEqual({"name": "first", "dockerfile": ".build/docker/first.docker",
                          "tag": "900150983cd24fb0d6963f7d28e17f72"}, images[0])

    def test_push_includes_changes_from_every_commit(self):
        self.write(".build/docker/first.docker", "changed")
        self.commit()
        self.write(".build/docker/third.docker", "new")
        self.commit()
        self.assertEqual(["first", "third"], [image["name"] for image in self.images(self.before)])

    def test_new_branch_selects_all_images(self):
        self.assertEqual(self.images(), self.images("0" * 40))

    def test_rename_publishes_new_name_and_deletion_publishes_nothing(self):
        self.git("mv", ".build/docker/first.docker", ".build/docker/renamed.docker")
        (self.root / ".build/docker/second.docker").unlink()
        self.commit()
        self.assertEqual(["renamed"], [image["name"] for image in self.images(self.before)])

    def test_deletion_only_produces_empty_matrix(self):
        (self.root / ".build/docker/first.docker").unlink()
        self.commit()
        self.assertEqual([], self.images(self.before))

    def test_non_images_and_nested_dockerfiles_are_excluded(self):
        self.write(".build/docker/helper.sh", "helper")
        self.write(".build/docker/nested/third.docker", "nested")
        self.write(".jenkins/agent.docker", "agent")
        self.commit()
        self.assertEqual([], self.images(self.before))

    def test_unchanged_or_reverted_dockerfiles_are_not_rebuilt(self):
        self.assertEqual([], self.images(self.before))
        self.write(".build/docker/first.docker", "temporary change")
        self.commit()
        self.write(".build/docker/first.docker", "abc")
        self.commit()
        self.assertEqual([], self.images(self.before))

    def test_missing_previous_commit_selects_all_images(self):
        warning = io.StringIO()
        with redirect_stderr(warning):
            self.assertEqual(self.images(), self.images("1" * 40))
        self.assertIn("1" * 40, warning.getvalue())

    def test_force_push_with_missing_previous_commit_produces_valid_cli_matrix(self):
        self.write(".build/docker/first.docker", "original change")
        before = self.commit()
        self.write(".build/docker/first.docker", "amended change")
        self.git("add", ".")
        self.git("commit", "--amend", "--no-edit", "-q")
        checkout = self.root / "checkout"
        # Clone reachable history only, as checkout does after a force push.
        self.git("clone", "--quiet", "--no-local", str(self.root), str(checkout))
        with self.assertRaises(subprocess.CalledProcessError):
            subprocess.check_output(["git", "cat-file", "-e", before], cwd=checkout,
                                    stderr=subprocess.PIPE)

        output, warning = io.StringIO(), io.StringIO()
        with patch.object(MATRIX, "__file__", str(checkout / ".build/ci/docker_image_matrix.py")), \
                patch("sys.argv", ["docker_image_matrix.py", "--before", before]), \
                redirect_stdout(output), redirect_stderr(warning):
            MATRIX.main()
        self.assertEqual(MATRIX.image_matrix(checkout), json.loads(output.getvalue()))
        self.assertIn(before, warning.getvalue())

    def test_force_push_with_available_previous_commit_selects_only_changes(self):
        self.write(".build/docker/first.docker", "original change")
        before = self.commit()
        self.git("branch", "previous", before)
        self.write(".build/docker/second.docker", "amended change")
        self.git("add", ".")
        self.git("commit", "--amend", "--no-edit", "-q")
        self.assertEqual(["second"], [image["name"] for image in self.images(before)])

    def test_invalid_image_name_is_rejected(self):
        self.write(".build/docker/Bad Name.docker", "invalid")
        self.commit()
        with self.assertRaisesRegex(ValueError, "Invalid Docker image name"):
            self.images()

    def test_symlink_is_rejected(self):
        (self.root / ".build/docker/link.docker").symlink_to("first.docker")
        self.commit()
        with self.assertRaisesRegex(ValueError, "regular file"):
            self.images()

    def test_cli_selects_one_image(self):
        output = io.StringIO()
        with patch.object(MATRIX, "__file__", str(self.root / ".build/ci/docker_image_matrix.py")), \
                patch("sys.argv", ["docker_image_matrix.py", "--all", "--image", "first"]), \
                redirect_stdout(output):
            MATRIX.main()
        self.assertEqual(["first"], [image["name"] for image in json.loads(output.getvalue())["include"]])

    def test_cli_rejects_unknown_image(self):
        with patch.object(MATRIX, "__file__", str(self.root / ".build/ci/docker_image_matrix.py")), \
                patch("sys.argv", ["docker_image_matrix.py", "--all", "--image", "missing"]), \
                redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
            MATRIX.main()
        self.assertEqual(2, caught.exception.code)


if __name__ == "__main__":
    unittest.main()
