#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
# Used to test `.build/run-ci`
# Run with `python .build/run-ci.d/run-ci-test.py`
#
#
# lint with:
#  `pylint --disable=C0301,W0511,C0114,C0103,W0702,C0415,C0116,C0115,R0914,W0603,R0915,R0913,R0911 run-ci-test.py`


import argparse
from pathlib import Path
import unittest
from unittest.mock import patch, MagicMock


# Import the functions from the script
from run_ci import (
    debug,
    install_jenkins,
    get_jenkins,
    trigger_jenkins_build,
    spin_while,
    delete_remote_junit_files,
    cleanup_and_maybe_teardown,
    helm_installation_lock,
)

class TestCIPipeline(unittest.TestCase):

    def setUp(self):
        print("\ntesting ", self._testMethodName)

    @patch('run_ci.os.environ.get')
    @patch('run_ci.print')
    def test_debug(self, mock_print, mock_get):
        mock_get.return_value = "1"
        debug("Test message")
        mock_print.assert_called_with("Test message")

    @patch('run_ci.subprocess.run')
    def test_install_jenkins(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        install_jenkins("test-namespace", Path("/fake/cassandra/dir"), "default")
        mock_run.assert_any_call(["helm", "repo", "add", "jenkins", "https://charts.jenkins.io"], check=True)
        mock_run.assert_any_call(["helm", "repo", "update"], check=True)

    @patch('run_ci.subprocess.run')
    @patch('run_ci.jenkins.Jenkins')
    def test_get_jenkins(self, mock_jenkins, mock_run):
        mock_k8s_client = MagicMock()
        mock_run.return_value = MagicMock(stdout="fake-password")
        mock_jenkins_instance = MagicMock()
        mock_jenkins.return_value = mock_jenkins_instance
        # hack – use False values instead of None
        args = argparse.Namespace(kubeconfig="/fake/kubeconfig", kubecontext="test-context", user=False, url=False)
        _, server = get_jenkins(mock_k8s_client, args, "default")
        self.assertEqual(server, mock_jenkins_instance)

    @patch('run_ci.jenkins.Jenkins.build_job')
    @patch('run_ci.wait_for_build_number')
    def test_trigger_jenkins_build(self, mock_wait_for_build_number, mock_build_job):
        mock_server = MagicMock()
        mock_build_job.return_value = mock_server.build_job.return_value = 123
        mock_wait_for_build_number.return_value = 456
        with patch('run_ci.spin_while', side_effect=lambda msg, condition: 0):
            queue_item = trigger_jenkins_build(mock_server, "test-job", param1="value1")
        self.assertEqual(queue_item, 123)

    def test_spin_while(self):
        result = spin_while("Testing", lambda: True)
        self.assertEqual(result, 0)

    @patch('run_ci.stream.stream')
    def test_delete_remote_junit_files(self, mock_stream):
        mock_k8s_client = MagicMock()
        delete_remote_junit_files(mock_k8s_client, "test-pod", "test-namespace", 456)
        mock_stream.assert_called()

    @patch('run_ci.subprocess.run')
    def test_cleanup_and_maybe_teardown(self, mock_run):
        cleanup_and_maybe_teardown(None, None, "test-namespace", True)
        mock_run.assert_called_with(["helm", "--namespace", "test-namespace", "uninstall", "cassius"], check=True)

    @patch('run_ci.fcntl.flock')
    def test_helm_installation_lock(self, mock_flock):
        with helm_installation_lock(Path("/tmp/.fake.lock")):
            mock_flock.assert_called()

if __name__ == '__main__':
    unittest.main()
