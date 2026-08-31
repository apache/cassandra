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
import contextlib
import io
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch, MagicMock
from urllib.error import HTTPError
import yaml

import jenkins

# Import the functions from the script
from run_ci import (
    DEPLOY_YAML,
    check_agent_capacity,
    debug,
    install_jenkins,
    get_jenkins,
    trigger_jenkins_build,
    spin_while,
    retry_call,
    wait_for_build_number,
    wait_for_build_complete,
    delete_remote_junit_files,
    cleanup_and_maybe_teardown,
    helm_installation_lock,
)


def quietly(action):
    """
    Calls action() with the spinner's output discarded and its sleep skipped.
    spin_while writes ten cursor-control frames per poll, which floods the test output.
    """
    with patch('run_ci.time.sleep'), contextlib.redirect_stdout(io.StringIO()):
        return action()


class TestCIPipeline(unittest.TestCase):

    def setUp(self):
        print("\ntesting ", self._testMethodName)

    @patch('run_ci.os.environ.get')
    @patch('run_ci.print')
    def test_debug(self, mock_print, mock_get):
        mock_get.return_value = "1"
        debug("Test message")
        mock_print.assert_called_with("Test message")

    # the pre-flight check is nested inside install_jenkins, so it is exercised through it: the mocked
    # `helm get values` stdout stands in for the values the site already has deployed
    LIVE_STORAGE_CLASS = "persistence:\n  storageClass: gp2\n"

    @patch('run_ci.subprocess.run')
    def test_install_jenkins(self, mock_run):
        # empty stdout, i.e. nothing deployed yet, so the pre-flight check has nothing to warn about
        mock_run.return_value = MagicMock(returncode=0, stdout="")
        install_jenkins("test-namespace", Path("/fake/cassandra/dir"), "default")
        mock_run.assert_any_call(["helm", "repo", "add", "jenkins", "https://charts.jenkins.io"], check=True)
        mock_run.assert_any_call(["helm", "repo", "update"], check=True)

    @patch('run_ci.subprocess.run')
    def test_install_jenkins_values_override(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="")
        with tempfile.NamedTemporaryFile("w", suffix=".yaml") as override:
            override.write(self.LIVE_STORAGE_CLASS)
            override.flush()
            install_jenkins(None, None, "default", override.name)
            upgrade_cmd = [c for c in [call.args[0] for call in mock_run.call_args_list] if "upgrade" in c][0]
            # the site's overrides must come after, and never replace, the repo's deployment yaml
            self.assertEqual(["-f", DEPLOY_YAML, "-f", override.name], upgrade_cmd[5:9])

    @patch('run_ci.sys.stdin.isatty')
    @patch('run_ci.print')
    @patch('run_ci.subprocess.run')
    def test_install_jenkins_aborts_non_interactively(self, mock_run, mock_print, mock_isatty):
        mock_isatty.return_value = False
        mock_run.return_value = MagicMock(returncode=0, stdout=self.LIVE_STORAGE_CLASS)
        with self.assertRaises(SystemExit):
            install_jenkins(None, None, "default")
        self.assertEqual([], [c for c in [call.args[0] for call in mock_run.call_args_list] if "upgrade" in c])
        # and continues when the customisation is passed back in as an override.  This also proves the merge is
        # per key: were the override to replace the whole persistence map, its other keys would now be reported lost
        with tempfile.NamedTemporaryFile("w", suffix=".yaml") as override:
            override.write(self.LIVE_STORAGE_CLASS)
            override.flush()
            install_jenkins(None, None, "default", override.name)
        self.assertEqual(1, len([c for c in [call.args[0] for call in mock_run.call_args_list] if "upgrade" in c]))

    @patch('run_ci.input')
    @patch('run_ci.sys.stdin.isatty')
    @patch('run_ci.print')
    @patch('run_ci.subprocess.run')
    def test_install_jenkins_prompts(self, mock_run, mock_print, mock_isatty, mock_input):
        mock_isatty.return_value = True
        mock_run.return_value = MagicMock(returncode=0, stdout=self.LIVE_STORAGE_CLASS)
        mock_input.return_value = "n"
        with self.assertRaises(SystemExit):
            install_jenkins(None, None, "default")
        mock_input.return_value = "y"
        install_jenkins(None, None, "default")

    @patch('run_ci.sys.stdin.isatty')
    @patch('run_ci.print')
    @patch('run_ci.subprocess.run')
    def test_install_jenkins_reports_only_detectable_losses(self, mock_run, mock_print, mock_isatty):
        mock_isatty.return_value = False
        # a plugin only this site installs is reported, as is a key the site alone holds; a key held in both but
        # locally edited (persistence.size) cannot be seen, and must not be claimed
        mock_run.return_value = MagicMock(returncode=0, stdout="persistence:\n  size: 1Ti\n"
                                          "controller:\n  installPlugins:\n    - site-only-plugin\n")
        with self.assertRaises(SystemExit):
            install_jenkins(None, None, "default")
        printed = " ".join(str(call.args[0]) for call in mock_print.call_args_list if call.args)
        self.assertIn("controller.installPlugins[]", printed)
        self.assertIn("site-only-plugin", printed)
        self.assertNotIn("persistence.size", printed)

    @patch('run_ci.print')
    @patch('run_ci.subprocess.run')
    def test_install_jenkins_when_nothing_deployed(self, mock_run, mock_print):
        # `helm get values` fails when there is no release, and nothing is then warned about
        mock_run.side_effect = lambda cmd, **kwargs: MagicMock(returncode=1 if "get" in cmd else 0,
                                                              stdout="", stderr="release: not found")
        install_jenkins(None, None, "default")
        self.assertEqual([], [call.args[0] for call in mock_print.call_args_list if "WARNING" in str(call.args)])

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
        # a MagicMock job_info shows no parameterDefinitions, so this takes the
        # non-parameter build path, which sleeps for six seconds
        with patch('run_ci.spin_while', side_effect=lambda msg, condition: 0):
            queue_item = quietly(lambda: trigger_jenkins_build(mock_server, "test-job", param1="value1"))
        self.assertEqual(queue_item, 123)

    def test_spin_while(self):
        result = quietly(lambda: spin_while("Testing", lambda: True))
        self.assertEqual(result, 0)

    def test_wait_for_build_complete_ignores_mid_build_result(self):
        """A pipeline latches UNSTABLE while later stages still run. That is not completion."""
        mock_server = MagicMock()
        mock_server.get_build_info.side_effect = [
            {'building': True, 'result': None},
            {'building': True, 'result': 'UNSTABLE'},
            {'building': True, 'result': 'UNSTABLE'},
            {'building': False, 'result': 'UNSTABLE'},
        ]
        quietly(lambda: wait_for_build_complete(mock_server, "test-job", 456))
        self.assertEqual(mock_server.get_build_info.call_count, 4)

    def test_wait_for_build_complete_missing_building_field(self):
        """An absent `building` field must never read as finished."""
        mock_server = MagicMock()
        mock_server.get_build_info.side_effect = [
            {'result': 'SUCCESS'},
            {'building': False, 'result': 'SUCCESS'},
        ]
        quietly(lambda: wait_for_build_complete(mock_server, "test-job", 456))
        self.assertEqual(mock_server.get_build_info.call_count, 2)

    def test_wait_for_build_complete_survives_api_error(self):
        mock_server = MagicMock()
        mock_server.get_build_info.side_effect = [
            jenkins.JenkinsException("connection reset"),
            jenkins.JenkinsException("connection reset"),
            {'building': False, 'result': 'SUCCESS'},
        ]
        quietly(lambda: wait_for_build_complete(mock_server, "test-job", 456))
        self.assertEqual(mock_server.get_build_info.call_count, 3)

    def test_wait_for_build_number_pending_executable(self):
        """A queued item can carry "executable": null before Jenkins starts it."""
        mock_server = MagicMock()
        mock_server.get_queue_item.side_effect = [
            {'executable': None},
            {'executable': {'number': 456}},
        ]
        self.assertEqual(quietly(lambda: wait_for_build_number(mock_server, 123)), 456)

    def test_retry_call_returns_result(self):
        self.assertEqual(retry_call(lambda: "downloaded", "download it", IOError, 3, 0), "downloaded")

    def test_retry_call_raises_after_retries(self):
        attempts = []

        def always_fails():
            attempts.append(1)
            raise IOError("connection reset by peer")

        with self.assertRaises(IOError):
            retry_call(always_fails, "download it", IOError, 3, 0)
        self.assertEqual(len(attempts), 3)

    def test_retry_call_does_not_retry_a_404(self):
        """An artifact a build never archived stays absent, so one attempt is enough."""
        attempts = []

        def not_found():
            attempts.append(1)
            raise HTTPError("http://ci/artifact", 404, "Not Found", {}, None)

        with self.assertRaises(HTTPError):
            retry_call(not_found, "download it", IOError, 5, 0)
        self.assertEqual(len(attempts), 1)

    def test_retry_call_retries_a_503(self):
        """A gateway error is transient, unlike a 404."""
        attempts = []

        def unavailable():
            attempts.append(1)
            raise HTTPError("http://ci/artifact", 503, "Service Unavailable", {}, None)

        with self.assertRaises(HTTPError):
            retry_call(unavailable, "download it", IOError, 3, 0)
        self.assertEqual(len(attempts), 3)

    def test_retry_call_always_attempts_once(self):
        """max_retries=0 must not raise None."""
        with self.assertRaises(IOError):
            retry_call(lambda: (_ for _ in ()).throw(IOError("nope")), "download it", IOError, 0, 0)

    @patch('run_ci.stream.stream')
    def test_delete_remote_junit_files(self, mock_stream):
        mock_k8s_client = MagicMock()
        delete_remote_junit_files(mock_k8s_client, "test-pod", "test-namespace", "test-job", 456)
        delete_remote_junit_files(mock_k8s_client, "test-pod", "test-namespace", "cassandra-6.0", 456)
        mock_stream.assert_called()

    @patch('run_ci.subprocess.run')
    def test_cleanup_and_maybe_teardown(self, mock_run):
        cleanup_and_maybe_teardown(None, None, "test-namespace", True)
        mock_run.assert_called_with(["helm", "--namespace", "test-namespace", "uninstall", "cassius"],
                                    capture_output=False, text=True, check=True)

    @patch('run_ci.fcntl.flock')
    def test_helm_installation_lock(self, mock_flock):
        with helm_installation_lock(Path("/tmp/.fake.lock")):
            mock_flock.assert_called()

    LARGE_NODE = ('{"items":[{"metadata":{"labels":{"eks.amazonaws.com/nodegroup":"amd64-large-ondemand-2",'
                  '"cassandra.jenkins.agent":"true","cassandra.jenkins.agent.large":"true"}}}]}')

    @staticmethod
    def ca_status(nested: bool = True) -> str:
        """
        The autoscaler's status configmap, holding the live cluster's node groups and maximums.

        maxSize is the only in-cluster record of what a pool can hold, and a pool at zero nodes has no
        nodes to count, so the check reads it from here.
        """
        # Two groups per size, each pair summing to that size's instanceCap in jenkins-deployment.yaml.  Raise
        # these whenever a cap is raised, or the committed values stop passing their own check.
        groups = [(f"eks-amd64-{size}-ondemand-{n}-{n}cfd1c1", 0, maximum)
                  for size, maximum in (("large", 153), ("medium", 75), ("small", 10), ("report", 2))
                  for n in (2, 3)]

        groups.append(("eks-jenkins-controller-0-2acd8787", 1, 1))
        return yaml.safe_dump({"nodeGroups": [
            {"name": name, **({"health": {"minSize": minimum, "maxSize": maximum}} if nested
                              else {"minSize": minimum, "maxSize": maximum})}
            for name, minimum, maximum in groups]})

    def capacity_check(self, values: dict, nodes: str = '{"items":[]}', autoscaler: bool = True,
                       nested: bool = True):
        """
        Runs check_agent_capacity against the autoscaler ceilings above, returning the exit code or 0.

        `autoscaler=False` stands in for a cluster whose ceilings cannot be read at all, a managed
        autoscaler that publishes no status configmap for instance, where kubectl exits non-zero.
        """
        def kubectl(_kubeconfig, _kubecontext, _ns, command):
            if "nodes" in command:
                return nodes
            if not autoscaler:
                raise subprocess.CalledProcessError(1, "kubectl", stderr="configmaps not found")
            return self.ca_status(nested)
        with patch('run_ci.run_kubectl_command', kubectl):
            try:
                check_agent_capacity(None, None, "default", values)
                return 0
            except SystemExit as e:
                return e.code

    def deployed_values(self, size: str = None, **overrides) -> dict:
        """The committed values, optionally with one podTemplate's keys replaced."""
        with open(DEPLOY_YAML, encoding="utf-8") as deploy_yaml:
            values = yaml.safe_load(deploy_yaml)
        if size:
            template = yaml.safe_load(values["agent"]["podTemplates"][f"agent-dind-{size}"])
            template[0].update(overrides)
            values["agent"]["podTemplates"][f"agent-dind-{size}"] = yaml.safe_dump(template)
        return values

    def test_check_agent_capacity_allows_the_committed_values(self):
        self.assertEqual(0, self.capacity_check(self.deployed_values()))
        self.assertEqual(0, self.capacity_check(self.deployed_values(), nodes=self.LARGE_NODE))

    def test_check_agent_capacity_blocks_a_cap_above_the_pool(self):
        # 400 against the 306 nodes two large groups can hold: 94 agents could never be scheduled, which is
        # not idle but a churn loop, and is what preceded the 2026-08-11 controller stall
        over = self.deployed_values("large", instanceCap=400, instanceCapStr="400")
        self.assertEqual(1, self.capacity_check(over))
        # a cluster whose ceilings cannot be read leaves it unchecked rather than blocking a valid deploy
        self.assertEqual(0, self.capacity_check(over, autoscaler=False))

    def test_check_agent_capacity_reads_maxsize_wherever_it_is_published(self):
        # the live cluster nests a group's maximum under its health condition, and the check also takes it
        # from the group.  Reading the wrong key costs nothing visible: the ceilings come out empty and
        # every cap passes unchecked, so the shapes are pinned here rather than in a deploy
        over = self.deployed_values("large", instanceCap=400, instanceCapStr="400")
        for nested in (True, False):
            self.assertEqual(1, self.capacity_check(over, nested=nested))
            self.assertEqual(0, self.capacity_check(self.deployed_values(), nested=nested))

    def test_check_agent_capacity_blocks_contradictory_config(self):
        # the plugin takes the cap from either key, so a disagreement resolves to whichever applies last
        self.assertEqual(1, self.capacity_check(self.deployed_values("large", instanceCapStr="400")))
        # a nodeSelector the live nodes contradict strands every agent of that size
        typo = self.deployed_values("large", nodeSelector="cassandra.jenkins.agent.large=ture")
        self.assertEqual(1, self.capacity_check(typo, nodes=self.LARGE_NODE))
        # unconfirmable is not the same as contradicted: with that pool at zero there is nothing to check against
        self.assertEqual(0, self.capacity_check(typo))

if __name__ == '__main__':
    unittest.main()
