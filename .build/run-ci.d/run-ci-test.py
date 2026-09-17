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
import json
import os
import stat
from pathlib import Path
import subprocess
import tempfile
import unittest
import warnings
from unittest.mock import patch, MagicMock
from urllib.error import HTTPError
import yaml

import jenkins
import run_ci

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


class TestNodeCleanerAccess(unittest.TestCase):
    EXPIRED_LOGIN = ('aws: [ERROR]: CreateOAuth2Token: The provided authorization grant is '
                     'invalid, expired, revoked, or malformed\nerror: INVALID_REQUEST')

    @contextlib.contextmanager
    def cleaner(self, commands):
        api = MagicMock()
        node = run_ci.client.V1Node(
            metadata=run_ci.client.V1ObjectMeta(name='agent-node'),
            spec=run_ci.client.V1NodeSpec(provider_id='aws:///us-west-2a/i-123'))
        api.list_node.return_value.items = [node]
        api.read_node.return_value = node
        api.list_pod_for_all_namespaces.return_value.items = [
            run_ci.client.V1Pod(metadata=run_ci.client.V1ObjectMeta(name='pod', namespace='default'))]
        output = io.StringIO()
        sleeps = 0

        def bounded_sleep(_seconds):
            nonlocal sleeps
            sleeps += 1
            if sleeps >= 9:
                run_ci.IS_RUNNING = False  # Bound retries in the unfixed implementation.

        def start_worker(*, target, args, **_kwargs):
            return MagicMock(start=lambda: target(*args))

        with patch('run_ci.IS_RUNNING', True), patch('run_ci.time.sleep', side_effect=bounded_sleep), \
                patch('run_ci.run_kubectl_command', side_effect=commands) as kubectl, \
                patch('run_ci.threading.Thread', side_effect=start_worker), \
                patch.dict('sys.modules', {'boto3': MagicMock()}) as modules, \
                contextlib.redirect_stderr(output):
            yield api, kubectl, modules['boto3'], output

    def command_failure(self, message=EXPIRED_LOGIN):
        return subprocess.CalledProcessError(1, ['kubectl', 'get', 'nodes'], stderr=message)

    def test_failed_token_refresh_stops_polling_and_leaves_build_monitor_running(self):
        with self.cleaner(self.command_failure()) as (api, kubectl, cloud, output):
            run_ci.node_cleaner(api, 'config', 'eks-context', 'default')
            self.assertTrue(run_ci.IS_RUNNING)
        kubectl.assert_called_once_with('config', 'eks-context', 'default', ['get', 'nodes', '-o', 'json'])
        api.list_node.assert_not_called()
        api.patch_node.assert_not_called()
        cloud.session.Session.assert_not_called()
        self.assertEqual(output.getvalue().count('Node cleaner stopped'), 1)
        self.assertIn('CreateOAuth2Token', output.getvalue())

    def test_failed_inspection_stops_repeated_worker_creation(self):
        def commands(_config, _context, _namespace, command):
            if command[0] == 'get':
                return json.dumps({'items': [{'metadata': {'name': name}}
                                            for name in ['agent-node', 'another-node']]})
            raise self.command_failure()

        with self.cleaner(commands) as (api, kubectl, cloud, output):
            run_ci.node_cleaner(api, None, None, 'default')
            self.assertTrue(run_ci.IS_RUNNING)
        self.assertEqual(kubectl.call_count, 2)
        api.read_node.assert_not_called()
        api.patch_node.assert_not_called()
        cloud.session.Session.assert_not_called()
        self.assertEqual(output.getvalue().count('Node cleaner stopped'), 1)

    def test_unreadable_node_list_stops_without_deletion(self):
        failures = [self.command_failure('Forbidden'), self.command_failure(None),
                    OSError('kubectl unavailable'), 'not JSON', '{}', '{"items": null}']
        for failure in failures:
            with self.subTest(failure=failure), self.cleaner([failure]) as (api, kubectl, cloud, output):
                run_ci.node_cleaner(api, None, None, 'default')
                self.assertTrue(run_ci.IS_RUNNING)
                kubectl.assert_called_once()
                api.patch_node.assert_not_called()
                cloud.session.Session.assert_not_called()
                self.assertEqual(output.getvalue().count('Node cleaner stopped'), 1)

    def test_api_access_failure_aborts_remaining_deletion_steps(self):
        steps = ['read_node', 'patch_node', 'list_pod_for_all_namespaces',
                 'delete_namespaced_pod', 'delete_node']

        def commands(_config, _context, _namespace, command):
            if command[0] == 'get':
                return json.dumps({'items': [{'metadata': {'name': 'agent-node'}}]})
            return 'No Jenkins pods'

        for index, step in enumerate(steps):
            with self.subTest(step=step), self.cleaner(commands) as (api, _, cloud, output):
                getattr(api, step).side_effect = run_ci.client.exceptions.ApiException(status=401)
                run_ci.node_cleaner(api, None, None, 'default')
                for remaining in steps[index + 1:]:
                    getattr(api, remaining).assert_not_called()
                cloud.session.Session.assert_not_called()
                self.assertTrue(run_ci.IS_RUNNING)
                self.assertEqual(output.getvalue().count('Node cleaner stopped'), 1)

    def test_provider_lookup_failure_stops_before_deletion(self):
        for lookup in ['list_node', 'current-context']:
            with self.subTest(lookup=lookup), self.cleaner(
                    [json.dumps({'items': [{'metadata': {'name': 'agent-node'}}]})]
                    + ['No Jenkins pods'] * 6 + [self.command_failure()]) as (api, _, cloud, output):
                api.read_node.return_value.spec.provider_id = None
                if lookup == 'list_node':
                    api.list_node.side_effect = run_ci.client.exceptions.ApiException(status=403)
                run_ci.node_cleaner(api, None, None, 'default')
                api.patch_node.assert_not_called()
                cloud.session.Session.assert_not_called()
                self.assertTrue(run_ci.IS_RUNNING)
                self.assertEqual(output.getvalue().count('Node cleaner stopped'), 1)

    def test_stopping_during_idle_wait_prevents_deletion(self):
        sleeps = 0

        def stop_after_idle_wait(_seconds):
            nonlocal sleeps
            sleeps += 1
            if sleeps == 6:
                run_ci.IS_RUNNING = False

        with self.cleaner([json.dumps({'items': [{'metadata': {'name': 'agent-node'}}]})]
                          + ['No Jenkins pods'] * 6) as (api, _, cloud, _), \
                patch('run_ci.time.sleep', side_effect=stop_after_idle_wait):
            run_ci.node_cleaner(api, None, None, 'default')
        api.read_node.assert_not_called()
        api.patch_node.assert_not_called()
        cloud.session.Session.assert_not_called()

    def test_successful_inspection_still_removes_idle_node(self):
        with self.cleaner([json.dumps({'items': [{'metadata': {'name': 'agent-node'}}]})]
                          + ['No Jenkins pods'] * 6 + [self.command_failure()]) as (api, _, cloud, _):
            run_ci.node_cleaner(api, None, None, 'default')
        api.patch_node.assert_called_once()
        api.delete_namespaced_pod.assert_called_once_with(name='pod', namespace='default')
        api.delete_node.assert_called_once_with('agent-node')
        cloud.session.Session.return_value.client.return_value.terminate_instance_in_auto_scaling_group.assert_called_once_with(
            InstanceId='i-123', ShouldDecrementDesiredCapacity=True)


class TestCIPipeline(unittest.TestCase):

    def setUp(self):
        print("\ntesting ", self._testMethodName)
        command_lookup = patch('run_ci.shutil.which', return_value='/mock/tool')
        command_lookup.start()
        self.addCleanup(command_lookup.stop)

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
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        install_jenkins("test-namespace", Path("/fake/cassandra/dir"), "default")
        mock_run.assert_any_call(["helm", "repo", "add", "jenkins", "https://charts.jenkins.io"], check=True)
        mock_run.assert_any_call(["helm", "repo", "update", "jenkins"], check=True)

    @patch('run_ci.subprocess.run')
    def test_install_jenkins_ignores_unrelated_repository_failure(self, mock_run):
        def run_command(cmd, **kwargs):
            if cmd[:3] == ["helm", "repo", "update"] and (len(cmd) == 3 or "autoscaler" in cmd[3:]):
                raise subprocess.CalledProcessError(1, cmd, stderr="lookup kubernetes.github.io: no such host")
            return subprocess.CompletedProcess(cmd, 0, "", "")

        mock_run.side_effect = run_command
        install_jenkins(None, "test-context", "default")
        self.assertTrue(any(call.args[0][0] == "helm" and "upgrade" in call.args[0]
                            for call in mock_run.call_args_list))

    @patch('run_ci.subprocess.run')
    def test_install_jenkins_stops_when_jenkins_repository_fails(self, mock_run):
        def run_command(cmd, **kwargs):
            if cmd[:3] == ["helm", "repo", "update"] and (len(cmd) == 3 or "jenkins" in cmd[3:]):
                raise subprocess.CalledProcessError(1, cmd, stderr="Jenkins repository unavailable")
            return subprocess.CompletedProcess(cmd, 0, "", "")

        mock_run.side_effect = run_command
        with self.assertRaises(subprocess.CalledProcessError):
            install_jenkins(None, "test-context", "default")
        self.assertFalse(any(call.args[0][0] == "helm" and "upgrade" in call.args[0]
                             for call in mock_run.call_args_list))

    @patch('run_ci.subprocess.run')
    def test_install_jenkins_values_override(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        with tempfile.NamedTemporaryFile("w", suffix=".yaml") as override:
            override.write(self.LIVE_STORAGE_CLASS)
            override.flush()
            install_jenkins(None, None, "default", override.name)
            upgrade_cmd = [c for c in [call.args[0] for call in mock_run.call_args_list] if "upgrade" in c][0]
            # the site's overrides must come after, and never replace, the repo's deployment yaml
            self.assertEqual(["-f", DEPLOY_YAML, "-f", override.name], upgrade_cmd[5:9])

    @patch('run_ci.subprocess.run')
    def test_install_jenkins_reports_helm_failure_before_pod_setup(self, mock_run):
        def run_command(cmd, **kwargs):
            if "upgrade" in cmd:
                result = subprocess.CompletedProcess(cmd, 1, "Release cassius failed.\n",
                                                     "Error: context deadline exceeded\n")
                if kwargs.get("check"):
                    result.check_returncode()
                return result
            return subprocess.CompletedProcess(cmd, 0, "", "")

        mock_run.side_effect = run_command
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as raised:
                install_jenkins(None, "test-context", "default")
        self.assertEqual(raised.exception.code, 1)
        self.assertIn("Release cassius failed.", stdout.getvalue())
        self.assertIn("Error: context deadline exceeded", stderr.getvalue())
        self.assertEqual([], [call for call in mock_run.call_args_list
                              if call.args[0][0] == "kubectl" and "exec" in call.args[0]])

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
        service = mock_k8s_client.read_namespaced_service.return_value
        service.metadata.annotations = {}
        service.status.load_balancer.ingress = [run_ci.client.V1LoadBalancerIngress(ip='192.0.2.1')]
        service.spec.ports = [run_ci.client.V1ServicePort(name='http', port=80)]
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(kubeconfig="/fake/kubeconfig", kubecontext="test-context", user=None, url=None,
                                      credentials_file=Path(directory) / 'credentials.json', save_credentials=False)
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

class TestJenkinsServiceDiscovery(unittest.TestCase):
    DNS = 'external-dns.alpha.kubernetes.io/hostname'
    CERT = 'service.beta.kubernetes.io/aws-load-balancer-ssl-cert'
    TLS_PORTS = 'service.beta.kubernetes.io/aws-load-balancer-ssl-ports'

    def service(self, annotations=None, ports=(('http', 80), ('https', 443)), address='example.elb.amazonaws.com'):
        return run_ci.client.V1Service(
            metadata=run_ci.client.V1ObjectMeta(annotations=annotations),
            spec=run_ci.client.V1ServiceSpec(ports=[run_ci.client.V1ServicePort(name=name, port=port, protocol='TCP')
                                                  for name, port in ports]),
            status=run_ci.client.V1ServiceStatus(load_balancer=run_ci.client.V1LoadBalancerStatus(
                ingress=[run_ci.client.V1LoadBalancerIngress(hostname=address)] if address else [])))

    def connect(self, service, explicit_url=None):
        api = MagicMock()
        api.read_namespaced_service.return_value = service
        args = argparse.Namespace(url=explicit_url, kubeconfig=None, kubecontext='selected-context', user=None,
                                  credentials_file=Path('/unused/test-credentials.json'), save_credentials=False)
        output = io.StringIO()
        with patch('run_ci.load_credentials', return_value={'user': 'alice', 'token': 'test-token'}) as credentials, \
                patch('run_ci.jenkins.Jenkins') as connection, \
                patch('run_ci.run_kubectl_command') as kubectl, contextlib.redirect_stdout(output):
            url, _server = get_jenkins(api, args, 'default')
        credentials.assert_called_once_with(args.credentials_file, url)
        connection.assert_called_once_with(url, username='alice', password='test-token')
        kubectl.assert_not_called()
        if explicit_url:
            api.read_namespaced_service.assert_not_called()
        else:
            api.read_namespaced_service.assert_called_once_with('cassius-jenkins', 'default')
        self.last_output = output.getvalue()
        return url

    def test_prefers_service_hostname_and_https(self):
        service = self.service({self.DNS: 'astro-cass.ci', self.CERT: 'test-cert', self.TLS_PORTS: 'https'})
        self.assertEqual(self.connect(service), 'https://astro-cass.ci')
        self.assertIn('Jenkins: https://astro-cass.ci', self.last_output)

    def test_dns_hostname_works_without_tls_or_load_balancer_status(self):
        service = self.service({self.DNS: 'ci.example'}, ports=(('http', 80),), address=None)
        self.assertEqual(self.connect(service), 'http://ci.example')

    def test_missing_or_unusable_dns_annotation_preserves_load_balancer_fallback(self):
        for annotations in (None, {}, {self.DNS: ''}, {self.DNS: '*.example'},
                            {self.DNS: 'https://ci.example'}, {self.DNS: 'user:token@ci.example'}):
            with self.subTest(annotations=annotations):
                service = self.service(annotations, ports=(('https', 443), ('http', 80)))
                self.assertEqual(self.connect(service), 'http://example.elb.amazonaws.com')

    def test_selects_first_concrete_dns_name_and_supports_both_annotation_prefixes(self):
        for key in (self.DNS, 'external-dns.kubernetes.io/hostname'):
            with self.subTest(key=key):
                service = self.service({key: ' *.example, Astro-Cass.ci., alias.example '})
                self.assertEqual(self.connect(service), 'https://astro-cass.ci')

    def test_preserves_custom_http_and_annotated_tls_ports(self):
        service = self.service({self.DNS: 'ci.example'}, ports=(('http', 8080),))
        self.assertEqual(self.connect(service), 'http://ci.example:8080')
        for tls_ports in ('secure', '8443', None):
            with self.subTest(tls_ports=tls_ports):
                annotations = {self.DNS: 'ci.example', self.CERT: 'test-cert'}
                if tls_ports is not None:
                    annotations[self.TLS_PORTS] = tls_ports
                service = self.service(annotations, ports=(('secure', 8443),))
                self.assertEqual(self.connect(service), 'https://ci.example:8443')

    def test_explicit_url_takes_precedence(self):
        self.assertEqual(self.connect(self.service({self.DNS: 'astro-cass.ci'}), 'https://override.example/jenkins'),
                         'https://override.example/jenkins')

    def test_ip_fallback_and_missing_address(self):
        for address, expected in [('192.0.2.1', 'http://192.0.2.1:8080'),
                                  ('2001:db8::1', 'http://[2001:db8::1]:8080')]:
            with self.subTest(address=address):
                service = self.service(ports=(('http', 8080),))
                service.status.load_balancer.ingress = [run_ci.client.V1LoadBalancerIngress(ip=address)]
                self.assertEqual(self.connect(service), expected)
        with self.assertRaisesRegex(ValueError, 'address'):
            self.connect(self.service(address=None))


class TestConnectionModes(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.credentials = Path(self.directory.name) / 'auth' / 'credentials.json'

    def args(self, **values):
        args = run_ci.argument_parser().parse_args([])
        args.credentials_file = str(self.credentials)
        args.save_credentials = False
        args.attach = None
        args.job = None
        vars(args).update(values)
        return args

    def parse(self, *arguments):
        with patch.dict(os.environ, {}, clear=True), patch('run_ci.sys.argv', ['run-ci', *arguments]), \
                patch('run_ci.DEFAULT_REPO_URL', None), patch('run_ci.require_tracking_remote') as tracking:
            result = run_ci.parse_arguments()
            tracking.assert_not_called()
            return result

    @patch('run_ci.client.CoreV1Api')
    @patch('run_ci.config.load_kube_config')
    @patch('run_ci.shutil.which', return_value=None)
    def test_connecting_to_cluster_does_not_require_cli_tools(self, which, load, core):
        self.assertIs(run_ci.setup_environment(None, 'cluster'), core.return_value)
        which.assert_not_called()
        load.assert_called_once_with(config_file=None, context='cluster')

    @patch('run_ci.shutil.which', return_value=None)
    @patch('run_ci.subprocess.run')
    def test_cli_tools_checked_only_when_used(self, run, _which):
        with self.assertRaisesRegex(EnvironmentError, 'kubectl'):
            run_ci.run_kubectl_command(None, None, 'default', ['get', 'pods'])
        with self.assertRaisesRegex(EnvironmentError, 'helm'):
            run_ci.run_helm_command(None, None, 'default', ['list'])
        run.assert_not_called()

    def test_credentials_are_private_and_scoped_to_url(self):
        run_ci.save_credentials(self.credentials, 'https://ci.example/jenkins', 'alice', 'test-token')
        self.assertEqual(stat.S_IMODE(self.credentials.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.credentials.parent.stat().st_mode), 0o700)
        self.assertEqual(run_ci.load_credentials(self.credentials, 'https://ci.example/jenkins'),
                         {'user': 'alice', 'token': 'test-token'})
        for other in ('http://ci.example/jenkins', 'https://other.example/jenkins', 'https://ci.example/other'):
            self.assertIsNone(run_ci.load_credentials(self.credentials, other))
        run_ci.save_credentials(self.credentials, 'https://other.example', 'bob', 'other-token')
        self.assertEqual(run_ci.load_credentials(self.credentials, 'https://ci.example/jenkins')['token'], 'test-token')

    def test_credentials_reject_public_file_and_symlink(self):
        self.credentials.parent.mkdir(mode=0o700)
        self.credentials.write_text('{}')
        self.credentials.chmod(0o644)
        with self.assertRaisesRegex(ValueError, '0600'):
            run_ci.load_credentials(self.credentials, 'https://ci.example')
        self.credentials.chmod(0o600)
        link = self.credentials.parent / 'link.json'
        link.symlink_to(self.credentials)
        with self.assertRaises((ValueError, OSError)):
            run_ci.save_credentials(link, 'https://ci.example', 'alice', 'test-token')
        self.assertEqual(self.credentials.read_text(), '{}')

    def test_credentials_reject_shared_parent(self):
        self.credentials.parent.mkdir(mode=0o777)
        self.credentials.parent.chmod(0o777)
        with self.assertRaisesRegex(ValueError, 'directory'):
            run_ci.save_credentials(self.credentials, 'https://ci.example', 'alice', 'test-token')

    @patch('run_ci.getpass.getpass', return_value='test-token')
    @patch('run_ci.jenkins.Jenkins')
    def test_saved_credentials_remove_future_prompts_and_keep_https(self, connection, prompt):
        connection.return_value.get_whoami.return_value = {'authenticated': True, 'name': 'alice'}
        args = self.args(url='https://ci.example/jenkins/', user='alice', save_credentials=True)
        run_ci.get_jenkins(None, args, 'default')
        connection.assert_called_with('https://ci.example/jenkins', username='alice', password='test-token')
        args.user, args.save_credentials = None, False
        run_ci.get_jenkins(None, args, 'default')
        self.assertEqual(prompt.call_count, 1)
        connection.assert_called_with('https://ci.example/jenkins', username='alice', password='test-token')
        args.user = 'bob'
        run_ci.get_jenkins(None, args, 'default')
        self.assertEqual(prompt.call_count, 2)

    @patch('run_ci.getpass.getpass', return_value='bad-token')
    @patch('run_ci.jenkins.Jenkins')
    def test_failed_authentication_never_saves_credentials(self, connection, _prompt):
        connection.return_value.get_whoami.return_value = {'authenticated': False, 'name': 'anonymous'}
        with self.assertRaises(jenkins.JenkinsException):
            run_ci.get_jenkins(None, self.args(url='https://ci.example', user='alice', save_credentials=True), 'default')
        self.assertFalse(self.credentials.exists())

    def test_prompt_refuses_an_echoing_terminal(self):
        def echoing_prompt(_message):
            warnings.warn('input could be echoed', run_ci.getpass.GetPassWarning)
            return 'test-token'
        with patch('run_ci.getpass.getpass', side_effect=echoing_prompt), \
                patch('run_ci.jenkins.Jenkins') as connection, self.assertRaisesRegex(ValueError, 'echo'):
            run_ci.get_jenkins(None, self.args(url='https://ci.example', user='alice'), 'default')
        connection.assert_not_called()

    def test_rejected_login_preserves_previous_credentials(self):
        run_ci.save_credentials(self.credentials, 'https://ci.example', 'alice', 'valid-token')
        original = self.credentials.read_bytes()
        with patch('run_ci.jenkins.Jenkins') as connection, patch('run_ci.getpass.getpass', return_value='bad-token'):
            connection.return_value.get_whoami.side_effect = jenkins.JenkinsException('denied')
            with self.assertRaises(jenkins.JenkinsException):
                run_ci.get_jenkins(None, self.args(url='https://ci.example', user='alice', save_credentials=True), 'default')
        self.assertEqual(self.credentials.read_bytes(), original)

    def test_credentials_reject_hard_links_and_malformed_data(self):
        run_ci.save_credentials(self.credentials, 'https://ci.example', 'alice', 'test-token')
        link = self.credentials.parent / 'link.json'
        os.link(self.credentials, link)
        with self.assertRaises(ValueError):
            run_ci.load_credentials(self.credentials, 'https://ci.example')
        link.unlink()
        for contents in ('not json', json.dumps({'https://ci.example': {'user': 'alice'}})):
            self.credentials.write_text(contents)
            with self.assertRaisesRegex(ValueError, 'Invalid credentials file'):
                run_ci.load_credentials(self.credentials, 'https://ci.example')

    def test_failed_credential_write_preserves_original(self):
        run_ci.save_credentials(self.credentials, 'https://ci.example', 'alice', 'valid-token')
        original = self.credentials.read_bytes()
        with patch('run_ci.os.replace', side_effect=OSError('write failed')), self.assertRaises(OSError):
            run_ci.save_credentials(self.credentials, 'https://ci.example', 'alice', 'new-token')
        self.assertEqual(self.credentials.read_bytes(), original)
        self.assertEqual(list(self.credentials.parent.iterdir()), [self.credentials])

    def test_url_preserves_https_context_and_rejects_embedded_secrets(self):
        self.assertEqual(run_ci.normalize_jenkins_url('HTTPS://CI.EXAMPLE:443/jenkins/'), 'https://ci.example/jenkins')
        self.assertEqual(run_ci.normalize_jenkins_url('ci.example:8080/jenkins'), 'http://ci.example:8080/jenkins')
        for value in ('https://alice:test-token@ci.example', 'ftp://ci.example', 'https://ci.example?token=secret'):
            with self.assertRaises(ValueError) as error:
                run_ci.normalize_jenkins_url(value)
            self.assertNotIn('test-token', str(error.exception))
            self.assertNotIn('secret', str(error.exception))

    def test_attach_url_needs_no_git_tracking_or_explicit_user(self):
        args = self.parse('--attach', 'https://ci.example/jenkins/job/team/job/cassandra-5.0/42/')
        self.assertEqual((args.url, args.job, args.attach), ('https://ci.example/jenkins', 'team/cassandra-5.0', 42))
        self.assertEqual(run_ci.base_job_name(args), 'team/cassandra-5.0')

    def test_attach_number_with_explicit_job(self):
        args = self.parse('--attach', '42', '--job', 'cassandra-5.0', '--url', 'ci.example', '--user', 'alice')
        self.assertEqual((args.url, args.job, args.attach), ('http://ci.example', 'cassandra-5.0', 42))

    def test_attach_number_requires_repository_for_job_inference(self):
        with patch.dict(os.environ, {}, clear=True), \
                patch('run_ci.sys.argv', ['run-ci', '--attach', '42']), \
                patch('run_ci.DEFAULT_REPO_URL', None), \
                patch('run_ci.require_tracking_remote', side_effect=SystemExit('no tracking remote')) as tracking:
            with self.assertRaises(SystemExit):
                run_ci.parse_arguments()
        tracking.assert_called_once()

    def test_attach_url_overrides_environment_server(self):
        with patch.dict(os.environ, {'JENKINS_URL': 'https://other.example'}, clear=True), \
                patch('run_ci.sys.argv', ['run-ci', '--attach', 'https://ci.example/job/cassandra/42/']):
            args = run_ci.parse_arguments()
        self.assertEqual(args.url, 'https://ci.example')

    def test_credential_location_never_defaults_to_working_directory(self):
        for directory in ('', 'relative/directory'):
            with patch.dict(os.environ, {'XDG_CONFIG_HOME': directory}):
                self.assertEqual(run_ci.default_credentials_file(),
                                 Path.home() / '.config/cassandra/run-ci-credentials.json')

    def test_attach_rejects_ambiguous_or_mutating_requests(self):
        for arguments in [('--attach', '0', '--job', 'cassandra'),
                          ('--attach', 'https://ci.example/job/cassandra/42/', '--setup'),
                          ('--attach', 'https://ci.example/job/cassandra/42/', '--url', 'https://other.example'),
                          ('--attach', 'https://ci.example/job/cassandra/42/', '--download-results', '43')]:
            with self.subTest(arguments=arguments), self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
                self.parse(*arguments)

    @patch('run_ci.cleanup_and_maybe_teardown')
    @patch('run_ci.setup_environment')
    @patch('run_ci.load_environment_file')
    def test_teardown_does_not_initialize_kubernetes(self, _env, setup, cleanup):
        with patch('run_ci.parse_arguments', return_value=self.args(only_tear_down=True)):
            run_ci.main()
        setup.assert_not_called()
        cleanup.assert_called_once_with(None, None, 'default', True)

    def test_attach_waits_for_existing_build_without_launch_or_cleanup(self):
        args = self.args(url='https://ci.example', job='cassandra-5.0', attach=42)
        server = MagicMock()
        server.get_build_info.side_effect = [{'building': False, 'result': 'SUCCESS', 'actions': [{'parameters': [
            {'name': 'repository', 'value': 'https://github.com/example/cassandra'},
            {'name': 'branch', 'value': 'feature/123'}]}]}, {'building': False, 'result': 'SUCCESS'}]
        with patch('run_ci.load_environment_file'), patch('run_ci.parse_arguments', return_value=args), \
                patch('run_ci.get_jenkins', return_value=('https://ci.example', server)), \
                patch('run_ci.setup_environment') as setup, patch('run_ci.is_local_git_dirty') as dirty, \
                patch('run_ci.wait_for_build_complete') as wait, \
                patch('run_ci.download_results_and_print_summary') as download, \
                patch('run_ci.delete_remote_junit_files') as delete, patch('run_ci.node_cleaner') as cleaner:
            quietly(run_ci.main)
        setup.assert_not_called()
        dirty.assert_not_called()
        cleaner.assert_not_called()
        delete.assert_not_called()
        server.build_job.assert_not_called()
        server.stop_build.assert_not_called()
        wait.assert_called_once_with(server, 'cassandra-5.0', 42)
        download.assert_called_once()
        self.assertEqual((args.repository, args.branch), ('https://github.com/example/cassandra', 'feature/123'))

    def test_attach_unknown_build_fails_without_polling_or_launch(self):
        args = self.args(url='https://ci.example', job='cassandra', attach=999)
        server = MagicMock()
        server.get_build_info.side_effect = jenkins.NotFoundException('missing')
        with patch('run_ci.load_environment_file'), patch('run_ci.parse_arguments', return_value=args), \
                patch('run_ci.get_jenkins', return_value=('https://ci.example', server)), \
                patch('run_ci.wait_for_build_complete') as wait, self.assertRaises(jenkins.NotFoundException):
            run_ci.main()
        wait.assert_not_called()
        server.build_job.assert_not_called()

    def test_attach_wait_handles_disconnects_and_completed_builds(self):
        args = self.args(url='https://ci.example', job='cassandra', attach=42)
        for states in ([{'building': False, 'result': 'SUCCESS'}],
                       [jenkins.JenkinsException('disconnected'), {'building': True, 'result': None},
                        {'building': False, 'result': 'SUCCESS'}]):
            server = MagicMock()
            server.get_build_info.side_effect = [{'actions': []}] + states
            with patch('run_ci.download_results_and_print_summary') as download:
                quietly(lambda: run_ci.attach_build(None, args.url, server, args))
            self.assertEqual(server.get_build_info.call_count, len(states) + 1)
            server.build_job.assert_not_called()
            server.stop_build.assert_not_called()
            download.assert_called_once()

    def test_interrupting_attach_leaves_build_running(self):
        args = self.args(url='https://ci.example', job='cassandra', attach=42)
        server = MagicMock()
        server.get_build_info.return_value = {'actions': []}
        with patch('run_ci.wait_for_build_complete', side_effect=KeyboardInterrupt), \
                patch('run_ci.download_results_and_print_summary') as download, self.assertRaises(KeyboardInterrupt):
            quietly(lambda: run_ci.attach_build(None, args.url, server, args))
        download.assert_not_called()
        server.stop_build.assert_not_called()
        server.build_job.assert_not_called()

    def test_download_uses_authenticated_client_and_exact_job(self):
        args = self.args(url='https://ci.example/jenkins', job='team/cassandra-5.0',
                         repository='https://github.com/example/cassandra', branch='feature/123')
        server = MagicMock()
        response = server.jenkins_request.return_value.__enter__.return_value
        response.iter_content.return_value = [b'test artifact']
        with patch('run_ci.LOCAL_RESULTS_BASEDIR', Path(self.directory.name) / 'results'):
            quietly(lambda: run_ci.download_results_and_print_summary(None, 'unused', 'default', 42,
                                                                      args.url, args, server=server))
        urls = [call.args[0].url for call in server.jenkins_request.call_args_list]
        self.assertIn('https://ci.example/jenkins/job/team/job/cassandra-5.0/42/artifact/results_details.tar.xz', urls)
        self.assertIn('https://ci.example/jenkins/job/team/job/cassandra-5.0/42/consoleText', urls)
        artifacts = list((Path(self.directory.name) / 'results').rglob('*.tar.xz'))
        self.assertEqual(len(artifacts), 1)
        self.assertEqual(artifacts[0].read_bytes(), b'test artifact')
        self.assertEqual(artifacts[0].name, 'results_details_example_feature-123_42.tar.xz')
        self.assertEqual(artifacts[0].parent.parent.name, 'team%2Fcassandra-5.0')

    def test_download_directory_uses_host_without_url_scheme(self):
        for url, directory in [('https://astro-cass.ci', 'astro-cass.ci'),
                               ('http://astro-cass.ci', 'astro-cass.ci'),
                               ('https://astro-cass.ci:443/', 'astro-cass.ci'),
                               ('http://ci.example:8080', 'ci.example:8080'),
                               ('http://[::1]:8080', '[::1]:8080'),
                               ('http://..', '%2E%2E'),
                               ('https://ci.example/jenkins', 'ci.example%2Fjenkins')]:
            with self.subTest(url=url):
                args = self.args(url=url, job='cassandra-6.0', repository=None, branch=None)
                server = MagicMock()
                response = server.jenkins_request.return_value.__enter__.return_value
                response.iter_content.return_value = [b'Finished: SUCCESS\n']
                with patch('run_ci.LOCAL_RESULTS_BASEDIR', Path(self.directory.name)), \
                        contextlib.redirect_stdout(io.StringIO()) as output:
                    run_ci.download_results_and_print_summary(None, 'unused', 'default', 58,
                                                              url, args, server=server)
                destination = Path(self.directory.name) / directory / 'cassandra-6.0' / '58'
                self.assertTrue((destination / 'console_log.txt.gz').is_file())
                self.assertTrue((destination / 'ci_summary_unknown_unknown_58.html').is_file())
                self.assertTrue((destination / 'results_details_unknown_unknown_58.tar.xz').is_file())
                self.assertIn(f'CI summary saved as {destination}/', output.getvalue())
                self.assertNotIn('https%3A', output.getvalue())
                self.assertNotIn('http%3A', output.getvalue())

    def test_interrupted_download_does_not_leave_partial_artifacts(self):
        args = self.args(url='https://ci.example', job='cassandra', repository=None, branch=None)
        server = MagicMock()
        response = server.jenkins_request.return_value.__enter__.return_value
        def interrupted(**_kwargs):
            yield b'partial data'
            raise run_ci.requests.exceptions.ConnectionError('connection lost')
        response.iter_content.side_effect = interrupted
        destination = Path(self.directory.name) / 'results'
        with patch('run_ci.LOCAL_RESULTS_BASEDIR', destination):
            quietly(lambda: run_ci.download_results_and_print_summary(None, 'unused', 'default', 42,
                                                                      args.url, args, server=server))
        self.assertFalse(any(path.is_file() for path in destination.rglob('*')))

    @patch('run_ci.shutil.which', return_value=None)
    def test_url_build_never_checks_cluster_tools(self, which):
        args = self.args(url='https://ci.example', job='cassandra', user='alice')
        server = MagicMock()
        server.jenkins_open.return_value = json.dumps({'allBuilds': [], 'property': []})
        with patch('run_ci.load_environment_file'), patch('run_ci.parse_arguments', return_value=args), \
                patch('run_ci.get_jenkins', return_value=(args.url, server)), \
                patch('run_ci.is_local_git_dirty', return_value=False), \
                patch('run_ci.trigger_jenkins_build', return_value=100) as trigger, \
                patch('run_ci.wait_for_build_number', return_value=42), \
                patch('run_ci.wait_for_build_complete'), patch('run_ci.download_results_and_print_summary'), \
                patch('run_ci.setup_environment') as setup:
            quietly(run_ci.main)
        setup.assert_not_called()
        which.assert_not_called()
        trigger.assert_called_once()

    def test_missing_authenticated_artifact_is_not_retried(self):
        calls = MagicMock(side_effect=jenkins.NotFoundException('not archived'))
        with self.assertRaises(jenkins.NotFoundException):
            run_ci.retry_call(calls, 'get artifact', run_ci.JENKINS_API_ERRORS, 5, 0)
        calls.assert_called_once()


class TestBuildResultSummary(unittest.TestCase):
    TOTALS = '''<table data-failure-count-capped="false">
        <tr><td>Passed</td><td></td><td> 112234</td></tr>
        <tr><td>Failed</td><td></td><td> 216</td></tr>
        <tr><td>Skipped</td><td></td><td> 1950</td></tr>
        <tr><td>Total</td><td>&nbsp;&nbsp;&nbsp;</td><td> 114400</td></tr>
        </table>'''

    def summary(self, console, totals=TOTALS):
        args = run_ci.argument_parser().parse_args([])
        args.url, args.job = 'https://ci.example', 'cassandra-6.0'
        args.repository, args.branch = None, None
        server = MagicMock()

        def response(request, **_kwargs):
            if request.url.endswith('/consoleText'):
                content = console
            elif request.url.endswith('/ci_summary.html'):
                content = totals
            else:
                content = 'details archive'
            if content is None:
                raise jenkins.NotFoundException('not archived')
            result = MagicMock()
            result.__enter__.return_value.iter_content.return_value = [content.encode()]
            return result

        server.jenkins_request.side_effect = response
        with tempfile.TemporaryDirectory() as directory, \
                patch('run_ci.LOCAL_RESULTS_BASEDIR', Path(directory)), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            run_ci.download_results_and_print_summary(None, 'unused', 'default', 58,
                                                      args.url, args, server=server)
            compressed = list(Path(directory).rglob('console_log.txt.gz'))
            if console is not None:
                self.assertEqual(len(compressed), 1)
                with run_ci.gzip.open(compressed[0], 'rt') as log:
                    self.assertEqual(log.read(), console)
            else:
                self.assertEqual(compressed, [])
            return output.getvalue().split('--- Build Summary ---\n', 1)[1]

    def test_unstable_result_does_not_print_interleaved_ant_failure_excerpt(self):
        console = ('BUILD FAILED\n'
                   '/home/cassandra/cassandra/build.xml:1985: The following error occurred while executing this line:\n'
                   '[Checks API] No suitable checks publisher found.\n[Pipeline] sh\n'
                   '     [java] DEBUG [org.apache.cassandra.test.microbench.SnapshotTakingBench]\n'
                   '/home/cassandra/cassandra/build.xml:1377: Some test(s) failed.\nFinished: UNSTABLE\n')
        output = self.summary(console)
        self.assertIn('Jenkins result: UNSTABLE\n', output)
        self.assertIn('Tests: Passed 112234 | Failed 216 | Skipped 1950 | Total 114400\n', output)
        self.assertRegex(output, r'Console log saved as .*/console_log.txt.gz\n')
        for excerpt in ('BUILD FAILED', '[Checks API]', '[Pipeline]', '[java]', 'build.xml:'):
            self.assertNotIn(excerpt, output)

    def test_final_result_wins_over_earlier_log_messages(self):
        output = self.summary('Example: Finished: FAILURE\nFinished: FAILURE\n'
                              'Retry succeeded\nFinished: SUCCESS\n')
        self.assertIn('Jenkins result: SUCCESS\n', output)
        self.assertNotIn('Jenkins result: FAILURE', output)

    def test_failed_build_without_reports_keeps_result_and_full_log(self):
        output = self.summary('BUILD FAILED\ncompiler error\nFinished: FAILURE\n', totals=None)
        self.assertIn('Jenkins result: FAILURE\n', output)
        self.assertIn('Test totals unavailable: CI summary missing.', output)
        self.assertNotIn('No tests were run', output)

    def test_log_without_final_result_does_not_infer_failure(self):
        output = self.summary('BUILD FAILED\n[Pipeline] More stages still running\n')
        self.assertIn('Jenkins result unavailable in console log.', output)
        self.assertNotIn('Jenkins result: FAILURE', output)

    def test_missing_console_preserves_test_totals(self):
        output = self.summary(None)
        self.assertIn('Missing console log.', output)
        self.assertIn('Tests: Passed 112234 | Failed 216 | Skipped 1950 | Total 114400', output)

    def test_report_without_totals_does_not_claim_no_tests_ran(self):
        output = self.summary('Finished: ABORTED\n', totals='<html>Incomplete report</html>')
        self.assertIn('Jenkins result: ABORTED\n', output)
        self.assertIn('Test totals unavailable in CI summary.', output)
        self.assertNotIn('No tests were run', output)

    def test_legacy_failure_limit_marks_failed_and_total_as_lower_bounds(self):
        for failures in (200, 216):
            with self.subTest(failures=failures):
                total = 112234 + failures + 1950
                report = self.TOTALS.replace(' data-failure-count-capped="false"', '')
                report = report.replace('216', str(failures)).replace('114400', str(total))
                output = self.summary('Finished: UNSTABLE\n', totals=report)
                self.assertIn(f'Tests: Passed 112234 | Failed {failures}+ | Skipped 1950 | Total {total}+\n', output)
                self.assertIn('lower bounds', output)

    def test_legacy_failure_count_below_limit_is_exact(self):
        report = self.TOTALS.replace(' data-failure-count-capped="false"', '')
        report = report.replace('216', '199').replace('114400', '114383')
        output = self.summary('Finished: UNSTABLE\n', totals=report)
        self.assertIn('Failed 199 | Skipped 1950 | Total 114383\n', output)
        self.assertNotIn('lower bounds', output)

    def test_uncapped_reports_keep_exact_counts_at_and_above_old_limit(self):
        for failures in (200, 528):
            with self.subTest(failures=failures):
                total = 112234 + failures + 1950
                report = self.TOTALS.replace('216', str(failures)).replace('114400', str(total))
                # HTML formatting must not decide which totals we can read.
                report = run_ci.BeautifulSoup(report, 'html.parser').prettify()
                output = self.summary('Finished: UNSTABLE\n', totals=report)
                self.assertIn(f'Failed {failures} | Skipped 1950 | Total {total}\n', output)
                self.assertNotIn('lower bounds', output)

    def test_partial_totals_are_unavailable(self):
        output = self.summary('Finished: ABORTED\n', totals='<table><tr><td>Passed</td><td></td><td>2</td></tr></table>')
        self.assertIn('Test totals unavailable in CI summary.', output)


class TestBuildAttachment(unittest.TestCase):
    PARAMETERS = {
        'repository': 'https://github.com/example/cassandra', 'branch': 'feature/123',
        'profile': 'skinny', 'profile_custom_regexp': '', 'jdk': '',
        'dtest_repository': 'https://github.com/apache/cassandra-dtest.git', 'dtest_branch': 'trunk',
    }

    def setUp(self):
        self.args = run_ci.argument_parser().parse_args([])
        vars(self.args).update(self.PARAMETERS, url='https://ci.example/jenkins', user='alice')
        self.server = MagicMock()
        self.builds = {}
        self.server.get_build_info.side_effect = lambda _job, number: self.builds[number]
        self.job_info = {'allBuilds': [], 'property': [{'parameterDefinitions': [
            {'name': 'architecture', 'defaultParameterValue': {'value': 'amd64'}}]}]}
        self.server.jenkins_open.side_effect = lambda *_args, **_kwargs: json.dumps(self.job_info)
        # Exercise normal version-based job inference, without using GitHub or the cluster.
        if hasattr(run_ci.base_job_name, '_cached_result'):
            del run_ci.base_job_name._cached_result
        self.addCleanup(lambda: vars(run_ci.base_job_name).pop('_cached_result', None))

    def build(self, number, building=True):
        parameters = dict(self.PARAMETERS, architecture='amd64')
        self.builds[number] = {'building': building, 'result': None if building else 'SUCCESS',
                               'actions': [{'parameters': [{'name': name, 'value': value}
                                                            for name, value in parameters.items()]}]}
        self.job_info['allBuilds'].append({'number': number, 'building': building})

    @contextlib.contextmanager
    def invocation(self, answer=''):
        with contextlib.ExitStack() as stack:
            mocks = {}
            for name, values in {
                'load_environment_file': {}, 'parse_arguments': {'return_value': self.args},
                'get_jenkins': {'return_value': ('https://ci.example/jenkins', self.server)},
                'setup_environment': {}, 'is_local_git_dirty': {'return_value': False},
                'trigger_jenkins_build': {'return_value': 100}, 'wait_for_build_number': {'return_value': 999},
                'wait_for_build_complete': {}, 'download_results_and_print_summary': {},
                'delete_remote_junit_files': {}, 'cleanup_and_maybe_teardown': {}, 'threading.Thread': {},
                'requests.head': {'return_value': MagicMock(status_code=200)},
                'requests.get': {'return_value': MagicMock(text='<property name="base.version" value="5.0.9"/>')},
            }.items():
                mocks[name] = stack.enter_context(patch('run_ci.' + name, **values))
            mocks['input'] = stack.enter_context(patch('builtins.input', return_value=answer))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            yield mocks

    def test_attach_number_infers_job_from_repository_and_branch(self):
        for version, job in [('5.0.9', 'cassandra-5.0'), ('6.0-alpha1', 'cassandra-6.0'), ('7.0', 'cassandra')]:
            with self.subTest(version=version):
                vars(run_ci.base_job_name).pop('_cached_result', None)
                with patch.dict(os.environ, {}, clear=True), \
                        patch('run_ci.DEFAULT_REPO_URL', self.PARAMETERS['repository']), \
                        patch('run_ci.DEFAULT_REPO_BRANCH', self.PARAMETERS['branch']), \
                        patch('run_ci.sys.argv', ['run-ci', '--attach', '42', '--url', 'https://ci.example/jenkins']):
                    self.args = run_ci.parse_arguments()
                self.assertIsNone(self.args.job)
                self.build(42)
                with self.invocation() as mocks:
                    mocks['requests.get'].return_value.text = f'<property name="base.version" value="{version}"/>'
                    run_ci.main()
                mocks['wait_for_build_complete'].assert_called_once_with(self.server, job, 42)
                mocks['download_results_and_print_summary'].assert_called_once()
                mocks['trigger_jenkins_build'].assert_not_called()
                mocks['input'].assert_not_called()
                self.assertEqual(self.args.job, job)

    def test_matching_build_offers_attach_before_launch_or_cleanup(self):
        self.args.url = None  # Cluster connection must not start its background cleaner when attaching.
        self.build(40)
        self.build(42)
        with self.invocation() as mocks:
            run_ci.main()
        mocks['input'].assert_called_once()
        self.assertIn('42', mocks['input'].call_args.args[0])
        mocks['wait_for_build_complete'].assert_called_once_with(self.server, 'cassandra-5.0', 42)
        mocks['download_results_and_print_summary'].assert_called_once()
        for name in ('trigger_jenkins_build', 'is_local_git_dirty', 'delete_remote_junit_files',
                     'cleanup_and_maybe_teardown', 'threading.Thread'):
            mocks[name].assert_not_called()
        self.assertEqual(self.args.attach, 42)

    def test_declining_attachment_starts_new_build(self):
        self.build(42)
        with self.invocation(answer='n') as mocks:
            run_ci.main()
        mocks['input'].assert_called_once()
        mocks['trigger_jenkins_build'].assert_called_once_with(self.server, 'cassandra-5.0', **self.PARAMETERS)
        mocks['wait_for_build_complete'].assert_called_once_with(self.server, 'cassandra-5.0', 999)

    def test_every_submitted_parameter_and_job_default_must_match(self):
        for name in (*self.PARAMETERS, 'architecture'):
            with self.subTest(parameter=name):
                self.build(42)
                parameters = self.builds[42]['actions'][0]['parameters']
                next(parameter for parameter in parameters if parameter['name'] == name)['value'] = 'different'
                with self.invocation() as mocks:
                    run_ci.main()
                mocks['input'].assert_not_called()
                mocks['trigger_jenkins_build'].assert_called_once()

    def test_completed_builds_and_missing_parameter_metadata_do_not_match(self):
        self.build(40, building=False)
        self.build(41)
        self.builds[41]['actions'] = []
        self.build(42)
        self.builds[42]['building'] = False  # Finished between the job query and build query.
        with self.invocation() as mocks:
            run_ci.main()
        mocks['input'].assert_not_called()
        mocks['trigger_jenkins_build'].assert_called_once()

    def test_finds_old_running_pipeline_without_querying_executors_or_completed_builds(self):
        for number in range(150, 42, -1):
            self.build(number, building=False)
        self.build(42)
        self.builds[42]['result'] = 'UNSTABLE'  # Pipelines can set a result before finishing.
        self.args.job = 'team/cassandra-5.0'
        with self.invocation() as mocks:
            run_ci.main()
        mocks['trigger_jenkins_build'].assert_not_called()
        mocks['wait_for_build_complete'].assert_called_once_with(self.server, 'team/cassandra-5.0', 42)
        self.server.get_running_builds.assert_not_called()
        self.assertTrue(all(call.args == ('team/cassandra-5.0', 42)
                            for call in self.server.get_build_info.call_args_list))
        request = self.server.jenkins_open.call_args.args[0]
        self.assertEqual(request.url, 'https://ci.example/jenkins/job/team/job/cassandra-5.0/api/json')
        self.assertIn('allBuilds[number,building]', request.params['tree'])

    def test_inspection_failure_does_not_launch_a_duplicate(self):
        self.server.jenkins_open.side_effect = jenkins.JenkinsException('unavailable')
        with self.invocation() as mocks, self.assertRaises(jenkins.JenkinsException):
            run_ci.main()
        mocks['trigger_jenkins_build'].assert_not_called()

    def test_incomplete_job_response_does_not_launch_a_duplicate(self):
        for response in ('not JSON', 'null', '{}'):
            with self.subTest(response=response):
                self.server.jenkins_open.side_effect = None
                self.server.jenkins_open.return_value = response
                with self.invocation() as mocks, self.assertRaises(jenkins.JenkinsException):
                    run_ci.main()
                mocks['trigger_jenkins_build'].assert_not_called()

    def test_deleted_build_does_not_hide_an_older_match(self):
        self.build(42)
        self.build(43)
        def lookup(_job, number):
            if number == 43:
                raise jenkins.NotFoundException('deleted')
            return self.builds[number]
        self.server.get_build_info.side_effect = lookup
        with self.invocation() as mocks:
            run_ci.main()
        mocks['wait_for_build_complete'].assert_called_once_with(self.server, 'cassandra-5.0', 42)
        mocks['trigger_jenkins_build'].assert_not_called()

    def test_requested_values_override_job_defaults_when_matching(self):
        self.job_info['property'][0]['parameterDefinitions'].extend([
            {'name': 'branch', 'defaultParameterValue': {'value': 'trunk'}},
            {'name': 'profile', 'defaultParameterValue': {'value': 'post-commit'}},
            {'name': 'optional', 'defaultParameterValue': None},
        ])
        self.build(42)
        with self.invocation() as mocks:
            run_ci.main()
        mocks['wait_for_build_complete'].assert_called_once_with(self.server, 'cassandra-5.0', 42)
        mocks['trigger_jenkins_build'].assert_not_called()

    def test_invalid_prompt_answer_does_not_start_a_duplicate(self):
        self.build(42)
        with self.invocation() as mocks:
            mocks['input'].side_effect = ['maybe', 'Y']
            run_ci.main()
        self.assertEqual(mocks['input'].call_count, 2)
        mocks['trigger_jenkins_build'].assert_not_called()

    def test_unavailable_prompt_leaves_matching_build_running(self):
        self.build(42)
        with self.invocation() as mocks:
            mocks['input'].side_effect = EOFError
            with self.assertRaises(SystemExit) as error:
                run_ci.main()
        self.assertIn('--attach 42', str(error.exception))
        mocks['trigger_jenkins_build'].assert_not_called()
        self.server.stop_build.assert_not_called()


class TestSplitProgress(unittest.TestCase):
    @staticmethod
    def node(number, name, state='running', kind='PARALLEL', children=None, **extra):
        return dict(id=str(number), name=name, state=state, type=kind, children=children or [], **extra)

    def tree(self, *branches):
        return {'status': 'ok', 'data': {'stages': [
            self.node(1, 'jar', kind='STAGE', children=[self.node(2, 'jar jdk11', 'success')]),
            self.node(3, 'Tests', kind='STAGE', children=list(branches)),
            self.node(10000, 'Summary', kind='STAGE'),
        ]}}

    def server(self, *graphs):
        server = MagicMock(server='https://ci.example/jenkins/')
        server.jenkins_open.side_effect = [json.dumps(graph) if isinstance(graph, dict) else graph for graph in graphs]
        return server

    def test_counts_selected_branches_and_terminal_outcomes(self):
        graph = self.tree(*(self.node(index + 10, f'test jdk11 {index + 1}/11', state)
                            for index, state in enumerate(['success', 'failure', 'unstable', 'aborted',
                                                           'skipped', 'not_built', 'finished', 'running',
                                                           'queued', 'paused', 'unknown'])))
        counts = run_ci.count_test_splits(graph)
        self.assertEqual(counts, dict(total=11, finished=7, failed=1, unstable=1, aborted=1, skipped=2,
                                     running=1, queued=1, paused=1, unknown=1))

    def test_retry_children_and_repeated_node_references_are_counted_once(self):
        retry_attempt = self.node(21, 'test jdk11 1/2', 'failure', kind='STAGE')
        branch = self.node(20, 'test jdk11 1/2', children=[retry_attempt])
        other = self.node(30, 'test jdk11 2/2', 'queued')
        branch['nextSibling'] = other
        graph = self.tree(self.node(10, 'parallel', kind='PARALLEL_BLOCK', children=[branch, other]))
        counts = run_ci.count_test_splits(graph)
        self.assertEqual((counts['total'], counts['finished'], counts['failed'], counts['running']), (2, 0, 0, 1))

    def test_single_split_tasks_and_different_jdks_are_separate(self):
        graph = self.tree(self.node(10, 'fqltool-test jdk11', 'success'),
                          self.node(11, 'fqltool-test jdk17', 'queued'),
                          self.node(12, 'test jdk11 1/20', 'running'))
        counts = run_ci.count_test_splits(graph)
        self.assertEqual((counts['total'], counts['finished']), (3, 1))

    def test_all_branches_are_counted_without_a_display_limit(self):
        graph = self.tree(*(self.node(i + 10, f'dtest jdk11 {i + 1}/400', 'success') for i in range(400)))
        self.assertEqual(run_ci.count_test_splits(graph)['finished'], 400)

    def test_before_fanout_does_not_claim_zero_of_zero_complete(self):
        for graph in (self.tree(), {'status': 'ok', 'data': {'stages': []}}):
            self.assertIsNone(run_ci.count_test_splits(graph))

    def test_unknown_state_is_not_counted_as_finished(self):
        for state in ('new-plugin-state', 'failed', 'total'):
            with self.subTest(state=state):
                counts = run_ci.count_test_splits(self.tree(self.node(10, 'test jdk11 1/1', state)))
                self.assertEqual((counts['total'], counts['finished'], counts['unknown']), (1, 0, 1))

    def test_placeholder_branches_are_not_counted_as_selected_splits(self):
        graph = self.tree(self.node(10, 'test jdk11 1/2', 'success'),
                          self.node(11, 'test jdk17 1/2', 'not_built', placeholder=True,
                                    children=[self.node(12, 'nested unused branch')]))
        self.assertEqual(run_ci.count_test_splits(graph)['total'], 1)

    def test_graph_poll_is_throttled_but_completion_refreshes_immediately(self):
        graph = self.tree(self.node(10, 'test jdk11 1/1', 'success'))
        server = self.server(graph, graph, graph)
        progress = run_ci.SplitProgress(server, 'cassandra', 42)
        with patch('run_ci.time.monotonic', side_effect=[0, 3, 14, 15, 16]):
            for _ in range(3):
                progress.update()
            self.assertEqual(server.jenkins_open.call_count, 1)
            progress.update()
            self.assertEqual(server.jenkins_open.call_count, 2)
            progress.update(complete=True)
        self.assertEqual(server.jenkins_open.call_count, 3)

    def test_overview_endpoint_is_reused_when_stages_endpoint_is_missing(self):
        graph = self.tree(self.node(10, 'test jdk11 1/1', 'success'))
        server = self.server(jenkins.NotFoundException('no stages route'), graph, graph)
        progress = run_ci.SplitProgress(server, 'cassandra', 42)
        with patch('run_ci.time.monotonic', side_effect=[0, 15]):
            progress.update()
            progress.update()
        self.assertEqual([call.args[0].url.rsplit('/42/', 1)[1] for call in server.jenkins_open.call_args_list],
                         ['stages/tree', 'pipeline-overview/tree', 'pipeline-overview/tree'])
        self.assertIn('1/1 finished', progress.text)

    def test_absent_graph_is_retried_after_startup_with_backoff(self):
        graph = self.tree(self.node(10, 'test jdk11 1/1', 'running'))
        server = self.server(jenkins.NotFoundException('not ready'), jenkins.NotFoundException('not ready'), graph)
        progress = run_ci.SplitProgress(server, 'cassandra', 42)
        with patch('run_ci.time.monotonic', side_effect=[0, 15, 59, 60]):
            for _ in range(3):
                progress.update()
            self.assertEqual(server.jenkins_open.call_count, 2)
            self.assertEqual(progress.text, 'Splits: unavailable')
            progress.update()
        self.assertIn('0/1 finished', progress.text)

    def test_failed_and_aborted_splits_are_visible_in_summary(self):
        counts = run_ci.count_test_splits(self.tree(self.node(10, 'test jdk11 1/3', 'failure'),
                                                   self.node(11, 'test jdk11 2/3', 'aborted'),
                                                   self.node(12, 'test jdk11 3/3', 'queued')))
        self.assertEqual(run_ci.format_split_progress(counts),
                         'Splits: 2/3 finished (1 failed, 1 aborted) | 0 running | 1 queued')

    def test_polling_displays_progress_then_recovers_after_graph_failure(self):
        graph = self.tree(self.node(10, 'test jdk11 1/2', 'success'), self.node(11, 'test jdk11 2/2', 'queued'))
        server = self.server(graph, jenkins.JenkinsException('temporary outage'), graph, graph)
        server.get_build_info.side_effect = [{'building': True, 'result': None}] * 3 + [
            {'building': False, 'result': 'SUCCESS'}]
        output = io.StringIO()
        with patch('run_ci.time.sleep'), patch('run_ci.time.monotonic', side_effect=range(0, 1000, 20)), \
                contextlib.redirect_stdout(output):
            run_ci.wait_for_build_complete(server, 'team/cassandra-5.0', 42)
        text = output.getvalue()
        self.assertIn('Splits: 1/2 finished', text)
        self.assertIn('1 queued', text)
        self.assertIn('Splits: unavailable', text)
        self.assertIn('status: SUCCESS', text)
        self.assertGreater(text.rfind('Splits: 1/2 finished'), text.find('Splits: unavailable'))
        request = server.jenkins_open.call_args.args[0]
        self.assertEqual(request.url, 'https://ci.example/jenkins/job/team/job/cassandra-5.0/42/stages/tree')
        self.assertFalse(server.jenkins_open.call_args.kwargs['add_crumb'])

    def test_missing_plugin_or_bad_response_does_not_stop_waiting(self):
        for result in (jenkins.NotFoundException('no plugin'), jenkins.JenkinsException('forbidden'),
                       run_ci.requests.exceptions.Timeout('timeout'), '<html>not JSON</html>',
                       {'status': 'error'}, {'status': 'ok', 'data': {'stages': 'invalid'}}):
            with self.subTest(result=result):
                server = self.server()
                server.jenkins_open.side_effect = result if isinstance(result, Exception) else None
                if not isinstance(result, Exception):
                    server.jenkins_open.return_value = json.dumps(result) if isinstance(result, dict) else result
                server.get_build_info.side_effect = [{'building': True, 'result': None},
                                                     {'building': False, 'result': 'SUCCESS'}]
                output = io.StringIO()
                with patch('run_ci.time.sleep'), contextlib.redirect_stdout(output):
                    run_ci.wait_for_build_complete(server, 'cassandra', 42)
                self.assertIn('Splits: unavailable', output.getvalue())
                self.assertIn('status: SUCCESS', output.getvalue())

    def test_attach_reconstructs_existing_progress(self):
        server = self.server(self.tree(self.node(10, 'test jdk11 1/2', 'success'),
                                      self.node(11, 'test jdk11 2/2', 'running')),
                             self.tree(self.node(10, 'test jdk11 1/2', 'success'),
                                       self.node(11, 'test jdk11 2/2', 'success')))
        server.get_build_info.side_effect = [{'actions': []}, {'building': True, 'result': None},
                                             {'building': False, 'result': 'SUCCESS'}]
        args = argparse.Namespace(job='cassandra', attach=42)
        output = io.StringIO()
        with patch('run_ci.time.sleep'), contextlib.redirect_stdout(output), \
                patch('run_ci.download_results_and_print_summary') as download:
            run_ci.attach_build(None, server.server, server, args)
        self.assertIn('Splits: 1/2 finished', output.getvalue())
        download.assert_called_once()
        server.build_job.assert_not_called()

    def test_spinner_clears_old_counts_and_restores_cursor_on_interrupt(self):
        output = io.StringIO()
        with patch('run_ci.time.sleep'), contextlib.redirect_stdout(output):
            with self.assertRaises(KeyboardInterrupt):
                run_ci.spin_while(lambda: 'Splits: 1/2 finished', MagicMock(side_effect=[False, KeyboardInterrupt]))
        self.assertIn('Splits: 1/2 finished', output.getvalue())
        self.assertTrue(output.getvalue().endswith('\r\033[K\033[?25h'))


if __name__ == '__main__':
    unittest.main()
