#!/usr/bin/env python3
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

import argparse
import math
import os
import sys

bin_dir = os.path.dirname(os.path.realpath(__file__))
src_dir = bin_dir
dep_dir = os.path.join(bin_dir, '.python3-deps')
sys.path.append(dep_dir)

try:
    import yaml
except ImportError:
    import subprocess
    # dependencies not installed; install them outside of the normal system

    subprocess.check_call([sys.executable, '-m', 'pip', 'install', "--target=" + dep_dir, 'pyyaml'])

    import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

targets = {
        'MIDRES': {
            # apply defaults to all jobs, so the below just overrides jobs which do not match the default
            '_default_job_':                            {'parallelism': 25,    'exec_resource_class': 'large'},
            'jobs': {
                'j8_jvm_upgrade_dtests':                {'parallelism': 4},
                'j11_jvm_dtests':                       {'parallelism': 5},
                'j11_jvm_dtests_vnode':                 {'parallelism': 5},
                'j8_jvm_dtests':                        {'parallelism': 5},
                'j8_jvm_dtests_vnode':                  {'parallelism': 5},

                'j11_unit_tests':                       {'parallelism': 50, 'exec_resource_class': 'medium'},
                'j11_cqlsh-dtests-py3-no-vnodes':       {'parallelism': 50, 'exec_resource_class': 'medium'},
                'j11_cqlsh-dtests-py3-with-vnodes':     {'parallelism': 50, 'exec_resource_class': 'medium'},
                'j11_cqlsh-dtests-py38-no-vnodes':      {'parallelism': 50, 'exec_resource_class': 'medium'},
                'j11_cqlsh-dtests-py38-with-vnodes':    {'parallelism': 50, 'exec_resource_class': 'medium'},
                'j11_dtests-no-vnodes':                 {'parallelism': 50},
                'j11_dtests-with-vnodes':               {'parallelism': 50},

                'repeated_upgrade_dtest':               {'exec_resource_class': 'xlarge'},
            },
        },
        'HIGHER': {
            # apply defaults to all jobs, so the below just overrides jobs which do not match the default
            '_default_job_':                            {'parallelism': 100,    'exec_resource_class': 'xlarge'},
            'jobs': {
                'j8_jvm_upgrade_dtests':                {'parallelism': 2},
                'j11_jvm_dtests':                       {'parallelism': 5},
                'j11_jvm_dtests_vnode':                 {'parallelism': 5},
                'j8_jvm_dtests':                        {'parallelism': 5},
                'j8_jvm_dtests_vnode':                  {'parallelism': 5},
            },
        },
}

# job spec which are consistent regardless of their target go here; avoids boilerplate
common_jobs_config = {
    'j11_build':                            {'parallelism': 1, 'exec_resource_class': 'medium'},
    'j11_cqlshlib_tests':                   {'parallelism': 1, 'exec_resource_class': 'medium'},
    'j8_build':                             {'parallelism': 1, 'exec_resource_class': 'medium'},
    'j8_cqlshlib_tests':                    {'parallelism': 1, 'exec_resource_class': 'medium'},
    'j8_dtest_jars_build':                  {'parallelism': 1, 'exec_resource_class': 'medium'},
    'utests_fqltool':                       {'parallelism': 1},
    'utests_long':                          {'parallelism': 1},
    'utests_stress':                        {'parallelism': 1},
}

for target in targets.values():
    if 'jobs' in target:
        target['jobs'].update(common_jobs_config)

def apply_job_spec(job, spec):
    if 'parallelism' in spec:
        job['parallelism'] = spec['parallelism']
    # executor can be a str (reference to .executors.value) or dict (name reference, and/or exec_resource_class)
    executor = job['executor']
    if type(executor) == str:
        # convert simple name to object with name reference, so its easier to add exec_resource_class
        executor = { 'name': executor }
        job['executor'] = executor
    if 'exec_resource_class' in spec:
        executor['exec_resource_class'] = spec['exec_resource_class']

def update_jobs(target, contents):
    # update jobs
    if 'jobs' in target:
        target_jobs = target['jobs']
        jobs = contents['jobs']
        default_spec = target.get('_default_job_')
        if default_spec:
            for name, job in jobs.items():
                apply_job_spec(job, default_spec)

        for name, spec in target_jobs.items():
            apply_job_spec(jobs[name], spec)

def parse_args():
    parser = argparse.ArgumentParser(description='Apache Cassandra Circle CI Generator')
    parser.add_argument('target', type=str, help=','.join(targets.keys()))
    parser.add_argument('--stdout', action='store_true', default=False, help='Write the output to stdout rather than file')
    return parser.parse_args()

def main():
    args = parse_args()
    target = targets[args.target]
    src = os.path.join(bin_dir, 'config-2_1.yml')

    with open(src, 'r') as r:
        contents = yaml.load(r, Loader=Loader)

    update_jobs(target, contents)

    def writeout(r):
            # to make sure RAT doesn't yell... dump out the license first...
            r.write("""#
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

""")
            yaml.dump(contents, r, Dumper=Dumper)

    if args.stdout:
        writeout(sys.stdout)
    else:
        output_path = os.path.join(bin_dir, f'config-2_1.yml.{args.target}')
        with open(output_path, 'w') as r:
            writeout(r)

if __name__ == "__main__":
    main()
