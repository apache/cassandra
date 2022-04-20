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
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


def parse_args():
    parser = argparse.ArgumentParser(description='Apache Cassandra Circle CI Job Resources')
    parser.add_argument('target', type=str, help='File to read')
    return parser.parse_args()

def main():
    args = parse_args()
    target = args.target

    with open(target, 'r') as r:
        contents = yaml.load(r, Loader=Loader)
    for name, job in contents['jobs'].items():
        parallelism = job['parallelism']
        executor = job['executor']
        if type(executor) == str:
            # convert simple name to object with name reference, so its easier to add exec_resource_class
            executor = { 'name': executor }
            job['executor'] = executor
        if 'exec_resource_class' in executor:
            exec_resource_class = executor['exec_resource_class']
        else:
            # need to lookup
            exec_resource_class = contents['executors'][executor['name']]['parameters']['exec_resource_class']['default']
        print(f'{name}\t{exec_resource_class}\t{parallelism}')


if __name__ == "__main__":
    main()
