#!/usr/bin/env python3

import argparse
import re

executors = re.compile("^([a-zA-Z0-9_-]+_executor):")
parallelism = re.compile("^(\s+)parallelism:\s*([0-9]+)")
exec_class = re.compile("(\s+)#?exec_resource_class:\s*([a-zA-Z]+)")
#top_level = re.compile("$([a-zA-Z0-9_-]+):\s*$")
top_level = re.compile("^([a-zA-Z0-9_-]+):\s*$")
tag = re.compile("^\s+([a-zA-Z0-9_-]+):\s*$")

types = {
        'midres': {
            'j11_par_executor':                         {'klass': 'medium', 'size': 50},
            'j11_repeated_dtest_executor':              {'klass': 'large', 'size': 25},
            'j11_repeated_utest_executor':              {'klass': 'large', 'size': 25},
            'j11_small_executor':                       {'klass': 'medium'},
            'j11_small_par_executor':                   {'klass': 'large', 'size': 5},

            'j8_medium_par_executor':                   {'klass': 'large', 'size': 4},
            'j8_par_executor':                          {'klass': 'large', 'size': 25},
            'j8_repeated_dtest_executor':               {'klass': 'large', 'size': 25},
            'j8_repeated_jvm_upgrade_dtest_executor':   {'klass': 'large', 'size': 25},
            'j8_repeated_upgrade_dtest_executor':       {'klass': 'xlarge', 'size': 25},
            'j8_repeated_utest_executor':               {'klass': 'large', 'size': 25},
            'j8_seq_executor':                          {'klass': 'large'},
            'j8_small_par_executor':                    {'klass': 'large', 'size': 5},
        },
        'highers': {
            'j11_par_executor':                         {'klass': 'xlarge', 'size': 100},
            'j11_repeated_dtest_executor':              {'klass': 'xlarge', 'size': 100},
            'j11_repeated_utest_executor':              {'klass': 'xlarge', 'size': 100},
            'j11_small_executor':                       {'klass': 'medium'},
            'j11_small_par_executor':                   {'klass': 'xlarge', 'size': 5},

            'j8_medium_par_executor':                   {'klass': 'xlarge', 'size': 2},
            'j8_par_executor':                          {'klass': 'xlarge', 'size': 100},
            'j8_repeated_dtest_executor':               {'klass': 'xlarge', 'size': 100},
            'j8_repeated_jvm_upgrade_dtest_executor':   {'klass': 'xlarge', 'size': 100},
            'j8_repeated_upgrade_dtest_executor':       {'klass': 'xlarge', 'size': 100},
            'j8_repeated_utest_executor':               {'klass': 'xlarge', 'size': 100},
            'j8_seq_executor':                          {'klass': 'xlarge'},
            'j8_small_par_executor':                    {'klass': 'xlarge', 'size': 5},
        },
}

def main():
    parser = argparse.ArgumentParser(description='Circle CI Resource Generator')
    parser.add_argument('target', type=str, help='HIGHER,MIDRES')
    target = types[parser.parse_args().target.lower()]

    name = None
    spec = None
    with open('config-2_1.yml', 'r') as r:
        context = 'root'
        for line in r.read().splitlines():
            m = top_level.match(line)
            if m:
                context = m.group(1)
            if context == 'jobs':
                m = tag.match(line)
                if m:

            print(f'context: {context}')
            ## this is top level, includes executors and workflows anchors
            #m = executors.match(line)
            #if m:
            #    name = m.group(1)
            #    spec = target.get(name)
            #elif name and spec:
            #    m = exec_class.match(line)
            #    if m and 'klass' in spec:
            #        spacing = m.group(1)
            #        klass = spec['klass']
            #        line=f'{spacing}exec_resource_class: {klass}'
            #    m = parallelism.match(line)
            #    if m and 'size' in spec:
            #        spacing = m.group(1)
            #        size = spec['size']
            #        line=f'{spacing}parallelism: {size}'
            #        name, spec = None, None
            #elif not line:
            #    name, spec = None, None
            #print(line)

if __name__ == "__main__":
    main()
