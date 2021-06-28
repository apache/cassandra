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

"""
A script to convert cassandra.yaml into Asciidoc for
the online documentation.

Usage:

YAML_INPUT=conf/cassandra.yaml
YAML_OUTPUT=modules/cassandra/pages/configuration/cass_yaml_file.adoc

    convert_yaml_to_adoc.py $YAML_INPUT $YAML_OUTPUT
"""

import sys
import re

# Detects options, whether commented or uncommented.
# Group 1 will be non-empty if the option is commented out.
# Group 2 will contain the option name.
# Group 3 will contain the default value, if one exists.
option_re = re.compile(r"^(# ?)?([a-z0-9_]+): ?([^/].*)")

# Detects normal comment lines.
commented_re = re.compile(r"^# ?(.*)")

# A set of option names that have complex values (i.e. lists or dicts).
# This list is hardcoded because there did not seem to be another
# good way to reliably detect this case, especially considering
# that these can be commented out (making it useless to use a yaml parser).
COMPLEX_OPTIONS = (
    'seed_provider',
    'data_file_directories',
    'commitlog_compression',
    'hints_compression',
    'server_encryption_options',
    'client_encryption_options',
    'transparent_data_encryption_options',
    'hinted_handoff_disabled_datacenters'
)

def convert(yaml_file, dest_file):
    with open(yaml_file, 'r') as f:
        # Trim off the boilerplate header
        lines = f.readlines()[7:]

    with open(dest_file, 'w') as outfile:
        outfile.write("= cassandra.yaml file configuration\n")

        # since comments preceed an option, this holds all of the comment
        # lines we've seen since the last option
        comments_since_last_option = []
        line_iter = iter(lines)
        while True:
            try:
                line = next(line_iter)
            except StopIteration:
                break

            match = option_re.match(line)
            if match:
                option_name = match.group(2)
                is_commented = bool(match.group(1))

                is_complex = option_name in COMPLEX_OPTIONS
                complex_option = read_complex_option(line_iter) if is_complex else None

                write_section_header(option_name, outfile)
                write_comments(comments_since_last_option, is_commented, outfile)
                if is_complex:
                    write_complex_option(complex_option, outfile)
                else:
                    maybe_write_default_value(match, outfile)
                comments_since_last_option = []
            else:
                comment_match = commented_re.match(line)
                if comment_match:
                    comments_since_last_option.append(comment_match.group(1))
                elif line == "\n":
                    comments_since_last_option.append('')


def write_section_header(option_name, outfile):
    outfile.write("\n")
    outfile.write("== `%s`\n\n" % (option_name,))


def write_comments(comment_lines, is_commented, outfile):
    if is_commented:
        outfile.write("*This option is commented out by default.*\n")

    for comment in comment_lines:
        if "SAFETY THRESHOLDS" not in comment_lines:
            outfile.write(comment + "\n")


def maybe_write_default_value(option_match, outfile):
    default_value = option_match.group(3)
    if default_value and default_value != "\n":
        outfile.write("\n_Default Value:_ %s\n" % (default_value,))


def read_complex_option(line_iter):
    option_lines = []
    try:
        while True:
            line = next(line_iter)
            if line == '\n':
                return option_lines
            else:
                option_lines.append(line)
    except StopIteration:
        return option_lines


def write_complex_option(lines, outfile):
    outfile.write("\n_Default Value (complex option)_:\n\n....\n")
    for line in lines:
        outfile.write((" " * 4) + line)
    outfile.write("....\n")


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: %s <yaml source file> <adoc dest file>" % (sys.argv[0],)
        sys.exit(1)

    yaml_file = sys.argv[1]
    dest_file = sys.argv[2]
    convert(yaml_file, dest_file)
