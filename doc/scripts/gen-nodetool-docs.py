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
A script to generate the nodetool AsciiDoc pages from the help text files
produced by the 'gen-nodetool-text' Ant target.
"""
from __future__ import print_function

import os
import re
import sys

if(os.environ.get("SKIP_NODETOOL") == "1"):
    sys.exit(0)


outdir = "modules/cassandra/pages/managing/tools/nodetool"
examplesdir = "modules/cassandra/examples/TEXT/NODETOOL"
helpfilename = outdir + "/nodetool.txt"
command_re = re.compile("(    )([_a-z]+)")
commandADOCContent = "= {0}\n\n== Usage\n[source,plaintext]\n----\ninclude::cassandra:example$TEXT/NODETOOL/{0}.txt[]\n----\n"

# create the documentation directory
if not os.path.exists(outdir):
    os.makedirs(outdir)

# create the base help file to use for discovering the commands
def create_help_file():
    rootfile = examplesdir + "/nodetool.txt"
    if not os.path.exists(rootfile):
        sys.exit("ERROR: '%s' is missing. Run 'ant gen-nodetool-text' (or 'ant gen-asciidoc') first." % rootfile)
    with open(rootfile, "r") as f:
        out = f.read()
    with open(helpfilename, "w+") as output_file:
        # Wrap the initial "usage: ..." block
        usage_block = re.search(r'usage:.*?\n\n', out, re.DOTALL | re.MULTILINE)
        if usage_block:
            output_file.write("[source,console]\n----\n")
            output_file.write(usage_block.group(0))
            output_file.write("----\n")
            out = out.replace(usage_block.group(0), '')
        else:
            raise ValueError("No usage block matched in nodetool help output")
        output_file.write(out)

# for a given command, create the ADOC file to contain it
def create_adoc(command):
    if command:
        cmdName = command.group(0).strip()
        adocFilename = outdir + "/" + cmdName + ".adoc"
        with open(adocFilename, "w+") as adocFile:
            adocFile.write(commandADOCContent.format(cmdName,cmdName,cmdName))

# create base file
create_help_file()

# create the main usage page
with open(outdir + "/nodetool.adoc", "w+") as output:
    with open(helpfilename, "r+") as helpfile:
        output.write("= Nodetool\n\n== Usage\n\n")
        for commandLine in helpfile:
            command = command_re.sub(r'\nxref:cassandra:managing/tools/nodetool/\2.adoc[\2] - ',commandLine)
            output.write(command)

# create the command usage pages
with open(helpfilename, "r+") as helpfile:
    for commandLine in helpfile:
        create_adoc(command_re.match(commandLine))
