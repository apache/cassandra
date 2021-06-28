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
A script to use nodetool to generate documentation for nodetool
"""
from __future__ import print_function

import os
import re
import sys
import subprocess
from subprocess import PIPE
from subprocess import Popen

if(os.environ.get("SKIP_NODETOOL") == "1"):
    sys.exit(0)


nodetool = "../bin/nodetool"
outdir = "modules/cassandra/pages/tools/nodetool"
examplesdir = "modules/cassandra/examples/TEXT/NODETOOL"
helpfilename = outdir + "/nodetool.txt"
command_re = re.compile("(    )([_a-z]+)")
commandADOCContent = "== {0}\n\n== Usage\n[source,plaintext]\n----\ninclude::example$TEXT/NODETOOL/{0}.txt[]\n----\n"

# create the documentation directory
if not os.path.exists(outdir):
    os.makedirs(outdir)

# create the base help file to use for discovering the commands
def create_help_file():
    with open(helpfilename, "w+") as output_file:
        try:
            subprocess.check_call([nodetool, "help"], stdout=output_file)
        except subprocess.CalledProcessError as cpe:
            print(
                'ERROR: Nodetool failed to run, you likely need to build '
                'cassandra using ant jar from the top level directory'
            )
            raise cpe

# for a given command, create the help file and an ADOC file to contain it
def create_adoc(command):
    if command:
        cmdName = command.group(0).strip()
        cmdFilename = examplesdir + "/" + cmdName + ".txt"
        adocFilename = outdir + "/" + cmdName + ".adoc"
        with open(cmdFilename, "wb+") as cmdFile:
            proc = Popen([nodetool, "help", cmdName], stdin=PIPE, stdout=PIPE)
            (out, err) = proc.communicate()
            cmdFile.write(out)
        with open(adocFilename, "w+") as adocFile:
            adocFile.write(commandADOCContent.format(cmdName,cmdName,cmdName))

# create base file
create_help_file()

# create the main usage page
with open(outdir + "/nodetool.adoc", "w+") as output:
    with open(helpfilename, "r+") as helpfile:
        output.write("== Nodetool\n\n== Usage\n\n")
        for commandLine in helpfile:
            command = command_re.sub(r'\nxref:tools/nodetool/\2.adoc[\2] - ',commandLine)
            output.write(command)

# create the command usage pages
with open(helpfilename, "r+") as helpfile:
    for commandLine in helpfile:
        command = command_re.match(commandLine)
        create_adoc(command)
