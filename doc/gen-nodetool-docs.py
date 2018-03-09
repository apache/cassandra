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

import os
import re
import subprocess
from subprocess import PIPE, Popen

nodetool = "../bin/nodetool"
outdir = "source/tools/nodetool"
helpfilename = outdir + "/nodetool.txt"
command_re = re.compile("(    )([_a-z]+)")
commandRSTContent = ".. _nodetool_{0}:\n\n{0}\n-------\n\nUsage\n---------\n\n.. include:: {0}.txt\n  :literal:\n\n"

# create the documentation directory
if not os.path.exists(outdir):
    os.makedirs(outdir)

# create the base help file to use for discovering the commands
def createHelpfile():
    with open(helpfilename, "w+") as file:
        subprocess.call([nodetool, "help"], stdout=file)

# for a given command, create the help file and an RST file to contain it
def createRST(command):
    if command:
        cmdName = command.group(0).strip()
        cmdFilename = outdir + "/" + cmdName + ".txt"
        rstFilename = outdir + "/" + cmdName + ".rst"
        with open(cmdFilename, "w+") as cmdFile:
            proc = Popen([nodetool, "help", cmdName], stdin=PIPE, stdout=PIPE)
            (out, err) = proc.communicate()
            cmdFile.write(out)
        with open(rstFilename, "w+") as rstFile:
            rstFile.write(commandRSTContent.format(cmdName))

# create base file
createHelpfile()

# create the main usage page
with open(outdir + "/nodetool.rst", "w+") as output:
    with open(helpfilename, "r+") as helpfile:
        output.write(".. _nodetool\n\nNodetool\n-------\n\nUsage\n---------\n\n")
        for commandLine in helpfile:
            command = command_re.sub(r'\n\1:doc:`\2` - ',commandLine)
            output.write(command)

# create the command usage pages
with open(helpfilename, "rw+") as helpfile:
    for commandLine in helpfile:
        command = command_re.match(commandLine)
        createRST(command)
