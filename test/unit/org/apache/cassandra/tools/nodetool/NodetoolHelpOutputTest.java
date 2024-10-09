/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tools.nodetool;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.apache.cassandra.tools.nodetool.NodetoolRunnerTester.computeDiff;
import static org.apache.cassandra.tools.nodetool.NodetoolRunnerTester.invokeNodetoolV1InJvm;
import static org.apache.cassandra.tools.nodetool.NodetoolRunnerTester.invokeNodetoolV2InJvm;
import static org.apache.cassandra.tools.nodetool.NodetoolRunnerTester.printFormattedDiffsMessage;
import static org.apache.cassandra.tools.nodetool.NodetoolRunnerTester.readCommandLines;
import static org.apache.cassandra.tools.nodetool.NodetoolRunnerTester.sliceStdout;
import static org.apache.cassandra.tools.nodetool.layout.CassandraHelpLayout.TOP_LEVEL_COMMAND_HEADING;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NodetoolHelpOutputTest extends CQLTester
{
    private static final String NODETOOL_COMMAND_HELP_FILE = "nodetool/help/nodetool";

    @Test
    public void testCompareHelpCommand() throws Exception
    {
        List<String> outNodeTool = readCommandLines(NODETOOL_COMMAND_HELP_FILE);
        List<String> outNodeToolV1 = sliceStdout(invokeNodetoolV1InJvm("help"));

        String diff = computeDiff(outNodeTool, outNodeToolV1);
        assertTrue(printFormattedDiffsMessage(outNodeTool, outNodeToolV1, "help", diff),
                   StringUtils.isBlank(diff));
    }

    @Test
    public void testBaseCommandOutput() throws Exception
    {
        List<String> commands = fetchCommandsNodeTool(readCommandLines(NODETOOL_COMMAND_HELP_FILE));
        List<String> outNodeToolV2 = sliceStdout(invokeNodetoolV2InJvm("help"));
        assertFalse(outNodeToolV2.isEmpty());

        List<String> commandsV2 = fetchCommandsNodeTool(outNodeToolV2);
        assertTrue(commands.containsAll(commandsV2));
    }

    /**
     * Get the list of commands available in the V2 version of nodetool.
     * @return List of available commands.
     */
    protected static List<String> fetchCommandsNodeTool(List<String> outNodeTool)
    {
        List<String> commands = new ArrayList<>();
        boolean headerFound = false;
        for (String commandOutput : outNodeTool)
        {
            if (headerFound && commandOutput.startsWith("    "))
            {
                if (commandOutput.startsWith(" ", 4))
                    continue;
                commands.add(commandOutput.substring(4, commandOutput.indexOf(' ', 4)));
            }

            headerFound = headerFound || commandOutput.equals(TOP_LEVEL_COMMAND_HEADING);
        }
        // Remove the help command from the list of commands, as it's not applicable.
        assertFalse(commands.isEmpty());
        return commands;
    }
}
