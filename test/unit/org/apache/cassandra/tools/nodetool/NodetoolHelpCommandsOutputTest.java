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
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertTrue;

public class NodetoolHelpCommandsOutputTest extends NodetoolRunnerTester
{
    private static final String NODETOOL_COMMAND_HELP_FILE_PATTERN = "nodetool/help/%s";
    private static final List<String> COMMANDS = fetchCommandsNodeTool(sliceStdout(invokeNodetoolV2InJvm("help")), c -> !c.equals("help"));

    @Parameterized.Parameter(1)
    public String command;

    @Parameterized.Parameters(name = "runner={0}, command={1}")
    public static Collection<Object[]> data()
    {
        List<Object[]> res = new ArrayList<>();
        for (String tool : runnersMap.keySet())
            for (String command : COMMANDS)
                res.add(new Object[]{ tool, command });
        return res;
    }

    @Test
    public void testCompareCommandHelpOutputBetweenTools() throws Exception
    {
        compareCommandHelpOutput(command);
    }

    private void compareCommandHelpOutput(String commandName) throws Exception
    {
        List<String> origLines = readCommandLines(String.format(NODETOOL_COMMAND_HELP_FILE_PATTERN, commandName));
        List<String> targetLines = sliceStdout(invokeNodetool("help", commandName));
        String diff = computeDiff(targetLines, origLines);
        assertTrue(printFormattedDiffsMessage(origLines, targetLines, commandName, diff),
                   StringUtils.isBlank(diff));
    }
}
