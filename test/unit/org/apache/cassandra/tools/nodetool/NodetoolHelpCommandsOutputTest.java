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

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.github.difflib.DiffUtils;
import com.github.difflib.patch.AbstractDelta;
import com.github.difflib.patch.Patch;
import com.google.common.collect.Streams;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.ToolRunner;

import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class NodetoolHelpCommandsOutputTest extends CQLTester
{
    private static final Map<String, ToolHandler> runnersMap = Map.of(
        "shell", ToolRunner::invokeNodetool,
        "injvm", ToolRunner::invokeNodetoolInJvm);

    public static final String COMMAND_FULL_NAME_SEPARATOR = "$";
    private static final List<String> COMMANDS = NodeTool.getCommandsWithoutRoot(COMMAND_FULL_NAME_SEPARATOR);
    private static final String NODETOOL_COMMAND_HELP_FILE_PATTERN = "nodetool/help/%s";

    private static final Pattern SPLIT_PATTERN = Pattern.compile('\\' + COMMAND_FULL_NAME_SEPARATOR);
    private static final String[] EMPTY_ARGS = new String[0];
    private static final String COMMAND_WITHOUT_ARGS = "nodetool";

    @Parameterized.Parameter
    public String runner;

    @Parameterized.Parameter(1)
    public String command;

    @Parameterized.Parameters(name = "runner={0}, command={1}")
    public static Collection<Object[]> data()
    {
        List<Object[]> res = new ArrayList<>();
        for (String tool : runnersMap.keySet())
        {
            for (String command : COMMANDS)
                res.add(new Object[]{ tool, command });
            // add a special case for the help command with no arguments
            res.add(new Object[]{ tool, COMMAND_WITHOUT_ARGS });
        }
        return res;
    }

    @Test
    public void testCompareCommandHelpOutputBetweenTools() throws Exception
    {
        compareCommandHelpOutput(command);
    }

    private void compareCommandHelpOutput(String commandName) throws Exception
    {
        Assume.assumeFalse("Skipping nodetool-injvmv2 nodetool during the migration period",
                           COMMAND_WITHOUT_ARGS.equals(commandName) && runner.equals("injvmv2"));

        List<String> targetLines = sliceStdout(invokeNodetool(commandName.equals(COMMAND_WITHOUT_ARGS) ? EMPTY_ARGS :
                                                              Streams.concat(Stream.of("help"), Stream.of(SPLIT_PATTERN.split(commandName)))
                                                                     .toArray(String[]::new)));
        List<String> origLines = readCommandLines(String.format(NODETOOL_COMMAND_HELP_FILE_PATTERN, commandName));

        String diff = computeDiff(targetLines, origLines);
        assertTrue(printFormattedDiffsMessage(origLines, targetLines, commandName, diff),
                   StringUtils.isBlank(diff));
    }

    public ToolRunner.ToolResult invokeNodetool(String... args)
    {
        return runnersMap.get(runner).execute(args);
    }

    public static List<String> sliceStdout(ToolRunner.ToolResult result)
    {
        result.assertOnCleanExit();
        return Arrays.asList(result.getStdout().trim().split("\\R"));
    }

    protected static String printFormattedDiffsMessage(List<String> stdoutOrig,
                                                       List<String> stdoutNew,
                                                       String commandName,
                                                       String diff)
    {
        return '\n' + ">> file_content <<" + '\n' +
               printFormattedNodeToolOutput(stdoutOrig) +
               '\n' + ">> command_output <<" +
               '\n' + printFormattedNodeToolOutput(stdoutNew) +
               '\n' + " difference for \"" + commandName + "\":" + diff + '\n' +
               "The difference between the original and the new output is shown above. Make sure the " +
               "changes are expected and update the test data if necessary. Use NodetoolHelpGenerator class.";
    }

    protected static String printFormattedNodeToolOutput(List<String> output)
    {
        StringBuilder sb = new StringBuilder();
        DecimalFormat df = new DecimalFormat("000");
        for(int i = 0; i < output.size(); i++)
        {
            sb.append(df.format(i)).append(':').append(output.get(i));
            if(i < output.size() - 1)
                sb.append('\n');
        }
        return sb.toString();
    }

    protected static String computeDiff(List<String> original, List<String> revised) {
        Patch<String> patch = DiffUtils.diff(original, revised);
        List<String> diffLines = new ArrayList<>();

        for (AbstractDelta<String> delta : patch.getDeltas()) {
            for (String line : delta.getSource().getLines()) {
                diffLines.add(delta.getType().toString().toLowerCase() + " command: " + line);
            }
            for (String line : delta.getTarget().getLines()) {
                diffLines.add(delta.getType().toString().toLowerCase() + " srcfile: " + line);
            }
        }

        return '\n' + String.join("\n", diffLines);
    }

    protected static List<String> readCommandLines(String resource) throws Exception
    {
        List<String> lines = new ArrayList<>();
        URL url = NodetoolHelpCommandsOutputTest.class.getClassLoader().getResource(resource);
        if (url == null)
        {
            logger.error("Command test output not found: {}", resource);
            return Collections.singletonList("Command test output not found: " + resource);
        }

        try (Stream<String> stream = Files.lines(Paths.get(url.toURI())))
        {
            stream.forEach(lines::add);
        }
        return lines;
    }

    public interface ToolHandler
    {
        ToolRunner.ToolResult execute(String... args);
        default ToolRunner.ToolResult execute(List<String> args) { return execute(args.toArray(new String[0])); }
    }
}
