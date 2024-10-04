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
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.github.difflib.DiffUtils;
import com.github.difflib.patch.AbstractDelta;
import com.github.difflib.patch.Patch;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.NodeToolV2;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.tools.nodetool.CassandraHelpLayout.TOP_LEVEL_COMMAND_HEADING;
import static org.junit.Assert.assertFalse;

@RunWith(Parameterized.class)
public abstract class NodetoolRunnerTester extends CQLTester
{
    public static final Map<String, ToolHandler> runnersMap = Map.of(
        "invokeNodetoolV1InJvm", NodetoolRunnerTester::invokeNodetoolV1InJvm,
        "invokeNodetoolV2InJvm", NodetoolRunnerTester::invokeNodetoolV2InJvm);

    @Parameterized.Parameter
    public String runner;

    @Parameterized.Parameters(name = "runner={0}")
    public static Collection<Object[]> data() {
        List<Object[]> res = new ArrayList<>();
        runnersMap.forEach((k, v) -> res.add(new Object[]{ k }));
        return res;
    }

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        requireNetwork();
        try
        {
            startJMXServer();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    protected ToolRunner.ToolResult invokeNodetool(String... args)
    {
        return runnersMap.get(runner).execute(args);
    }

    public static ToolRunner.ToolResult invokeNodetoolV2InJvm(String... commands)
    {
        return ToolRunner.invokeNodetoolInJvm(NodeToolV2::new, commands);
    }

    public static ToolRunner.ToolResult invokeNodetoolV1InJvm(String... commands)
    {
        return ToolRunner.invokeNodetoolInJvm(NodeTool::new, commands);
    }

    public static List<String> sliceStdout(ToolRunner.ToolResult result)
    {
        return Arrays.asList(result.getStdout().split("\\R"));
    }

    public interface ToolHandler
    {
        ToolRunner.ToolResult execute(String... args);
        default ToolRunner.ToolResult execute(List<String> args) { return execute(args.toArray(new String[0])); }
    }


    protected static String printFormattedDiffsMessage(List<String> stdoutOrig,
                                                     List<String> stdoutNew,
                                                     String commandName,
                                                     String diff)
    {
        return '\n' + ">> command <<" + '\n' +
               printFormattedNodeToolOutput(stdoutOrig) +
               '\n' + ">> srcfile <<" +
               '\n' + printFormattedNodeToolOutput(stdoutNew) +
               '\n' + " difference for \"" + commandName + "\":" + diff;
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

    /**
     * Get the list of commands available in the V2 version of nodetool.
     * @return List of available commands.
     */
    protected static List<String> fetchCommandsNodeTool(List<String> outNodeTool, Predicate<String> filter)
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
        return commands.stream().filter(filter).collect(Collectors.toList());
    }

    protected static List<String> readCommandLines(String resource) throws Exception
    {
        List<String> lines = new ArrayList<>();
        URL url = NodetoolHelpCommandsOutputTest.class.getClassLoader().getResource(resource);
        if (url == null)
            throw new IllegalStateException("Command test output not found: " + resource);
        try (Stream<String> stream = Files.lines(Paths.get(url.toURI())))
        {
            stream.forEach(lines::add);
        }
        return lines;
    }
}
