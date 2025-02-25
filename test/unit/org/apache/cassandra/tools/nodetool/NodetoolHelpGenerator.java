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

import java.io.File; //checkstyle: permit this import
import java.io.FileWriter; //checkstyle: permit this import
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.tools.ToolRunner;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Generates files with commands help output for all available nodetool commands. The {@code $} character is used as
 * a separator between command hierarchy levels in the file names (e.g. {@code "info$threads"}) due to the fact that
 * a command name can contain special characters like {@code -} or {@code _}.
 * <p>
 * The generator calls the {@code ./nodetool help} command to get the list of available commands and their descriptions,
 * in order to generate the latest help output for each command be sure to run the generator after the jars are built
 * (e.g. {@code ant jar}).
 */
public class NodetoolHelpGenerator
{
    private static final Logger logger = LoggerFactory.getLogger(NodetoolHelpGenerator.class);
    private static final Map<String, String> ENV = ImmutableMap.of("JAVA_HOME", CassandraRelevantProperties.JAVA_HOME.getString());
    private static final String NODETOOL_COMMAND_HELP_WRITE_DIR = "test/resources/nodetool/help/";
    private static final String IGNORE_LINE = "        With no arguments,";
    private static final String NODETOOL_COMMAND_LIST_START_AFTER = "The most commonly used nodetool commands are:";
    private static final String NODETOOL_SUBCOMMAND_LIST_START_AFTER = "COMMANDS";
    private static final Pattern NODETOOL_COMMAND_DESCRIPTION_SPACES = Pattern.compile("^ {4}(\\S+)");
    private static final Pattern NODETOOL_SUBCOMMAND_DESCRIPTION_SPACES = Pattern.compile("^ {8}(\\S+)");
    private static final String COMMAND_FULL_NAME_SEPARATOR = "$";

    /**
     * Main method to generate help files for all nodetool commands to the {@code test/resources/nodetool/help/}.
     * <p>
     * For example, the {@code nodetool help bootstrap resume} help output results in a file
     * {@code test/resources/nodetool/help/bootstrap$resume}, where the {@code $} character
     * is used as a separator for the subcommand. The arguments are passed as a list of commands
     * to generate help files for. For example, {@code bootstrap resume} is passed.
     * <p>
     * By default, the files are written to {@code test/resources/nodetool/help/}.
     */
    public static void main(String[] args)
    {
        List<String> commands = new ArrayList<>(List.of(args));
//        commands.add("assassinate");

        if (commands.isEmpty())
            new NodetoolHelpGenerator().writeCommandsHelpOutput();
        else
            new NodetoolHelpGenerator().writer(commands);
    }

    public void writeCommandsHelpOutput()
    {
        List<String> roots = find(() -> ToolRunner.invoke(ENV, newArrayList("bin/nodetool", "help")),
                                  NODETOOL_COMMAND_LIST_START_AFTER, NODETOOL_COMMAND_DESCRIPTION_SPACES);

        for (String command : roots)
            writeToFileRecursively(newArrayList(command), this::writer);
    }

    private void writeToFileRecursively(List<String> hierarchy, Consumer<List<String>> writer)
    {
        List<String> subcommands = find(() -> ToolRunner.invoke(ENV, Lists.asList("bin/nodetool",
                                                                                  "help",
                                                                                  hierarchy.toArray(new String[0]))),
                                        NODETOOL_SUBCOMMAND_LIST_START_AFTER, NODETOOL_SUBCOMMAND_DESCRIPTION_SPACES);
        for (String subcommand : subcommands)
        {
            List<String> subhierarchy = new ArrayList<>(hierarchy);
            subhierarchy.add(subcommand);
            writeToFileRecursively(subhierarchy, writer);
        }

        writer.accept(hierarchy);
    }

    public void writer(List<String> fullCommand)
    {
        ToolRunner.ToolResult result = ToolRunner.invoke(ENV, Lists.asList("bin/nodetool", "help",
                                                                           fullCommand.toArray(new String[0])));
        result.assertOnCleanExit();

        try
        {
            File commandHelpOut = new File(NODETOOL_COMMAND_HELP_WRITE_DIR, String.join(COMMAND_FULL_NAME_SEPARATOR, fullCommand)); //checkstyle: permit this instantiation
            boolean created = commandHelpOut.getParentFile().mkdirs();
            if (created)
                logger.debug("Created directory: {}", commandHelpOut.getParentFile().getAbsolutePath());

            boolean success = commandHelpOut.createNewFile();
            if (success)
                logger.debug("Created file: {}", commandHelpOut.getAbsolutePath());

            try (FileWriter fw = new FileWriter(commandHelpOut))
            {
                fw.write(result.getStdout().trim());
                fw.write("\n");
            }
            logger.info("The help is written for '{}' to '{}'", fullCommand, commandHelpOut.getAbsolutePath());
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error creating file", e);
        }
    }

    private static List<String> find(Supplier<ToolRunner.ToolResult> cmdResult, String afterLine, Pattern commandPattern)
    {
        ToolRunner.ToolResult result = cmdResult.get();
        result.assertOnCleanExit();
        String[] lines = result.getStdout().split("\n");
        List<String> commands = new ArrayList<>();
        boolean start = false;
        for (String line : lines)
        {
            if (line.contains(IGNORE_LINE))
                continue;

            if (line.contains(afterLine))
            {
                start = true;
                continue;
            }

            if (start)
            {
                Matcher matcher = commandPattern.matcher(line);
                if (matcher.find())
                    commands.add(matcher.group(1));
            }
        }
        return commands;
    }
}
