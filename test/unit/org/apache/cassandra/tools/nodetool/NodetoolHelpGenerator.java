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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.Output;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Generates files with commands help output for all available nodetool commands. The {@code $} character is used as
 * a separator between command hierarchy levels in the file names (e.g. {@code "info$threads"}) due to the fact that
 * a command name can contain special characters like {@code -} or {@code _}.
 * <p>
 * The help output is produced in-process (single JVM); be sure to run the generator after the jars are built
 * (e.g. {@code ant jar}). Pass {@code --dir <path>} to override the output directory and {@code --txt} to append
 * a {@code .txt} extension (used by {@code doc/scripts/gen-nodetool-docs.py}).
 */
public class NodetoolHelpGenerator
{
    private static final Logger logger = LoggerFactory.getLogger(NodetoolHelpGenerator.class);
    private static final String NODETOOL_COMMAND_HELP_WRITE_DIR = "test/resources/nodetool/help/";
    private static final String ROOT_COMMAND_FILE = "nodetool";
    private static final String IGNORE_LINE = "        With no arguments,";
    private static final String NODETOOL_COMMAND_LIST_START_AFTER = "The most commonly used nodetool commands are:";
    private static final String NODETOOL_SUBCOMMAND_LIST_START_AFTER = "COMMANDS";
    private static final Pattern NODETOOL_COMMAND_DESCRIPTION_SPACES = Pattern.compile("^ {4}(\\S+)");
    private static final Pattern NODETOOL_SUBCOMMAND_DESCRIPTION_SPACES = Pattern.compile("^ {8}(\\S+)");
    private static final String COMMAND_FULL_NAME_SEPARATOR = "$";
    private static final INodeProbeFactory NO_PROBE = new INodeProbeFactory()
    {
        public NodeProbe create(String host, int port) { throw new UnsupportedOperationException(); }
        public NodeProbe create(String host, int port, String user, String pass) { throw new UnsupportedOperationException(); }
    };

    private final String writeDir;
    private final String extension;

    public NodetoolHelpGenerator(String writeDir, String extension)
    {
        this.writeDir = writeDir;
        this.extension = extension;
    }

    /**
     * Main method to generate help files for all nodetool commands to the {@code test/resources/nodetool/help/}
     * (or to the directory specified via {@code --dir}).
     * <p>
     * For example, the {@code nodetool help bootstrap resume} help output results in a file
     * {@code test/resources/nodetool/help/bootstrap$resume}, where the {@code $} character
     * is used as a separator for the subcommand. Trailing positional arguments generate the help
     * for a single command only (e.g. {@code bootstrap resume}).
     */
    public static void main(String[] args)
    {
        String dir = NODETOOL_COMMAND_HELP_WRITE_DIR;
        String extension = "";
        List<String> commands = new ArrayList<>();
        for (int i = 0; i < args.length; i++)
        {
            switch (args[i])
            {
                case "--dir":
                    if (++i >= args.length)
                        throw new IllegalArgumentException("--dir requires a path");
                    dir = args[i];
                    break;
                case "--txt":
                    extension = ".txt";
                    break;
                default: commands.add(args[i]);
            }
        }

        NodetoolHelpGenerator generator = new NodetoolHelpGenerator(dir, extension);
        if (commands.isEmpty())
            generator.writeCommandsHelpOutput();
        else
            generator.writer(commands);
    }

    public void writeCommandsHelpOutput()
    {
        List<String> roots = find(() -> help(new ArrayList<>()),
                                  NODETOOL_COMMAND_LIST_START_AFTER, NODETOOL_COMMAND_DESCRIPTION_SPACES);
        writer(new ArrayList<>());

        for (String command : roots)
            writeToFileRecursively(newArrayList(command), this::writer);
    }

    private void writeToFileRecursively(List<String> hierarchy, Consumer<List<String>> writer)
    {
        List<String> subcommands = find(() -> help(hierarchy),
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
        String stdout = help(fullCommand);
        String name = fullCommand.isEmpty() ? ROOT_COMMAND_FILE : String.join(COMMAND_FULL_NAME_SEPARATOR, fullCommand);

        try
        {
            File commandHelpOut = new File(writeDir, name + extension); //checkstyle: permit this instantiation
            boolean created = commandHelpOut.getParentFile().mkdirs();
            if (created)
                logger.debug("Created directory: {}", commandHelpOut.getParentFile().getAbsolutePath());

            boolean success = commandHelpOut.createNewFile();
            if (success)
                logger.debug("Created file: {}", commandHelpOut.getAbsolutePath());

            try (FileWriter fw = new FileWriter(commandHelpOut))
            {
                fw.write(stdout.trim());
                fw.write("\n");
            }
            logger.info("The help is written for '{}' to '{}'", fullCommand, commandHelpOut.getAbsolutePath());
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error creating file", e);
        }
    }

    private static String help(List<String> command)
    {
        List<String> args = new ArrayList<>();
        if (!command.isEmpty())
        {
            args.add("help");
            args.addAll(command);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        PrintStream outStream = new PrintStream(out, true, StandardCharsets.UTF_8);
        PrintStream errStream = new PrintStream(err, true, StandardCharsets.UTF_8);
        int rc = new NodeTool(NO_PROBE, new Output(outStream, errStream)).execute(args.toArray(new String[0]));
        outStream.flush();
        errStream.flush();
        String stderr = err.toString(StandardCharsets.UTF_8);
        if (rc != 0 || !stderr.trim().isEmpty())
            throw new RuntimeException("nodetool help " + String.join(" ", command) + " failed (rc=" + rc + "): "
                                       + stderr);
        return out.toString(StandardCharsets.UTF_8);
    }

    private static List<String> find(Supplier<String> stdout, String afterLine, Pattern commandPattern)
    {
        String[] lines = stdout.get().split("\n");
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
