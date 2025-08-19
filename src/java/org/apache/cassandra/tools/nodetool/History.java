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

import java.util.List;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "history", description = "Print previously executed nodetool commands")
public class History extends AbstractCommand
{
    @Option(paramLabel = "commands",
    names = { "-n", "--num", "--number-of-commands" },
    description = "Number of commands to print, defaults to 1000.")
    public int commands = 1000;

    @Override
    protected void execute(NodeProbe probe)
    {
        if (commands < 1)
            throw new IllegalArgumentException("Number of commands to display has to be at least 1.");

        File historyFile = getHistoryFile();
        validateHistoryFile(historyFile);

        for (String line : commandsToPrint(historyFile))
            output.out.println(line);
    }

    @Override
    protected boolean shouldConnect() throws CommandLine.ExecutionException
    {
        return false;
    }

    File getHistoryFile()
    {
        return NodeTool.getHistoryFile();
    }

    List<String> commandsToPrint(File file)
    {
        List<String> historyCommands = FileUtils.readLines(file);
        int size = historyCommands.size();
        List<String> commandLines;

        if (commands > size)
            commandLines = historyCommands;
        else
            commandLines = historyCommands.subList(size - commands, size);

        return commandLines;
    }

    /**
     * Nodetool is appending command to history file before it is executed so us checking on its
     * existence and validating it is not technically necessary however nodetool is also swallowing
     * all errors when it was not succesful in appending to the history file so better to check here in that case.
     *
     * @param historyFile file to check that it is actually a file which exists and it is readable
     */
    void validateHistoryFile(File historyFile)
    {
        if (!historyFile.exists())
            throw new IllegalStateException(String.format("History file %s does not exist.%n", historyFile.absolutePath()));

        if (!historyFile.isFile())
            throw new IllegalStateException(String.format("History file %s is not a file.%n", historyFile.absolutePath()));

        if (!historyFile.isReadable())
            throw new IllegalStateException(String.format("History file %s is not readable.%n", historyFile.absolutePath()));
    }
}
