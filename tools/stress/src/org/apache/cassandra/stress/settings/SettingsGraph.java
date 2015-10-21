package org.apache.cassandra.stress.settings;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class SettingsGraph implements Serializable
{
    public final String file;
    public final String revision;
    public final String title;
    public final String operation;
    public final File temporaryLogFile;

    public SettingsGraph(GraphOptions options, SettingsCommand stressCommand)
    {
        file = options.file.value();
        revision = options.revision.value();
        title = options.revision.value() == null
            ? "cassandra-stress - " + new SimpleDateFormat("yyyy-mm-dd hh:mm:ss").format(new Date())
            : options.title.value();

        operation = options.operation.value() == null
            ? stressCommand.type.name()
            : options.operation.value();

        if (inGraphMode())
        {
            try
            {
                temporaryLogFile = File.createTempFile("cassandra-stress", ".log");
            }
            catch (IOException e)
            {
                throw new RuntimeException("Cannot open temporary file");
            }
        }
        else
        {
            temporaryLogFile = null;
        }
    }

    public boolean inGraphMode()
    {
        return this.file == null ? false : true;
    }

    // Option Declarations
    private static final class GraphOptions extends GroupedOptions
    {
        final OptionSimple file = new OptionSimple("file=", ".*", null, "HTML file to create or append to", true);
        final OptionSimple revision = new OptionSimple("revision=", ".*", "unknown", "Unique name to assign to the current configuration being stressed", false);
        final OptionSimple title = new OptionSimple("title=", ".*", null, "Title for chart (current date by default)", false);
        final OptionSimple operation = new OptionSimple("op=", ".*", null, "Alternative name for current operation (stress op name used by default)", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(file, revision, title, operation);
        }
    }

    // CLI Utility Methods
    public static SettingsGraph get(Map<String, String[]> clArgs, SettingsCommand stressCommand)
    {
        String[] params = clArgs.remove("-graph");
        if (params == null)
        {
            return new SettingsGraph(new GraphOptions(), stressCommand);
        }
        GraphOptions options = GroupedOptions.select(params, new GraphOptions());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -graph options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsGraph(options, stressCommand);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-graph", new GraphOptions());
    }

    public static Runnable helpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                printHelp();
            }
        };
    }
}

