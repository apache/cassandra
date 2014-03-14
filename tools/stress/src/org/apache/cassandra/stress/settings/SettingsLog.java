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
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SettingsLog implements Serializable
{
    public static enum Level
    {
        MINIMAL, NORMAL, VERBOSE
    }

    public final boolean noSummary;
    public final File file;
    public final int intervalMillis;
    public final Level level;

    public SettingsLog(Options options)
    {
        noSummary = options.noSummmary.setByUser();

        if (options.outputFile.setByUser())
            file = new File(options.outputFile.value());
        else
            file = null;

        String interval = options.interval.value();
        if (interval.endsWith("ms"))
            intervalMillis = Integer.parseInt(interval.substring(0, interval.length() - 2));
        else if (interval.endsWith("s"))
            intervalMillis = 1000 * Integer.parseInt(interval.substring(0, interval.length() - 1));
        else
            intervalMillis = 1000 * Integer.parseInt(interval);
        if (intervalMillis <= 0)
            throw new IllegalArgumentException("Log interval must be greater than zero");
        level = Level.valueOf(options.level.value().toUpperCase());
    }

    public PrintStream getOutput() throws FileNotFoundException
    {
        return file == null ? new PrintStream(System.out) : new PrintStream(file);
    }

    // Option Declarations

    public static final class Options extends GroupedOptions
    {
        final OptionSimple noSummmary = new OptionSimple("no-summary", "", null, "Disable printing of aggregate statistics at the end of a test", false);
        final OptionSimple outputFile = new OptionSimple("file=", ".*", null, "Log to a file", false);
        final OptionSimple interval = new OptionSimple("interval=", "[0-9]+(ms|s|)", "1s", "Log progress every <value> seconds or milliseconds", false);
        final OptionSimple level = new OptionSimple("level=", "(minimal|normal|verbose)", "normal", "Logging level (minimal, normal or verbose)", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(level, noSummmary, outputFile, interval);
        }
    }

    // CLI Utility Methods

    public static SettingsLog get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-log");
        if (params == null)
            return new SettingsLog(new Options());

        GroupedOptions options = GroupedOptions.select(params, new Options());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -log options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsLog((Options) options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-log", new Options());
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
