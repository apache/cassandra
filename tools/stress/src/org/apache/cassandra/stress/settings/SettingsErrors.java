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


import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.stress.util.ResultLogger;

public class SettingsErrors implements Serializable
{

    public final boolean ignore;
    public final int tries;
    public final boolean skipReadValidation;

    public SettingsErrors(Options options)
    {
        ignore = options.ignore.setByUser();
        this.tries = Math.max(1, Integer.parseInt(options.retries.value()) + 1);
        skipReadValidation = options.skipReadValidation.setByUser();
    }

    // Option Declarations

    public static final class Options extends GroupedOptions
    {
        final OptionSimple retries = new OptionSimple("retries=", "[0-9]+", "9", "Number of tries to perform for each operation before failing", false);
        final OptionSimple ignore = new OptionSimple("ignore", "", null, "Do not fail on errors", false);
        final OptionSimple skipReadValidation = new OptionSimple("skip-read-validation", "", null, "Skip read validation and message output", false);
        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(retries, ignore, skipReadValidation);
        }
    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  Ignore: %b%n", ignore);
        out.printf("  Tries: %d%n", tries);
    }


    public static SettingsErrors get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-errors");
        if (params == null)
            return new SettingsErrors(new Options());

        GroupedOptions options = GroupedOptions.select(params, new Options());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -errors options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsErrors((Options) options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-errors", new Options());
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
