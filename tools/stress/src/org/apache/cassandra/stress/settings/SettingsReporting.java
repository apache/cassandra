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

package org.apache.cassandra.stress.settings;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.stress.util.ResultLogger;

public class SettingsReporting implements Serializable
{
    public final int outputFrequency;
    private final String outputFrequencyString;
    public final int headerFrequency;
    private final String headerFrequencyString;

    public SettingsReporting(SettingsReporting.Options reporting)
    {
        if (reporting.headerFrequency.present())
        {
            headerFrequencyString = reporting.headerFrequency.value();
            headerFrequency = new DurationSpec.IntSecondsBound(headerFrequencyString).toSeconds();
        }
        else
        {
            headerFrequency = 0;
            headerFrequencyString = "*not set*";
        }

        if (reporting.outputFrequency.present())
        {
            outputFrequencyString = reporting.outputFrequency.value();
            outputFrequency = new DurationSpec.IntSecondsBound(outputFrequencyString).toSeconds();
        }
        else
        {
            outputFrequency = 0;
            outputFrequencyString = "*not set*";
        }
    }

    // Option Declarations

    public static final class Options extends GroupedOptions
    {
        final OptionSimple outputFrequency = new OptionSimple("output-frequency=",
                                                              ".*",
                                                              "1s",
                                                              "Frequency each line of output will be printed out when running a stress test, defaults to '1s'.",
                                                              false);

        final OptionSimple headerFrequency = new OptionSimple("header-frequency=",
                                                              ".*",
                                                              null,
                                                              "Frequency the header for the statistics will be printed out. " +
                                                              "If not specified, the header will be printed at the beginning of the test only.",
                                                              false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(outputFrequency, headerFrequency);
        }
    }

    public static SettingsReporting get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-reporting");
        if (params == null)
            return new SettingsReporting(new SettingsReporting.Options());

        GroupedOptions options = GroupedOptions.select(params, new SettingsReporting.Options());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -reporting options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsReporting((SettingsReporting.Options) options);
    }

    public void printSettings(ResultLogger out)
    {
        out.printf("  Output frequency: %s%n", outputFrequencyString);
        out.printf("  Header frequency: %s%n", headerFrequencyString);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-reporting", new SettingsReporting.Options());
    }

    public static Runnable helpPrinter()
    {
        return SettingsReporting::printHelp;
    }
}
