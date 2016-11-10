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

public class SettingsSamples implements Serializable
{

    public final int liveCount;
    public final int historyCount;
    public final int reportCount;

    public SettingsSamples(SampleOptions options)
    {
        liveCount = (int) OptionDistribution.parseLong(options.liveCount.value());
        historyCount = (int) OptionDistribution.parseLong(options.historyCount.value());
        reportCount = (int) OptionDistribution.parseLong(options.reportCount.value());
    }

    // Option Declarations

    private static final class SampleOptions extends GroupedOptions
    {
        final OptionSimple historyCount = new OptionSimple("history=", "[0-9]+[bmk]?", "50K", "The number of samples to save across the whole run", false);
        final OptionSimple liveCount = new OptionSimple("live=", "[0-9]+[bmk]?", "1M", "The number of samples to save between reports", false);
        final OptionSimple reportCount = new OptionSimple("report=", "[0-9]+[bmk]?", "100K", "The maximum number of samples to use when building a report", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(historyCount, liveCount, reportCount);
        }
    }

    // CLI Utility Methods

    public static SettingsSamples get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-sample");
        if (params == null)
        {
            return new SettingsSamples(new SampleOptions());
        }
        SampleOptions options = GroupedOptions.select(params, new SampleOptions());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -sample options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsSamples(options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-sample", new SampleOptions());
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

