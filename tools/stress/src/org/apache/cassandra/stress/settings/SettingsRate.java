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

public class SettingsRate implements Serializable
{

    public final boolean auto;
    public final int minThreads;
    public final int maxThreads;
    public final int threadCount;
    public final int opRateTargetPerSecond;

    public SettingsRate(ThreadOptions options)
    {
        auto = false;
        threadCount = Integer.parseInt(options.threads.value());
        String rateOpt = options.rate.value();
        opRateTargetPerSecond = Integer.parseInt(rateOpt.substring(0, rateOpt.length() - 2));
        minThreads = -1;
        maxThreads = -1;
    }

    public SettingsRate(AutoOptions auto)
    {
        this.auto = auto.auto.setByUser();
        this.minThreads = Integer.parseInt(auto.minThreads.value());
        this.maxThreads = Integer.parseInt(auto.maxThreads.value());
        this.threadCount = -1;
        this.opRateTargetPerSecond = 0;
    }


    // Option Declarations

    private static final class AutoOptions extends GroupedOptions
    {
        final OptionSimple auto = new OptionSimple("auto", "", null, "stop increasing threads once throughput saturates", false);
        final OptionSimple minThreads = new OptionSimple("threads>=", "[0-9]+", "4", "run at least this many clients concurrently", false);
        final OptionSimple maxThreads = new OptionSimple("threads<=", "[0-9]+", "1000", "run at most this many clients concurrently", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(minThreads, maxThreads, auto);
        }
    }

    private static final class ThreadOptions extends GroupedOptions
    {
        final OptionSimple threads = new OptionSimple("threads=", "[0-9]+", null, "run this many clients concurrently", true);
        final OptionSimple rate = new OptionSimple("limit=", "[0-9]+/s", "0/s", "limit operations per second across all clients", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(threads, rate);
        }
    }

    // CLI Utility Methods

    public static SettingsRate get(Map<String, String[]> clArgs, SettingsCommand command)
    {
        String[] params = clArgs.remove("-rate");
        if (params == null)
        {
            switch (command.type)
            {
                case WRITE:
                case COUNTER_WRITE:
                    if (command.count > 0)
                    {
                        ThreadOptions options = new ThreadOptions();
                        options.accept("threads=200");
                        return new SettingsRate(options);
                    }
            }
            AutoOptions options = new AutoOptions();
            options.accept("auto");
            return new SettingsRate(options);
        }
        GroupedOptions options = GroupedOptions.select(params, new AutoOptions(), new ThreadOptions());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -rate options provided, see output for valid options");
            System.exit(1);
        }
        if (options instanceof AutoOptions)
            return new SettingsRate((AutoOptions) options);
        else if (options instanceof ThreadOptions)
            return new SettingsRate((ThreadOptions) options);
        else
            throw new IllegalStateException();
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-rate", new ThreadOptions(), new AutoOptions());
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

