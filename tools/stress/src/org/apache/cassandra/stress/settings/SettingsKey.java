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

import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.SeedGenerator;
import org.apache.cassandra.stress.generate.SeedRandomGenerator;
import org.apache.cassandra.stress.generate.SeedSeriesGenerator;

// Settings for key generation
public class SettingsKey implements Serializable
{

    final int keySize;
    private final DistributionFactory distribution;
    private final DistributionFactory clustering;
    private final long[] range;

    public SettingsKey(DistributionOptions options)
    {
        this.keySize = Integer.parseInt(options.size.value());
        this.distribution = options.dist.get();
        this.clustering = options.clustering.get();
        this.range = null;
    }

    public SettingsKey(PopulateOptions options)
    {
        this.keySize = Integer.parseInt(options.size.value());
        this.distribution = null;
        this.clustering = null;
        String[] bounds = options.populate.value().split("\\.\\.+");
        this.range = new long[] { OptionDistribution.parseLong(bounds[0]), OptionDistribution.parseLong(bounds[1]) };
    }

    // Option Declarations

    private static final class DistributionOptions extends GroupedOptions
    {
        final OptionDistribution dist;
        final OptionDistribution clustering = new OptionDistribution("cluster=", "fixed(1)", "Keys are clustered in adjacent value runs of this size");
        final OptionSimple size = new OptionSimple("size=", "[0-9]+", "10", "Key size in bytes", false);

        public DistributionOptions(String defaultLimit)
        {
            dist = new OptionDistribution("dist=", "GAUSSIAN(1.." + defaultLimit + ")", "Keys are selected from this distribution");
        }

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(dist, size, clustering);
        }
    }

    private static final class PopulateOptions extends GroupedOptions
    {
        final OptionSimple populate;
        final OptionSimple size = new OptionSimple("size=", "[0-9]+", "10", "Key size in bytes", false);

        public PopulateOptions(String defaultLimit)
        {
            populate = new OptionSimple("populate=", "[0-9]+\\.\\.+[0-9]+[MBK]?",
                    "1.." + defaultLimit,
                    "Populate all keys in sequence", true);
        }

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(populate, size);
        }
    }

    public SeedGenerator newSeedGenerator()
    {
        return range == null ? new SeedRandomGenerator(distribution.get(), clustering.get()) : new SeedSeriesGenerator(range[0], range[1]);
    }

    // CLI Utility Methods

    public static SettingsKey get(Map<String, String[]> clArgs, SettingsCommand command)
    {
        // set default size to number of commands requested, unless set to err convergence, then use 1M
        String defaultLimit = command.count <= 0 ? "1000000" : Long.toString(command.count);

        String[] params = clArgs.remove("-key");
        if (params == null)
        {
            // return defaults:
            switch(command.type)
            {
                case WRITE:
                case COUNTER_WRITE:
                    return new SettingsKey(new PopulateOptions(defaultLimit));
                default:
                    return new SettingsKey(new DistributionOptions(defaultLimit));
            }
        }
        GroupedOptions options = GroupedOptions.select(params, new PopulateOptions(defaultLimit), new DistributionOptions(defaultLimit));
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -key options provided, see output for valid options");
            System.exit(1);
        }
        return options instanceof PopulateOptions ?
                new SettingsKey((PopulateOptions) options) :
                new SettingsKey((DistributionOptions) options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-key", new PopulateOptions("N"), new DistributionOptions("N"));
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

