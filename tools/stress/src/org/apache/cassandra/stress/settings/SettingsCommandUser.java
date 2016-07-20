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
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.StressProfile;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.SeedManager;
import org.apache.cassandra.stress.generate.TokenRangeIterator;
import org.apache.cassandra.stress.operations.OpDistributionFactory;
import org.apache.cassandra.stress.operations.SampledOpDistributionFactory;
import org.apache.cassandra.stress.report.Timer;
import org.apache.cassandra.stress.util.ResultLogger;

// Settings unique to the mixed command type
public class SettingsCommandUser extends SettingsCommand
{

    // Ratios for selecting commands - index for each Command, NaN indicates the command is not requested
    private final Map<String, Double> ratios;
    private final DistributionFactory clustering;
    public final StressProfile profile;
    private final Options options;

    public SettingsCommandUser(Options options)
    {
        super(Command.USER, options.parent);

        this.options = options;
        clustering = options.clustering.get();
        ratios = options.ops.ratios();

        String yamlPath = options.profile.value();
        File yamlFile = new File(yamlPath);
        profile = StressProfile.load(yamlFile.exists() ? yamlFile.toURI() : URI.create(yamlPath));

        if (ratios.size() == 0)
            throw new IllegalArgumentException("Must specify at least one command with a non-zero ratio");
    }

    public boolean hasInsertOnly()
    {
        return ratios.size() == 1 && ratios.containsKey("insert");
    }

    public OpDistributionFactory getFactory(final StressSettings settings)
    {
        final SeedManager seeds = new SeedManager(settings);
        final TokenRangeIterator tokenRangeIterator = profile.tokenRangeQueries.isEmpty()
                                                      ? null
                                                      : new TokenRangeIterator(settings,
                                                                               profile.maybeLoadTokenRanges(settings));

        return new SampledOpDistributionFactory<String>(ratios, clustering)
        {
            protected List<? extends Operation> get(Timer timer, PartitionGenerator generator, String key, boolean isWarmup)
            {
                if (key.equalsIgnoreCase("insert"))
                    return Collections.singletonList(profile.getInsert(timer, generator, seeds, settings));
                if (key.equalsIgnoreCase("validate"))
                    return profile.getValidate(timer, generator, seeds, settings);

                if (profile.tokenRangeQueries.containsKey(key))
                    return Collections.singletonList(profile.getBulkReadQueries(key, timer, settings, tokenRangeIterator, isWarmup));

                return Collections.singletonList(profile.getQuery(key, timer, generator, seeds, settings, isWarmup));
            }

            protected PartitionGenerator newGenerator()
            {
                return profile.newGenerator(settings);
            }
        };
    }

    public void truncateTables(StressSettings settings)
    {
        profile.truncateTable(settings);
    }

    static final class Options extends GroupedOptions
    {
        final SettingsCommand.Options parent;
        protected Options(SettingsCommand.Options parent)
        {
            this.parent = parent;
        }
        final OptionDistribution clustering = new OptionDistribution("clustering=", "gaussian(1..10)", "Distribution clustering runs of operations of the same kind");
        final OptionSimple profile = new OptionSimple("profile=", ".*", null, "Specify the path to a yaml cql3 profile", true);
        final OptionAnyProbabilities ops = new OptionAnyProbabilities("ops", "Specify the ratios for inserts/queries to perform; e.g. ops(insert=2,<query1>=1) will perform 2 inserts for each query1");

        @Override
        public List<? extends Option> options()
        {
            return merge(Arrays.asList(ops, profile, clustering), parent.options());
        }
    }

    // CLI utility methods

    public void printSettings(ResultLogger out)
    {
        super.printSettings(out);
        out.printf("  Command Ratios: %s%n", ratios);
        out.printf("  Command Clustering Distribution: %s%n", options.clustering.getOptionAsString());
        out.printf("  Profile File: %s%n", options.profile.value());
        // profile.noSettings(out);
    }


    public static SettingsCommandUser build(String[] params)
    {
        GroupedOptions options = GroupedOptions.select(params,
                new Options(new Uncertainty()),
                new Options(new Duration()),
                new Options(new Count()));
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid USER options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsCommandUser((Options) options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "user",
                                    new Options(new Uncertainty()),
                                    new Options(new Count()),
                                    new Options(new Duration()));
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
