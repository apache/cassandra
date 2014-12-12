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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.SeedManager;
import org.apache.cassandra.stress.operations.OpDistributionFactory;
import org.apache.cassandra.stress.operations.SampledOpDistributionFactory;
import org.apache.cassandra.stress.operations.predefined.PredefinedOperation;
import org.apache.cassandra.stress.util.Timer;

// Settings unique to the mixed command type
public class SettingsCommandPreDefinedMixed extends SettingsCommandPreDefined
{

    // Ratios for selecting commands - index for each Command, NaN indicates the command is not requested
    private final Map<Command, Double> ratios;
    private final DistributionFactory clustering;

    public SettingsCommandPreDefinedMixed(Options options)
    {
        super(Command.MIXED, options);

        clustering = options.clustering.get();
        ratios = options.probabilities.ratios();
        if (ratios.size() == 0)
            throw new IllegalArgumentException("Must specify at least one command with a non-zero ratio");
    }

    public OpDistributionFactory getFactory(final StressSettings settings)
    {
        final SeedManager seeds = new SeedManager(settings);
        return new SampledOpDistributionFactory<Command>(ratios, clustering)
        {
            protected Operation get(Timer timer, PartitionGenerator generator, Command key)
            {
                return PredefinedOperation.operation(key, timer, generator, seeds, settings, add);
            }

            protected PartitionGenerator newGenerator()
            {
                return SettingsCommandPreDefinedMixed.this.newGenerator(settings);
            }
        };
    }

    // Option Declarations

    static class Options extends SettingsCommandPreDefined.Options
    {
        static List<OptionEnumProbabilities.Opt<Command>> probabilityOptions = new ArrayList<>();
        static
        {
            for (Command command : Command.values())
            {
                if (command.category == null)
                    continue;
                String defaultValue;
                switch (command)
                {
                    case MIXED:
                        continue;
                    case READ:
                    case WRITE:
                        defaultValue = "1";
                        break;
                    default:
                        defaultValue = null;
                }
                probabilityOptions.add(new OptionEnumProbabilities.Opt<>(command, defaultValue));
            }
        }

        protected Options(SettingsCommand.Options parent)
        {
            super(parent);
        }
        final OptionDistribution clustering = new OptionDistribution("clustering=", "GAUSSIAN(1..10)", "Distribution clustering runs of operations of the same kind");
        final OptionEnumProbabilities probabilities = new OptionEnumProbabilities<>(probabilityOptions, "ratio", "Specify the ratios for operations to perform; e.g. (read=2,write=1) will perform 2 reads for each write");

        @Override
        public List<? extends Option> options()
        {
            return merge(Arrays.asList(clustering, probabilities), super.options());
        }

    }

    // CLI utility methods

    public static SettingsCommandPreDefinedMixed build(String[] params)
    {
        GroupedOptions options = GroupedOptions.select(params,
                new Options(new SettingsCommand.Uncertainty()),
                new Options(new SettingsCommand.Count()),
                new Options(new SettingsCommand.Duration()));
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid MIXED options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsCommandPreDefinedMixed((Options) options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "mixed",
                                    new Options(new SettingsCommand.Uncertainty()),
                                    new Options(new SettingsCommand.Count()),
                                    new Options(new SettingsCommand.Duration()));
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
