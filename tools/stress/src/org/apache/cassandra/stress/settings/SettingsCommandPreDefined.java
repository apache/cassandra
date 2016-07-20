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
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.stress.StressAction.MeasurementSink;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.SeedManager;
import org.apache.cassandra.stress.generate.values.Bytes;
import org.apache.cassandra.stress.generate.values.Generator;
import org.apache.cassandra.stress.generate.values.GeneratorConfig;
import org.apache.cassandra.stress.generate.values.HexBytes;
import org.apache.cassandra.stress.operations.FixedOpDistribution;
import org.apache.cassandra.stress.operations.OpDistribution;
import org.apache.cassandra.stress.operations.OpDistributionFactory;
import org.apache.cassandra.stress.operations.predefined.PredefinedOperation;
import org.apache.cassandra.stress.report.Timer;
import org.apache.cassandra.stress.util.ResultLogger;

// Settings unique to the mixed command type
public class SettingsCommandPreDefined extends SettingsCommand
{

    public final DistributionFactory add;
    public final int keySize;
    public final Options options;

    public OpDistributionFactory getFactory(final StressSettings settings)
    {
        final SeedManager seeds = new SeedManager(settings);
        return new OpDistributionFactory()
        {
            public OpDistribution get(boolean isWarmup, MeasurementSink sink)
            {
                final Timer timer1 = new Timer(type.toString(), sink);
                final Timer timer = timer1;
                return new FixedOpDistribution(PredefinedOperation.operation(type, timer,
                                               newGenerator(settings), seeds, settings, add));
            }

            public String desc()
            {
                return type.toString();
            }

            public Iterable<OpDistributionFactory> each()
            {
                return Collections.<OpDistributionFactory>singleton(this);
            }
        };
    }

    PartitionGenerator newGenerator(StressSettings settings)
    {
        List<String> names = settings.columns.namestrs;
        List<Generator> partitionKey = Collections.<Generator>singletonList(new HexBytes("key",
                                       new GeneratorConfig("randomstrkey", null,
                                                           OptionDistribution.get("fixed(" + keySize + ")"), null)));

        List<Generator> columns = new ArrayList<>();
        for (int i = 0 ; i < settings.columns.maxColumnsPerKey ; i++)
            columns.add(new Bytes(names.get(i), new GeneratorConfig("randomstr" + names.get(i), null, settings.columns.sizeDistribution, null)));
        return new PartitionGenerator(partitionKey, Collections.<Generator>emptyList(), columns, PartitionGenerator.Order.ARBITRARY);
    }

    public SettingsCommandPreDefined(Command type, Options options)
    {
        super(type, options.parent);
        this.options = options;
        add = options.add.get();
        keySize = Integer.parseInt(options.keysize.value());
    }

    // Option Declarations

    static class Options extends GroupedOptions
    {
        final SettingsCommand.Options parent;
        protected Options(SettingsCommand.Options parent)
        {
            this.parent = parent;
        }
        final OptionDistribution add = new OptionDistribution("add=", "fixed(1)", "Distribution of value of counter increments");
        final OptionSimple keysize = new OptionSimple("keysize=", "[0-9]+", "10", "Key size in bytes", false);

        @Override
        public List<? extends Option> options()
        {
            return merge(parent.options(), Arrays.asList(add, keysize));
        }

    }

    public void truncateTables(StressSettings settings)
    {
        truncateTables(settings, settings.schema.keyspace, "standard1", "counter1", "counter3");
    }

    // CLI utility methods

    public void printSettings(ResultLogger out)
    {
        super.printSettings(out);
        out.printf("  Key Size (bytes): %d%n", keySize);
        out.printf("  Counter Increment Distibution: %s%n", options.add.getOptionAsString());
    }

    public static SettingsCommandPreDefined build(Command type, String[] params)
    {
        GroupedOptions options = GroupedOptions.select(params,
                new Options(new Uncertainty()),
                new Options(new Count()),
                new Options(new Duration()));
        if (options == null)
        {
            printHelp(type);
            System.out.println("Invalid " + type + " options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsCommandPreDefined(type, (Options) options);
    }

    static void printHelp(Command type)
    {
        printHelp(type.toString().toLowerCase());
    }

    static void printHelp(String type)
    {
        GroupedOptions.printOptions(System.out, type.toLowerCase(), new Uncertainty(), new Count(), new Duration());
    }

    static Runnable helpPrinter(final Command type)
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                printHelp(type);
            }
        };
    }
}
