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
import java.util.List;

import org.apache.cassandra.stress.generatedata.Distribution;
import org.apache.cassandra.stress.generatedata.DistributionFactory;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

// Settings unique to the mixed command type
public class SettingsCommandMixed extends SettingsCommandMulti
{

    // Ratios for selecting commands - index for each Command, NaN indicates the command is not requested
    private final List<Pair<Command, Double>> ratios;
    private final DistributionFactory clustering;

    public SettingsCommandMixed(Options options)
    {
        super(Command.MIXED, options.parent);

        OptionSimple[] ratiosIn = options.probabilities.ratios;
        List<Pair<Command, Double>> ratiosOut = new ArrayList<>();
        for (int i = 0 ; i < ratiosIn.length ; i++)
        {
            if (ratiosIn[i] != null && ratiosIn[i].present())
            {
                double d = Double.parseDouble(ratiosIn[i].value());
                if (d > 0)
                    ratiosOut.add(new Pair<>(Command.values()[i], d));
            }
        }

        ratios = ratiosOut;
        clustering = options.clustering.get();

        if (ratios.size() == 0)
            throw new IllegalArgumentException("Must specify at least one command with a non-zero ratio");
    }

    public List<Command> getCommands()
    {
        final List<Command> r = new ArrayList<>();
        for (Pair<Command, Double> p : ratios)
            r.add(p.getFirst());
        return r;
    }

    public CommandSelector selector()
    {
        return new CommandSelector(ratios, clustering.get());
    }

    // Class for randomly selecting the next command type

    public static final class CommandSelector
    {

        final EnumeratedDistribution<Command> selector;
        final Distribution count;
        private Command cur;
        private long remaining;

        public CommandSelector(List<Pair<Command, Double>> ratios, Distribution count)
        {
            selector = new EnumeratedDistribution<>(ratios);
            this.count = count;
        }

        public Command next()
        {
            while (remaining == 0)
            {
                remaining = count.next();
                cur = selector.sample();
            }
            remaining--;
            return cur;
        }
    }

    // Option Declarations

    static final class Probabilities extends OptionMulti
    {
        // entry for each in Command.values()
        final OptionSimple[] ratios;
        final List<OptionSimple> grouping;

        public Probabilities()
        {
            super("ratio", "Specify the ratios for operations to perform; e.g. (reads=2,writes=1) will perform 2 reads for each write");
            OptionSimple[] ratios = new OptionSimple[Command.values().length];
            List<OptionSimple> grouping = new ArrayList<>();
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
                OptionSimple ratio = new OptionSimple(command.toString().toLowerCase() +
                        "=", "[0-9]+(\\.[0-9]+)?", defaultValue, "Performs this many " + command + " operations out of total", false);
                ratios[command.ordinal()] = ratio;
                grouping.add(ratio);
            }
            this.grouping = grouping;
            this.ratios = ratios;
        }

        @Override
        public List<? extends Option> options()
        {
            return grouping;
        }
    }

    static final class Options extends GroupedOptions
    {
        final SettingsCommandMulti.Options parent;
        protected Options(SettingsCommandMulti.Options parent)
        {
            this.parent = parent;
        }
        final OptionDistribution clustering = new OptionDistribution("clustering=", "GAUSSIAN(1..10)");
        final Probabilities probabilities = new Probabilities();

        @Override
        public List<? extends Option> options()
        {
            final List<Option> options = new ArrayList<>();
            options.add(clustering);
            options.add(probabilities);
            options.addAll(parent.options());
            return options;
        }

    }

    // CLI utility methods

    public static SettingsCommandMixed build(String[] params)
    {
        GroupedOptions options = GroupedOptions.select(params,
                new Options(new SettingsCommandMulti.Options(new Uncertainty())),
                new Options(new SettingsCommandMulti.Options(new Count())));
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid MIXED options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsCommandMixed((Options) options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "mixed",
                new Options(new SettingsCommandMulti.Options(new Uncertainty())),
                new Options(new SettingsCommandMulti.Options(new Count())));
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
