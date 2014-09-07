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

import org.apache.cassandra.stress.generate.SeedManager;
import org.apache.cassandra.stress.operations.OpDistributionFactory;
import org.apache.cassandra.thrift.ConsistencyLevel;

// Generic command settings - common to read/write/etc
public abstract class SettingsCommand implements Serializable
{

    public final Command type;
    public final long count;
    public final boolean noWarmup;
    public final ConsistencyLevel consistencyLevel;
    public final double targetUncertainty;
    public final int minimumUncertaintyMeasurements;
    public final int maximumUncertaintyMeasurements;

    public abstract OpDistributionFactory getFactory(StressSettings settings);

    public SettingsCommand(Command type, GroupedOptions options)
    {
        this(type, (Options) options,
                options instanceof Count ? (Count) options : null,
                options instanceof Uncertainty ? (Uncertainty) options : null
        );
    }

    public SettingsCommand(Command type, Options options, Count count, Uncertainty uncertainty)
    {
        this.type = type;
        this.consistencyLevel = ConsistencyLevel.valueOf(options.consistencyLevel.value().toUpperCase());
        this.noWarmup = options.noWarmup.setByUser();
        if (count != null)
        {
            this.count = Long.parseLong(count.count.value());
            this.targetUncertainty = -1;
            this.minimumUncertaintyMeasurements = -1;
            this.maximumUncertaintyMeasurements = -1;
        }
        else
        {
            this.count = -1;
            this.targetUncertainty = Double.parseDouble(uncertainty.uncertainty.value());
            this.minimumUncertaintyMeasurements = Integer.parseInt(uncertainty.minMeasurements.value());
            this.maximumUncertaintyMeasurements = Integer.parseInt(uncertainty.maxMeasurements.value());
        }
    }

    // Option Declarations

    static abstract class Options extends GroupedOptions
    {
        final OptionSimple noWarmup = new OptionSimple("no-warmup", "", null, "Do not warmup the process", false);
        final OptionSimple consistencyLevel = new OptionSimple("cl=", "ONE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|ALL|ANY", "ONE", "Consistency level to use", false);
    }

    static class Count extends Options
    {
        final OptionSimple count = new OptionSimple("n=", "[0-9]+", null, "Number of operations to perform", true);
        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(count, noWarmup, consistencyLevel);
        }
    }

    static class Uncertainty extends Options
    {
        final OptionSimple uncertainty = new OptionSimple("err<", "0\\.[0-9]+", "0.02", "Run until the standard error of the mean is below this fraction", false);
        final OptionSimple minMeasurements = new OptionSimple("n>", "[0-9]+", "30", "Run at least this many iterations before accepting uncertainty convergence", false);
        final OptionSimple maxMeasurements = new OptionSimple("n<", "[0-9]+", "200", "Run at most this many iterations before accepting uncertainty convergence", false);
        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(uncertainty, minMeasurements, maxMeasurements, noWarmup, consistencyLevel);
        }
    }

    // CLI Utility Methods

    static SettingsCommand get(Map<String, String[]> clArgs)
    {
        for (Command cmd : Command.values())
        {
            if (cmd.category == null)
                continue;

            for (String name : cmd.names)
            {
                final String[] params = clArgs.remove(name);
                if (params == null)
                    continue;

                switch (cmd.category)
                {
                    case BASIC:
                        return SettingsCommandPreDefined.build(cmd, params);
                    case MIXED:
                        return SettingsCommandPreDefinedMixed.build(params);
                    case USER:
                        return SettingsCommandUser.build(params);
                }
            }
        }
        return null;
    }

}

