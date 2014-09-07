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

import com.datastax.driver.core.BatchStatement;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.RatioDistributionFactory;

public class SettingsInsert implements Serializable
{

    public final DistributionFactory revisit;
    public final DistributionFactory visits;
    public final DistributionFactory batchsize;
    public final RatioDistributionFactory selectRatio;
    public final BatchStatement.Type batchType;

    private SettingsInsert(InsertOptions options)
    {
        this.visits= options.visits.get();
        this.revisit = options.revisit.get();
        this.batchsize = options.partitions.get();
        this.selectRatio = options.selectRatio.get();
        this.batchType = !options.batchType.setByUser() ? null : BatchStatement.Type.valueOf(options.batchType.value());
    }

    // Option Declarations

    private static class InsertOptions extends GroupedOptions
    {
        final OptionDistribution visits = new OptionDistribution("visits=", "fixed(1)", "The target number of inserts to split a partition into; if more than one, the partition will be placed in the revisit set");
        final OptionDistribution revisit = new OptionDistribution("revisit=", "uniform(1..1M)", "The distribution with which we revisit partial writes (see visits); implicitly defines size of revisit collection");
        final OptionDistribution partitions = new OptionDistribution("partitions=", null, "The number of partitions to update in a single batch", false);
        final OptionSimple batchType = new OptionSimple("batchtype=", "unlogged|logged|counter", null, "Specify the type of batch statement (LOGGED, UNLOGGED or COUNTER)", false);
        final OptionRatioDistribution selectRatio = new OptionRatioDistribution("select-ratio=", null, "The uniform probability of visiting any CQL row in the generated partition", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(revisit, visits, partitions, batchType, selectRatio);
        }
    }

    // CLI Utility Methods

    public static SettingsInsert get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-insert");
        if (params == null)
            return new SettingsInsert(new InsertOptions());

        InsertOptions options = GroupedOptions.select(params, new InsertOptions());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -insert options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsInsert(options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-insert", new InsertOptions());
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

