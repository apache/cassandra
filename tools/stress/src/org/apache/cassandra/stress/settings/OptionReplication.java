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


import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;

import org.apache.cassandra.locator.AbstractReplicationStrategy;

/**
 * For specifying replication options
 */
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6451
class OptionReplication extends OptionMulti
{

    private final OptionSimple strategy = new OptionSimple("strategy=", new StrategyAdapter(), "org.apache.cassandra.locator.SimpleStrategy", "The replication strategy to use", false);
    private final OptionSimple factor = new OptionSimple("factor=", "[0-9]+", "1", "The number of replicas", false);

    public OptionReplication()
    {
        super("replication", "Define the replication strategy and any parameters", true);
    }

    public String getStrategy()
    {
        return strategy.value();
    }

    public Map<String, String> getOptions()
    {
        Map<String, String> options = extraOptions();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7945
        if (!options.containsKey("replication_factor") && (strategy.value().equals("org.apache.cassandra.locator.SimpleStrategy") || factor.setByUser()))
            options.put("replication_factor", factor.value());
        return options;
    }

    protected List<? extends Option> options()
    {
        return Arrays.asList(strategy, factor);
    }

    @Override
    public boolean happy()
    {
        return true;
    }

    private static final class StrategyAdapter implements Function<String, String>
    {
        public String apply(String name)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6451
            String strategy = null;
            for (String fullname : new String[] { name, "org.apache.cassandra.locator." + name })
            {
                try
                {
                    Class<?> clazz = Class.forName(fullname);
                    if (!AbstractReplicationStrategy.class.isAssignableFrom(clazz))
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6835
                        throw new IllegalArgumentException(clazz + " is not a replication strategy");
                    strategy = fullname;
                    break;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7675
                } catch (Exception ignore)
                {
                    // will throw below if strategy is still null
                }
            }
            if (strategy == null)
                throw new IllegalArgumentException("Invalid replication strategy: " + name);
            return strategy;
        }
    }

}
