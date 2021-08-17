/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stress.settings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompactionParams;

/**
 * For specifying replication options
 */
class OptionCompaction extends OptionMulti
{

    private final OptionSimple strategy = new OptionSimple("strategy=", new StrategyAdapter(), null, "The compaction strategy to use", false);

    public OptionCompaction()
    {
        super("compaction", "Define the compaction strategy and any parameters", true);
    }

    public String getStrategy()
    {
        return strategy.value();
    }

    public Map<String, String> getOptions()
    {
        return extraOptions();
    }

    protected List<? extends Option> options()
    {
        return Arrays.asList(strategy);
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
            try
            {
                CompactionParams.classFromName(name);
            }
            catch (ConfigurationException e)
            {
                throw new IllegalArgumentException("Invalid compaction strategy: " + name);
            }
            return name;
        }
    }

}
