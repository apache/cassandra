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
package org.apache.cassandra.repair.autorepair;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.ParameterizedClass;

/**
 * Interface that defines how to generate {@link KeyspaceRepairAssignments}.
 * <p/>
 * The default is {@link RepairTokenRangeSplitter} which aims to provide sensible defaults for all repair types.
 * <p/>
 * Custom implementations class should require a constructor accepting
 * ({@link AutoRepairConfig.RepairType}, {@link java.util.Map}) with the {@link java.util.Map} parameter accepting
 * custom configuration for your splitter. If such a constructor does not exist,
 * {@link AutoRepairConfig#newAutoRepairTokenRangeSplitter(AutoRepairConfig.RepairType, ParameterizedClass)}
 * will fall back on invoking a default zero argument constructor.
 */
public interface IAutoRepairTokenRangeSplitter
{
    /**
     * Split the token range you wish to repair into multiple assignments.
     * The autorepair framework will repair the assignments from returned subrange iterator in the sequence it's
     * provided.
     * @param primaryRangeOnly Whether to repair only this node's primary ranges or all of its ranges.
     * @param repairPlans A list of ordered prioritized repair plans to generate assignments for in order.
     * @return iterator of repair assignments, with each element representing a grouping of repair assignments for a given keyspace.
     * The iterator is traversed lazily {@link KeyspaceRepairAssignments} at a time with the intent to try to get the
     * most up-to-date representation of your data (e.g. how much data exists and is unrepaired at a given time).
     */
    Iterator<KeyspaceRepairAssignments> getRepairAssignments(boolean primaryRangeOnly, List<PrioritizedRepairPlan> repairPlans);

    /**
     * Update a configuration parameter.  This is meant to be used by <code>nodetool setautorepairconfig</code> to
     * update configuration dynamically.
     * @param key parameter to update
     * @param value The value to set to.
     */
    default void setParameter(String key, String value)
    {
        throw new IllegalArgumentException(this.getClass().getName() + " does not support custom configuration");
    }

    /**
     * @return custom configuration.  This is meant to be used by <code>nodetool getautorepairconfig</code> for
     * retrieving the splitter configuration.
     */
    default Map<String, String> getParameters()
    {
        return Collections.emptyMap();
    }
}
