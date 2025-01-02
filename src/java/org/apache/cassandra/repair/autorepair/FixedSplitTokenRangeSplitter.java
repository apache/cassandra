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


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.service.AutoRepairService;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.split;

public class FixedSplitTokenRangeSplitter implements IAutoRepairTokenRangeSplitter
{
    /**
     * The number of subranges to split each to-be-repaired token range into,
     * the higher this number, the smaller the repair sessions will be
     * How many subranges to divide one range into? The default is 1.
     * If you are using v-node, say 256, then the repair will always go one v-node range at a time, this parameter, additionally, will let us further subdivide a given v-node range into sub-ranges.
     * With the value “1” and v-nodes of 256, a given table on a node will undergo the repair 256 times. But with a value “2,” the same table on a node will undergo a repair 512 times because every v-node range will be further divided by two.
     * If you do not use v-nodes or the number of v-nodes is pretty small, say 8, setting this value to a higher number, say 16, will be useful to repair on a smaller range, and the chance of succeeding is higher.
     */
    static final String NUMBER_OF_SUBRANGES = "number_of_subranges";

    private final AutoRepairConfig.RepairType repairType;
    private int numberOfSubranges;

    public FixedSplitTokenRangeSplitter(AutoRepairConfig.RepairType repairType, Map<String, String> parameters)
    {
        this.repairType = repairType;

        if (parameters.containsKey(NUMBER_OF_SUBRANGES))
        {
            numberOfSubranges = Integer.parseInt(parameters.get(NUMBER_OF_SUBRANGES));
        }
        else
        {
            numberOfSubranges = 1;
        }
    }

    @Override
    public Iterator<KeyspaceRepairAssignments> getRepairAssignments(boolean primaryRangeOnly, List<PrioritizedRepairPlan> repairPlans)
    {
        return new RepairAssignmentIterator(repairPlans) {

            @Override
            KeyspaceRepairAssignments nextInternal(int priority, KeyspaceRepairPlan repairPlan)
            {
                return getRepairAssignmentsForKeyspace(primaryRangeOnly, priority, repairPlan);
            }
        };
    }

    private KeyspaceRepairAssignments getRepairAssignmentsForKeyspace(boolean primaryRangeOnly, int priority, KeyspaceRepairPlan repairPlan)
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        List<RepairAssignment> repairAssignments = new ArrayList<>();
        String keyspaceName = repairPlan.getKeyspaceName();
        List<String> tableNames = repairPlan.getTableNames();

        Collection<Range<Token>> tokens = StorageService.instance.getPrimaryRanges(keyspaceName);
        if (!primaryRangeOnly)
        {
            // if we need to repair non-primary token ranges, then change the tokens accrodingly
            tokens = StorageService.instance.getLocalReplicas(keyspaceName).onlyFull().ranges();
        }

        boolean byKeyspace = config.getRepairByKeyspace(repairType);
        // collect all token ranges.
        List<Range<Token>> allRanges = new ArrayList<>();
        for (Range<Token> token : tokens)
        {
            allRanges.addAll(split(token, numberOfSubranges));
        }

        if (byKeyspace)
        {
            for (Range<Token> splitRange : allRanges)
            {
                // add repair assignment for each range entire keyspace's tables
                repairAssignments.add(new RepairAssignment(splitRange, keyspaceName, tableNames));
            }
        }
        else
        {
            // add repair assignment per table
            for (String tableName : tableNames)
            {
                for (Range<Token> splitRange : allRanges)
                {
                    repairAssignments.add(new RepairAssignment(splitRange, keyspaceName, Collections.singletonList(tableName)));
                }
            }
        }

        return new KeyspaceRepairAssignments(priority, keyspaceName, repairAssignments);
    }

    @Override
    public void setParameter(String key, String value)
    {
        if (key.equals(NUMBER_OF_SUBRANGES))
        {
            setNumberOfSubranges(Integer.parseInt(value));
        }
        throw new IllegalArgumentException("Unexpected parameter '" + key + "', must be " + NUMBER_OF_SUBRANGES);
    }

    @Override
    public Map<String, String> getParameters()
    {
        return Collections.unmodifiableMap(new LinkedHashMap<String, String>()
        {{
            put(NUMBER_OF_SUBRANGES, Integer.toString(getNumberOfSubranges()));
        }});
    }

    public void setNumberOfSubranges(int numberOfSubranges)
    {
        this.numberOfSubranges = numberOfSubranges;
    }

    public int getNumberOfSubranges()
    {
        return this.numberOfSubranges;
    }

}