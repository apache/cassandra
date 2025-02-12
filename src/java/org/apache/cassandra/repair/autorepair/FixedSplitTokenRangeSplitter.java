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
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.service.AutoRepairService;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.compatibility.TokenRingUtils;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.split;

/**
 * An implementation that splits token ranges into a fixed number of subranges.
 */
public class FixedSplitTokenRangeSplitter implements IAutoRepairTokenRangeSplitter
{
    private static final Logger logger = LoggerFactory.getLogger(FixedSplitTokenRangeSplitter.class);

    /**
     * The number of subranges to split each to-be-repaired token range into. Defaults to 1.
     * <p>
     * The higher this number, the smaller the repair sessions will be.
     * <p>
     * If you are using vnodes, say 256, then the repair will always go one vnode range at a time.  This parameter,
     * additionally, will let us further subdivide a given vnode range into subranges.
     * <p>
     * With the value "1" and vnodes of 256, a given table on a node will undergo the repair 256 times. But with a
     * value "2", the same table on a node will undergo a repair 512 times because every vnode range will be further
     * divided by two.
     * <p>
     * If you do not use vnodes or the number of vnodes is pretty small, say 8, setting this value to a higher number,
     * such as 16, will be useful to repair on a smaller range, and the chance of succeeding is higher.
     */
    static final String NUMBER_OF_SUBRANGES = "number_of_subranges";

    private final AutoRepairConfig.RepairType repairType;
    private int numberOfSubranges;

    public FixedSplitTokenRangeSplitter(AutoRepairConfig.RepairType repairType, Map<String, String> parameters)
    {
        this.repairType = repairType;

        numberOfSubranges = Integer.parseInt(parameters.getOrDefault(NUMBER_OF_SUBRANGES, "1"));
    }

    @Override
    public Iterator<KeyspaceRepairAssignments> getRepairAssignments(boolean primaryRangeOnly, List<PrioritizedRepairPlan> repairPlans)
    {
        return new RepairAssignmentIterator(repairPlans)
        {
            @Override
            protected KeyspaceRepairAssignments next(int priority, KeyspaceRepairPlan repairPlan)
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

        Collection<Range<Token>> tokens = TokenRingUtils.getPrimaryRangesForEndpoint(keyspaceName, FBUtilities.getBroadcastAddressAndPort());
        if (!primaryRangeOnly)
        {
            // if we need to repair non-primary token ranges, then change the tokens accordingly
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
        if (!key.equals(NUMBER_OF_SUBRANGES))
        {
            throw new IllegalArgumentException("Unexpected parameter '" + key + "', must be " + NUMBER_OF_SUBRANGES);
        }
        logger.info("Setting {} to {} for repair type {}", key, value, repairType);
        this.numberOfSubranges = Integer.parseInt(value);
    }

    @Override
    public Map<String, String> getParameters()
    {
        return Collections.singletonMap(NUMBER_OF_SUBRANGES, Integer.toString(numberOfSubranges));
    }
}
