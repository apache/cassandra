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
     * Selecting the default value is tricky. If we select a small number, individual repairs would be heavy.
     * On the other hand, if we select a large number, too many repair sessions would be created.
     * <p>
     * If vnodes are configured using <code>num_tokens</code>, attempts to evenly subdivide subranges by each range
     * using the following formula:
     * <p>
     *      Math.max(1, numberOfSubranges / tokens.size())
     * <p>
     * To maintain balance, 32 serves as a good default that accommodates both vnodes and non-vnodes effectively.
     */
    public static final int DEFAULT_NUMBER_OF_SUBRANGES = 32;

    /**
     * Number of evenly split subranges to create for each node that repair runs for.
     * <p>
     * If vnodes are configured using <code>num_tokens</code>, attempts to evenly subdivide subranges by each range.
     * For example, for <code>num_tokens: 16</code> and <code>number_of_subranges: 32</code>, <code>2 (32/16)</code>
     * repair assignments will be created for each token range.  At least one repair assignment will be
     * created for each token range.
     */
    static final String NUMBER_OF_SUBRANGES = "number_of_subranges";

    private final AutoRepairConfig.RepairType repairType;
    private int numberOfSubranges;

    public FixedSplitTokenRangeSplitter(AutoRepairConfig.RepairType repairType, Map<String, String> parameters)
    {
        this.repairType = repairType;

        numberOfSubranges = Integer.parseInt(parameters.getOrDefault(NUMBER_OF_SUBRANGES, Integer.toString(DEFAULT_NUMBER_OF_SUBRANGES)));
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
        // this is done to avoid micro splits in the case of vnodes
        int splitsPerRange = Math.max(1, numberOfSubranges / tokens.size());
        for (Range<Token> token : tokens)
        {
            allRanges.addAll(split(token, splitsPerRange));
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
