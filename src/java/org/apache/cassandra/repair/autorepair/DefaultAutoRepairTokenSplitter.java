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
import java.util.NoSuchElementException;

import org.apache.cassandra.service.AutoRepairService;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.split;

public class DefaultAutoRepairTokenSplitter implements IAutoRepairTokenRangeSplitter
{
    @Override
    public Iterator<KeyspaceRepairAssignments> getRepairAssignments(AutoRepairConfig.RepairType repairType, boolean primaryRangeOnly, List<PrioritizedRepairPlan> repairPlans)
    {
        return new RepairAssignmentIterator(repairType, primaryRangeOnly, repairPlans);
    }

    private class RepairAssignmentIterator implements Iterator<KeyspaceRepairAssignments>
    {

        private final AutoRepairConfig.RepairType repairType;
        private final boolean primaryRangeOnly;

        private final Iterator<PrioritizedRepairPlan> repairPlanIterator;

        private Iterator<KeyspaceRepairPlan> currentIterator = null;
        private PrioritizedRepairPlan currentPlan = null;

        RepairAssignmentIterator(AutoRepairConfig.RepairType repairType, boolean primaryRangeOnly, List<PrioritizedRepairPlan> repairPlans)
        {
            this.repairType = repairType;
            this.primaryRangeOnly = primaryRangeOnly;
            this.repairPlanIterator = repairPlans.iterator();
        }

        private synchronized Iterator<KeyspaceRepairPlan> currentIterator()
        {
            if (currentIterator == null || !currentIterator.hasNext())
            {
                // Advance the repair plan iterator if the current repair plan is exhausted, but only
                // if there are more repair plans.
                if (repairPlanIterator.hasNext())
                {
                    currentPlan = repairPlanIterator.next();
                    currentIterator = currentPlan.getKeyspaceRepairPlans().iterator();
                }
            }

            return currentIterator;
        }

        @Override
        public boolean hasNext()
        {
            return currentIterator().hasNext();
        }

        @Override
        public KeyspaceRepairAssignments next()
        {
            if (!currentIterator.hasNext())
            {
                throw new NoSuchElementException("No remaining repair plans");
            }

            final KeyspaceRepairPlan repairPlan = currentIterator().next();
            return getRepairAssignmentsForKeyspace(repairType, primaryRangeOnly, currentPlan.getPriority(), repairPlan);
        }
    }

    private KeyspaceRepairAssignments getRepairAssignmentsForKeyspace(AutoRepairConfig.RepairType repairType, boolean primaryRangeOnly, int priority, KeyspaceRepairPlan repairPlan)
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
        int numberOfSubranges = config.getRepairSubRangeNum(repairType);

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
}