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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Convenience {@link Iterator} implementation to assist implementations of
 * {@link IAutoRepairTokenRangeSplitter#getRepairAssignments(boolean, List)} by passing {@link KeyspaceRepairPlan}
 * to a custom {@link #next(int, KeyspaceRepairPlan)} method in priority order.
 */
public abstract class RepairAssignmentIterator implements Iterator<KeyspaceRepairAssignments>
{
    private final Iterator<PrioritizedRepairPlan> repairPlanIterator;

    private Iterator<KeyspaceRepairPlan> currentIterator = null;
    private PrioritizedRepairPlan currentPlan = null;

    public RepairAssignmentIterator(List<PrioritizedRepairPlan> repairPlans)
    {
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
        Iterator<KeyspaceRepairPlan> iterator = currentIterator();
        return (iterator != null && iterator.hasNext());
    }

    @Override
    public KeyspaceRepairAssignments next()
    {
        if (!hasNext())
        {
            throw new NoSuchElementException("No remaining repair plans");
        }

        final KeyspaceRepairPlan repairPlan = currentIterator().next();
        return next(currentPlan.getPriority(), repairPlan);
    }

    /**
     * Invoked by {@link #next()} with the next {@link KeyspaceRepairPlan} for the given priority.
     * @param priority current priority being processed.
     * @param repairPlan the next keyspace repair plan to process
     * @return assignments for the given keyspace at this priority.  Should never return null, if one desires to
     * short-circuit the iterator, override {@link #hasNext()}.
     */
    protected abstract KeyspaceRepairAssignments next(int priority, KeyspaceRepairPlan repairPlan);
}
