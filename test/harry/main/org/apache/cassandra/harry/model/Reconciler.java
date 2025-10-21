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

package org.apache.cassandra.harry.model;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.cassandra.harry.gen.ValueGenerators;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.op.Operations;

import static org.apache.cassandra.harry.op.Operations.Operation;

// TODO: unused at the moment
public class Reconciler
{
    private final ValueGenerators valueGenerators;

    public Reconciler(ValueGenerators valueGenerators)
    {
        this.valueGenerators = valueGenerators;
    }

    public Map<Long, PartitionState> inflate(Iterator<Visit> iter)
    {
        Map<Long, PartitionStateBuilder> state = new HashMap<>();
        apply(state, iter);

        Map<Long, PartitionState> result = new HashMap<>();
        for (Map.Entry<Long, PartitionStateBuilder> e : state.entrySet())
            result.put(e.getKey(), e.getValue().build());
        return result;
    }

    public void apply(Map<Long, PartitionStateBuilder> state, Iterator<Visit> iter)
    {
        assert iter.hasNext();

        while (iter.hasNext())
        {
            Visit visit = iter.next();
            for (Long pd : visit.visitedPartitions)
                state.computeIfAbsent(pd, pd_ -> new PartitionStateBuilder(valueGenerators, new PartitionState(pd, valueGenerators)))
                     .beginLts(visit.lts);


            for (Operation operation : visit.operations)
            {
                if (operation instanceof Operations.PartitionOperation)
                    state.get(((Operations.PartitionOperation) operation).pd()).operation(operation);
            }

            for (Long pd : visit.visitedPartitions)
                state.computeIfAbsent(pd, pd_ -> new PartitionStateBuilder(valueGenerators, new PartitionState(pd, valueGenerators)))
                     .endLts(visit.lts);
        }
    }
}