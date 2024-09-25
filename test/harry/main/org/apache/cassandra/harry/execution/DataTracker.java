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

package org.apache.cassandra.harry.execution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import accord.utils.Invariants;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.model.Model;

import static org.apache.cassandra.harry.op.Operations.Kind.CUSTOM;
import static org.apache.cassandra.harry.op.Operations.Kind.SELECT_CUSTOM;
import static org.apache.cassandra.harry.op.Operations.Kind.SELECT_PARTITION;
import static org.apache.cassandra.harry.op.Operations.Kind.SELECT_RANGE;
import static org.apache.cassandra.harry.op.Operations.Kind.SELECT_ROW;

/**
 * Data tracker tracks every operation that was started and finished.
 *
 * In principle, it should allow operation to get timed out and remain in the "undecided" state.
 * However, there is no implementation as of now that supports this. Right now, every operation
 * should be started or should be reliably failed / invisible.
 */
public interface DataTracker
{
    void begin(Visit visit);
    void end(Visit visit);

    Iterable<Model.LtsOperationPair> potentialVisits(long pd);
    boolean isFinished(long lts);
    boolean allFinished();

    Set<Operations.Kind> OPS_WITHOUT_EFFECT = Set.of(SELECT_CUSTOM, SELECT_PARTITION, SELECT_ROW, SELECT_RANGE, CUSTOM);

    /**
     * Data tracker that only allows partition visits to be done _in sequence_
     */
    class SequentialDataTracker implements DataTracker
    {
        private final AtomicLong started = new AtomicLong();
        private final AtomicLong finished = new AtomicLong();

        private Map<Long, List<Model.LtsOperationPair>> partitionVisits = new HashMap<>();

        public void begin(Visit visit)
        {
            long prev = started.get();
            Invariants.checkState(prev == 0 || visit.lts == (prev + 1));
            started.set(visit.lts);
            for (int i = 0; i < visit.operations.length; i++)
            {
                Operations.Operation operation = visit.operations[i];

                // SELECT statements have no effect on the model
                if (OPS_WITHOUT_EFFECT.contains(operation.kind()))
                    continue;

                Operations.PartitionOperation partitionOp = (Operations.PartitionOperation) operation;
                partitionVisits.computeIfAbsent(partitionOp.pd, pd_ -> new ArrayList<>())
                               .add(new Model.LtsOperationPair(visit.lts, i));
            }
        }

        public void end(Visit visit)
        {
            long current = started.get();
            Invariants.checkState(current == visit.lts,
                                  "Current stated %d, current visit: %d", current, visit.lts);
            finished.set(visit.lts);
        }

        public Iterable<Model.LtsOperationPair> potentialVisits(long pd)
        {
            Iterable<Model.LtsOperationPair> res = partitionVisits.get(pd);
            if (res != null)
                return res;

            return Collections.emptyList();
        }

        public boolean isFinished(long lts)
        {
            return finished.get() >= lts;
        }

        @Override
        public boolean allFinished()
        {
            return started.get() == finished.get();
        }
    }

    // TODO: optimize for sequential accesses

    /**
     * Data tracker able to track LTS out of order
     */
    class SimpleDataTracker implements DataTracker
    {
        private final Set<Long> started = new HashSet<>();
        private final Set<Long> finished = new HashSet<>();

        private Map<Long, List<Model.LtsOperationPair>> partitionVisits = new HashMap<>();

        public void begin(Visit visit)
        {
            started.add(visit.lts);
            for (int i = 0; i < visit.operations.length; i++)
            {
                Operations.Operation operation = visit.operations[i];

                // SELECT statements have no effect on the model
                if (OPS_WITHOUT_EFFECT.contains(operation.kind()))
                    continue;

                Operations.PartitionOperation partitionOp = (Operations.PartitionOperation) operation;
                partitionVisits.computeIfAbsent(partitionOp.pd, pd_ -> new ArrayList<>())
                               .add(new Model.LtsOperationPair(visit.lts, i));
            }
        }

        public void end(Visit visit)
        {
            finished.add(visit.lts);
        }

        public Iterable<Model.LtsOperationPair> potentialVisits(long pd)
        {
            Iterable<Model.LtsOperationPair> res = partitionVisits.get(pd);
            if (res != null)
                return res;

            return Collections.emptyList();
        }

        public boolean isFinished(long lts)
        {
            return finished.contains(lts);
        }

        @Override
        public boolean allFinished()
        {
            return started.size() == finished.size();
        }
    }
}