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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import accord.utils.Invariants;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.op.Kind;
import org.apache.cassandra.harry.op.Visit;

import static org.apache.cassandra.harry.op.Kind.CUSTOM;
import static org.apache.cassandra.harry.op.Kind.SELECT_CUSTOM;
import static org.apache.cassandra.harry.op.Kind.SELECT_PARTITION;
import static org.apache.cassandra.harry.op.Kind.SELECT_RANGE;
import static org.apache.cassandra.harry.op.Kind.SELECT_ROW;
import static org.apache.cassandra.harry.op.Operations.Operation;
import static org.apache.cassandra.harry.op.Operations.PartitionOperation;

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
    default void gc(long pd) {}

    Set<Kind> OPS_WITHOUT_EFFECT = Set.of(SELECT_CUSTOM, SELECT_PARTITION, SELECT_ROW, SELECT_RANGE, CUSTOM);

    /**
     * Data tracker that only allows partition visits to be done _in sequence_.
     *
     * This data tracker is _not_ thread safe.
     */
    class SequentialDataTracker implements DataTracker, Model.PartialReplay
    {
        private final AtomicLong started = new AtomicLong();
        private final AtomicLong finished = new AtomicLong();

        private Map<Long, List<Operation>> partitionVisits = new HashMap<>();

        public void begin(Visit visit)
        {
            long prev = started.get();
            Invariants.require(prev == 0 || visit.lts == (prev + 1));
            started.set(visit.lts);
            for (int i = 0; i < visit.operations.length; i++)
            {
                Operation operation = visit.operations[i];

                // SELECT statements have no effect on the model
                if (OPS_WITHOUT_EFFECT.contains(operation.kind()))
                    continue;

                PartitionOperation partitionOp = (PartitionOperation) operation;
                partitionVisits.computeIfAbsent(partitionOp.pd, pd_ -> new ArrayList<>())
                               .add(operation);
            }
        }

        public void end(Visit visit)
        {
            long current = started.get();
            Invariants.require(current == visit.lts, "Current stated %d, current visit: %d", current, visit.lts);
            finished.set(visit.lts);
        }

        @Override
        public Iterable<Operation> potentialVisits(long pd)
        {
            Iterable<Operation> res = partitionVisits.get(pd);
            if (res != null)
                return res;

            return Collections.emptyList();
        }
    }

    public static interface ReplayingDataTracker extends DataTracker, Model.PartialReplay {}

    public static class NoOpDataTracker implements ReplayingDataTracker
    {

        @Override
        public void begin(Visit visit)
        {
        }

        @Override
        public void end(Visit visit)
        {
        }

        @Override
        public Iterable<Operation> potentialVisits(long pd)
        {
            return Collections.emptyList();
        }
    }
    /**
     * Data tracker able to track LTS out of order.
     *
     * Intended to be used either in a single-threaded environment, or in conjuction with a locking/concurrent tracker
     */
    // TODO: optimize for sequential accesses
    class SimpleDataTracker implements ReplayingDataTracker
    {
        // WARNING: you can access partitions concurrently, but make sure to use locking tracker to guard the op list
        private Map<Long, List<Operation>> partitionVisits = new ConcurrentHashMap<>();

        public void begin(Visit visit)
        {
            for (int i = 0; i < visit.operations.length; i++)
            {
                Operation operation = visit.operations[i];

                // SELECT statements have no effect on the model
                if (OPS_WITHOUT_EFFECT.contains(operation.kind()))
                    continue;

                PartitionOperation partitionOp = (PartitionOperation) operation;
                partitionVisits.computeIfAbsent(partitionOp.pd, pd_ -> new ArrayList<>())
                               .add(operation);
            }
        }

        @Override
        public void gc(long pd)
        {
            partitionVisits.remove(pd);
        }

        public void end(Visit visit)
        {
        }

        @Override
        public Iterable<Operation> potentialVisits(long pd)
        {
            List<Operation> res = partitionVisits.get(pd);
            if (res == null)
                return Collections.emptyList();

            // TODO: this won't hold for Accord or Paxos, so we will need to also have a separate wall clock
            //       tracker for operations.
            // Operations are appended in begin() order, which under concurrent execution is not logical-timestamp
            // order. The quiescent checker requires them grouped and applied in increasing lts (matching the DB's
            // last-write-wins by USING TIMESTAMP lts), so return an lts-sorted copy. The caller holds the partition
            // read lock, so no writer is appending to the underlying list while we copy it.
            List<Operation> sorted = new ArrayList<>(res);
            sorted.sort(Comparator.comparingLong(Operation::lts));
            return sorted;
        }
    }
}
