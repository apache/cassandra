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

package org.apache.cassandra.harry.dsl;

import java.util.function.Consumer;

import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.tracker.DataTracker;
import org.apache.cassandra.harry.visitors.ReplayingVisitor;

public class ReplayingHistoryBuilder extends HistoryBuilder
{
    private final ReplayingVisitor visitor;
    private final SystemUnderTest sut;
    public final DataTracker tracker;

    public ReplayingHistoryBuilder(long seed,
                                   int maxPartitionSize,
                                   int interleaveWindowSize,
                                   DataTracker tracker,
                                   SystemUnderTest sut,
                                   SchemaSpec schema,
                                   TokenPlacementModel.ReplicationFactor rf,
                                   SystemUnderTest.ConsistencyLevel writeCl)
    {
        super(seed, maxPartitionSize, interleaveWindowSize, schema, rf);
        this.visitor = visitor(tracker, sut, writeCl);
        this.tracker = tracker;
        this.sut = sut;
    }

    @Override
    protected SingleOperationVisitBuilder singleOpVisitBuilder(long pd, long lts, Consumer<PartitionVisitState> setupPs)
    {
        PartitionVisitStateImpl partitionState = presetSelector.register(lts, pd, setupPs);
        return new SingleOperationVisitBuilder(partitionState, lts, pureRng, descriptorSelector, schema, valueHelper, (visit) -> {
            log.put(lts, visit);
        }) {
            @Override
            void end()
            {
                super.end();
                visitor.replayAll();
            }
        };
    }

    @Override
    public BatchVisitBuilder beginBatch()
    {
        long visitLts = clock.nextLts();
        return batchVisitBuilder(defaultSelector.pd(visitLts, schema), visitLts);
    }

    /**
     * Begin batch for a partition descriptor at a specific index.
     *
     * Imagine all partition descriptors were longs in an array. Index of a descriptor
     * is a sequential number of the descriptor in this imaginary array.
     */
    @Override
    public BatchVisitBuilder beginBatch(long pdIdx)
    {
        long visitLts = clock.nextLts();
        return batchVisitBuilder(presetSelector.pdAtPosition(pdIdx), visitLts);
    }

    @Override
    protected BatchVisitBuilder batchVisitBuilder(long pd, long lts)
    {
        PartitionVisitStateImpl partitionState = presetSelector.register(lts, pd, (ps) -> {});
        return new BatchVisitBuilder(this, partitionState, lts, pureRng, descriptorSelector, schema, valueHelper, (visit) -> {
            log.put(lts, visit);
        }) {
            @Override
            public HistoryBuilder endBatch()
            {
                super.endBatch();
                visitor.replayAll();
                return ReplayingHistoryBuilder.this;
            }
        };
    }

    public ReplayingHistoryBuilder validate(int... partitions)
    {
        validate(tracker, sut, partitions);
        return this;
    }

    public Model quiescentChecker()
    {
        return quiescentChecker(tracker, sut);
    }

    public Model quiescentLocalChecker()
    {
        return quiescentLocalChecker(tracker, sut);
    }

}
