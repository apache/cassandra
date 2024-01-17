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

package org.apache.cassandra.fuzz.harry.integration.dsl;

import java.util.Random;
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.fuzz.harry.integration.model.IntegrationTestBase;
import org.apache.cassandra.harry.checker.ModelChecker;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.dsl.BatchVisitBuilder;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.SingleOperationBuilder;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.tracker.DataTracker;
import org.apache.cassandra.harry.tracker.DefaultDataTracker;
import org.apache.cassandra.harry.visitors.ReplayingVisitor;

public class HistoryBuilderIntegrationTest extends IntegrationTestBase
{
    private final long seed = 1L;
    private final int STEPS_PER_ITERATION = 1_000;
    private final int MAX_PARTITIONS = 50;

    @Test
    public void simpleDSLTest() throws Throwable
    {
        Supplier<SchemaSpec> supplier = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);
        for (int i = 0; i < SchemaGenerators.DEFAULT_RUNS; i++)
        {
            SchemaSpec schema = supplier.get();
            DataTracker tracker = new DefaultDataTracker();
            beforeEach();
            sut.schemaChange(schema.compile().cql());

            ModelChecker<SingleOperationBuilder> modelChecker = new ModelChecker<>();
            JdkRandomEntropySource rng = new JdkRandomEntropySource(new Random(seed));

            TokenPlacementModel.ReplicationFactor rf = new TokenPlacementModel.SimpleReplicationFactor(1);

            int maxPartitionSize = 100;
            modelChecker.init(new HistoryBuilder(seed, maxPartitionSize, 10, schema, rf))
                        .step((history) -> {
                            return history.insert();
                        })
                        .step((history) -> {
                            return history.insert(rng.nextInt(maxPartitionSize));
                        })
                        .step((history) -> {
                            int row = rng.nextInt(maxPartitionSize);
                            long[] vIdxs = new long[schema.regularColumns.size()];
                            for (int j = 0; j < schema.regularColumns.size(); j++)
                                vIdxs[j] = rng.nextInt(20);

                            return history.insert(row, vIdxs);
                        })
                        .step((history) -> {
                            return history.deleteRow();
                        })
                        .step((history) -> {
                            return history.deleteRow(rng.nextInt(maxPartitionSize));
                        })
                        .step(SingleOperationBuilder::deletePartition)
                        .step(SingleOperationBuilder::deleteColumns)
                        .step(SingleOperationBuilder::deleteRowSlice)
                        .step((history) -> {
                            return history.deleteRowRange();
                        })
                        .step((history) -> {
                            return history.deleteRowRange(rng.nextInt(maxPartitionSize),
                                                          rng.nextInt(maxPartitionSize),
                                                          rng.nextBoolean(),
                                                          rng.nextBoolean());
                        })
                        .step((history) -> history instanceof HistoryBuilder,
                              (history) -> ((HistoryBuilder) history).beginBatch())
                        .step((history) -> (history instanceof BatchVisitBuilder) && ((BatchVisitBuilder) history).size() > 1,
                              (history) -> ((BatchVisitBuilder) history).endBatch())
                        .exitCondition((history) -> {
                            if (!(history instanceof HistoryBuilder))
                                return false;

                            HistoryBuilder historyBuilder = (HistoryBuilder) history;
                            ReplayingVisitor visitor = historyBuilder.visitor(tracker, sut, SystemUnderTest.ConsistencyLevel.ALL);
                            visitor.replayAll();

                            if (historyBuilder.visitedPds().size() < MAX_PARTITIONS)
                                return false;

                            Model model = historyBuilder.quiescentChecker(tracker, sut);

                            for (Long pd : historyBuilder.visitedPds())
                                model.validate(Query.selectPartition(historyBuilder.schema(), pd,false));

                            return true;
                        })
                        .run(STEPS_PER_ITERATION, seed);
        }
    }

    @Test
    public void overrideCkTest() throws Throwable
    {
        Supplier<SchemaSpec> supplier = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);
        for (int schemaIdx = 0; schemaIdx < SchemaGenerators.DEFAULT_RUNS; schemaIdx++)
        {
            SchemaSpec schema = supplier.get();
            DataTracker tracker = new DefaultDataTracker();
            beforeEach();
            sut.schemaChange(schema.compile().cql());

            ModelChecker<HistoryBuilder> modelChecker = new ModelChecker<>();
            JdkRandomEntropySource rng = new JdkRandomEntropySource(new Random(seed));

            TokenPlacementModel.ReplicationFactor rf = new TokenPlacementModel.SimpleReplicationFactor(1);

            int maxPartitionSize = 10;
            modelChecker.init(new HistoryBuilder(seed, maxPartitionSize, 10, schema, rf))
                        .beforeAll((history) -> {
                            for (int i = 0; i < MAX_PARTITIONS; i++)
                                history.forPartition(i).ensureClustering(schema.ckGenerator.inflate(rng.nextLong()));
                        })
                        .step((history) -> {
                            history.visitPartition(rng.nextInt(MAX_PARTITIONS))
                                   .insert();
                        })
                        .step((history) -> {
                            history.visitPartition(rng.nextInt(MAX_PARTITIONS))
                                   .insert(rng.nextInt(maxPartitionSize));
                        })
                        .step((history) -> {
                            history.visitPartition(rng.nextInt(MAX_PARTITIONS))
                                   .deleteRow();
                        })
                        .step((history) -> {
                            history.visitPartition(rng.nextInt(MAX_PARTITIONS))
                                   .deleteRow(rng.nextInt(maxPartitionSize));
                        })
                        .step((history) -> {
                            history.visitPartition(rng.nextInt(MAX_PARTITIONS))
                                   .deletePartition();
                        })
                        .step((history) -> {
                            history.visitPartition(rng.nextInt(MAX_PARTITIONS))
                                   .deleteColumns();
                        })
                        .step((history) -> {
                            history.visitPartition(rng.nextInt(MAX_PARTITIONS))
                                   .deleteRowRange();
                        })
                        .step((history) -> {
                            history.visitPartition(rng.nextInt(MAX_PARTITIONS))
                                   .deleteRowSlice();
                        })
                        .exitCondition((history) -> {
                            ReplayingVisitor visitor = history.visitor(tracker, sut, SystemUnderTest.ConsistencyLevel.ALL);
                            visitor.replayAll();

                            if (history.visitedPds().size() < MAX_PARTITIONS)
                                return false;

                            Model model = history.quiescentChecker(tracker, sut);

                            for (Long pd : history.visitedPds())
                                model.validate(Query.selectPartition(history.schema(), pd,false));

                            return true;
                        })
                        .run(STEPS_PER_ITERATION, seed);
        }
    }
}