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

package org.apache.cassandra.fuzz.harry.examples;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.harry.checker.ModelChecker;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.model.AgainstSutChecker;
import org.apache.cassandra.fuzz.harry.integration.model.IntegrationTestBase;
import org.apache.cassandra.harry.sut.QueryModifyingSut;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.tracker.DataTracker;
import org.apache.cassandra.harry.tracker.DefaultDataTracker;

public class RangeTombstoneBurnTest extends IntegrationTestBase
{
    private final long seed = 1;
    private final int ITERATIONS = 5;
    private final int STEPS_PER_ITERATION = 100;

    @Test
    public void rangeTombstoneBurnTest() throws Throwable
    {
        Supplier<SchemaSpec> supplier = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);

        for (int i = 0; i < SchemaGenerators.DEFAULT_RUNS; i++)
        {
            SchemaSpec schema = supplier.get();
            beforeEach();
            SchemaSpec doubleWriteSchema = schema.cloneWithName(schema.keyspace, schema.keyspace + "_debug");

            sut.schemaChange(schema.compile().cql());
            sut.schemaChange(doubleWriteSchema.compile().cql());

            QueryModifyingSut sut = new QueryModifyingSut(this.sut,
                                                          schema.table,
                                                          doubleWriteSchema.table);

            cluster.get(1).nodetool("disableautocompaction");

            for (int iteration = 0; iteration < ITERATIONS; iteration++)
            {
                ModelChecker<ReplayingHistoryBuilder, Void> modelChecker = new ModelChecker<>();
                EntropySource entropySource = new JdkRandomEntropySource(iteration);

                int maxPartitionSize = entropySource.nextInt(1, 1 << entropySource.nextInt(5, 11));

                int[] partitions = new int[10];
                for (int j = 0; j < partitions.length; j++)
                    partitions[j] = iteration * partitions.length + j;

                float deleteRowChance = entropySource.nextFloat(0.99f, 1.0f);
                float deletePartitionChance = entropySource.nextFloat(0.999f, 1.0f);
                float deleteRangeChance = entropySource.nextFloat(0.95f, 1.0f);
                float flushChance = entropySource.nextFloat(0.999f, 1.0f);
                AtomicInteger flushes = new AtomicInteger();

                TokenPlacementModel.ReplicationFactor rf = new TokenPlacementModel.SimpleReplicationFactor(1);

                DataTracker tracker = new DefaultDataTracker();
                modelChecker.init(new ReplayingHistoryBuilder(seed, maxPartitionSize, STEPS_PER_ITERATION, new DefaultDataTracker(), sut, schema, rf, SystemUnderTest.ConsistencyLevel.ALL))
                            .step((history, rng) -> {
                                      int rowIdx = rng.nextInt(maxPartitionSize);
                                      int partitionIdx = partitions[rng.nextInt(partitions.length)];
                                      history.visitPartition(partitionIdx).insert(rowIdx);
                                  })
                            .step((history, rng) -> rng.nextFloat() > deleteRowChance,
                                  (history, rng) -> {
                                      int partitionIdx = partitions[rng.nextInt(partitions.length)];
                                      history.visitPartition(partitionIdx).deleteRow();
                                  })
                            .step((history, rng) -> rng.nextFloat() > deleteRowChance,
                                  (history, rng) -> {
                                      int partitionIdx = partitions[rng.nextInt(partitions.length)];
                                      history.visitPartition(partitionIdx).deleteColumns();
                                  })
                            .step((history, rng) -> rng.nextFloat() > deletePartitionChance,
                                  (history, rng) -> {
                                      int partitionIdx = partitions[rng.nextInt(partitions.length)];
                                      history.visitPartition(partitionIdx).deletePartition();
                                  })
                            .step((history, rng) -> rng.nextFloat() > flushChance,
                                  (history, rng) -> {
                                      cluster.get(1).nodetool("flush", schema.keyspace, schema.table);
                                      flushes.incrementAndGet();
                                  })
                            .step((history, rng) -> rng.nextFloat() > deleteRangeChance,
                                  (history, rng) -> {
                                      int partitionIdx = partitions[rng.nextInt(partitions.length)];
                                      history.visitPartition(partitionIdx).deleteRowSlice();
                                  })
                            .step((history, rng) -> rng.nextFloat() > deleteRangeChance,
                                  (history, rng) -> {
                                      int row1 = rng.nextInt(maxPartitionSize);
                                      int row2 = rng.nextInt(maxPartitionSize);
                                      int partitionIdx = partitions[rng.nextInt(partitions.length)];
                                      history.visitPartition(partitionIdx).deleteRowRange(Math.min(row1, row2),
                                                                                          Math.max(row1, row2),
                                                                                          entropySource.nextBoolean(),
                                                                                          entropySource.nextBoolean());
                                  })
                            .afterAll((history) -> {
                                // Sanity check
                                history.validate(new AgainstSutChecker(tracker, history.clock(), sut, schema, doubleWriteSchema),
                                                 partitions);
                                history.validate(partitions);
                            })
                            .run(STEPS_PER_ITERATION, seed, entropySource);
            }
        }
    }
}