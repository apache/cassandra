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

import org.junit.Test;

import org.apache.cassandra.distributed.test.IntegrationTestBase;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.checker.ModelChecker;
import org.apache.cassandra.harry.dsl.HistoryBuilderHelper;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.dsl.SingleOperationBuilder;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class RangeTombstoneBurnTest extends IntegrationTestBase
{
    private final int ITERATIONS = 10;
    private final int STEPS_PER_ITERATION = 1000;

    @Test
    public void rangeTombstoneBurnTest()
    {
        Generator<SchemaSpec> schemaGen = SchemaGenerators.schemaSpecGen(KEYSPACE, "range_tombstone", 100);
        withRandom(rng -> {
            SchemaSpec schema = schemaGen.generate(rng);
            cluster.get(1).nodetool("disableautocompaction");
            cluster.schemaChange(schema.compile());

            int perIteration = Math.min(10, schema.valueGenerators.pkPopulation());;
            int maxPartitions = Math.max(perIteration, schema.valueGenerators.pkPopulation());

            for (int iteration = 0; iteration < ITERATIONS; iteration++)
            {
                Integer[] partitions = new Integer[perIteration];
                for (int j = 0; j < partitions.length && iteration * 10 < maxPartitions; j++)
                    partitions[j] = iteration * partitions.length + j;

                float deleteRowChance = rng.nextFloat(0.99f, 1.0f);
                float deletePartitionChance = rng.nextFloat(0.999f, 1.0f);
                float deleteColumnsChance = rng.nextFloat(0.95f, 1.0f);
                float deleteRangeChance = rng.nextFloat(0.95f, 1.0f);
                float flushChance = rng.nextFloat(0.999f, 1.0f);
                int maxPartitionSize = Math.min(rng.nextInt(1, 1 << rng.nextInt(5, 11)), schema.valueGenerators.ckPopulation());

                Generator<Integer> partitionPicker = Generators.pick(partitions);
                Generator<Integer> rowPicker = Generators.int32(0, maxPartitionSize);
                ModelChecker<SingleOperationBuilder, Void> model = new ModelChecker<>();
                ReplayingHistoryBuilder historyBuilder = new ReplayingHistoryBuilder(schema.valueGenerators,
                                                                                     (hb) -> InJvmDTestVisitExecutor.builder().build(schema, hb, cluster));

                model.init(historyBuilder)
                     .step((history, rng_) -> {
                         int pdIdx = partitionPicker.generate(rng);
                         history.insert(pdIdx, rowPicker.generate(rng));
                         history.selectPartition(pdIdx);
                     })
                     .step((history, rng_) -> rng.nextDouble() >= deleteRowChance,
                           (history, rng_) -> {
                               int pdIdx = partitionPicker.generate(rng);
                               history.deleteRow(pdIdx, rowPicker.generate(rng));
                               history.selectPartition(pdIdx);
                           })
                     .step((history, rng_) -> rng.nextDouble() >= deletePartitionChance,
                           (history, rng_) -> {
                               int pdIdx = partitionPicker.generate(rng);
                               history.deletePartition(pdIdx);
                               history.selectPartition(pdIdx);
                           })
                     .step((history, rng_) -> rng.nextDouble() >= deleteColumnsChance,
                           (history, rng_) -> {
                               int pdIdx = partitionPicker.generate(rng);
                               HistoryBuilderHelper.deleteRandomColumns(schema, pdIdx, rowPicker.generate(rng), rng, history);
                               history.selectPartition(pdIdx);
                           })
                     .step((history, rng_) -> rng.nextDouble() >= deleteRangeChance,
                           (history, rng_) -> {
                               int pdIdx = partitionPicker.generate(rng);
                               history.deleteRowRange(pdIdx,
                                                      rowPicker.generate(rng),
                                                      rowPicker.generate(rng),
                                                      rng.nextInt(schema.clusteringKeys.size()),
                                                      rng.nextBoolean(),
                                                      rng.nextBoolean()
                               );
                               history.selectPartition(pdIdx);
                           })
                     .step((history, rng_) -> {
                         int pdIdx = partitionPicker.generate(rng);
                         history.selectRow(pdIdx, rowPicker.generate(rng));
                     })
                     .step((history, rng_) -> {
                         int pdIdx = partitionPicker.generate(rng);
                         history.selectRowRange(pdIdx,
                                                rowPicker.generate(rng),
                                                rowPicker.generate(rng),
                                                rng.nextInt(schema.clusteringKeys.size()),
                                                rng.nextBoolean(),
                                                rng.nextBoolean());
                     })
                     .step((history, rng_) -> rng.nextDouble() >= flushChance,
                           (history, rng_) -> {
                               history.custom(() -> cluster.get(1).nodetool("flush", schema.keyspace, schema.table), "FLUSH");
                           })
                     .exitCondition((history) -> {
                         if (historyBuilder.size() < STEPS_PER_ITERATION)
                             return false;
                         return true;
                     })
                     .run(0, Long.MAX_VALUE, rng);
            }
        });
    }
}