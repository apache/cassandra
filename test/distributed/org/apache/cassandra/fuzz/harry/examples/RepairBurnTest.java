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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.IntegrationTestBase;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.checker.ModelChecker;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.HistoryBuilderHelper;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class RepairBurnTest extends IntegrationTestBase
{
    @BeforeClass
    public static void before() throws Throwable
    {
        init(3,
             (cfg) -> defaultConfig().accept(cfg.with(Feature.NETWORK, Feature.GOSSIP)));
    }

    @Test
    public void repairBurnTest()
    {
        int maxPartitionSize = 10;
        int partitions = 1000;
        Generator<SchemaSpec> schemaGen = SchemaGenerators.schemaSpecGen(KEYSPACE, "repair_burn", 1000);

        withRandom(rng -> {
            SchemaSpec schema = schemaGen.generate(rng);

            Generators.TrackingGenerator<Integer > pkGen = Generators.tracking(Generators.int32(0, Math.min(schema.valueGenerators.pkPopulation(), partitions)));
            Generator<Integer> ckGen = Generators.int32(0, Math.min(schema.valueGenerators.ckPopulation(), maxPartitionSize));

            ModelChecker<HistoryBuilder, Void> modelChecker = new ModelChecker<>();

            modelChecker.init(new HistoryBuilder(schema.valueGenerators))
                        .step((history, rng_) -> HistoryBuilderHelper.insertRandomData(schema, pkGen, ckGen, rng, history))
                        .step((history, rng_) -> history.deleteRow(pkGen.generate(rng), ckGen.generate(rng)))
                        .exitCondition((history) -> {
                            if (history.size() < 10_000)
                                return false;

                            history.custom(() -> cluster.get(1).nodetool("repair", "--full"),
                                           "Repair");

                            for (Integer pkIdx : pkGen.generated())
                                history.selectPartition(pkIdx);

                            cluster.schemaChange(schema.compile());

                            InJvmDTestVisitExecutor.replay(InJvmDTestVisitExecutor.builder().build(schema, history, cluster),
                                                           history);

                            return true;
                    })
                    .run(rng);
        });
    }
}