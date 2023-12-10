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
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.fuzz.harry.integration.model.ModelTestBase;
import org.apache.cassandra.harry.checker.ModelChecker;
import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.dsl.BatchVisitBuilder;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.SingleOperationBuilder;
import org.apache.cassandra.harry.dsl.ValueDescriptorIndexGenerator;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.visitors.ReplayingVisitor;

public class HistoryBuilderIntegrationTest extends ModelTestBase
{
    private final long seed = 1L;
    private final int STEPS_PER_ITERATION = 1_000;
    private final int MAX_PARTITIONS = 100;

    @Override
    protected Configuration.ModelConfiguration modelConfiguration()
    {
        return new Configuration.QuiescentCheckerConfig();
    }

    public Configuration.ConfigurationBuilder configuration(long seed, SchemaSpec schema)
    {
        return super.configuration(seed, schema)
                    .setPartitionDescriptorSelector(new Configuration.DefaultPDSelectorConfiguration(2, 2))
                    .setClusteringDescriptorSelector((builder) -> builder.setOperationsPerLtsDistribution(new Configuration.ConstantDistributionConfig(100_000)));
    }

    @Test
    public void simpleDSLTest() throws Throwable
    {
        Supplier<SchemaSpec> supplier = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);
        for (int i = 0; i < SchemaGenerators.DEFAULT_RUNS; i++)
        {
            SchemaSpec schema = supplier.get();
            Configuration config = configuration(i, schema).build();

            Run run = config.createRun();
            beforeEach();
            run.sut.schemaChange(schema.compile().cql());

            ModelChecker<SingleOperationBuilder> modelChecker = new ModelChecker<>();
            JdkRandomEntropySource entropySource = new JdkRandomEntropySource(new Random(seed));

            LongSupplier[] valueGenerators = new LongSupplier[run.schemaSpec.regularColumns.size()];
            for (int j = 0; j < valueGenerators.length; j++)
            {
                valueGenerators[j] = new ValueDescriptorIndexGenerator(run.schemaSpec.regularColumns.get(j),
                                                                       run.rng)
                                     .toSupplier(entropySource.derive(), 20, 0.2f);
            }

            TokenPlacementModel.ReplicationFactor rf = new TokenPlacementModel.SimpleReplicationFactor(1);

            int maxPartitionSize = 100;
            modelChecker.init(new HistoryBuilder(seed, maxPartitionSize, 10, schema, rf))
                        .step((history) -> {
                            return history.insert();
                        })
                        .step((history) -> {
                            return history.insert(entropySource.nextInt(maxPartitionSize));
                        })
                        .step((history) -> {
                            int row = entropySource.nextInt(maxPartitionSize);
                            long[] vds = new long[valueGenerators.length];
                            for (int j = 0; j < valueGenerators.length; j++)
                                vds[j] = valueGenerators[j].getAsLong();

                            return history.insert(row, vds);
                        })
                        .step((history) -> {
                            return history.deleteRow();
                        })
                        .step((history) -> {
                            return history.deleteRow(entropySource.nextInt(maxPartitionSize));
                        })
                        .step(SingleOperationBuilder::deletePartition)
                        .step(SingleOperationBuilder::deleteColumns)
                        .step(SingleOperationBuilder::deleteRowSlice)
                        .step((history) -> {
                            return history.deleteRowRange();
                        })
                        .step((history) -> {
                            return history.deleteRowRange(entropySource.nextInt(maxPartitionSize),
                                                          entropySource.nextInt(maxPartitionSize),
                                                          entropySource.nextBoolean(),
                                                          entropySource.nextBoolean());
                        })
                        .step((history) -> history instanceof HistoryBuilder,
                              (history) -> ((HistoryBuilder) history).beginBatch())
                        .step((history) -> (history instanceof BatchVisitBuilder) && ((BatchVisitBuilder) history).size() > 1,
                              (history) -> ((BatchVisitBuilder) history).endBatch())
                        .exitCondition((history) -> {
                            if (!(history instanceof HistoryBuilder))
                                return false;

                            HistoryBuilder historyBuilder = (HistoryBuilder) history;
                            ReplayingVisitor visitor = historyBuilder.visitor(run.tracker, run.sut, SystemUnderTest.ConsistencyLevel.ALL);
                            visitor.replayAll();

                            if (historyBuilder.presetSelector.pds().size() < MAX_PARTITIONS)
                                return false;

                            Model model = historyBuilder.quiescentChecker(run.tracker, sut);

                            for (Long pd : historyBuilder.presetSelector.pds())
                                model.validate(Query.selectPartition(run.schemaSpec, pd,false));

                            return true;
                        })
                        .run(STEPS_PER_ITERATION, seed);
        }
    }
}