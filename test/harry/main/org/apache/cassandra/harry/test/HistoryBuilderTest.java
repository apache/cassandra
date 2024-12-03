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

package org.apache.cassandra.harry.test;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.checker.ModelChecker;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.HistoryBuilderHelper;
import org.apache.cassandra.harry.execution.CQLTesterVisitExecutor;
import org.apache.cassandra.harry.execution.CQLVisitExecutor;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.op.Visit;

import static org.apache.cassandra.harry.Relations.RelationKind.GTE;
import static org.apache.cassandra.harry.Relations.RelationKind.LTE;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;
import static org.apache.cassandra.harry.dsl.SingleOperationBuilder.IdxRelation;

public class HistoryBuilderTest extends CQLTester
{
    // TODO: go through all basic features of History builder here and test them!!!
    // TODO: for example, inverse
    private static final int STEPS_PER_ITERATION = 1_000;

    private static final Logger logger = LoggerFactory.getLogger(HistoryBuilderTest.class);

    private final Generator<SchemaSpec> simple_schema = rng -> {
        return new SchemaSpec(rng.next(),
                              1000,
                              KEYSPACE,
                              "harry" + rng.nextLong(0, Long.MAX_VALUE),
                              Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.asciiType),
                                            ColumnSpec.pk("pk2", ColumnSpec.int64Type)),
                              Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType, false),
                                            ColumnSpec.ck("ck2", ColumnSpec.int64Type, false)),
                              Arrays.asList(ColumnSpec.regularColumn("r1", ColumnSpec.asciiType),
                                            ColumnSpec.regularColumn("r2", ColumnSpec.int64Type),
                                            ColumnSpec.regularColumn("r3", ColumnSpec.asciiType)),
                              Arrays.asList(ColumnSpec.staticColumn("s1", ColumnSpec.asciiType),
                                            ColumnSpec.staticColumn("s2", ColumnSpec.int64Type),
                                            ColumnSpec.staticColumn("s3", ColumnSpec.asciiType)));
    };

    private final Generator<SchemaSpec> simple_schema_with_desc_ck = rng -> {
        return new SchemaSpec(rng.next(),
                              1000,
                              KEYSPACE,
                              "harry" + rng.nextLong(0, Long.MAX_VALUE),
                              Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.asciiType),
                                            ColumnSpec.pk("pk2", ColumnSpec.int64Type)),
                              Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType, true),
                                            ColumnSpec.ck("ck2", ColumnSpec.int64Type, false)),
                              Arrays.asList(ColumnSpec.regularColumn("r1", ColumnSpec.asciiType),
                                            ColumnSpec.regularColumn("r2", ColumnSpec.int64Type),
                                            ColumnSpec.regularColumn("r3", ColumnSpec.asciiType)),
                              Arrays.asList(ColumnSpec.staticColumn("s1", ColumnSpec.asciiType),
                                            ColumnSpec.staticColumn("s2", ColumnSpec.int64Type),
                                            ColumnSpec.staticColumn("s3", ColumnSpec.asciiType)));
    };

    @Test
    public void orderByTest()
    {
        withRandom(rng -> {
            for (Generator<SchemaSpec> gen : new Generator[]{ simple_schema, simple_schema_with_desc_ck })
            {
                SchemaSpec schema = gen.generate(rng);
                createTable(schema.compile());

                HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);
                for (int i = 0; i < 100; i++)
                    history.insert(1);

                history.custom((lts, opId) -> new Operations.SelectPartition(lts,
                                                                             history.valueGenerators().pkGen().descriptorAt(1),
                                                                             Operations.ClusteringOrderBy.DESC));

                replay(schema, history);
            }
        });
    }

    @Test
    public void historyBuilderInsertTest()
    {
        withRandom(rng -> {
            for (Generator<SchemaSpec> gen : new Generator[]{ simple_schema, simple_schema_with_desc_ck })
            {
                SchemaSpec schema = gen.generate(rng);
                createTable(schema.compile());

                HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);
                for (int i = 0; i < 100; i++)
                    history.insert(1, i, values(i, i, i), values(i, i, i));

                history.selectPartition(1);

                replay(schema, history);
            }
        });
    }

    @Test
    public void historyBuilderInsertWithUnsetTest()
    {
        withRandom(rng -> {
            for (Generator<SchemaSpec> gen : new Generator[]{ simple_schema, simple_schema_with_desc_ck })
            {
                SchemaSpec schema = gen.generate(rng);
                createTable(schema.compile());

                HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);
                for (int i = 0; i < 100; i++)
                {
                    int v = i % 2 == 0 ? MagicConstants.UNSET_IDX : i;
                    history.insert(1, i, values(v, v, v), values(v, v, v));
                }

                history.selectPartition(1);

                replay(schema, history);
            }
        });
    }

    @Test
    public void historyBuilderFilteringTest()
    {
        withRandom(rng -> {
            for (Generator<SchemaSpec> gen : new Generator[]{ simple_schema, simple_schema_with_desc_ck })
            {
                for (boolean useUnset : new boolean[]{ false, true })
                {
                    SchemaSpec schema = gen.generate(rng);
                    createTable(schema.compile());

                    HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);
                    for (int i = 0; i < 100; i++)
                    {
                        int v = (useUnset && i % 2 == 0) ? MagicConstants.UNSET_IDX : i;
                        history.insert(1, i, values(v, v, v), values(v, v, v));
                    }

                    history.select(1,
                                   Arrays.asList().toArray(new IdxRelation[0]),
                                   Arrays.asList(new IdxRelation(GTE, 20, 0),
                                                 new IdxRelation(LTE, 80, 0),
                                                 new IdxRelation(GTE, 30, 1),
                                                 new IdxRelation(LTE, 70, 1),
                                                 new IdxRelation(GTE, 40, 2),
                                                 new IdxRelation(LTE, 60, 2))
                                         .toArray(new IdxRelation[0]),
                                   Arrays.asList().toArray(new IdxRelation[0]));

                    replay(schema, history);
                }
            }
        });
    }

    @Test
    public void testSimpleFuzz()
    {
        Generator<SchemaSpec> schemaGen = SchemaGenerators.schemaSpecGen(KEYSPACE, "harry", 100);
        withRandom(rng -> {
            SchemaSpec schema = schemaGen.generate(rng);
            // Generate at most X values, but not more than entropy allows
            int maxPartitions = Math.min(1, schema.valueGenerators.pkPopulation());
            int maxPartitionSize = Math.min(100, schema.valueGenerators.ckPopulation());

            Generator<Integer> partitionPicker = Generators.pick(0, maxPartitions);
            Generator<Integer> rowPicker = Generators.int32(0, maxPartitionSize);
            ModelChecker<HistoryBuilder, Void> modelChecker = new ModelChecker<>();
            HistoryBuilder historyBuilder = new HistoryBuilder(schema.valueGenerators);
            modelChecker.init(historyBuilder)
                        .step((history, rng_) -> {
                            HistoryBuilderHelper.insertRandomData(schema, partitionPicker.generate(rng), rowPicker.generate(rng), rng, history);
                            history.selectPartition(partitionPicker.generate(rng));
                        })
                        .step((history, rng_) -> {
                            history.deleteRow(partitionPicker.generate(rng), rowPicker.generate(rng));
                            history.selectPartition(partitionPicker.generate(rng));
                        })
                        .step((history, rng_) -> {
                            history.deletePartition(partitionPicker.generate(rng));
                            history.selectPartition(partitionPicker.generate(rng));
                        })
                        .step((history, rng_) -> {
                            HistoryBuilderHelper.deleteRandomColumns(schema, partitionPicker.generate(rng), rowPicker.generate(rng), rng, history);
                            history.selectPartition(partitionPicker.generate(rng));
                        })
                        .step((history, rng_) -> {
                            history.deleteRowRange(partitionPicker.generate(rng),
                                                   rowPicker.generate(rng),
                                                   rowPicker.generate(rng),
                                                   rng.nextInt(schema.clusteringKeys.size()),
                                                   rng.nextBoolean(),
                                                   rng.nextBoolean()
                            );
                            history.selectPartition(partitionPicker.generate(rng));
                        })
                        .step((history, rng_) -> {
                            history.selectRow(partitionPicker.generate(rng), rowPicker.generate(rng));
                        })
                        .step((history, rng_) -> {
                            history.selectRowRange(partitionPicker.generate(rng),
                                                   rowPicker.generate(rng),
                                                   rowPicker.generate(rng),
                                                   rng.nextInt(schema.clusteringKeys.size()),
                                                   rng.nextBoolean(),
                                                   rng.nextBoolean());
                        })
                        .step((history, rng_) -> {
                            history.custom(() -> flush(schema.keyspace, schema.table), "FLUSH");
                        })
                        .exitCondition((history) -> {
                            if (historyBuilder.size() < 1000)
                                return false;

                            createTable(schema.compile());
                            replay(schema, historyBuilder);

                            return true;
                        })
                        .run(STEPS_PER_ITERATION, Long.MAX_VALUE, rng);
        });
    }

    @Test
    public void fuzzFiltering()
    {
        Generator<SchemaSpec> schemaGen = SchemaGenerators.schemaSpecGen(KEYSPACE, "fuzz_filtering", 100);
        withRandom(rng -> {
            SchemaSpec schema = schemaGen.generate(rng);
            // Generate at most X values, but not more than entropy allows
            int maxPartitions = Math.min(1, schema.valueGenerators.ckPopulation());
            int maxPartitionSize = Math.min(100, schema.valueGenerators.ckPopulation());

            Generator<Integer> pkGen = Generators.int32(0, Math.min(schema.valueGenerators.pkPopulation(), maxPartitionSize));
            Generator<Integer> ckGen = Generators.int32(0, Math.min(schema.valueGenerators.ckPopulation(), maxPartitionSize));

            ModelChecker<HistoryBuilder, Void> modelChecker = new ModelChecker<>();
            HistoryBuilder historyBuilder = new HistoryBuilder(schema.valueGenerators);

            modelChecker.init(historyBuilder)
                        .step((history, rng_) -> HistoryBuilderHelper.insertRandomData(schema, pkGen, ckGen, rng, history))
                        .step((history, rng_) -> {
                            for (int i = 0; i < 10; i++)
                            {
                                List<IdxRelation> ckRelations = HistoryBuilderHelper.generateClusteringRelations(rng, schema.clusteringKeys.size(), ckGen);
                                List<IdxRelation> regularRelations = HistoryBuilderHelper.generateValueRelations(rng, schema.regularColumns.size(),
                                                                                                                 column -> Math.min(schema.valueGenerators.regularPopulation(column), maxPartitionSize));
                                List<IdxRelation> staticRelations = HistoryBuilderHelper.generateValueRelations(rng, schema.staticColumns.size(),
                                                                                                                column -> Math.min(schema.valueGenerators.staticPopulation(column), maxPartitionSize));
                                history.select(rng.nextInt(maxPartitions),
                                               ckRelations.toArray(new IdxRelation[0]),
                                               regularRelations.toArray(new IdxRelation[0]),
                                               staticRelations.toArray(new IdxRelation[0]));
                            }
                        })
                        .exitCondition((history) -> {
                            if (historyBuilder.size() < 10)
                                return false;

                            createTable(schema.compile());
                            replay(schema, historyBuilder);

                            return true;
                        })
                        .run(STEPS_PER_ITERATION, Long.MAX_VALUE, rng);
        });
    }

    public void replay(SchemaSpec schema, HistoryBuilder historyBuilder)
    {
        CQLVisitExecutor executor = create(schema, historyBuilder);
        for (Visit visit : historyBuilder)
            executor.execute(visit);
    }

    public CQLVisitExecutor create(SchemaSpec schema, HistoryBuilder historyBuilder)
    {
        DataTracker tracker = new DataTracker.SequentialDataTracker();
        return new CQLTesterVisitExecutor(schema, tracker,
                                          new QuiescentChecker(schema.valueGenerators, tracker, historyBuilder),
                                          statement -> {
                                              if (logger.isTraceEnabled())
                                                  logger.trace(statement.toString());
                                              return execute(statement.cql(), statement.bindings());
                                          });
    }

    public int[] values(int... values)
    {
        return values;
    }
}