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

import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.checker.ModelChecker;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.HistoryBuilderHelper;
import org.apache.cassandra.harry.dsl.IndexedValueGenerators;
import org.apache.cassandra.harry.execution.CQLVisitExecutor;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.harry.op.ClusteringOrderBy;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.op.Visit;

import static org.apache.cassandra.harry.Relations.RelationKind.GTE;
import static org.apache.cassandra.harry.Relations.RelationKind.LTE;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;
import static org.apache.cassandra.harry.dsl.SingleOperationBuilder.IdxRelation;

public abstract class HistoryBuilderTest
{
    protected static final Logger logger = LoggerFactory.getLogger(HistoryBuilderTest.class);

    protected abstract String keyspace();
    protected abstract void createTable(String schema);
    protected abstract void flush(String keyspace, String table);

    public abstract CQLVisitExecutor create(SchemaSpec schema, HistoryBuilder historyBuilder);

    // TODO: go through all basic features of History builder here and test them!!!
    // TODO: for example, inverse
    private static final int STEPS_PER_ITERATION = 1_000;

    public final Generator<SchemaSpec> simple_schema = rng -> {
        return new SchemaSpec(keyspace(),
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

    public final Generator<SchemaSpec> simple_schema_with_desc_ck = rng -> {
        return new SchemaSpec(keyspace(),
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

                HistoryBuilder history = HistoryBuilder.fromSchema(schema, rng.next(), 1000);
                for (int i = 0; i < 100; i++)
                    history.insert(1);

                history.custom((lts, opId) -> new Operations.SelectPartition(lts,
                                                                             history.valueGenerators().pkGen().descriptorAt(1),
                                                                             ClusteringOrderBy.DESC));

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

                HistoryBuilder history = HistoryBuilder.fromSchema(schema, rng.next(), 1000);
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

                HistoryBuilder history = HistoryBuilder.fromSchema(schema, rng.next(), 1000);
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

                    HistoryBuilder history = HistoryBuilder.fromSchema(schema, rng.next(), 1000);
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
        Generator<SchemaSpec> schemaGen = SchemaGenerators.schemaSpecGen(keyspace(), "harry", 100);
        withRandom(rng -> {
            SchemaSpec schema = schemaGen.generate(rng);
            IndexedValueGenerators valueGenerators = HistoryBuilder.valueGenerators(schema, rng.next(), 1000);
            HistoryBuilder historyBuilder = new HistoryBuilder(valueGenerators);
            Generator<Integer> partitionPicker = Generators.adaptLongToInt(valueGenerators.pkIdxGen());
            ModelChecker<HistoryBuilder, Void> modelChecker = new ModelChecker<>();
            modelChecker.init(historyBuilder)
                        .step((history, rng_) -> {
                            int pdIdx = partitionPicker.generate(rng);
                            history.insert(pdIdx);
                            history.selectPartition(pdIdx);
                        })
                        .step((history, rng_) -> {
                            int pdIdx = partitionPicker.generate(rng);
                            history.deleteRow(pdIdx, valueGenerators.forPdIdx(pdIdx).ckIdxGen().generate(rng));
                            history.selectPartition(pdIdx);
                        })
                        .step((history, rng_) -> {
                            int pdIdx = partitionPicker.generate(rng);
                            history.deletePartition(pdIdx);
                            history.selectPartition(pdIdx);
                        })
                        .step((history, rng_) -> {
                            int pdIdx = partitionPicker.generate(rng);
                            HistoryBuilderHelper.deleteRandomColumns(schema, pdIdx, valueGenerators.forPdIdx(pdIdx).ckIdxGen().generate(rng), rng, history);
                            history.selectPartition(partitionPicker.generate(rng));
                        })
                        .step((history, rng_) -> {
                            int pdIdx = partitionPicker.generate(rng);
                            history.deleteRowRange(pdIdx,
                                                   valueGenerators.forPdIdx(pdIdx).ckIdxGen().generate(rng),
                                                   valueGenerators.forPdIdx(pdIdx).ckIdxGen().generate(rng),
                                                   rng.nextInt(schema.clusteringKeys.size()),
                                                   rng.nextBoolean(),
                                                   rng.nextBoolean()
                            );
                            history.selectPartition(partitionPicker.generate(rng));
                        })
                        .step((history, rng_) -> {
                            int pdIdx = partitionPicker.generate(rng);
                            history.selectRow(pdIdx, valueGenerators.forPdIdx(pdIdx).ckIdxGen().generate(rng));
                        })
                        .step((history, rng_) -> {
                            int pdIdx = partitionPicker.generate(rng);
                            history.selectRowRange(pdIdx,
                                                   valueGenerators.forPdIdx(pdIdx).ckIdxGen().generate(rng),
                                                   valueGenerators.forPdIdx(pdIdx).ckIdxGen().generate(rng),
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
        Generator<SchemaSpec> schemaGen = SchemaGenerators.schemaSpecGen(keyspace(), "fuzz_filtering", 100);
        withRandom(rng -> {
            SchemaSpec schema = schemaGen.generate(rng);
            IndexedValueGenerators valueGenerators = HistoryBuilder.valueGenerators(schema, rng.next(), 1000);

            ModelChecker<HistoryBuilder, Void> modelChecker = new ModelChecker<>();
            HistoryBuilder historyBuilder = new HistoryBuilder(valueGenerators);

            modelChecker.init(historyBuilder)
                        .step((history, rng_) -> history.insert())
                        .step((history, rng_) -> {
                            for (int i = 0; i < 10; i++)
                            {
                                long pdIdx = valueGenerators.pkIdxGen().generate(rng);
                                List<IdxRelation> ckRelations = HistoryBuilderHelper.generateClusteringRelations(rng, schema.clusteringKeys.size(), valueGenerators.forPd(pdIdx).ckIdxGen());
                                List<IdxRelation> regularRelations = HistoryBuilderHelper.generateValueRelations(rng, schema.regularColumns.size(),
                                                                                                                 column -> Math.toIntExact(valueGenerators.forPdIdx(pdIdx).regularColumnGen(column).population()));
                                List<IdxRelation> staticRelations = HistoryBuilderHelper.generateValueRelations(rng, schema.staticColumns.size(),
                                                                                                                column -> Math.toIntExact(valueGenerators.forPdIdx(pdIdx).staticColumnGen(column).population()));
                                history.select((int) pdIdx,
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

    public static int[] values(int... values)
    {
        return values;
    }
}