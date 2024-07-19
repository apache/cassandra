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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.fuzz.harry.integration.model.IntegrationTestBase;
import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.gen.Bijections;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.tracker.DataTracker;
import org.apache.cassandra.harry.tracker.DefaultDataTracker;
import org.apache.cassandra.harry.visitors.ReplayingVisitor;

public class HistoryBuilderOverridesIntegrationTest extends IntegrationTestBase
{
    private static final long SEED = 1L;

    public static SchemaSpec SIMPLE_SCHEMA = new SchemaSpec("harry",
                                                            "test_overrides",
                                                            Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.asciiType(4, 10)),
                                                                          ColumnSpec.pk("pk2", ColumnSpec.int64Type),
                                                                          ColumnSpec.pk("pk3", ColumnSpec.int64Type),
                                                                          ColumnSpec.pk("pk4", ColumnSpec.asciiType(2, 10))
                                                            ),
                                                            Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType(2, 0)),
                                                                          ColumnSpec.ck("ck2", ColumnSpec.asciiType(2, 0)),
                                                                          ColumnSpec.ck("ck3", ColumnSpec.int64Type),
                                                                          ColumnSpec.ck("ck4", ColumnSpec.asciiType(4, 100)),
                                                                          ColumnSpec.ck("ck5", ColumnSpec.asciiType(8, 100))
                                                            ),
                                                            Arrays.asList(ColumnSpec.regularColumn("regular1", ColumnSpec.asciiType(8, 100)),
                                                                          ColumnSpec.regularColumn("regular2", ColumnSpec.asciiType(8, 100)),
                                                                          ColumnSpec.regularColumn("regular3", ColumnSpec.asciiType(8, 100))
                                                            ),
                                                            Arrays.asList(ColumnSpec.staticColumn("static1", ColumnSpec.asciiType(8, 100)),
                                                                          ColumnSpec.staticColumn("static2", ColumnSpec.asciiType(8, 100)),
                                                                          ColumnSpec.staticColumn("static3", ColumnSpec.asciiType(8, 100))
                                                            ));
    @Test
    public void simpleCkOverrideTest()
    {
        SchemaSpec schema = SIMPLE_SCHEMA;

        DataTracker tracker = new DefaultDataTracker();
        beforeEach();
        sut.schemaChange(schema.compile().cql());

        TokenPlacementModel.ReplicationFactor rf = new TokenPlacementModel.SimpleReplicationFactor(1);

        HistoryBuilder history = new HistoryBuilder(SEED, 5, 10, schema, rf);
        Object[] override = new Object[]{ "", "b", -1L, "c", "d" };
        history.forPartition(1).ensureClustering(override);
        for (int i = 0; i < 5; i++)
            history.visitPartition(1).insert(i);

        history.visitor(tracker, sut, SystemUnderTest.ConsistencyLevel.ALL).replayAll();

        Object[][] res = sut.execute(Query.selectAllColumns(history.schema(), history.visitedPds().get(0), false).toSelectStatement(),
                                     SystemUnderTest.ConsistencyLevel.ALL);
        int found = 0;
        for (Object[] row : res)
        {
            if (Arrays.equals(override, Arrays.copyOfRange(row, 4, 9)))
                found++;
        }
        Assert.assertEquals("Should have mutated exactly one CK", found, 1);

        history.validateAll(tracker, sut);
    }

    @Test
    public void ckOverrideSortingTest()
    {
        for (boolean reverse : new boolean[]{ true, false })
        {
            SchemaSpec schema = new SchemaSpec("harry",
                                               "test_overrides" + (reverse ? "_reverse" : ""),
                                               Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.asciiType(4, 10))),
                                               Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType(2, 0), reverse)),
                                               Arrays.asList(ColumnSpec.regularColumn("regular1", ColumnSpec.asciiType(8, 100))),
                                               Arrays.asList(ColumnSpec.staticColumn("static1", ColumnSpec.asciiType(8, 100))));

            DataTracker tracker = new DefaultDataTracker();
            beforeEach();
            sut.schemaChange(schema.compile().cql());

            TokenPlacementModel.ReplicationFactor rf = new TokenPlacementModel.SimpleReplicationFactor(1);

            int partitionSize = 10;
            HistoryBuilder history = new HistoryBuilder(SEED, partitionSize, 10, schema, rf);
            ReplayingVisitor visitor = history.visitor(tracker, sut, SystemUnderTest.ConsistencyLevel.ALL);
            Set<Integer> foundAt = new HashSet<>();
            for (int pdIdx = 0; pdIdx < 128; pdIdx++)
            {
                Object[] override = new Object[]{ Character.toString(pdIdx) };
                history.forPartition(pdIdx).ensureClustering(override);
                for (int i = 0; i < partitionSize; i++)
                    history.visitPartition(pdIdx).insert(i);

                visitor.replayAll();
                long visitedPd = history.forPartition(pdIdx).pd();
                {
                    Object[][] res = sut.execute(Query.selectAllColumns(history.schema(), visitedPd, false).toSelectStatement(),
                                                 SystemUnderTest.ConsistencyLevel.ALL);

                    int found = 0;
                    for (int i = 0; i < res.length; i++)
                    {
                        Object[] row = res[i];
                        if (Arrays.equals(override, Arrays.copyOfRange(row, 1, 2)))
                        {
                            found++;
                            foundAt.add(i);
                        }
                    }
                    Assert.assertEquals("Should have mutated exactly one CK", found, 1);
                }
                history.validateAll(tracker, sut);
            }
            Assert.assertEquals(10, foundAt.size());
        }
    }

    @Test
    public void ckOverrideManySortingTest()
    {
        int counter = 0;
        for (boolean reverse : new boolean[]{ true, false })
        {
            for (ColumnSpec.DataType type : new ColumnSpec.DataType[]{ ColumnSpec.asciiType(2, 0), ColumnSpec.int64Type })
            {
                SchemaSpec schema = new SchemaSpec("harry",
                                                   "test_overrides" + (counter++),
                                                   Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.asciiType(4, 10))),
                                                   Arrays.asList(ColumnSpec.ck("ck1", type, reverse)),
                                                   Arrays.asList(ColumnSpec.regularColumn("regular1", ColumnSpec.asciiType(8, 100))),
                                                   Arrays.asList(ColumnSpec.staticColumn("static1", ColumnSpec.asciiType(8, 100))));

                DataTracker tracker = new DefaultDataTracker();
                beforeEach();
                sut.schemaChange(schema.compile().cql());

                TokenPlacementModel.ReplicationFactor rf = new TokenPlacementModel.SimpleReplicationFactor(1);

                int partitionSize = 10;
                HistoryBuilder history = new HistoryBuilder(SEED, partitionSize, 10, schema, rf);
                ReplayingVisitor visitor = history.visitor(tracker, sut, SystemUnderTest.ConsistencyLevel.ALL);
                EntropySource rng = new JdkRandomEntropySource(SEED);
                for (int pdIdx = 0; pdIdx < 100; pdIdx++)
                {
                    Set<Object> overrides = new HashSet<>();
                    for (int i = 0; i < 5; i++)
                    {
                        Object override = schema.clusteringKeys.get(0).generator().inflate(rng.next());
                        try
                        {
                            history.forPartition(pdIdx).ensureClustering(new Object[]{ override });
                            overrides.add(override);
                        }
                        catch (IllegalStateException t)
                        {
                            // could not override twice
                        }
                    }

                    for (int i = 0; i < partitionSize; i++)
                        history.visitPartition(pdIdx).insert(i);

                    visitor.replayAll();
                    long visitedPd = history.forPartition(pdIdx).pd();
                    {
                        Object[][] res = sut.execute(Query.selectAllColumns(history.schema(), visitedPd, false).toSelectStatement(),
                                                     SystemUnderTest.ConsistencyLevel.ALL);

                        int found = 0;
                        for (int i = 0; i < res.length; i++)
                        {
                            Object[] row = res[i];
                            Object v = row[1];
                            if (overrides.contains(v))
                                found++;
                        }
                        Assert.assertEquals("Should have mutated exactly one CK", found, overrides.size());
                    }
                    history.validateAll(tracker, sut);
                }
            }
        }
    }

    @Test
    public void ckOverrideWithDeleteTestSingleColumn()
    {
        Supplier<SchemaSpec> supplier = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);

        int partitionSize = 5;
        for (int cnt = 0; cnt < SchemaGenerators.DEFAULT_RUNS; cnt++)
        {
            SchemaSpec schema = supplier.get();
            beforeEach();

            DataTracker tracker = new DefaultDataTracker();
            beforeEach();
            sut.schemaChange(schema.compile().cql());

            TokenPlacementModel.ReplicationFactor rf = new TokenPlacementModel.SimpleReplicationFactor(1);

            HistoryBuilder history = new HistoryBuilder(SEED, partitionSize, 1, schema, rf);
            ReplayingVisitor visitor = history.visitor(tracker, sut, SystemUnderTest.ConsistencyLevel.ALL);

            EntropySource rng = new JdkRandomEntropySource(SEED);
            for (int i = 0; i < partitionSize; i++)
            {
                history.visitPartition(1,
                                       (e) -> schema.ckGenerator.inflate(rng.next()))
                       .insert(i);
            }

            for (int i = 0; i < partitionSize; i++)
            {
                history.visitPartition(1)
                       .deleteRow(i);

                visitor.replayAll();
                history.validateAll(tracker, sut);
            }
        }
    }

    @Test
    public void regularAndStaticOverrideTest()
    {
        for (ColumnSpec.DataType<?> type : new ColumnSpec.DataType[]{ ColumnSpec.asciiType(2, 0), ColumnSpec.int64Type })
        {
            SchemaSpec schema = new SchemaSpec("harry",
                                               "test_overrides",
                                               Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.asciiType(4, 10))),
                                               Arrays.asList(ColumnSpec.ck("ck1", type, false)),
                                               Arrays.asList(ColumnSpec.regularColumn("regular1", ColumnSpec.asciiType(2, 2)),
                                                             ColumnSpec.regularColumn("regular2", ColumnSpec.int64Type)),
                                               Arrays.asList(ColumnSpec.staticColumn("static1", ColumnSpec.asciiType(2, 2)),
                                                             ColumnSpec.staticColumn("static2", ColumnSpec.int64Type)
                                               ));

            Map<String, Bijections.Bijection<?>> reGenerators = new HashMap<>();
            reGenerators.put("regular1", ColumnSpec.asciiType(4, 4).generator());
            reGenerators.put("regular2", ColumnSpec.int64Type.generator());
            reGenerators.put("static1", ColumnSpec.asciiType(8, 4).generator());
            reGenerators.put("static2", ColumnSpec.int64Type.generator());

            DataTracker tracker = new DefaultDataTracker();
            beforeEach();
            sut.schemaChange(schema.compile().cql());

            TokenPlacementModel.ReplicationFactor rf = new TokenPlacementModel.SimpleReplicationFactor(1);

            int partitionSize = 100;
            HistoryBuilder history = new HistoryBuilder(SEED, partitionSize, 10, schema, rf);
            ReplayingVisitor visitor = history.visitor(tracker, sut, SystemUnderTest.ConsistencyLevel.ALL);
            EntropySource rng = new JdkRandomEntropySource(SEED);

            Map<String, Set<Object>> perColumnOverrides = new HashMap<>();
            for (ColumnSpec<?> column : schema.regularColumns)
            {
                perColumnOverrides.put(column.name, new HashSet<>());
                for (int i = 0; i < partitionSize; i++)
                {
                    Object override = reGenerators.get(column.name).inflate(rng.next());
                    history.valueOverrides().override(column.name, i, override);
                    perColumnOverrides.get(column.name).add(override);
                }
            }

            for (ColumnSpec<?> column : schema.staticColumns)
            {
                perColumnOverrides.put(column.name, new HashSet<>());
                for (int i = 0; i < partitionSize; i++)
                {
                    Object override = reGenerators.get(column.name).inflate(rng.next());
                    history.valueOverrides().override(column.name, i, override);
                    perColumnOverrides.get(column.name).add(override);
                }
            }
            for (int pdIdx = 0; pdIdx < 10; pdIdx++)
            {
                Map<String, Set<Object>> results = new HashMap<>();
                for (ColumnSpec<?> column : schema.regularColumns)
                    results.put(column.name, new HashSet<>());
                for (ColumnSpec<?> column : schema.staticColumns)
                    results.put(column.name, new HashSet<>());

                for (int i = 0; i < partitionSize; i++)
                {
                    history.visitPartition(pdIdx)
                           .insert(i,
                                   new long[]{ rng.nextInt(100), rng.nextInt(100) },
                                   new long[]{ rng.nextInt(100), rng.nextInt(100) });
                }

                visitor.replayAll();
                history.validateAll(tracker, sut);

                long visitedPd = history.forPartition(pdIdx).pd();
                Object[][] res = sut.execute(Query.selectAllColumns(history.schema(), visitedPd, false).toSelectStatement(),
                                             SystemUnderTest.ConsistencyLevel.ALL);

                for (int i = 0; i < res.length; i++)
                {
                    Object[] row = res[i];
                    results.get("regular1").add(row[4]);
                    results.get("regular2").add(row[5]);
                    results.get("static1").add(row[2]);
                    results.get("static2").add(row[3]);
                }

                for (Map.Entry<String, Set<Object>> e : results.entrySet())
                {
                    for (Object o : e.getValue())
                    {
                        Assert.assertTrue(String.format("Found a non-overriden value for %s: %s", e.getKey(), e.getValue()),
                                          perColumnOverrides.get(e.getKey()).contains(o));
                    }
                }
            }
        }
    }
}
