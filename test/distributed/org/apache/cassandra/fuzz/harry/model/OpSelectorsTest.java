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

package org.apache.cassandra.fuzz.harry.model;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.harry.core.MetricReporter;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.gen.Surjections;
import org.apache.cassandra.harry.gen.distribution.Distribution;
import org.apache.cassandra.harry.clock.OffsetClock;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.operations.CompiledStatement;
import org.apache.cassandra.harry.tracker.DataTracker;
import org.apache.cassandra.harry.visitors.LtsVisitor;
import org.apache.cassandra.harry.visitors.MutatingVisitor;
import org.apache.cassandra.harry.visitors.OperationExecutor;
import org.apache.cassandra.harry.util.BitSet;
import org.apache.cassandra.harry.visitors.VisitExecutor;

public class OpSelectorsTest
{
    private static int RUNS = 1000;
    private static SchemaSpec SCHEMA = new SchemaSpec("ks", "tbl",
                                                      Collections.singletonList(ColumnSpec.pk("pk", ColumnSpec.int64Type)),
                                                      Collections.singletonList(ColumnSpec.pk("ck", ColumnSpec.int64Type)),
                                                      Collections.emptyList(), Collections.emptyList());

    @Test
    public void testRowDataDescriptorSupplier()
    {
        OpSelectors.PureRng rng = new OpSelectors.PCGFast(1);
        SchemaSpec schema = new SchemaSpec("ks", "tbl1",
                                           Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.asciiType),
                                                         ColumnSpec.pk("pk2", ColumnSpec.int64Type)),
                                           Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType, false),
                                                         ColumnSpec.ck("ck2", ColumnSpec.int64Type, false)),
                                           Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.int32Type),
                                                         ColumnSpec.regularColumn("v2", ColumnSpec.int64Type)),
                                           Arrays.asList(ColumnSpec.staticColumn("static1", ColumnSpec.asciiType),
                                                         ColumnSpec.staticColumn("static2", ColumnSpec.int64Type)));
        OpSelectors.DefaultDescriptorSelector descriptorSelector = new OpSelectors.DefaultDescriptorSelector(rng,
                                                                                                             new OpSelectors.ColumnSelectorBuilder().forAll(schema)
                                                                                                                                                    .build(),
                                                                                                             OpSelectors.DefaultDescriptorSelector.DEFAULT_OP_SELECTOR,
                                                                                                             new Distribution.ScaledDistribution(2, 10),
                                                                                                             50);

        OpSelectors.PdSelector pdSupplier = new OpSelectors.DefaultPdSelector(rng,
                                                                              100,
                                                                              100);

        for (int lts = 0; lts < RUNS; lts++)
        {
            long pd = pdSupplier.pd(lts, schema);

            int opsPerLts = descriptorSelector.operationsPerLts(lts);
            for (int opId = 0; opId < opsPerLts; opId++)
            {
                long cd = descriptorSelector.cd(pd, lts, opId, schema);
                Assert.assertEquals(opId, descriptorSelector.opId(pd, lts, cd));
                Assert.assertTrue(descriptorSelector.isCdVisitedBy(pd, lts, cd));
                for (int col = 0; col < 10; col++)
                {
                    long vd = descriptorSelector.vd(pd, cd, lts, opId, col);
                }
            }
        }
    }

    @Test
    public void pdSelectorSymmetryTest()
    {
        OpSelectors.PureRng rng = new OpSelectors.PCGFast(1);
        Supplier<SchemaSpec> gen = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);
        SchemaSpec schema = gen.get();

        for (long[] positions : new long[][]{ { 0, Long.MAX_VALUE }, { 100, Long.MAX_VALUE }, { 1000, Long.MAX_VALUE } })
        {
            for (int repeats = 2; repeats <= 1000; repeats++)
            {
                for (int windowSize = 2; windowSize <= 10; windowSize++)
                {
                    OpSelectors.DefaultPdSelector pdSelector = new OpSelectors.DefaultPdSelector(rng, windowSize, repeats, positions[0], positions[1]);

                    Map<Long, List<Long>> m = new HashMap<>();
                    final long maxLts = 10_000;
                    for (long lts = 0; lts <= maxLts; lts++)
                    {
                         long pd = pdSelector.pd(lts, schema);
                         m.computeIfAbsent(pd, (k) -> new ArrayList<>()).add(lts);
                    }

                    for (Long pd : m.keySet())
                    {
                        long currentLts =  pdSelector.minLtsFor(pd);
                        List<Long> predicted = new ArrayList<>();
                        while (currentLts <= maxLts && currentLts >= 0)
                        {
                            predicted.add(currentLts);
                            currentLts = pdSelector.nextLts(currentLts);
                        }
                        Assert.assertEquals(m.get(pd), predicted);
                    }


                }
            }
        }
    }

    @Test
    public void pdSelectorTest()
    {
        OpSelectors.PureRng rng = new OpSelectors.PCGFast(1);
        int cycles = 10000;
        for (long[] positions : new long[][]{ { 0, Long.MAX_VALUE }, { 100, Long.MAX_VALUE }, { 1000, Long.MAX_VALUE } })
        {
            for (int repeats = 2; repeats <= 1000; repeats++)
            {
                for (int windowSize = 2; windowSize <= 10; windowSize++)
                {
                    OpSelectors.DefaultPdSelector pdSupplier = new OpSelectors.DefaultPdSelector(rng, windowSize, repeats, positions[0], positions[1]);
                    long[] pds = new long[cycles];
                    for (int i = 0; i < cycles; i++)
                    {
                        long pd = pdSupplier.pd(i, SCHEMA);
                        pds[i] = pd;
                        Assert.assertEquals(pdSupplier.positionFor(i), pdSupplier.positionForPd(pd));
                    }

                    Set<Long> noNext = new HashSet<>();
                    for (int i = 0; i < cycles; i++)
                    {
                        long nextLts = pdSupplier.nextLts(i);
                        Assert.assertFalse(noNext.contains(pds[i]));
                        if (nextLts == -1)
                        {
                            noNext.add(nextLts);
                        }
                        else if (nextLts < cycles)
                        {
                            Assert.assertEquals(pds[(int) nextLts], pdSupplier.pd(i, SCHEMA));
                        }
                    }

                    Set<Long> noPrev = new HashSet<>();
                    for (int i = cycles - 1; i >= 0; i--)
                    {
                        long prevLts = pdSupplier.prevLts(i);
                        Assert.assertFalse(noPrev.contains(pds[i]));
                        if (prevLts == -1)
                        {
                            noPrev.add(prevLts);
                        }
                        else if (prevLts >= 0)
                        {
                            Assert.assertEquals(pds[(int) prevLts], pdSupplier.pd(i, SCHEMA));
                        }
                    }

                    Set<Long> seen = new HashSet<>();
                    for (int i = 0; i < cycles; i++)
                    {
                        long pd = pdSupplier.pd(i, SCHEMA);
                        if (!seen.contains(pd))
                        {
                            Assert.assertEquals(i, pdSupplier.minLtsAt(pdSupplier.positionFor(i)));
                            seen.add(pd);
                        }
                    }

                    for (int i = 0; i < cycles; i++)
                    {
                        long pd = pdSupplier.pd(i, SCHEMA);
                        long maxLts = pdSupplier.maxLtsFor(pd);
                        Assert.assertEquals(-1, pdSupplier.nextLts(maxLts));
                        Assert.assertEquals(pdSupplier.pd(i, SCHEMA), pdSupplier.pd(maxLts, SCHEMA));
                    }
                }
            }
        }
    }

    @Test
    public void ckSelectorTest()
    {
        Supplier<SchemaSpec> gen = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);
        for (int i = 0; i < SchemaGenerators.DEFAULT_RUNS; i++)
            ckSelectorTest(gen.get());
    }

    public void ckSelectorTest(SchemaSpec schema)
    {
        OpSelectors.PureRng rng = new OpSelectors.PCGFast(1);
        OpSelectors.PdSelector pdSelector = new OpSelectors.DefaultPdSelector(rng, 10, 10);
        OpSelectors.DescriptorSelector ckSelector = new OpSelectors.DefaultDescriptorSelector(rng,
                                                                                              new OpSelectors.ColumnSelectorBuilder().forAll(schema, Surjections.pick(schema.regularColumnsMask())).build(),
                                                                                              OpSelectors.OperationSelector.weighted(Surjections.weights(10, 10, 40, 40),
                                                                                                                                     OpSelectors.OperationKind.DELETE_ROW,
                                                                                                                                     OpSelectors.OperationKind.DELETE_COLUMN,
                                                                                                                                     OpSelectors.OperationKind.INSERT,
                                                                                                                                     OpSelectors.OperationKind.UPDATE),
                                                                                              new Distribution.ConstantDistribution(10),
                                                                                              10);

        Map<Long, Set<Long>> partitionMap = new HashMap<>();
        CompiledStatement compiledStatement = new CompiledStatement("");
        BiConsumer<Long, Long> consumer = (pd, cd) -> {
            partitionMap.compute(pd, (pk, list) -> {
                if (list == null)
                    list = new HashSet<>();
                list.add(cd);
                return list;
            });
        };

        Run run = new Run(rng,
                          new OffsetClock(0),
                          pdSelector,
                          ckSelector,
                          schema,
                          DataTracker.NO_OP,
                          SystemUnderTest.NO_OP,
                          MetricReporter.NO_OP);

        LtsVisitor visitor = new MutatingVisitor(run,
                                                 (r) -> new OperationExecutor()
                                                        {
                                                            public CompiledStatement insert(VisitExecutor.WriteOp op)
                                                            {
                                                                consumer.accept(op.pd(), op.cd());
                                                                return compiledStatement;
                                                            }

                                                            public CompiledStatement update(VisitExecutor.WriteOp op)
                                                            {
                                                                consumer.accept(op.pd(), op.cd());
                                                                return compiledStatement;
                                                            }

                                                            public CompiledStatement insertWithStatics(VisitExecutor.WriteStaticOp op)
                                                            {
                                                                consumer.accept(op.pd(), op.cd());
                                                                return compiledStatement;
                                                            }

                                                            public CompiledStatement updateWithStatics(VisitExecutor.WriteStaticOp op)
                                                            {
                                                                consumer.accept(op.pd(), op.cd());
                                                                return compiledStatement;
                                                            }

                                                            public CompiledStatement deleteColumn(VisitExecutor.DeleteColumnsOp op)
                                                            {
                                                                consumer.accept(op.pd(), op.cd());
                                                                return compiledStatement;
                                                            }

                                                            public CompiledStatement deleteColumnWithStatics(VisitExecutor.DeleteColumnsOp op)
                                                            {
                                                                consumer.accept(op.pd(), op.cd());
                                                                return compiledStatement;
                                                            }

                                                            public CompiledStatement deleteRow(VisitExecutor.DeleteRowOp op)
                                                            {
                                                                consumer.accept(op.pd(), op.cd());
                                                                return compiledStatement;
                                                            }

                                                            public CompiledStatement deletePartition(VisitExecutor.DeleteOp op)
                                                            {
                                                                // ignore
                                                                return compiledStatement;
                                                            }

                                                            public CompiledStatement deleteRange(VisitExecutor.DeleteOp op)
                                                            {
                                                                // ignore
                                                                return compiledStatement;
                                                            }

                                                            public CompiledStatement deleteSlice(VisitExecutor.DeleteOp op)
                                                            {
                                                                // ignore
                                                                return compiledStatement;
                                                            }
                                                        });

        for (int lts = 0; lts < 1000; lts++)
            visitor.visit();

        for (Collection<Long> value : partitionMap.values())
            Assert.assertEquals(10, value.size());
    }

    @Test
    public void hierarchicalDescriptorSelector()
    {
        SchemaSpec schema = new SchemaSpec("ks", "tbl1",
                                           Collections.singletonList(ColumnSpec.pk("pk1", ColumnSpec.asciiType)),
                                           Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType),
                                                         ColumnSpec.ck("ck2", ColumnSpec.asciiType),
                                                         ColumnSpec.ck("ck3", ColumnSpec.asciiType)),
                                           Collections.singletonList(ColumnSpec.regularColumn("v1", ColumnSpec.asciiType)),
                                           Collections.emptyList());

        OpSelectors.PureRng rng = new OpSelectors.PCGFast(1);
        OpSelectors.DescriptorSelector ckSelector = new OpSelectors.HierarchicalDescriptorSelector(rng,
                                                                                                   new int[] {10, 20},
                                                                                                   OpSelectors.columnSelectorBuilder().forAll(schema, Surjections.pick(BitSet.allUnset(0))).build(),
                                                                                                   OpSelectors.OperationSelector.weighted(Surjections.weights(10, 10, 40, 40),
                                                                                                                                          OpSelectors.OperationKind.DELETE_ROW,
                                                                                                                                          OpSelectors.OperationKind.DELETE_COLUMN,
                                                                                                                                          OpSelectors.OperationKind.INSERT,
                                                                                                                                          OpSelectors.OperationKind.UPDATE),
                                                                                                   new Distribution.ConstantDistribution(10),
                                                                                                   100);

        Set<Long> ck1 = new TreeSet<>();
        Set<Long> ck2 = new TreeSet<>();
        Set<Long> ck3 = new TreeSet<>();
        for (int i = 0; i < 1000; i++)
        {
            long[] part = schema.ckGenerator.slice(ckSelector.cd(0, i, 0, schema));
            ck1.add(part[0]);
            ck2.add(part[1]);
            ck3.add(part[2]);
        }
        Assert.assertEquals(10, ck1.size());
        Assert.assertEquals(20, ck2.size());
        Assert.assertEquals(100, ck3.size());
    }

    @Test
    public void testWeights()
    {
        Map<OpSelectors.OperationKind, Integer> config = new EnumMap<>(OpSelectors.OperationKind.class);
        config.put(OpSelectors.OperationKind.DELETE_RANGE, 1);
        config.put(OpSelectors.OperationKind.DELETE_SLICE, 1);
        config.put(OpSelectors.OperationKind.DELETE_ROW, 1);
        config.put(OpSelectors.OperationKind.DELETE_COLUMN, 1);
        config.put(OpSelectors.OperationKind.DELETE_PARTITION, 1);
        config.put(OpSelectors.OperationKind.DELETE_COLUMN_WITH_STATICS, 1);
        config.put(OpSelectors.OperationKind.UPDATE, 500);
        config.put(OpSelectors.OperationKind.INSERT, 500);
        config.put(OpSelectors.OperationKind.UPDATE_WITH_STATICS, 500);
        config.put(OpSelectors.OperationKind.INSERT_WITH_STATICS, 500);

        int[] weights = new int[config.size()];
        for (int i = 0; i < config.values().size(); i++)
            weights[i] = config.get(OpSelectors.OperationKind.values()[i]);
        OpSelectors.OperationSelector selector = OpSelectors.OperationSelector.weighted(Surjections.weights(weights),
        OpSelectors.OperationKind.values());

        OpSelectors.PureRng rng = new OpSelectors.PCGFast(1);
        OpSelectors.PdSelector pdSelector = new OpSelectors.DefaultPdSelector(rng, 10, 10);
        OpSelectors.DescriptorSelector descriptorSelector = new OpSelectors.DefaultDescriptorSelector(rng,
                                                                                                      null,
                                                                                                      selector,
                                                                                                      new Distribution.ConstantDistribution(10),
                                                                                                      100);

        EnumMap<OpSelectors.OperationKind, Integer> m = new EnumMap<OpSelectors.OperationKind, Integer>(OpSelectors.OperationKind.class);
        for (int lts = 0; lts < 1000000; lts++)
        {
            int total = descriptorSelector.operationsPerLts(lts);
            long pd = pdSelector.pd(lts, SCHEMA);
            for (int opId = 0; opId < total; opId++)
            {
                m.compute(descriptorSelector.operationType(pd, lts, opId),
                          (OpSelectors.OperationKind k, Integer old) -> {
                              if (old == null) return 1;
                              else return old + 1;
                          });
            }
        }

        for (OpSelectors.OperationKind l : OpSelectors.OperationKind.values())
        {
            for (OpSelectors.OperationKind r : OpSelectors.OperationKind.values())
            {
                if (l != r)
                {
                    Assert.assertEquals(m.get(l) * 1.0 / m.get(r),
                                        config.get(l) * 1.0 / config.get(r),
                                        (config.get(l) * 1.0 / config.get(r)) * 0.10);
                }
            }
        }
    }
}