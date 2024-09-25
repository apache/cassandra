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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.gen.Bijections;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.IndexGenerators;
import org.apache.cassandra.harry.gen.InvertibleGenerator;
import org.apache.cassandra.harry.gen.ValueGenerators;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.util.BitSet;
import org.apache.cassandra.harry.util.IteratorsUtil;

import static org.apache.cassandra.harry.SchemaSpec.cumulativeEntropy;
import static org.apache.cassandra.harry.SchemaSpec.forKeys;
import static org.apache.cassandra.harry.gen.InvertibleGenerator.fromType;

// TODO: either create or replay timestamps out of order
public class HistoryBuilder implements SingleOperationBuilder, Model.Replay
{
    protected final IndexedValueGenerators valueGenerators;
    protected final IndexGenerators indexGenerators;

    protected int nextOpIdx = 0;

    // TODO: would be great to have a very simple B-Tree here
    protected final Map<Long, Visit> log;

    public static HistoryBuilder fromSchema(SchemaSpec schemaSpec, long seed, int population)
    {
        IndexedValueGenerators generators = valueGenerators(schemaSpec, seed, population);
        return new HistoryBuilder(generators, IndexGenerators.withDefaults(generators));
    }

    public HistoryBuilder(ValueGenerators generators)
    {
        this((IndexedValueGenerators) generators, IndexGenerators.withDefaults(generators));
    }

    public HistoryBuilder(IndexedValueGenerators valueGenerators,
                          IndexGenerators indexGenerators)
    {
        this.log = new HashMap<>();
        this.valueGenerators = valueGenerators;
        this.indexGenerators = indexGenerators;
    }

    public IndexedValueGenerators valueGenerators()
    {
        return valueGenerators;
    }

    public int size()
    {
        return log.size();
    }

    @Override
    public Iterator<Visit> iterator()
    {
        return new Iterator<>()
        {
            long replayed = 0;

            public boolean hasNext()
            {
                return replayed < nextOpIdx;
            }

            public Visit next()
            {
                return log.get(replayed++);
            }
        };
    }

    @Override
    public Visit replay(long lts)
    {
        return log.get(lts);
    }

    @Override
    public Operations.Operation replay(long lts, int opId)
    {
        return replay(lts).operations[opId];
    }

    SingleOperationVisitBuilder singleOpVisitBuilder()
    {
        long visitLts = nextOpIdx++;
        return new SingleOperationVisitBuilder(visitLts,
                                               valueGenerators,
                                               indexGenerators,
                                               (visit) -> log.put(visit.lts, visit));
    }

    ;

    public MultiOperationVisitBuilder multistep()
    {
        long visitLts = nextOpIdx++;
        return new MultiOperationVisitBuilder(visitLts,
                                              valueGenerators,
                                              indexGenerators,
                                              visit -> log.put(visit.lts, visit));
    }

    @Override
    public SingleOperationBuilder custom(Runnable runnable, String tag)
    {
        singleOpVisitBuilder().custom(runnable, tag);
        return this;
    }

    @Override
    public SingleOperationBuilder custom(OperationFactory factory)
    {
        singleOpVisitBuilder().custom(factory);
        return this;
    }

    @Override
    public SingleOperationBuilder update()
    {
        singleOpVisitBuilder().update();
        return this;
    }

    @Override
    public SingleOperationBuilder update(int pdIdx)
    {
        singleOpVisitBuilder().update(pdIdx);
        return this;
    }

    @Override
    public SingleOperationBuilder update(int pdIdx, int cdIdx)
    {
        singleOpVisitBuilder().update(pdIdx, cdIdx);
        return this;
    }

    @Override
    public SingleOperationBuilder update(int pdIdx, int rowIdx, int[] valueIdxs, int[] sValueIdxs)
    {
        singleOpVisitBuilder().update(pdIdx, rowIdx, valueIdxs, sValueIdxs);
        return this;
    }

    @Override
    public SingleOperationBuilder insert()
    {
        singleOpVisitBuilder().insert();
        return this;
    }

    @Override
    public SingleOperationBuilder insert(int pdIdx)
    {
        singleOpVisitBuilder().insert(pdIdx);
        return this;
    }

    @Override
    public SingleOperationBuilder insert(int pdIdx, int cdIdx)
    {
        singleOpVisitBuilder().insert(pdIdx, cdIdx);
        return this;
    }

    @Override
    public SingleOperationBuilder insert(int pdIdx, int rowIdx, int[] valueIdxs, int[] sValueIdxs)
    {
        singleOpVisitBuilder().insert(pdIdx, rowIdx, valueIdxs, sValueIdxs);
        return this;
    }

    @Override
    public SingleOperationBuilder deleteRowRange(int pdIdx, int lowerBoundRowIdx, int upperBoundRowIdx,
                                                 int nonEqFrom, boolean includeLowerBound, boolean includeUpperBound)
    {
        singleOpVisitBuilder().deleteRowRange(pdIdx, lowerBoundRowIdx, upperBoundRowIdx,
                                              nonEqFrom, includeLowerBound, includeUpperBound);
        return this;
    }

    @Override
    public SingleOperationBuilder select(int pdIdx, IdxRelation[] ckRelations, IdxRelation[] regularRelations, IdxRelation[] staticRelations)
    {
        singleOpVisitBuilder().select(pdIdx, ckRelations, regularRelations, staticRelations);
        return this;
    }

    @Override
    public SingleOperationBuilder selectRowRange(int pdIdx, int lowerBoundRowIdx, int upperBoundRowIdx,
                                                 int nonEqFrom, boolean includeLowerBound, boolean includeUpperBound)
    {
        singleOpVisitBuilder().selectRowRange(pdIdx, lowerBoundRowIdx, upperBoundRowIdx,
                                              nonEqFrom, includeLowerBound, includeUpperBound);
        return this;
    }

    @Override
    public SingleOperationBuilder selectPartition(int pdIdx)
    {
        singleOpVisitBuilder().selectPartition(pdIdx);
        return this;
    }

    @Override
    public SingleOperationBuilder selectPartition(int pdIdx, Operations.ClusteringOrderBy orderBy)
    {
        singleOpVisitBuilder().selectPartition(pdIdx, orderBy);
        return this;
    }

    @Override
    public SingleOperationBuilder selectRow(int pdIdx, int rowIdx)
    {
        singleOpVisitBuilder().selectRow(pdIdx, rowIdx);
        return this;
    }

    @Override
    public SingleOperationBuilder selectRowSliceByLowerBound(int pdIdx, int lowerBoundRowIdx, int nonEqFrom, boolean isEq)
    {
        singleOpVisitBuilder().selectRowSliceByLowerBound(pdIdx, lowerBoundRowIdx, nonEqFrom, isEq);
        return this;
    }

    @Override
    public SingleOperationBuilder selectRowSliceByUpperBound(int pdIdx, int upperBoundRowIdx, int nonEqFrom, boolean isEq)
    {
        singleOpVisitBuilder().selectRowSliceByUpperBound(pdIdx, upperBoundRowIdx, nonEqFrom, isEq);
        return this;
    }

    @Override
    public SingleOperationBuilder deletePartition(int pdIdx)
    {
        singleOpVisitBuilder().deletePartition(pdIdx);
        return this;
    }

    @Override
    public SingleOperationBuilder deleteRow(int pdIdx, int rowIdx)
    {
        singleOpVisitBuilder().deleteRow(pdIdx, rowIdx);
        return this;
    }

    @Override
    public SingleOperationBuilder deleteColumns(int pdIdx, int rowIdx, BitSet regularSelection, BitSet staticSelection)
    {
        singleOpVisitBuilder().deleteRow(pdIdx, rowIdx);
        return this;
    }

    @Override
    public SingleOperationBuilder deleteRowSliceByLowerBound(int pdIdx, int lowerBoundRowIdx, int nonEqFrom, boolean isEq)
    {
        singleOpVisitBuilder().deleteRowSliceByLowerBound(pdIdx, lowerBoundRowIdx, nonEqFrom, isEq);
        return this;
    }

    @Override
    public SingleOperationBuilder deleteRowSliceByUpperBound(int pdIdx, int upperBoundRowIdx, int nonEqFrom, boolean isEq)
    {
        singleOpVisitBuilder().deleteRowSliceByUpperBound(pdIdx, upperBoundRowIdx, nonEqFrom, isEq);
        return this;
    }


    /**
     * Indexed bijection allows to decouple descriptor order from value order, which makes data generation simpler.
     * <p>
     * For regular Harry bijections, this is done at no cost, since values are inflated in a way that preserves
     * descriptor order, which means that idx order is consistent with descriptor order and consistent with value order.
     * <p>
     * An indexed bijection allows order to be established via index, and use descriptor simply as a seed for random values.
     */
    public interface IndexedBijection<T> extends Bijections.Bijection<T>
    {
        int idxFor(long descriptor);

        long descriptorAt(int idx);

        @Override
        default String toString(long pd)
        {
            if (pd == MagicConstants.UNSET_DESCR)
                return Integer.toString(MagicConstants.UNSET_IDX);

            if (pd == MagicConstants.NIL_DESCR)
                return Integer.toString(MagicConstants.NIL_IDX);

            return Integer.toString(idxFor(pd));
        }
    }

    public static IndexedValueGenerators valueGenerators(SchemaSpec schema, long seed)
    {
        return valueGenerators(schema, seed, 1000);
    }

    @SuppressWarnings({ "unchecked" })
    public static IndexedValueGenerators valueGenerators(SchemaSpec schema, long seed, int populationPerColumn)
    {
        List<Comparator<Object>> pkComparators = new ArrayList<>();
        List<Comparator<Object>> ckComparators = new ArrayList<>();
        List<Comparator<Object>> regularComparators = new ArrayList<>();
        List<Comparator<Object>> staticComparators = new ArrayList<>();

        EntropySource rng = new JdkRandomEntropySource(seed);
        for (int i = 0; i < schema.partitionKeys.size(); i++)
            pkComparators.add((Comparator<Object>) schema.partitionKeys.get(i).type.comparator());
        for (int i = 0; i < schema.clusteringKeys.size(); i++)
            ckComparators.add((Comparator<Object>) schema.clusteringKeys.get(i).type.comparator());
        for (int i = 0; i < schema.regularColumns.size(); i++)
            regularComparators.add((Comparator<Object>) schema.regularColumns.get(i).type.comparator());
        for (int i = 0; i < schema.staticColumns.size(); i++)
            staticComparators.add((Comparator<Object>) schema.staticColumns.get(i).type.comparator());

        Map<ColumnSpec<?>, InvertibleGenerator<Object>> map = new HashMap<>();
        for (ColumnSpec<?> column : IteratorsUtil.concat(schema.regularColumns, schema.staticColumns))
            map.computeIfAbsent(column, (a) -> (InvertibleGenerator<Object>) fromType(rng, populationPerColumn, column));

        // TODO: empty gen
        return new IndexedValueGenerators(new InvertibleGenerator<>(rng, cumulativeEntropy(schema.partitionKeys), populationPerColumn, forKeys(schema.partitionKeys), keyComparator(schema.partitionKeys)),
                                          new InvertibleGenerator<>(rng, cumulativeEntropy(schema.clusteringKeys), populationPerColumn, forKeys(schema.clusteringKeys), keyComparator(schema.clusteringKeys)),
                                          schema.regularColumns.stream()
                                                               .map(map::get)
                                                               .collect(Collectors.toList()),
                                          schema.staticColumns.stream()
                                                              .map(map::get)
                                                              .collect(Collectors.toList()),
                                          pkComparators,
                                          ckComparators,
                                          regularComparators,
                                          staticComparators);
    }

    public static class IndexedValueGenerators extends ValueGenerators
    {
        public IndexedValueGenerators(IndexedBijection<Object[]> pkGen,
                                      IndexedBijection<Object[]> ckGen,
                                      List<IndexedBijection<Object>> regularColumnGens,
                                      List<IndexedBijection<Object>> staticColumnGens,
                                      List<Comparator<Object>> pkComparators,
                                      List<Comparator<Object>> ckComparators,
                                      List<Comparator<Object>> regularComparators,
                                      List<Comparator<Object>> staticComparators)
        {
            super(pkGen, ckGen,
                  (List<Bijections.Bijection<Object>>) (List<?>) regularColumnGens,
                  (List<Bijections.Bijection<Object>>) (List<?>) staticColumnGens,
                  pkComparators, ckComparators, regularComparators, staticComparators);
        }

        @Override
        public IndexedBijection<Object[]> pkGen()
        {
            return (IndexedBijection<Object[]>) super.pkGen();
        }

        @Override
        public IndexedBijection<Object[]> ckGen()
        {
            return (IndexedBijection<Object[]>) super.ckGen();
        }

        @Override
        public IndexedBijection<Object> regularColumnGen(int idx)
        {
            return (IndexedBijection<Object>) super.regularColumnGen(idx);
        }

        @Override
        public IndexedBijection<Object> staticColumnGen(int idx)
        {
            return (IndexedBijection<Object>) super.staticColumnGen(idx);
        }
    }


    private static Comparator<Object[]> keyComparator(List<ColumnSpec<?>> columns)
    {
        return (o1, o2) -> compareKeys(columns, o1, o2);
    }

    public static int compareKeys(List<ColumnSpec<?>> columns, Object[] v1, Object[] v2)
    {
        assert v1.length == v2.length : String.format("Values should be of same length: %d != %d\n%s\n%s",
                                                      v1.length, v2.length, Arrays.toString(v1), Arrays.toString(v2));

        for (int i = 0; i < v1.length; i++)
        {
            int res;
            ColumnSpec column = columns.get(i);
            if (column.type.isReversed())
                res = column.type.comparator().reversed().compare(v1[i], v2[i]);
            else
                res = column.type.comparator().compare(v1[i], v2[i]);
            if (res != 0)
                return res;
        }
        return 0;
    }
}