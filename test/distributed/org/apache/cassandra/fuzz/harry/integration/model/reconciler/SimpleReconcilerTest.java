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

package org.apache.cassandra.fuzz.harry.integration.model.reconciler;

import java.util.*;

import org.junit.Test;

import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.gen.Surjections;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.model.SelectHelper;
import org.apache.cassandra.harry.model.reconciler.PartitionState;
import org.apache.cassandra.harry.sut.injvm.InJvmSut;
import org.apache.cassandra.harry.operations.CompiledStatement;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.operations.WriteHelper;
import org.apache.cassandra.harry.util.BitSet;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.fuzz.harry.integration.model.IntegrationTestBase;

public class SimpleReconcilerTest extends IntegrationTestBase
{
    public static Surjections.Surjection<SchemaSpec> defaultSchemaSpecGen(String ks, String table)
    {
        return new SchemaGenerators.Builder(ks, () -> table)
               .partitionKeySpec(1, 3,
                                 ColumnSpec.int64Type,
                                 ColumnSpec.asciiType(5, 256))
               .clusteringKeySpec(1, 3,
                                  ColumnSpec.int64Type,
                                  ColumnSpec.asciiType(2, 3),
                                  ColumnSpec.ReversedType.getInstance(ColumnSpec.int64Type),
                                  ColumnSpec.ReversedType.getInstance(ColumnSpec.asciiType(2, 3)))
               .regularColumnSpec(50, 50,
                                  ColumnSpec.int64Type,
                                  ColumnSpec.asciiType(5, 256))
               .staticColumnSpec(50, 50,
                                 ColumnSpec.int64Type,
                                 ColumnSpec.asciiType(4, 256))
               .surjection();
    }

    @Test
    public void testStatics() throws Throwable
    {
        int rowsPerPartition = 50;
        SchemaSpec schema = defaultSchemaSpecGen("harry", "tbl").inflate(1);
        Configuration config = sharedConfiguration(1, schema).build();
        Run run = config.createRun();
        SyntheticTest test = new SyntheticTest(run.rng, schema);
        beforeEach();
        cluster.schemaChange(schema.compile().cql());

        ModelState state = new ModelState(new HashMap<>());
        InJvmSut sut = (InJvmSut) run.sut;
        Random rng = new Random(1);

        int partitionIdx = 0;

        for (int i = 0; i < 100; i++)
        {
            BitSet subset = BitSet.allUnset(schema.allColumns.size());
            for (int j = 0; j < subset.size(); j++)
            {
                if (rng.nextBoolean())
                    subset.set(j);
            }
            if (!isValidSubset(schema.allColumns, subset))
                continue;
            int pdIdx = partitionIdx++;
            long pd = test.pd(pdIdx);

            for (int j = 0; j < 10; j++)
            {
                int cdIdx = rng.nextInt(rowsPerPartition);
                long cd = test.cd(pdIdx, cdIdx);

                long[] vds = run.descriptorSelector.descriptors(pd, cd, state.lts, 0, schema.regularColumns,
                                                                schema.regularColumnsMask(),
                                                                subset,
                                                                schema.regularColumnsOffset);
                long[] sds = run.descriptorSelector.descriptors(pd, cd, state.lts, 0, schema.staticColumns,
                                                                schema.staticColumnsMask,
                                                                subset,
                                                                schema.staticColumnsOffset);

                CompiledStatement statement = WriteHelper.inflateUpdate(schema, pd, cd, vds, sds, run.clock.rts(state.lts));
                sut.cluster.coordinator(1).execute(statement.cql(), ConsistencyLevel.QUORUM, statement.bindings());

                PartitionState partitionState = state.state.get(pd);
                if (partitionState == null)
                {
                    partitionState = new PartitionState(pd, -1,  schema);
                    state.state.put(pd, partitionState);
                }

                partitionState.writeStaticRow(sds, state.lts);
                partitionState.write(cd, vds, state.lts, true);

                state.lts++;
            }
        }

        // Validate that all partitions correspond to our expectations
        for (Long pd : state.state.keySet())
        {
            ArrayList<Long> clusteringDescriptors = new ArrayList<>(state.state.get(pd).rows().keySet());

            // TODO: allow sub-selection
            // Try different column subsets
            for (int i = 0; i < 10; i++)
            {
                BitSet bitset = BitSet.allUnset(schema.allColumns.size());
                for (int j = 0; j < bitset.size(); j++)
                {
                    if (rng.nextBoolean())
                        bitset.set(j);
                }
                Set<ColumnSpec<?>> subset = i == 0 ? null : subset(schema.allColumns, bitset);
                if (subset != null && !isValidSubset(schema.allColumns, bitset))
                    continue;

                int a = rng.nextInt(clusteringDescriptors.size());
                long cd1tmp = clusteringDescriptors.get(a);
                long cd2tmp;
                int b;
                while (true)
                {
                    b = rng.nextInt(clusteringDescriptors.size());
                    long tmp = clusteringDescriptors.get(b);
                    if (tmp != cd1tmp)
                    {
                        cd2tmp = tmp;
                        break;
                    }
                }

                long cd1 = Math.min(cd1tmp, cd2tmp);
                long cd2 = Math.max(cd1tmp, cd2tmp);

                for (boolean reverse : new boolean[]{ true, false })
                {
                    Query query;

                    query = Query.selectAllColumns(schema, pd, reverse);

                    QuiescentChecker.validate(schema,
                                              run.tracker,
                                              subset,
                                              state.state.get(pd),
                                              SelectHelper.execute(sut, run.clock, query, subset),
                                              query);

                    query = Query.singleClustering(schema, pd, cd1, false);
                    QuiescentChecker.validate(schema,
                                              run.tracker,
                                              subset,
                                              state.state.get(pd).apply(query),
                                              SelectHelper.execute(sut, run.clock, query, subset),
                                              query);

                    for (boolean isGt : new boolean[]{ true, false })
                    {
                        for (boolean isEquals : new boolean[]{ true, false })
                        {
                            try
                            {
                                query = Query.clusteringSliceQuery(schema, pd, cd1, rng.nextLong(), isGt, isEquals, reverse);
                            }
                            catch (IllegalArgumentException impossibleQuery)
                            {
                                continue;
                            }

                            QuiescentChecker.validate(schema,
                                                      run.tracker,
                                                      subset,
                                                      state.state.get(pd).apply(query),
                                                      SelectHelper.execute(sut, run.clock, query, subset),
                                                      query);
                        }
                    }

                    for (boolean isMinEq : new boolean[]{ true, false })
                    {
                        for (boolean isMaxEq : new boolean[]{ true, false })
                        {
                            try
                            {
                                query = Query.clusteringRangeQuery(schema, pd, cd1, cd2, rng.nextLong(), isMinEq, isMaxEq, reverse);
                            }
                            catch (IllegalArgumentException impossibleQuery)
                            {
                                continue;
                            }
                            QuiescentChecker.validate(schema,
                                                      run.tracker,
                                                      subset,
                                                      state.state.get(pd).apply(query),
                                                      SelectHelper.execute(sut, run.clock, query, subset),
                                                      query);
                        }
                    }
                }
            }
        }
    }

    public static boolean isValidSubset(List<ColumnSpec<?>> superset, BitSet bitSet)
    {
        boolean hasRegular = false;
        for (int i = 0; i < superset.size(); i++)
        {
            ColumnSpec<?> column = superset.get(i);
            if (column.kind == ColumnSpec.Kind.PARTITION_KEY && !bitSet.isSet(i))
                return false;
            if (column.kind == ColumnSpec.Kind.CLUSTERING && !bitSet.isSet(i))
                return false;
            if (column.kind == ColumnSpec.Kind.REGULAR && bitSet.isSet(i))
                hasRegular = true;
        }

        return hasRegular;
    }

    public static Set<ColumnSpec<?>> subset(List<ColumnSpec<?>> superset, BitSet bitSet)
    {
        Set<ColumnSpec<?>> subset = new HashSet<>();
        for (int i = 0; i < superset.size(); i++)
        {
            if (bitSet.isSet(i))
                subset.add(superset.get(i));
        }

        return subset;
    }

    public static Set<ColumnSpec<?>> randomSubset(List<ColumnSpec<?>> superset, Random e)
    {
        Set<ColumnSpec<?>> set = new HashSet<>();
        boolean hadRegular = false;
        for (ColumnSpec<?> v : superset)
        {
            // TODO: allow selecting without partition and clustering key, too
            if (e.nextBoolean() || v.kind == ColumnSpec.Kind.CLUSTERING || v.kind == ColumnSpec.Kind.PARTITION_KEY)
            {
                set.add(v);
                hadRegular |= v.kind == ColumnSpec.Kind.REGULAR;
            }
        }

        // TODO: this is an oversimplification and a workaround for "Invalid restrictions on clustering columns since the UPDATE statement modifies only static columns"
        if (!hadRegular)
            return randomSubset(superset, e);

        return set;
    }

    public static <T> BitSet subsetToBitset(List<T> superset, Set<T> subset)
    {
        BitSet bitSet = new BitSet.BitSet64Bit(superset.size());
        for (int i = 0; i < superset.size(); i++)
        {
            if (subset.contains(superset.get(i)))
                bitSet.set(i);
        }
        return bitSet;
    }

    public static class ModelState
    {
        public long lts = 0;
        public final Map<Long, PartitionState> state;

        public ModelState(Map<Long, PartitionState> state)
        {
            this.state = state;
        }
    }

    public static class SyntheticTest // TODO: horrible name
    {
        private static long PD_STREAM = System.nanoTime();
        private final OpSelectors.PureRng rng;
        private final SchemaSpec schema;

        public SyntheticTest(OpSelectors.PureRng rng, SchemaSpec schema)
        {
            this.schema = schema;
            this.rng = rng;
        }

        public long pd(int pdIdx)
        {
            long pd = this.rng.randomNumber(pdIdx + 1, PD_STREAM);
            long adjusted = schema.adjustPdEntropy(pd);
            assert adjusted == pd : "Partition descriptors not utilising all entropy bits are not supported.";
            return pd;
        }

        public long pdIdx(long pd)
        {
            return this.rng.sequenceNumber(pd, PD_STREAM) - 1;
        }

        public long cd(int pdIdx, int cdIdx)
        {
            long cd = this.rng.randomNumber(cdIdx + 1, pd(pdIdx));
            long adjusted = schema.adjustCdEntropy(cd);
            assert adjusted == cd : "Clustering descriptors not utilising all entropy bits are not supported.";
            return cd;
        }

        public long cdIdx(long pd)
        {
            return this.rng.sequenceNumber(pd, PD_STREAM) - 1;
        }
    }
}
