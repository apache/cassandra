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
import java.util.List;
import java.util.function.Consumer;

import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.util.BitSet;
import org.apache.cassandra.harry.visitors.GeneratingVisitor;
import org.apache.cassandra.harry.visitors.ReplayingVisitor;
import org.apache.cassandra.harry.visitors.VisitExecutor;

class SingleOperationVisitBuilder implements SingleOperationBuilder
{
    // TODO: singleton collection for this op class
    private final List<VisitExecutor.BaseOperation> operations;
    private final PartitionVisitStateImpl partitionState;

    private final long lts;
    private final long pd;

    private final OpSelectors.PureRng rng;

    private final OpSelectors.DescriptorSelector descriptorSelector;
    private final ValueHelper valueHelper;
    private final SchemaSpec schema;

    private final Consumer<ReplayingVisitor.Visit> appendToLog;
    private final WithEntropySource rngSupplier = new WithEntropySource();

    private int opIdCounter;

    public SingleOperationVisitBuilder(PartitionVisitStateImpl partitionState,
                                       long lts,
                                       OpSelectors.PureRng rng,
                                       OpSelectors.DescriptorSelector descriptorSelector,
                                       SchemaSpec schema,
                                       ValueHelper valueHelper,
                                       Consumer<ReplayingVisitor.Visit> appendToLog)
    {
        this.operations = new ArrayList<>();
        this.partitionState = partitionState;

        this.pd = partitionState.pd;
        this.lts = lts;

        this.rng = rng;

        this.descriptorSelector = descriptorSelector;
        this.valueHelper = valueHelper;
        this.schema = schema;

        this.appendToLog = appendToLog;
        this.opIdCounter = 0;
    }

    @Override
    public SingleOperationVisitBuilder insert()
    {
        int clusteringOffset = rngSupplier.withSeed(lts).nextInt(0, partitionState.possibleCds.length - 1);
        return insert(clusteringOffset);
    }

    @Override
    public SingleOperationVisitBuilder insert(int rowIdx)
    {
        int opId = opIdCounter++;
        long cd = partitionState.possibleCds[rowIdx];
        operations.add(new GeneratingVisitor.GeneratedWriteOp(lts, pd, cd, opId,
                                                              OpSelectors.OperationKind.INSERT)
        {
            public long[] vds()
            {
                return descriptorSelector.vds(pd, cd, lts, opId, kind(), schema);
            }
        });
        end();
        return this;
    }

    @Override
    public SingleOperationVisitBuilder insert(int rowIdx, long[] valueIdxs)
    {
        assert valueIdxs.length == valueHelper.regularColumns.size();
        int opId = opIdCounter++;
        long cd = partitionState.possibleCds[rowIdx];
        operations.add(new GeneratingVisitor.GeneratedWriteOp(lts, pd, cd, opId,
                                                              OpSelectors.OperationKind.INSERT)
        {
            public long[] vds()
            {
                long[] vds = new long[valueIdxs.length];
                for (int i = 0; i < valueHelper.regularColumns.size(); i++)
                {
                    vds[i] = valueHelper.descriptorGenerators
                             .get(valueHelper.regularColumns.get(i).name)
                             .inflate(valueIdxs[i]);
                }
                return vds;
            }
        });
        end();
        return this;
    }

    @Override
    public SingleOperationBuilder insert(int rowIdx, long[] valueIdxs, long[] sValueIdxs)
    {
        assert valueIdxs.length == valueHelper.regularColumns.size();
        assert sValueIdxs.length == valueHelper.staticColumns.size();
        int opId = opIdCounter++;
        long cd = partitionState.possibleCds[rowIdx];
        operations.add(new GeneratingVisitor.GeneratedWriteWithStaticOp(lts, pd, cd, opId,
                                                                        OpSelectors.OperationKind.INSERT_WITH_STATICS)
        {
            @Override
            public long[] vds()
            {
                long[] vds = new long[valueIdxs.length];
                for (int i = 0; i < valueHelper.regularColumns.size(); i++)
                {
                    vds[i] = valueHelper.descriptorGenerators
                             .get(valueHelper.regularColumns.get(i).name)
                             .inflate(valueIdxs[i]);
                }
                return vds;
            }

            @Override
            public long[] sds()
            {
                long[] sds = new long[sValueIdxs.length];
                for (int i = 0; i < valueHelper.staticColumns.size(); i++)
                {
                    sds[i] = valueHelper.descriptorGenerators
                             .get(valueHelper.staticColumns.get(i).name)
                             .inflate(sValueIdxs[i]);
                }
                return sds;
            }
        });
        end();
        return this;
    }

    @Override
    public SingleOperationVisitBuilder deletePartition()
    {
        int opId = opIdCounter++;
        operations.add(new GeneratingVisitor.GeneratedDeleteOp(lts, pd, opId, OpSelectors.OperationKind.DELETE_PARTITION,
                                                               Query.selectAllColumns(schema, pd, false)));
        end();
        return this;
    }

    @Override
    public SingleOperationVisitBuilder deleteRow()
    {
        int opId = opIdCounter++;
        long queryDescriptor = rng.next(opId, lts);
        rngSupplier.withSeed(queryDescriptor, (rng) -> {
            int cdIdx = rngSupplier.withSeed(queryDescriptor).nextInt(partitionState.possibleCds.length);
            long cd = partitionState.possibleCds[cdIdx];
            operations.add(new GeneratingVisitor.GeneratedDeleteRowOp(lts, pd, cd, opId,
                                                                      OpSelectors.OperationKind.DELETE_ROW));
        });
        end();
        return this;
    }

    @Override
    public SingleOperationVisitBuilder deleteRow(int cdIdx)
    {
        int opId = opIdCounter++;
        long cd = partitionState.possibleCds[cdIdx];
        operations.add(new GeneratingVisitor.GeneratedDeleteRowOp(lts, pd, cd, opId,
                                                                  OpSelectors.OperationKind.DELETE_ROW));
        end();
        return this;
    }

    @Override
    public SingleOperationVisitBuilder deleteColumns()
    {
        int opId = opIdCounter++;
        long queryDescriptor = rng.next(opId, lts);
        rngSupplier.withSeed(queryDescriptor, (rng) -> {
            int cdIdx = rng.nextInt(partitionState.possibleCds.length);
            long cd = partitionState.possibleCds[cdIdx];
            BitSet columns = descriptorSelector.columnMask(pd, lts, opId, OpSelectors.OperationKind.DELETE_COLUMN);
            operations.add(new GeneratingVisitor.GeneratedDeleteColumnsOp(lts, pd, cd, opId,
                                                                          OpSelectors.OperationKind.DELETE_COLUMN, columns));
        });
        end();
        return this;
    }

    @Override
    public SingleOperationVisitBuilder deleteRowRange()
    {
        int opId = opIdCounter++;
        long queryDescriptor = rng.next(opId, lts);
        rngSupplier.withSeed(queryDescriptor, (rng) -> {
            Query query = null;
            while (query == null)
            {
                try
                {
                    long cd1 = partitionState.possibleCds[rng.nextInt(partitionState.possibleCds.length)];
                    long cd2 = partitionState.possibleCds[rng.nextInt(partitionState.possibleCds.length)];
                    while (cd2 == cd1)
                        cd2 = partitionState.possibleCds[rng.nextInt(partitionState.possibleCds.length)];

                    boolean isMinEq = rng.nextBoolean();
                    boolean isMaxEq = rng.nextBoolean();
                    query = Query.clusteringRangeQuery(schema, pd, cd1, cd2, queryDescriptor, isMinEq, isMaxEq, false);
                    break;
                }
                catch (IllegalArgumentException retry)
                {
                    continue;
                }
            }
            operations.add(new GeneratingVisitor.GeneratedDeleteOp(lts, pd, opId, OpSelectors.OperationKind.DELETE_SLICE, query));
        });
        end();
        return this;
    }

    @Override
    public SingleOperationVisitBuilder deleteRowRange(int lowBoundRowIdx, int highBoundRowIdx, boolean isMinEq, boolean isMaxEq)
    {
        int opId = opIdCounter++;
        long queryDescriptor = rng.next(opId, lts);

        long cd1 = partitionState.possibleCds[lowBoundRowIdx];
        long cd2 = partitionState.possibleCds[highBoundRowIdx];
        Query query = Query.clusteringRangeQuery(schema, pd, cd1, cd2, queryDescriptor, isMinEq, isMaxEq, false);
        operations.add(new GeneratingVisitor.GeneratedDeleteOp(lts, pd, opId, OpSelectors.OperationKind.DELETE_SLICE, query));
        end();
        return this;
    }

    @Override
    public SingleOperationVisitBuilder deleteRowSlice()
    {
        int opId = opIdCounter++;
        long queryDescriptor = rng.next(opId, lts);
        rngSupplier.withSeed(queryDescriptor, (rng) -> {
            Query query = null;
            while (query == null)
            {
                try
                {
                    int cdIdx = rng.nextInt(partitionState.possibleCds.length);
                    long cd = partitionState.possibleCds[cdIdx];

                    boolean isGt = rng.nextBoolean();
                    boolean isEquals = rng.nextBoolean();
                    query = Query.clusteringSliceQuery(schema, pd, cd, queryDescriptor, isGt, isEquals, false);
                    break;
                }
                catch (IllegalArgumentException retry)
                {
                    continue;
                }
            }
            operations.add(new GeneratingVisitor.GeneratedDeleteOp(lts, pd, opId, OpSelectors.OperationKind.DELETE_SLICE, query));
        });
        end();
        return this;
    }

    int size()
    {
        return this.operations.size();
    }

    void end()
    {
        VisitExecutor.Operation[] ops = new VisitExecutor.Operation[operations.size()];
        operations.toArray(ops);
        ReplayingVisitor.Visit visit = new ReplayingVisitor.Visit(lts, pd, ops);
        appendToLog.accept(visit);
    }

    private static class WithEntropySource
    {
        private final EntropySource entropySource = new JdkRandomEntropySource(0);

        public void withSeed(long seed, Consumer<EntropySource> rng)
        {
            entropySource.seed(seed);
            rng.accept(entropySource);
        }

        public EntropySource withSeed(long seed)
        {
            entropySource.seed(seed);
            return entropySource;
        }
    }
}
