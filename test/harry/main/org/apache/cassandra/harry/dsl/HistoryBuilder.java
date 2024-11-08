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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.cassandra.harry.gen.IndexGenerators;
import org.apache.cassandra.harry.gen.ValueGenerators;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.util.BitSet;

// TODO: either create or replay timestamps out of order
public class HistoryBuilder implements SingleOperationBuilder, Model.Replay
{
    protected final ValueGenerators valueGenerators;
    protected final IndexGenerators indexGenerators;

    protected int nextOpIdx = 0;

    // TODO: would be great to have a very simple B-Tree here
    protected final Map<Long, Visit> log;

    public HistoryBuilder(ValueGenerators valueGenerators)
    {
        this(valueGenerators, IndexGenerators.withDefaults(valueGenerators));
    }

    public HistoryBuilder(ValueGenerators valueGenerators,
                          IndexGenerators indexGenerators)
    {
        this.log = new HashMap<>();
        this.valueGenerators = valueGenerators;
        this.indexGenerators = indexGenerators;
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
}
