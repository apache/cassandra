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

import java.io.Closeable;
import java.util.function.Consumer;

import org.apache.cassandra.harry.gen.IndexGenerators;
import org.apache.cassandra.harry.gen.ValueGenerators;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.util.BitSet;

public class MultiOperationVisitBuilder extends SingleOperationVisitBuilder implements Closeable
{
    MultiOperationVisitBuilder(long lts, ValueGenerators valueGenerators, IndexGenerators indexGenerators, Consumer<Visit> appendToLog)
    {
        super(lts, valueGenerators, indexGenerators, appendToLog);
    }

    @Override
    public MultiOperationVisitBuilder custom(Runnable runnable, String tag)
    {
        super.custom(runnable, tag);
        return this;
    }

    @Override
    public MultiOperationVisitBuilder update(int pdIdx, int rowIdx, int[] valueIdxs, int[] sValueIdxs)
    {
        super.update(pdIdx, rowIdx, valueIdxs, sValueIdxs);
        return this;
    }

    @Override
    public MultiOperationVisitBuilder insert(int partitionIdx, int rowIdx, int[] valueIdxs, int[] sValueIdxs)
    {
        super.insert(partitionIdx, rowIdx, valueIdxs, sValueIdxs);
        return this;
    }

    @Override
    public MultiOperationVisitBuilder selectRowRange(int partitionIdx, int lowBoundRowIdx, int highBoundRowIdx,
                                                     int nonEqFrom, boolean includeLowerBound, boolean includeHighBound)
    {
        super.selectRowRange(partitionIdx, lowBoundRowIdx, highBoundRowIdx, nonEqFrom, includeLowerBound, includeHighBound);
        return this;
    }

    @Override
    public MultiOperationVisitBuilder selectPartition(int partitionIdx)
    {
        super.selectPartition(partitionIdx);
        return this;
    }

    @Override
    public MultiOperationVisitBuilder selectRow(int partitionIdx, int rowIdx)
    {
        super.selectRow(partitionIdx, rowIdx);
        return this;
    }

    @Override
    public MultiOperationVisitBuilder selectRowSliceByLowerBound(int partitionIdx, int lowBoundRowIdx, int nonEqFrom, boolean isEq)
    {
        super.selectRowSliceByLowerBound(partitionIdx, lowBoundRowIdx, nonEqFrom, isEq);
        return this;
    }

    @Override
    public MultiOperationVisitBuilder selectRowSliceByUpperBound(int partitionIdx, int highBoundRowIdx, int nonEqFrom, boolean isEq)
    {
        super.selectRowSliceByUpperBound(partitionIdx, highBoundRowIdx, nonEqFrom, isEq);
        return this;
    }

    @Override
    public MultiOperationVisitBuilder deletePartition(int partitionIdx)
    {
        super.deletePartition(partitionIdx);
        return this;
    }

    @Override
    public MultiOperationVisitBuilder deleteRow(int partitionIdx, int rowIdx)
    {
        super.deleteRow(partitionIdx, rowIdx);
        return this;
    }

    @Override
    public MultiOperationVisitBuilder deleteColumns(int partitionIdx, int rowIdx, BitSet regularSelection, BitSet staticSelection)
    {
        super.deleteColumns(partitionIdx, rowIdx, regularSelection, staticSelection);
        return this;
    }

    @Override
    public MultiOperationVisitBuilder deleteRowSliceByLowerBound(int partitionIdx, int lowBoundRowIdx, int nonEqFrom, boolean isEq)
    {
        super.deleteRowSliceByLowerBound(partitionIdx, lowBoundRowIdx, nonEqFrom, isEq);
        return this;
    }

    @Override
    public MultiOperationVisitBuilder deleteRowSliceByUpperBound(int partitionIdx, int highBoundRowIdx, int nonEqFrom, boolean isEq)
    {
        super.deleteRowSliceByUpperBound(partitionIdx, highBoundRowIdx, nonEqFrom, isEq);
        return this;
    }

    @Override
    public MultiOperationVisitBuilder deleteRowRange(int pdIdx, int lowerBoundRowIdx, int upperBoundRowIdx,
                                                     int nonEqFrom, boolean includeLowerBound, boolean includeUpperBound)
    {
        super.deleteRowRange(pdIdx, lowerBoundRowIdx, upperBoundRowIdx,
                             nonEqFrom, includeLowerBound, includeUpperBound);
        return this;
    }

    @Override
    public MultiOperationVisitBuilder select(int partitionIdx,
                                             IdxRelation[] ckRelations,
                                             IdxRelation[] regularRelations,
                                             IdxRelation[] staticRelations)
    {
        super.select(partitionIdx, ckRelations, regularRelations, staticRelations);
        return this;
    }

    @Override
    Visit build()
    {
        throw new IllegalStateException("Shuold not be called directly, use auto-close");
    }

    Visit buildInternal()
    {
        return super.build();
    }


    @Override
    int size()
    {
        return super.size();
    }

    public void close()
    {
        buildInternal();
    }
}