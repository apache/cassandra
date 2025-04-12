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

import org.apache.cassandra.harry.Relations;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.util.BitSet;
import org.apache.cassandra.harry.util.ThrowingRunnable;

public interface SingleOperationBuilder
{
    default SingleOperationBuilder customThrowing(ThrowingRunnable runnable, String tag)
    {
        return custom(runnable.toRunnable(), tag);
    }
    SingleOperationBuilder custom(Runnable runnable, String tag);
    SingleOperationBuilder custom(OperationFactory factory);

    SingleOperationBuilder update();
    SingleOperationBuilder update(int pdIdx);
    SingleOperationBuilder update(int pdIdx, int cdIdx);
    SingleOperationBuilder update(int pdIdx, int cdIdx, int[] valueIdxs, int[] sValueIdxs);

    SingleOperationBuilder insert();
    SingleOperationBuilder insert(int pdIdx);
    SingleOperationBuilder insert(int pdIdx, int cdIdx);
    SingleOperationBuilder insert(int pdIdx, int cdIdx, int[] valueIdxs, int[] sValueIdxs);

    // TODO: selection
    SingleOperationBuilder selectRowRange(int pdIdx, int lowerBoundRowIdx, int upperBoundRowIdx,
                                          int nonEqFrom, boolean includeLowBound, boolean includeHighBound);
    SingleOperationBuilder selectPartition(int pdIdx);
    SingleOperationBuilder selectPartition(int pdIdx, Operations.ClusteringOrderBy orderBy);
    SingleOperationBuilder selectRow(int pdIdx, int cdIdx);
    SingleOperationBuilder selectRowSliceByLowerBound(int pdIdx, int lowerBoundRowIdx, int nonEqFrom, boolean isEq);
    SingleOperationBuilder selectRowSliceByUpperBound(int pdIdx, int upperBoundRowIdx, int nonEqFrom, boolean isEq);

    SingleOperationBuilder deletePartition(int pdIdx);
    SingleOperationBuilder deleteRow(int pdIdx, int cdIdx);
    SingleOperationBuilder deleteColumns(int pdIdx, int cdIdx, BitSet regularSelection, BitSet staticSelection);
    SingleOperationBuilder deleteRowSliceByLowerBound(int pdIdx, int lowerBoundRowIdx, int nonEqFrom, boolean isEq);
    SingleOperationBuilder deleteRowSliceByUpperBound(int pdIdx, int upperBoundRowIdx, int nonEqFrom, boolean isEq);
    SingleOperationBuilder deleteRowRange(int pdIdx, int lowerBoundRowIdx, int upperBoundRowIdx,
                                          int nonEqFrom, boolean includeLowBound, boolean includeHighBound);

    SingleOperationBuilder select(int pdIdx,
                                  IdxRelation[] ckRelations,
                                  IdxRelation[] regularRelations,
                                  IdxRelation[] staticRelations);

    class IdxRelation
    {
        public final Relations.RelationKind kind;
        public final int idx;
        public final int column;

        public IdxRelation(Relations.RelationKind kind, int idx, int column)
        {
            this.kind = kind;
            this.idx = idx;
            this.column = column;
        }
    }

    interface OperationFactory
    {
        Operations.Operation make(long lts, long opId);
    }
}
