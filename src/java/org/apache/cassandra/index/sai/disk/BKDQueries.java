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
package org.apache.cassandra.index.sai.disk;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.disk.v1.BKDReader;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.FutureArrays;

import static org.apache.lucene.index.PointValues.Relation.CELL_INSIDE_QUERY;

//TODO: possible perf improvements when running on Java 9+ after replacing FutureArrays with Arrays; this needs a MR jar;
class BKDQueries
{
    private static final BKDReader.IntersectVisitor MATCH_ALL = new BKDReader.IntersectVisitor()
    {
        @Override
        public boolean visit(byte[] packedValue)
        {
            return true;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            return CELL_INSIDE_QUERY;
        }
    };

    static BKDReader.IntersectVisitor bkdQueryFrom(Expression expression, int numDim, int bytesPerDim)
    {
        if (expression.lower == null && expression.upper == null)
        {
            return MATCH_ALL;
        }

        Bound lower = null ;
        if (expression.lower != null)
        {
            final byte[] lowerBound = toComparableBytes(numDim, bytesPerDim, expression.lower.value.encoded, expression.validator);
            lower = new Bound(lowerBound, !expression.lower.inclusive);
        }

        Bound upper = null;
        if (expression.upper != null)
        {
            final byte[] upperBound = toComparableBytes(numDim, bytesPerDim, expression.upper.value.encoded, expression.validator);
            upper = new Bound(upperBound, !expression.upper.inclusive);
        }

        return new RangeQueryVisitor(numDim, bytesPerDim, lower, upper);
    }

    // TODO: probably move this to TypeUtil
    private static byte[] toComparableBytes(int numDim, int bytesPerDim, ByteBuffer value, AbstractType<?> type)
    {
        byte[] buffer = new byte[TypeUtil.fixedSizeOf(type)];
        assert buffer.length == bytesPerDim * numDim;
        TypeUtil.toComparableBytes(value, type, buffer);
        return buffer;
    }

    private static abstract class RangeQuery implements BKDReader.IntersectVisitor
    {
        final int numDims;
        final int bytesPerDim;

        RangeQuery(int numDims, int bytesPerDim)
        {
            this.numDims = numDims;
            this.bytesPerDim = bytesPerDim;
        }

        int compareUnsigned(byte[] packedValue, int dim, Bound bound)
        {
            final int offset = dim * bytesPerDim;
            return FutureArrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, bound.bound, offset, offset + bytesPerDim);
        }
    }

    private static class Bound
    {
        private final byte[] bound;
        private final boolean exclusive;

        Bound(byte[] bound, boolean exclusive)
        {
            this.bound = bound;
            this.exclusive = exclusive;
        }

        boolean smallerThan(int cmp)
        {
            return cmp > 0 || (cmp == 0 && exclusive);
        }

        boolean greaterThan(int cmp)
        {
            return cmp < 0 || (cmp == 0 && exclusive);
        }
    }

    private static class RangeQueryVisitor extends RangeQuery
    {
        private final Bound lower;
        private final Bound upper;

        private RangeQueryVisitor(int numDims, int bytesPerDim, Bound lower, Bound upper)
        {
            super(numDims, bytesPerDim);
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean visit(byte[] packedValue)
        {
            for (int dim = 0; dim < numDims; dim++)
            {
                if (lower != null)
                {
                    int cmp = compareUnsigned(packedValue, dim, lower);
                    if (lower.greaterThan(cmp))
                    {
                        // value is too low, in this dimension
                        return false;
                    }
                }

                if (upper != null)
                {
                    int cmp = compareUnsigned(packedValue, dim, upper);
                    if (upper.smallerThan(cmp))
                    {
                        // value is too high, in this dimension
                        return false;
                    }
                }
            }

            return true;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            boolean crosses = false;

            for (int dim = 0; dim < numDims; dim++)
            {
                if (lower != null)
                {
                    int maxCmp = compareUnsigned(maxPackedValue, dim, lower);
                    if (lower.greaterThan(maxCmp))
                        return Relation.CELL_OUTSIDE_QUERY;

                    int minCmp = compareUnsigned(minPackedValue, dim, lower);
                    crosses |= lower.greaterThan(minCmp);
                }

                if (upper != null)
                {
                    int minCmp = compareUnsigned(minPackedValue, dim, upper);
                    if (upper.smallerThan(minCmp))
                        return Relation.CELL_OUTSIDE_QUERY;

                    int maxCmp = compareUnsigned(maxPackedValue, dim, upper);
                    crosses |= upper.smallerThan(maxCmp);
                }
            }

            if (crosses)
            {
                return Relation.CELL_CROSSES_QUERY;
            }
            else
            {
                return Relation.CELL_INSIDE_QUERY;
            }
        }
    }
}
