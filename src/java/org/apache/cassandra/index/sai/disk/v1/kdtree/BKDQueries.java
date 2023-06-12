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
package org.apache.cassandra.index.sai.disk.v1.kdtree;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.FutureArrays;

import static org.apache.lucene.index.PointValues.Relation.CELL_INSIDE_QUERY;

public class BKDQueries
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

    public static BKDReader.IntersectVisitor bkdQueryFrom(Expression expression, int bytesPerValue)
    {
        if (expression.lower == null && expression.upper == null)
        {
            return MATCH_ALL;
        }

        Bound lower = null ;
        if (expression.lower != null)
        {
            final byte[] lowerBound = toComparableBytes(bytesPerValue, expression.lower.value.encoded, expression.validator);
            lower = new Bound(lowerBound, !expression.lower.inclusive);
        }

        Bound upper = null;
        if (expression.upper != null)
        {
            final byte[] upperBound = toComparableBytes(bytesPerValue, expression.upper.value.encoded, expression.validator);
            upper = new Bound(upperBound, !expression.upper.inclusive);
        }

        return new RangeQueryVisitor(bytesPerValue, lower, upper);
    }

    private static byte[] toComparableBytes(int bytesPerDim, ByteBuffer value, AbstractType<?> type)
    {
        byte[] buffer = new byte[TypeUtil.fixedSizeOf(type)];
        assert buffer.length == bytesPerDim;
        TypeUtil.toComparableBytes(value, type, buffer);
        return buffer;
    }

    private static abstract class RangeQuery implements BKDReader.IntersectVisitor
    {
        final int bytesPerValue;

        RangeQuery(int bytesPerValue)
        {
            this.bytesPerValue = bytesPerValue;
        }

        int compareUnsigned(byte[] packedValue, Bound bound)
        {
            return FutureArrays.compareUnsigned(packedValue, 0, bytesPerValue, bound.bound, 0, bytesPerValue);
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

        private RangeQueryVisitor(int bytesPerValue, Bound lower, Bound upper)
        {
            super(bytesPerValue);
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean visit(byte[] packedValue)
        {
            if (lower != null)
            {
                if (lower.greaterThan(compareUnsigned(packedValue, lower)))
                {
                    // value is too low, in this dimension
                    return false;
                }
            }

            if (upper != null)
            {
                return !upper.smallerThan(compareUnsigned(packedValue, upper));
            }

            return true;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            boolean crosses = false;

            if (lower != null)
            {
                int maxCmp = compareUnsigned(maxPackedValue, lower);
                if (lower.greaterThan(maxCmp))
                    return Relation.CELL_OUTSIDE_QUERY;

                int minCmp = compareUnsigned(minPackedValue, lower);
                crosses = lower.greaterThan(minCmp);
            }

            if (upper != null)
            {
                int minCmp = compareUnsigned(minPackedValue, upper);
                if (upper.smallerThan(minCmp))
                    return Relation.CELL_OUTSIDE_QUERY;

                int maxCmp = compareUnsigned(maxPackedValue, upper);
                crosses |= upper.smallerThan(maxCmp);
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
