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
package org.apache.cassandra.index.sai.disk.v1.bbtree;

import java.nio.ByteBuffer;

import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.lucene.index.PointValues.Relation;

public class BlockBalancedTreeQueries
{
    private static final BlockBalancedTreeReader.IntersectVisitor MATCH_ALL = new BlockBalancedTreeReader.IntersectVisitor()
    {
        @Override
        public boolean contains(byte[] packedValue)
        {
            return true;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            return Relation.CELL_INSIDE_QUERY;
        }
    };

    public static BlockBalancedTreeReader.IntersectVisitor balancedTreeQueryFrom(Expression expression, int bytesPerValue)
    {
        if (expression.lower() == null && expression.upper() == null)
        {
            return MATCH_ALL;
        }

        Bound lower = null ;
        if (expression.lower() != null)
        {
            final byte[] lowerBound = toComparableBytes(bytesPerValue, expression.lower().value.encoded, expression.getIndexTermType());
            lower = new Bound(lowerBound, !expression.lower().inclusive);
        }

        Bound upper = null;
        if (expression.upper() != null)
        {
            final byte[] upperBound = toComparableBytes(bytesPerValue, expression.upper().value.encoded, expression.getIndexTermType());
            upper = new Bound(upperBound, !expression.upper().inclusive);
        }

        return new RangeQueryVisitor(lower, upper);
    }

    private static byte[] toComparableBytes(int bytesPerDim, ByteBuffer value, IndexTermType indexTermType)
    {
        byte[] buffer = new byte[indexTermType.fixedSizeOf()];
        assert buffer.length == bytesPerDim;
        indexTermType.toComparableBytes(value, buffer);
        return buffer;
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

        boolean smallerThan(byte[] packedValue)
        {
            int cmp = compareTo(packedValue);
            return cmp < 0 || (cmp == 0 && exclusive);
        }

        boolean greaterThan(byte[] packedValue)
        {
            int cmp = compareTo(packedValue);
            return cmp > 0 || (cmp == 0 && exclusive);
        }

        private int compareTo(byte[] packedValue)
        {
            return ByteArrayUtil.compareUnsigned(bound, 0, packedValue, 0, bound.length);
        }
    }

    private static class RangeQueryVisitor implements BlockBalancedTreeReader.IntersectVisitor
    {
        private final Bound lower;
        private final Bound upper;

        private RangeQueryVisitor(Bound lower, Bound upper)
        {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean contains(byte[] packedValue)
        {
            if (lower != null)
            {
                if (lower.greaterThan(packedValue))
                {
                    // value is too low, in this dimension
                    return false;
                }
            }

            if (upper != null)
            {
                return !upper.smallerThan(packedValue);
            }

            return true;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            boolean crosses = false;

            if (lower != null)
            {
                if (lower.greaterThan(maxPackedValue))
                    return Relation.CELL_OUTSIDE_QUERY;

                crosses = lower.greaterThan(minPackedValue);
            }

            if (upper != null)
            {
                if (upper.smallerThan(minPackedValue))
                    return Relation.CELL_OUTSIDE_QUERY;

                crosses |= upper.smallerThan(maxPackedValue);
            }

            return crosses ? Relation.CELL_CROSSES_QUERY : Relation.CELL_INSIDE_QUERY;
        }
    }
}
