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

import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.apache.lucene.index.PointValues.Relation.CELL_CROSSES_QUERY;
import static org.apache.lucene.index.PointValues.Relation.CELL_INSIDE_QUERY;
import static org.apache.lucene.index.PointValues.Relation.CELL_OUTSIDE_QUERY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BlockBalancedTreeQueriesTest extends SAIRandomizedTester
{
    @Test
    public void testMatchesAll()
    {
        Expression expression = Expression.create(createMockIndex(Int32Type.instance));
        BlockBalancedTreeReader.IntersectVisitor query = BlockBalancedTreeQueries.balancedTreeQueryFrom(expression, 4);

        for (int visit = 0; visit < between(100, 1000); visit++)
            assertTrue(query.contains(toSortableBytes(nextInt(Integer.MAX_VALUE))));

        for (int compare = 0; compare < between(100, 1000); compare++)
            assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(between(0, Integer.MAX_VALUE/2)),
                                                          toSortableBytes(between(Integer.MAX_VALUE/2, Integer.MAX_VALUE))));
    }

    @Test
    public void testInclusiveLowerBound()
    {
        int lowerBound = between(-10, 10);
        Expression expression = buildExpression(Operator.GTE, lowerBound);
        BlockBalancedTreeReader.IntersectVisitor query = BlockBalancedTreeQueries.balancedTreeQueryFrom(expression, 4);

        assertFalse(query.contains(toSortableBytes(lowerBound - 1)));
        assertTrue(query.contains(toSortableBytes(lowerBound)));
        assertTrue(query.contains(toSortableBytes(lowerBound + 1)));

        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(lowerBound - 2), toSortableBytes(lowerBound - 1)));
        assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(lowerBound), toSortableBytes(lowerBound + 1)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(lowerBound - 1), toSortableBytes(lowerBound)));
    }

    @Test
    public void testExclusiveLowerBound()
    {
        int lowerBound = between(-10, 10);
        Expression expression = buildExpression(Operator.GT, lowerBound);
        BlockBalancedTreeReader.IntersectVisitor query = BlockBalancedTreeQueries.balancedTreeQueryFrom(expression, 4);

        assertFalse(query.contains(toSortableBytes(lowerBound - 1)));
        assertFalse(query.contains(toSortableBytes(lowerBound)));
        assertTrue(query.contains(toSortableBytes(lowerBound + 1)));

        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(lowerBound - 1), toSortableBytes(lowerBound)));
        assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(lowerBound + 1), toSortableBytes(lowerBound + 2)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(lowerBound), toSortableBytes(lowerBound + 1)));
    }

    @Test
    public void testInclusiveUpperBound()
    {
        int upperBound = between(-10, 10);
        Expression expression = buildExpression(Operator.LTE, upperBound);
        BlockBalancedTreeReader.IntersectVisitor query = BlockBalancedTreeQueries.balancedTreeQueryFrom(expression, 4);

        assertTrue(query.contains(toSortableBytes(upperBound - 1)));
        assertTrue(query.contains(toSortableBytes(upperBound)));
        assertFalse(query.contains(toSortableBytes(upperBound + 1)));

        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(upperBound + 1), toSortableBytes(upperBound + 2)));
        assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(upperBound - 1), toSortableBytes(upperBound)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(upperBound), toSortableBytes(upperBound + 1)));
    }

    @Test
    public void testExclusiveUpperBound()
    {
        int upper = between(-10, 10);
        Expression expression = buildExpression(Operator.LT, upper);
        BlockBalancedTreeReader.IntersectVisitor query = BlockBalancedTreeQueries.balancedTreeQueryFrom(expression, 4);

        assertTrue(query.contains(toSortableBytes(upper - 1)));
        assertFalse(query.contains(toSortableBytes(upper)));
        assertFalse(query.contains(toSortableBytes(upper + 1)));

        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(upper), toSortableBytes(upper + 1)));
        assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(upper - 2), toSortableBytes(upper - 1)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(upper - 1), toSortableBytes(upper)));
    }

    @Test
    public void testInclusiveLowerAndUpperBound()
    {
        int lowerBound = between(-15, 15);
        int upperBound = lowerBound + 5;
        Expression expression = buildExpression(Operator.GTE, lowerBound).add(Operator.LTE, Int32Type.instance.decompose(upperBound));
        BlockBalancedTreeReader.IntersectVisitor query = BlockBalancedTreeQueries.balancedTreeQueryFrom(expression, 4);

        assertFalse(query.contains(toSortableBytes(lowerBound - 1)));
        assertTrue(query.contains(toSortableBytes(lowerBound)));
        assertTrue(query.contains(toSortableBytes(lowerBound + 1)));
        assertTrue(query.contains(toSortableBytes(upperBound - 1)));
        assertTrue(query.contains(toSortableBytes(upperBound)));
        assertFalse(query.contains(toSortableBytes(upperBound + 1)));

        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(lowerBound - 2), toSortableBytes(lowerBound - 1)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(lowerBound - 1), toSortableBytes(lowerBound)));
        assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(lowerBound), toSortableBytes(upperBound)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(upperBound), toSortableBytes(upperBound + 1)));
        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(upperBound + 1), toSortableBytes(upperBound + 2)));
    }

    @Test
    public void testExclusiveLowerAndUpperBound()
    {
        int lowerBound = between(-15, 15);
        int upperBound = lowerBound + 5;
        Expression expression = buildExpression(Operator.GT, lowerBound).add(Operator.LT, Int32Type.instance.decompose(upperBound));
        BlockBalancedTreeReader.IntersectVisitor query = BlockBalancedTreeQueries.balancedTreeQueryFrom(expression, 4);

        assertFalse(query.contains(toSortableBytes(lowerBound - 1)));
        assertFalse(query.contains(toSortableBytes(lowerBound)));
        assertTrue(query.contains(toSortableBytes(lowerBound + 1)));
        assertTrue(query.contains(toSortableBytes(upperBound - 1)));
        assertFalse(query.contains(toSortableBytes(upperBound)));
        assertFalse(query.contains(toSortableBytes(upperBound + 1)));

        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(lowerBound - 1), toSortableBytes(lowerBound)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(lowerBound), toSortableBytes(lowerBound + 1)));
        assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(lowerBound + 1), toSortableBytes(upperBound - 1)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(upperBound - 1), toSortableBytes(upperBound)));
        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(upperBound), toSortableBytes(upperBound + 1)));
    }

    @Test
    public void testExclusiveLowerAndInclusiveUpperBound()
    {
        int lowerBound = between(-15, 15);
        int upperBound = lowerBound + 5;
        Expression expression = buildExpression(Operator.GT, lowerBound).add(Operator.LTE, Int32Type.instance.decompose(upperBound));
        BlockBalancedTreeReader.IntersectVisitor query = BlockBalancedTreeQueries.balancedTreeQueryFrom(expression, 4);

        assertFalse(query.contains(toSortableBytes(lowerBound - 1)));
        assertFalse(query.contains(toSortableBytes(lowerBound)));
        assertTrue(query.contains(toSortableBytes(lowerBound + 1)));
        assertTrue(query.contains(toSortableBytes(upperBound - 1)));
        assertTrue(query.contains(toSortableBytes(upperBound)));
        assertFalse(query.contains(toSortableBytes(upperBound + 1)));

        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(lowerBound - 1), toSortableBytes(lowerBound)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(lowerBound), toSortableBytes(lowerBound + 1)));
        assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(lowerBound + 1), toSortableBytes(upperBound)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(upperBound), toSortableBytes(upperBound + 1)));
        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(upperBound + 1), toSortableBytes(upperBound + 2)));
    }

    @Test
    public void testInclusiveLowerAndExclusiveUpperBound()
    {
        int lowerBound = between(-15, 15);
        int upperBound = lowerBound + 5;
        Expression expression = buildExpression(Operator.GTE, lowerBound).add(Operator.LT, Int32Type.instance.decompose(upperBound));
        BlockBalancedTreeReader.IntersectVisitor query = BlockBalancedTreeQueries.balancedTreeQueryFrom(expression, 4);

        assertFalse(query.contains(toSortableBytes(lowerBound - 1)));
        assertTrue(query.contains(toSortableBytes(lowerBound)));
        assertTrue(query.contains(toSortableBytes(lowerBound + 1)));
        assertTrue(query.contains(toSortableBytes(upperBound - 1)));
        assertFalse(query.contains(toSortableBytes(upperBound)));
        assertFalse(query.contains(toSortableBytes(upperBound + 1)));

        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(lowerBound - 2), toSortableBytes(lowerBound - 1)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(lowerBound - 1), toSortableBytes(lowerBound)));
        assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(lowerBound), toSortableBytes(upperBound - 1)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(upperBound - 1), toSortableBytes(upperBound)));
        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(upperBound), toSortableBytes(upperBound + 1)));
    }

    private byte[] toSortableBytes(int value)
    {
        byte[] buffer = new byte[4];
        ByteSource source = Int32Type.instance.asComparableBytes(Int32Type.instance.decompose(value), ByteComparable.Version.OSS50);
        ByteSourceInverse.copyBytes(source, buffer);
        return buffer;
    }

    private Expression buildExpression(Operator op, int value)
    {
        Expression expression = Expression.create(createMockIndex(Int32Type.instance));
        expression.add(op, Int32Type.instance.decompose(value));
        return expression;
    }
}
