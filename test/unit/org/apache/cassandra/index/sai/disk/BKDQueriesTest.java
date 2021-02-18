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

import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.BKDReader;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.lucene.index.PointValues.Relation.CELL_CROSSES_QUERY;
import static org.apache.lucene.index.PointValues.Relation.CELL_INSIDE_QUERY;
import static org.apache.lucene.index.PointValues.Relation.CELL_OUTSIDE_QUERY;

public class BKDQueriesTest extends NdiRandomizedTest
{
    @Test
    public void testInclusiveLowerBound()
    {
        final int lowerBound = between(-10, 10);
        final Expression expression = buildExpression(Operator.GTE, lowerBound);
        final BKDReader.IntersectVisitor query = BKDQueries.bkdQueryFrom(expression, 1, 4);

        assertFalse(query.visit(toSortableBytes(lowerBound - 1)));
        assertTrue(query.visit(toSortableBytes(lowerBound)));
        assertTrue(query.visit(toSortableBytes(lowerBound + 1)));

        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(lowerBound - 2), toSortableBytes(lowerBound - 1)));
        assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(lowerBound), toSortableBytes(lowerBound + 1)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(lowerBound - 1), toSortableBytes(lowerBound)));
    }

    @Test
    public void testExclusiveLowerBound()
    {
        final int lowerBound = between(-10, 10);
        final Expression expression = buildExpression(Operator.GT, lowerBound);
        final BKDReader.IntersectVisitor query = BKDQueries.bkdQueryFrom(expression, 1, 4);

        assertFalse(query.visit(toSortableBytes(lowerBound - 1)));
        assertFalse(query.visit(toSortableBytes(lowerBound)));
        assertTrue(query.visit(toSortableBytes(lowerBound + 1)));

        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(lowerBound - 1), toSortableBytes(lowerBound)));
        assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(lowerBound + 1), toSortableBytes(lowerBound + 2)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(lowerBound), toSortableBytes(lowerBound + 1)));
    }

    @Test
    public void testInclusiveUpperBound()
    {
        final int upperBound = between(-10, 10);
        final Expression expression = buildExpression(Operator.LTE, upperBound);
        final BKDReader.IntersectVisitor query = BKDQueries.bkdQueryFrom(expression, 1, 4);

        assertTrue(query.visit(toSortableBytes(upperBound - 1)));
        assertTrue(query.visit(toSortableBytes(upperBound)));
        assertFalse(query.visit(toSortableBytes(upperBound + 1)));

        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(upperBound + 1), toSortableBytes(upperBound + 2)));
        assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(upperBound - 1), toSortableBytes(upperBound)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(upperBound), toSortableBytes(upperBound + 1)));
    }

    @Test
    public void testExclusiveUpperBound()
    {
        final int upper = between(-10, 10);
        final Expression expression = buildExpression(Operator.LT, upper);
        final BKDReader.IntersectVisitor query = BKDQueries.bkdQueryFrom(expression, 1, 4);

        assertTrue(query.visit(toSortableBytes(upper - 1)));
        assertFalse(query.visit(toSortableBytes(upper)));
        assertFalse(query.visit(toSortableBytes(upper + 1)));

        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(upper), toSortableBytes(upper + 1)));
        assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(upper - 2), toSortableBytes(upper - 1)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(upper - 1), toSortableBytes(upper)));
    }

    @Test
    public void testInclusiveLowerAndUpperBound()
    {
        final int lowerBound = between(-15, 15);
        final int upperBound = lowerBound + 5;
        final Expression expression = buildExpression(Operator.GTE, lowerBound)
                .add(Operator.LTE, Int32Type.instance.decompose(upperBound));
        final BKDReader.IntersectVisitor query = BKDQueries.bkdQueryFrom(expression, 1, 4);

        assertFalse(query.visit(toSortableBytes(lowerBound - 1)));
        assertTrue(query.visit(toSortableBytes(lowerBound)));
        assertTrue(query.visit(toSortableBytes(lowerBound + 1)));
        assertTrue(query.visit(toSortableBytes(upperBound - 1)));
        assertTrue(query.visit(toSortableBytes(upperBound)));
        assertFalse(query.visit(toSortableBytes(upperBound + 1)));

        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(lowerBound - 2), toSortableBytes(lowerBound - 1)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(lowerBound - 1), toSortableBytes(lowerBound)));
        assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(lowerBound), toSortableBytes(upperBound)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(upperBound), toSortableBytes(upperBound + 1)));
        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(upperBound + 1), toSortableBytes(upperBound + 2)));
    }

    @Test
    public void testExclusiveLowerAndUpperBound()
    {
        final int lowerBound = between(-15, 15);
        final int upperBound = lowerBound + 5;
        final Expression expression = buildExpression(Operator.GT, lowerBound)
                .add(Operator.LT, Int32Type.instance.decompose(upperBound));
        final BKDReader.IntersectVisitor query = BKDQueries.bkdQueryFrom(expression, 1, 4);

        assertFalse(query.visit(toSortableBytes(lowerBound - 1)));
        assertFalse(query.visit(toSortableBytes(lowerBound)));
        assertTrue(query.visit(toSortableBytes(lowerBound + 1)));
        assertTrue(query.visit(toSortableBytes(upperBound - 1)));
        assertFalse(query.visit(toSortableBytes(upperBound)));
        assertFalse(query.visit(toSortableBytes(upperBound + 1)));

        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(lowerBound - 1), toSortableBytes(lowerBound)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(lowerBound), toSortableBytes(lowerBound + 1)));
        assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(lowerBound + 1), toSortableBytes(upperBound - 1)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(upperBound - 1), toSortableBytes(upperBound)));
        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(upperBound), toSortableBytes(upperBound + 1)));
    }

    @Test
    public void testExclusiveLowerAndInclusiveUpperBound()
    {
        final int lowerBound = between(-15, 15);
        final int upperBound = lowerBound + 5;
        final Expression expression = buildExpression(Operator.GT, lowerBound)
                .add(Operator.LTE, Int32Type.instance.decompose(upperBound));
        final BKDReader.IntersectVisitor query = BKDQueries.bkdQueryFrom(expression, 1, 4);

        assertFalse(query.visit(toSortableBytes(lowerBound - 1)));
        assertFalse(query.visit(toSortableBytes(lowerBound)));
        assertTrue(query.visit(toSortableBytes(lowerBound + 1)));
        assertTrue(query.visit(toSortableBytes(upperBound - 1)));
        assertTrue(query.visit(toSortableBytes(upperBound)));
        assertFalse(query.visit(toSortableBytes(upperBound + 1)));

        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(lowerBound - 1), toSortableBytes(lowerBound)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(lowerBound), toSortableBytes(lowerBound + 1)));
        assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(lowerBound + 1), toSortableBytes(upperBound)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(upperBound), toSortableBytes(upperBound + 1)));
        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(upperBound + 1), toSortableBytes(upperBound + 2)));
    }

    @Test
    public void testInclusiveLowerAndExclusiveUpperBound()
    {
        final int lowerBound = between(-15, 15);
        final int upperBound = lowerBound + 5;
        final Expression expression = buildExpression(Operator.GTE, lowerBound)
                .add(Operator.LT, Int32Type.instance.decompose(upperBound));
        final BKDReader.IntersectVisitor query = BKDQueries.bkdQueryFrom(expression, 1, 4);

        assertFalse(query.visit(toSortableBytes(lowerBound - 1)));
        assertTrue(query.visit(toSortableBytes(lowerBound)));
        assertTrue(query.visit(toSortableBytes(lowerBound + 1)));
        assertTrue(query.visit(toSortableBytes(upperBound - 1)));
        assertFalse(query.visit(toSortableBytes(upperBound)));
        assertFalse(query.visit(toSortableBytes(upperBound + 1)));

        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(lowerBound - 2), toSortableBytes(lowerBound - 1)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(lowerBound - 1), toSortableBytes(lowerBound)));
        assertEquals(CELL_INSIDE_QUERY, query.compare(toSortableBytes(lowerBound), toSortableBytes(upperBound - 1)));
        assertEquals(CELL_CROSSES_QUERY, query.compare(toSortableBytes(upperBound - 1), toSortableBytes(upperBound)));
        assertEquals(CELL_OUTSIDE_QUERY, query.compare(toSortableBytes(upperBound), toSortableBytes(upperBound + 1)));
    }

    private byte[] toSortableBytes(int value)
    {
        byte[] buffer = new byte[4];
        ByteSource source = Int32Type.instance.asComparableBytes(Int32Type.instance.decompose(value), ByteComparable.Version.OSS41);
        ByteBufferUtil.toBytes(source, buffer);
        return buffer;
    }

    private Expression buildExpression(Operator op, int value)
    {
        final Expression expression = new Expression(SAITester.createColumnContext("meh", Int32Type.instance));
        expression.add(op, Int32Type.instance.decompose(value));
        return expression;
    }
}
