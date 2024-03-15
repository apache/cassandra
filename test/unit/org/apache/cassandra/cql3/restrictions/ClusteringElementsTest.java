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

package org.apache.cassandra.cql3.restrictions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.RangeSet;
import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.harry.util.ByteUtils;
import org.apache.cassandra.schema.ColumnMetadata;

import static java.util.Arrays.asList;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClusteringElementsTest
{
    public static final AbstractType<?> DESC = ReversedType.getInstance(Int32Type.instance);
    public static final AbstractType<?> ASC = Int32Type.instance;

    @Test
    public void testCompareToWithOneAscColumn()
    {
        ColumnMetadata column = newColumn(ASC);
        ClusteringElements empty = ClusteringElements.of();
        assertOrder(empty.bottom(),
                    elements(column, 1),
                    elements(column, 4),
                    elements(column, 6),
                    empty.top());
    }

    @Test
    public void testCompareToWithOneDescColumn()
    {
        ColumnMetadata column = newColumn(DESC);
        ClusteringElements empty = ClusteringElements.of();
        assertOrder(empty.bottom(),
                    elements(column, 6),
                    elements(column, 4),
                    elements(column, 1),
                    empty.top());
    }

    @Test
    public void testCompareToWithTwoAscColumns()
    {
        List<ColumnMetadata> columns = newColumns(ASC, ASC);

        ClusteringElements empty = ClusteringElements.of();
        ClusteringElements one = elements(columns.get(0), 1);
        ClusteringElements oneZero = elements(columns, 1, 0);
        ClusteringElements oneThree = elements(columns, 1, 3);

        assertCompareToEquality(one, oneZero, oneThree, one.bottom(), one.top(), oneThree.top());
        assertOrder(empty.bottom(),
                    one.bottom(),
                    oneZero,
                    oneThree,
                    one.top(),
                    elements(columns.get(0), 4),
                    elements(columns, 6, 1),
                    elements(columns, 6, 4),
                    empty.top());
    }

    @Test
    public void testCompareToWithTwoDescColumns()
    {
        List<ColumnMetadata> columns = newColumns(DESC, DESC);

        ClusteringElements empty = ClusteringElements.of();
        ClusteringElements one = elements(columns.get(0), 1);
        ClusteringElements oneZero = elements(columns, 1, 0);
        ClusteringElements oneThree = elements(columns, 1, 3);

        assertCompareToEquality(one, oneZero, oneThree, one.bottom(), one.top(), oneThree.top());

        assertOrder(empty.bottom(),
                    elements(columns, 6, 4),
                    elements(columns, 6, 1),
                    elements(columns.get(0), 4),
                    one.bottom(),
                    oneThree,
                    oneZero,
                    one.top(),
                    empty.top());
    }

    @Test
    public void testCompareToWithAscDescColumns()
    {
        List<ColumnMetadata> columns = newColumns(ASC, DESC);

        ClusteringElements empty = ClusteringElements.of();
        ClusteringElements one = elements(columns.get(0), 1);
        ClusteringElements oneZero = elements(columns, 1, 0);
        ClusteringElements oneThree = elements(columns, 1, 3);

        assertCompareToEquality(one, oneZero, oneThree, one.bottom(), one.top(), oneThree.top());

        assertOrder(empty.bottom(),
                    one.bottom(),
                    oneThree,
                    oneZero,
                    one.top(),
                    elements(columns.get(0), 4),
                    elements(columns, 6, 4),
                    elements(columns, 6, 1),
                    empty.top());
    }

    @Test
    public void testCompareToWithDescAscColumns()
    {
        List<ColumnMetadata> columns = newColumns(DESC, ASC);

        ClusteringElements empty = ClusteringElements.of();
        ClusteringElements one = elements(columns.get(0), 1);
        ClusteringElements oneZero = elements(columns, 1, 0);
        ClusteringElements oneThree = elements(columns, 1, 3);

        assertCompareToEquality(one, oneZero, oneThree, one.bottom(), one.top(), oneThree.top());

        assertOrder(empty.bottom(),
                    elements(columns, 6, 1),
                    elements(columns, 6, 4),
                    elements(columns.get(0), 4),
                    one.bottom(),
                    oneZero,
                    oneThree,
                    one.top(),
                    empty.top());
    }

    @Test
    public void testAtMostWithOneColumn()
    {
        for (ColumnMetadata type : newColumns(ASC, DESC))
        {
            ClusteringElements one = elements(type, 1);
            ClusteringElements four = elements(type, 4);
            ClusteringElements six = elements(type, 6);

            RangeSet<ClusteringElements> rangeSet = ClusteringElements.atMost(four);
            assertTrue(rangeSet.contains(one));
            assertTrue(rangeSet.contains(four));
            assertFalse(rangeSet.contains(six));
        }
    }

    @Test
    public void testAtMostWithTwoColumns()
    {
        for (List<ColumnMetadata> columns : asList(newColumns(ASC, ASC),
                                                   newColumns(DESC, DESC),
                                                   newColumns(ASC, DESC),
                                                   newColumns(DESC, ASC)))
        {
            ClusteringElements zeroZero = elements(columns, 0, 0);
            ClusteringElements oneZero = elements(columns, 1, 0);
            ClusteringElements oneThree = elements(columns, 1, 3);
            ClusteringElements oneFive = elements(columns, 1, 5);
            ClusteringElements twoFive = elements(columns, 2, 5);

            RangeSet<ClusteringElements> rangeSet = ClusteringElements.atMost(oneThree);

            assertTrue(rangeSet.contains(zeroZero));
            assertTrue(rangeSet.contains(oneZero));
            assertTrue(rangeSet.contains(oneThree));
            assertFalse(rangeSet.contains(oneFive));
            assertFalse(rangeSet.contains(twoFive));

            for (AbstractType<?> type : asList(ASC, DESC))
            {
                List<ColumnMetadata> newColumns = extend(columns, type);
                ClusteringElements zeroZeroZero = elements(newColumns, 0, 0, 0);
                ClusteringElements oneZeroOne = elements(newColumns, 1, 0, 1);
                ClusteringElements oneThreeZero = elements(newColumns, 1, 3, 0);
                ClusteringElements oneThreeOne = elements(newColumns, 1, 3, 1);
                ClusteringElements oneThreeFive = elements(newColumns, 1, 3, 5);
                ClusteringElements oneFiveOne = elements(newColumns, 1, 5, 1);
                ClusteringElements twoFiveFive = elements(newColumns, 2, 5, 5);

                assertTrue(rangeSet.contains(zeroZeroZero));
                assertTrue(rangeSet.contains(oneZeroOne));
                assertTrue(rangeSet.contains(oneThreeZero));
                assertTrue(rangeSet.contains(oneThreeOne));
                assertTrue(rangeSet.contains(oneThreeFive));
                assertFalse(rangeSet.contains(oneFiveOne));
                assertFalse(rangeSet.contains(twoFiveFive));
            }
        }
    }

    @Test
    public void testAtMostWithThreeColumns()
    {
        for (List<ColumnMetadata> columns : asList(newColumns(ASC, ASC, ASC),
                                                   newColumns(ASC, ASC, DESC),
                                                   newColumns(DESC, DESC, ASC),
                                                   newColumns(DESC, DESC, DESC),
                                                   newColumns(ASC, DESC, ASC),
                                                   newColumns(ASC, DESC, DESC),
                                                   newColumns(DESC, ASC, ASC),
                                                   newColumns(DESC, ASC, DESC)))
        {
            ClusteringElements zeroZeroZero = elements(columns, 0, 0, 0);
            ClusteringElements oneZeroOne = elements(columns, 1, 0, 1);
            ClusteringElements oneThreeZero = elements(columns, 1, 3, 0);
            ClusteringElements oneThreeOne = elements(columns, 1, 3, 1);
            ClusteringElements oneThreeFive = elements(columns, 1, 3, 5);
            ClusteringElements oneFiveOne = elements(columns, 1, 5, 1);
            ClusteringElements twoFiveFive = elements(columns, 2, 5, 5);

            RangeSet<ClusteringElements> rangeSet = ClusteringElements.atMost(oneThreeOne);

            assertTrue(rangeSet.contains(zeroZeroZero));
            assertTrue(rangeSet.contains(oneZeroOne));
            assertTrue(rangeSet.contains(oneThreeZero));
            assertTrue(rangeSet.contains(oneThreeOne));
            assertFalse(rangeSet.contains(oneThreeFive));
            assertFalse(rangeSet.contains(oneFiveOne));
            assertFalse(rangeSet.contains(twoFiveFive));
        }
    }

    @Test
    public void testLessThanWithOneColumn()
    {
        for (ColumnMetadata column : asList(newColumn(ASC), newColumn(DESC)))
        {
            ClusteringElements one = elements(column, 1);
            ClusteringElements four = elements(column, 4);
            ClusteringElements six = elements(column, 6);

            RangeSet<ClusteringElements> rangeSet = ClusteringElements.lessThan(four);
            assertTrue(rangeSet.contains(one));
            assertFalse(rangeSet.contains(four));
            assertFalse(rangeSet.contains(six));
        }
    }

    @Test
    public void testLessThanWithTwoColumns()
    {
        for (List<ColumnMetadata> columns : asList(newColumns(ASC, ASC),
                                                   newColumns(DESC, DESC),
                                                   newColumns(ASC, DESC),
                                                   newColumns(DESC, ASC)))
        {
            ClusteringElements zeroZero = elements(columns, 0, 0);
            ClusteringElements oneZero = elements(columns, 1, 0);
            ClusteringElements oneThree = elements(columns, 1, 3);
            ClusteringElements oneFive = elements(columns, 1, 5);
            ClusteringElements twoFive = elements(columns, 2, 5);

            RangeSet<ClusteringElements> rangeSet = ClusteringElements.lessThan(oneThree);

            assertTrue(rangeSet.contains(zeroZero));
            assertTrue(rangeSet.contains(oneZero));
            assertFalse(rangeSet.contains(oneThree));
            assertFalse(rangeSet.contains(oneFive));
            assertFalse(rangeSet.contains(twoFive));

            for (AbstractType<?> type : asList(ASC, DESC))
            {
                List<ColumnMetadata> newColumns = extend(columns, type);

                ClusteringElements zeroZeroZero = elements(newColumns, 0, 0, 0);
                ClusteringElements oneZeroOne = elements(newColumns, 1, 0, 1);
                ClusteringElements oneThreeZero = elements(newColumns, 1, 3, 0);
                ClusteringElements oneThreeOne = elements(newColumns, 1, 3, 1);
                ClusteringElements oneThreeFive = elements(newColumns, 1, 3, 5);
                ClusteringElements oneFiveOne = elements(newColumns, 1, 5, 1);
                ClusteringElements twoFiveFive = elements(newColumns, 2, 5, 5);

                assertTrue(rangeSet.contains(zeroZeroZero));
                assertTrue(rangeSet.contains(oneZeroOne));
                assertFalse(rangeSet.contains(oneThreeZero));
                assertFalse(rangeSet.contains(oneThreeOne));
                assertFalse(rangeSet.contains(oneThreeFive));
                assertFalse(rangeSet.contains(oneFiveOne));
                assertFalse(rangeSet.contains(twoFiveFive));
            }
        }
    }

    @Test
    public void testLessThanWithThreeColumns()
    {
        for (List<ColumnMetadata> columns : asList(newColumns(ASC, ASC, ASC),
                                                   newColumns(ASC, ASC, DESC),
                                                   newColumns(DESC, DESC, ASC),
                                                   newColumns(DESC, DESC, DESC),
                                                   newColumns(ASC, DESC, ASC),
                                                   newColumns(ASC, DESC, DESC),
                                                   newColumns(DESC, ASC, ASC),
                                                   newColumns(DESC, ASC, DESC)))
        {
            ClusteringElements zeroZeroZero = elements(columns, 0, 0, 0);
            ClusteringElements oneZeroOne = elements(columns, 1, 0, 1);
            ClusteringElements oneThreeZero = elements(columns, 1, 3, 0);
            ClusteringElements oneThreeOne = elements(columns, 1, 3, 1);
            ClusteringElements oneThreeFive = elements(columns, 1, 3, 5);
            ClusteringElements oneFiveOne = elements(columns, 1, 5, 1);
            ClusteringElements twoFiveFive = elements(columns, 2, 5, 5);

            RangeSet<ClusteringElements> rangeSet = ClusteringElements.lessThan(oneThreeOne);

            assertTrue(rangeSet.contains(zeroZeroZero));
            assertTrue(rangeSet.contains(oneZeroOne));
            assertTrue(rangeSet.contains(oneThreeZero));
            assertFalse(rangeSet.contains(oneThreeOne));
            assertFalse(rangeSet.contains(oneThreeFive));
            assertFalse(rangeSet.contains(oneFiveOne));
            assertFalse(rangeSet.contains(twoFiveFive));
        }
    }

    @Test
    public void testAtLeastWithOneColumn()
    {
        for (ColumnMetadata column : asList(newColumn(ASC), newColumn(DESC)))
        {
            ClusteringElements one = elements(column, 1);
            ClusteringElements four = elements(column, 4);
            ClusteringElements six = elements(column, 6);

            RangeSet<ClusteringElements> rangeSet = ClusteringElements.atLeast(four);
            assertFalse(rangeSet.contains(one));
            assertTrue(rangeSet.contains(four));
            assertTrue(rangeSet.contains(six));
        }
    }

    @Test
    public void testAtLeastWithTwoColumns()
    {
        for (List<ColumnMetadata> columns : asList(newColumns(ASC, ASC),
                                                   newColumns(DESC, DESC),
                                                   newColumns(ASC, DESC),
                                                   newColumns(DESC, ASC)))
        {
            ClusteringElements zeroZero = elements(columns, 0, 0);
            ClusteringElements oneZero = elements(columns, 1, 0);
            ClusteringElements oneThree = elements(columns, 1, 3);
            ClusteringElements oneFive = elements(columns, 1, 5);
            ClusteringElements twoFive = elements(columns, 2, 5);

            RangeSet<ClusteringElements> rangeSet = ClusteringElements.atLeast(oneThree);

            assertFalse(rangeSet.contains(zeroZero));
            assertFalse(rangeSet.contains(oneZero));
            assertTrue(rangeSet.contains(oneThree));
            assertTrue(rangeSet.contains(oneFive));
            assertTrue(rangeSet.contains(twoFive));

            for (AbstractType<?> type : asList(ASC, DESC))
            {
                List<ColumnMetadata> newColumns = extend(columns, type);

                ClusteringElements zeroZeroZero = elements(newColumns, 0, 0, 0);
                ClusteringElements oneZeroOne = elements(newColumns, 1, 0, 1);
                ClusteringElements oneThreeZero = elements(newColumns, 1, 3, 0);
                ClusteringElements oneThreeOne = elements(newColumns, 1, 3, 1);
                ClusteringElements oneThreeFive = elements(newColumns, 1, 3, 5);
                ClusteringElements oneFiveOne = elements(newColumns, 1, 5, 1);
                ClusteringElements twoFiveFive = elements(newColumns, 2, 5, 5);

                assertFalse(rangeSet.contains(zeroZeroZero));
                assertFalse(rangeSet.contains(oneZeroOne));
                assertTrue(rangeSet.contains(oneThreeZero));
                assertTrue(rangeSet.contains(oneThreeOne));
                assertTrue(rangeSet.contains(oneThreeFive));
                assertTrue(rangeSet.contains(oneFiveOne));
                assertTrue(rangeSet.contains(twoFiveFive));
            }
        }
    }

    @Test
    public void testAtLeastWithThreeColumns()
    {
        for (List<ColumnMetadata> columns : asList(newColumns(ASC, ASC, ASC),
                                                   newColumns(ASC, ASC, DESC),
                                                   newColumns(DESC, DESC, ASC),
                                                   newColumns(DESC, DESC, DESC),
                                                   newColumns(ASC, DESC, ASC),
                                                   newColumns(ASC, DESC, DESC),
                                                   newColumns(DESC, ASC, ASC),
                                                   newColumns(DESC, ASC, DESC)))
        {
            ClusteringElements zeroZeroZero = elements(columns, 0, 0, 0);
            ClusteringElements oneZeroOne = elements(columns, 1, 0, 1);
            ClusteringElements oneThreeZero = elements(columns, 1, 3, 0);
            ClusteringElements oneThreeOne = elements(columns, 1, 3, 1);
            ClusteringElements oneThreeFive = elements(columns, 1, 3, 5);
            ClusteringElements oneFiveOne = elements(columns, 1, 5, 1);
            ClusteringElements twoFiveFive = elements(columns, 2, 5, 5);

            RangeSet<ClusteringElements> rangeSet = ClusteringElements.atLeast(oneThreeOne);

            assertFalse(rangeSet.contains(zeroZeroZero));
            assertFalse(rangeSet.contains(oneZeroOne));
            assertFalse(rangeSet.contains(oneThreeZero));
            assertTrue(rangeSet.contains(oneThreeOne));
            assertTrue(rangeSet.contains(oneThreeFive));
            assertTrue(rangeSet.contains(oneFiveOne));
            assertTrue(rangeSet.contains(twoFiveFive));
        }
    }

    @Test
    public void testGreaterThanWithOneColumn()
    {
        for (ColumnMetadata column : asList(newColumn(ASC), newColumn(DESC)))
        {
            ClusteringElements one = elements(column, 1);
            ClusteringElements four = elements(column, 4);
            ClusteringElements six = elements(column, 6);

            RangeSet<ClusteringElements> rangeSet = ClusteringElements.greaterThan(four);
            assertFalse(rangeSet.contains(one));
            assertFalse(rangeSet.contains(four));
            assertTrue(rangeSet.contains(six));
        }
    }

    @Test
    public void testGreaterThanWithTwoColumns()
    {
        for (List<ColumnMetadata> columns : asList(newColumns(ASC, ASC),
                                                   newColumns(DESC, DESC),
                                                   newColumns(ASC, DESC),
                                                   newColumns(DESC, ASC)))
        {
            ClusteringElements zeroZero = elements(columns, 0, 0);
            ClusteringElements oneZero = elements(columns, 1, 0);
            ClusteringElements oneThree = elements(columns, 1, 3);
            ClusteringElements oneFive = elements(columns, 1, 5);
            ClusteringElements twoFive = elements(columns, 2, 5);

            RangeSet<ClusteringElements> rangeSet = ClusteringElements.greaterThan(oneThree);

            assertFalse(rangeSet.contains(zeroZero));
            assertFalse(rangeSet.contains(oneZero));
            assertFalse(rangeSet.contains(oneThree));
            assertTrue(rangeSet.contains(oneFive));
            assertTrue(rangeSet.contains(twoFive));

            for (AbstractType<?> type : asList(ASC, DESC))
            {
                List<ColumnMetadata> newColumns = extend(columns, type);

                ClusteringElements zeroZeroZero = elements(newColumns, 0, 0, 0);
                ClusteringElements oneZeroOne = elements(newColumns, 1, 0, 1);
                ClusteringElements oneThreeZero = elements(newColumns, 1, 3, 0);
                ClusteringElements oneThreeOne = elements(newColumns, 1, 3, 1);
                ClusteringElements oneThreeFive = elements(newColumns, 1, 3, 5);
                ClusteringElements oneFiveOne = elements(newColumns, 1, 5, 1);
                ClusteringElements twoFiveFive = elements(newColumns, 2, 5, 5);

                assertFalse(rangeSet.contains(zeroZeroZero));
                assertFalse(rangeSet.contains(oneZeroOne));
                assertFalse(rangeSet.contains(oneThreeZero));
                assertFalse(rangeSet.contains(oneThreeOne));
                assertFalse(rangeSet.contains(oneThreeFive));
                assertTrue(rangeSet.contains(oneFiveOne));
                assertTrue(rangeSet.contains(twoFiveFive));
            }
        }
    }

    @Test
    public void testGreaterThanWithThreeColumns()
    {
        for (List<ColumnMetadata> columns : asList(newColumns(ASC, ASC, ASC),
                                                   newColumns(ASC, ASC, DESC),
                                                   newColumns(DESC, DESC, ASC),
                                                   newColumns(DESC, DESC, DESC),
                                                   newColumns(ASC, DESC, ASC),
                                                   newColumns(ASC, DESC, DESC),
                                                   newColumns(DESC, ASC, ASC),
                                                   newColumns(DESC, ASC, DESC)))
        {
            ClusteringElements zeroZeroZero = elements(columns, 0, 0, 0);
            ClusteringElements oneZeroOne = elements(columns, 1, 0, 1);
            ClusteringElements oneThreeZero = elements(columns, 1, 3, 0);
            ClusteringElements oneThreeOne = elements(columns, 1, 3, 1);
            ClusteringElements oneThreeFive = elements(columns, 1, 3, 5);
            ClusteringElements oneFiveOne = elements(columns, 1, 5, 1);
            ClusteringElements twoFiveFive = elements(columns, 2, 5, 5);

            RangeSet<ClusteringElements> rangeSet = ClusteringElements.greaterThan(oneThreeOne);

            assertFalse(rangeSet.contains(zeroZeroZero));
            assertFalse(rangeSet.contains(oneZeroOne));
            assertFalse(rangeSet.contains(oneThreeZero));
            assertFalse(rangeSet.contains(oneThreeOne));
            assertTrue(rangeSet.contains(oneThreeFive));
            assertTrue(rangeSet.contains(oneFiveOne));
            assertTrue(rangeSet.contains(twoFiveFive));
        }
    }

    private static ClusteringElements elements(ColumnMetadata column, int value)
    {
        return ClusteringElements.of(column, bytes(value));
    }

    private static ClusteringElements elements(List<ColumnMetadata> columns, int... values)
    {
        return ClusteringElements.of(columns, Arrays.stream(values).mapToObj(ByteUtils::bytes).collect(Collectors.toList()));
    }

    private static ColumnMetadata newColumn(AbstractType<?> type)
    {
        return newColumn(type, 0);
    }

    private static ColumnMetadata newColumn(AbstractType<?> type, int position)
    {
        return ColumnMetadata.clusteringColumn("ks", "tbl", "c" + position, type, position);
    }

    private static List<ColumnMetadata> newColumns(AbstractType<?>... types)
    {
        List<ColumnMetadata> columns = new ArrayList<>(types.length);
        for (int i = 0, m = types.length; i < m; i++)
        {
            columns.add(newColumn(types[i], i));
        }
        return columns;
    }

    private static List<ColumnMetadata> extend(List<ColumnMetadata> columns, AbstractType<?> type)
    {
        List<ColumnMetadata> newColumns = new ArrayList<>(columns);
        newColumns.add(newColumn(type, columns.size()));
        return newColumns;
    }

    @SafeVarargs
    private <T extends Comparable<T>> void assertCompareToEquality(T... comparables)
    {
        for (int i = 0, m = comparables.length; i < m; i++)
        {
            for (int j = i; j < m; j++)
            {
                assertEquals(0, comparables[i].compareTo(comparables[i]));
            }
        }
    }

    @SafeVarargs
    private <T extends Comparable<T>> void assertOrder(T... comparables)
    {
        for (int i = 0, m = comparables.length; i < m; i++)
        {
            for (int j = i; j < m; j++)
            {
                if (i == j)
                {
                    assertEquals(0, comparables[i].compareTo(comparables[i]));
                }
                else
                {
                    T smaller = comparables[i];
                    T greater = comparables[j];
                    assertTrue(greater + " should be greater than " + smaller, greater.compareTo(smaller) > 0);
                    assertTrue(smaller + " should be smaller than " + greater, smaller.compareTo(greater) < 0);
                }
            }
        }
    }
}
