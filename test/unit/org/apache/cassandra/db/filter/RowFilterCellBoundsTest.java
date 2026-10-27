/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.filter;

import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.db.filter.RowFilter.CellBounds.EQUALITY_WITH_BOUNDS_MESSAGE;
import static org.apache.cassandra.db.filter.RowFilter.CellBounds.MULTIPLE_LOWER_BOUNDS_MESSAGE;
import static org.apache.cassandra.db.filter.RowFilter.CellBounds.MULTIPLE_UPPER_BOUNDS_MESSAGE;
import static org.apache.cassandra.db.filter.RowFilter.CellBounds.UNEXPECTED_OPERATOR_MESSAGE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/// Tests for the assertions in `RowFilter.CellBounds` about restrictions that are forbidden by CQL validation,
/// and therefore can't be tested with CQL queries (see [RowFilterEmptyRangesTest]).
public class RowFilterCellBoundsTest
{
    private static TableMetadata table;
    private static ColumnMetadata regularColumn;
    private static ColumnMetadata mapColumn;
    private static ColumnMetadata counterColumn;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();

        // a virtual table is enough, and it doesn't require the table to be part of the schema
        table = TableMetadata.builder("ks", "vt")
                             .kind(TableMetadata.Kind.VIRTUAL)
                             .addPartitionKeyColumn("k", Int32Type.instance)
                             .addRegularColumn("r", Int32Type.instance)
                             .addRegularColumn("m", MapType.getInstance(UTF8Type.instance, Int32Type.instance, true))
                             .addRegularColumn("c", CounterColumnType.instance)
                             .build();
        counterColumn = table.getColumn(ByteBufferUtil.bytes("c"));
        regularColumn = table.getColumn(ByteBufferUtil.bytes("r"));
        mapColumn = table.getColumn(ByteBufferUtil.bytes("m"));
    }

    @Test
    public void testMultipleLowerBounds()
    {
        for (Operator first : Arrays.asList(Operator.GT, Operator.GTE))
        {
            for (Operator second : Arrays.asList(Operator.GT, Operator.GTE))
            {
                assertFails(MULTIPLE_LOWER_BOUNDS_MESSAGE, simple(first, 1), simple(second, 2));
                assertFails(MULTIPLE_LOWER_BOUNDS_MESSAGE, simple(first, 2), simple(second, 1));
                assertFails(MULTIPLE_LOWER_BOUNDS_MESSAGE, simple(first, 1), simple(second, 1));
                assertFails(MULTIPLE_LOWER_BOUNDS_MESSAGE, map(first, "k", 1), map(second, "k", 2));
            }
        }
    }

    @Test
    public void testMultipleUpperBounds()
    {
        for (Operator first : Arrays.asList(Operator.LT, Operator.LTE))
        {
            for (Operator second : Arrays.asList(Operator.LT, Operator.LTE))
            {
                assertFails(MULTIPLE_UPPER_BOUNDS_MESSAGE, simple(first, 1), simple(second, 2));
                assertFails(MULTIPLE_UPPER_BOUNDS_MESSAGE, simple(first, 2), simple(second, 1));
                assertFails(MULTIPLE_UPPER_BOUNDS_MESSAGE, simple(first, 1), simple(second, 1));
                assertFails(MULTIPLE_UPPER_BOUNDS_MESSAGE, map(first, "k", 1), map(second, "k", 2));
            }
        }
    }

    @Test
    public void testEqualityWithLowerBound()
    {
        for (Operator slice : Arrays.asList(Operator.GT, Operator.GTE))
        {
            assertFails(EQUALITY_WITH_BOUNDS_MESSAGE, simple(Operator.EQ, 0), simple(slice, 1));
            assertFails(EQUALITY_WITH_BOUNDS_MESSAGE, simple(slice, 1), simple(Operator.EQ, 0));
            assertFails(EQUALITY_WITH_BOUNDS_MESSAGE, simple(Operator.EQ, 2), simple(slice, 1));
            assertFails(EQUALITY_WITH_BOUNDS_MESSAGE, simple(slice, 1), simple(Operator.EQ, 2));
            assertFails(EQUALITY_WITH_BOUNDS_MESSAGE, map(Operator.EQ, "k", 0), map(slice, "k", 1));
            assertFails(EQUALITY_WITH_BOUNDS_MESSAGE, map(slice, "k", 1), map(Operator.EQ, "k", 0));
        }
    }

    @Test
    public void testEqualityWithUpperBound()
    {
        for (Operator slice : Arrays.asList(Operator.LT, Operator.LTE))
        {
            assertFails(EQUALITY_WITH_BOUNDS_MESSAGE, simple(Operator.EQ, 2), simple(slice, 1));
            assertFails(EQUALITY_WITH_BOUNDS_MESSAGE, simple(slice, 1), simple(Operator.EQ, 2));
            assertFails(EQUALITY_WITH_BOUNDS_MESSAGE, simple(Operator.EQ, 0), simple(slice, 1));
            assertFails(EQUALITY_WITH_BOUNDS_MESSAGE, simple(slice, 1), simple(Operator.EQ, 0));
            assertFails(EQUALITY_WITH_BOUNDS_MESSAGE, map(Operator.EQ, "k", 2), map(slice, "k", 1));
            assertFails(EQUALITY_WITH_BOUNDS_MESSAGE, map(slice, "k", 1), map(Operator.EQ, "k", 2));
        }
    }

    @Test
    public void testEqualityWithBothBounds()
    {
        assertFails(EQUALITY_WITH_BOUNDS_MESSAGE, simple(Operator.GT, 0), simple(Operator.LT, 2), simple(Operator.EQ, 1));
        assertFails(EQUALITY_WITH_BOUNDS_MESSAGE, simple(Operator.EQ, 1), simple(Operator.GT, 0), simple(Operator.LT, 2));
    }

    /// Verifies that the assertions are not triggered by combinations of expressions that are valid in CQL.
    @Test
    public void testValidCombinations()
    {
        // one lower bound and one upper bound
        assertIsNotUnsatisfiable(simple(Operator.GT, 1), simple(Operator.LT, 2));
        assertIsUnsatisfiable(simple(Operator.GT, 2), simple(Operator.LT, 1));

        // equalities in different cells, and with bounds in other cells
        assertIsNotUnsatisfiable(simple(Operator.EQ, 1), map(Operator.GT, "k", 2));
        assertIsNotUnsatisfiable(map(Operator.EQ, "k1", 1), map(Operator.GT, "k2", 2));
        assertIsNotUnsatisfiable(map(Operator.GT, "k1", 1), map(Operator.GT, "k2", 2));
        assertIsNotUnsatisfiable(map(Operator.LT, "k1", 1), map(Operator.LT, "k2", 2));

        // equalities and inequalities
        assertIsNotUnsatisfiable(simple(Operator.EQ, 1), simple(Operator.NEQ, 1));
        assertIsNotUnsatisfiable(simple(Operator.NEQ, 1), simple(Operator.NEQ, 2));
    }

    /// Verifies the expressions that are ignored.
    @Test
    public void testIgnoredExpressions()
    {
        // a single expression can't be empty
        assertIsNotUnsatisfiable(simple(Operator.GT, 1));
        assertIsNotUnsatisfiable();

        // operators other than slices, EQ and NEQ are ignored
        assertIsNotUnsatisfiable(simple(Operator.CONTAINS, 1), simple(Operator.GT, 1));
        assertIsUnsatisfiable(simple(Operator.GT, 2), simple(Operator.CONTAINS, 1), simple(Operator.LT, 1));

        // counters are not comparable
        assertIsNotUnsatisfiable(counter(Operator.GT, 2), counter(Operator.LT, 1));
    }

    /// Verifies NEQ in map elements.
    @Test
    public void testMapNotEquals()
    {
        assertIsUnsatisfiable(map(Operator.GTE, "k", 1), map(Operator.LTE, "k", 1), map(Operator.NEQ, "k", 1));
        assertIsNotUnsatisfiable(map(Operator.GTE, "k", 1), map(Operator.LTE, "k", 1), map(Operator.NEQ, "k", 2));
        assertIsNotUnsatisfiable(map(Operator.GTE, "k1", 1), map(Operator.LTE, "k1", 1), map(Operator.NEQ, "k2", 1));
    }

    @Test
    public void testUnexpectedOperator()
    {
        for (Operator op : Operator.values())
        {
            if (op.isSlice() || op == Operator.EQ || op == Operator.NEQ)
                continue;

            Assertions.assertThatThrownBy(() -> new RowFilter.CellBounds(Int32Type.instance).add(op, ByteBufferUtil.bytes(1)))
                      .isInstanceOf(AssertionError.class)
                      .hasMessage(UNEXPECTED_OPERATOR_MESSAGE + op);
        }
    }

    private static void assertFails(String message, RowFilter.Expression... expressions)
    {
        Assertions.assertThatThrownBy(() -> isUnsatisfiable(expressions))
                  .isInstanceOf(AssertionError.class)
                  .hasMessage(message);
    }

    private static RowFilter.Expression simple(Operator operator, int value)
    {
        return new RowFilter.SimpleExpression(regularColumn, operator, ByteBufferUtil.bytes(value), null, null);
    }

    private static RowFilter.Expression counter(Operator operator, long value)
    {
        return new RowFilter.SimpleExpression(counterColumn, operator, ByteBufferUtil.bytes(value), null, null);
    }

    private static RowFilter.Expression map(Operator operator, String key, int value)
    {
        return new RowFilter.MapComparisonExpression(mapColumn, ByteBufferUtil.bytes(key), operator, ByteBufferUtil.bytes(value));
    }

    private static void assertIsUnsatisfiable(RowFilter.Expression... expressions)
    {
        assertTrue(isUnsatisfiable(expressions));
    }

    private static void assertIsNotUnsatisfiable(RowFilter.Expression... expressions)
    {
        assertFalse(isUnsatisfiable(expressions));
    }

    private static boolean isUnsatisfiable(RowFilter.Expression... expressions)
    {
        return RowFilter.CellBounds.isUnsatisfiableConjunction(Arrays.asList(expressions), table, IndexHints.NONE);
    }
}
