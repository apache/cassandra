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
package org.apache.cassandra.cql3.conditions;

import java.nio.ByteBuffer;
import java.util.*;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.cql3.terms.*;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.IVersionedSerializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.TimeUUID;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;
import org.quicktheories.generators.SourceDSL;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static java.util.Arrays.asList;

import static org.apache.cassandra.cql3.Operator.EQ;
import static org.apache.cassandra.cql3.Operator.NEQ;
import static org.apache.cassandra.cql3.Operator.LT;
import static org.apache.cassandra.cql3.Operator.LTE;
import static org.apache.cassandra.cql3.Operator.GT;
import static org.apache.cassandra.cql3.Operator.GTE;
import static org.apache.cassandra.cql3.Operator.CONTAINS;
import static org.apache.cassandra.cql3.Operator.CONTAINS_KEY;
import static org.apache.cassandra.cql3.conditions.ColumnCondition.Raw.simpleCondition;
import static org.apache.cassandra.cql3.conditions.ColumnCondition.Raw.collectionElementCondition;
import static org.apache.cassandra.cql3.conditions.ColumnCondition.Raw.udtFieldCondition;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;


public class ColumnConditionTest
{
    public static final ByteBuffer ZERO = Int32Type.instance.fromString("0");
    public static final ByteBuffer ONE = Int32Type.instance.fromString("1");
    public static final ByteBuffer TWO = Int32Type.instance.fromString("2");
    public static final String KEYSPACE = "ks";
    public static final FieldIdentifier UDT_FIELD_A = FieldIdentifier.forUnquoted("a");
    public static final FieldIdentifier UDT_FIELD_B = FieldIdentifier.forUnquoted("b");
    public static final UserType UDT_FROZEN = new UserType(KEYSPACE, ByteBufferUtil.bytes("simple"),
                                                           Arrays.asList(UDT_FIELD_A, UDT_FIELD_B),
                                                           Arrays.asList(Int32Type.instance, Int32Type.instance),
                                                           false);
    public static final UserType UDT_MULTI_CELL = new UserType(KEYSPACE, ByteBufferUtil.bytes("simple"),
                                                               Arrays.asList(UDT_FIELD_A, UDT_FIELD_B),
                                                               Arrays.asList(Int32Type.instance, Int32Type.instance),
                                                               true);

    private static Row newRow(ColumnMetadata definition, ByteBuffer value)
    {
        BufferCell cell = new BufferCell(definition, 0L, Cell.NO_TTL, Cell.NO_DELETION_TIME, value, null);
        return BTreeRow.singleCellRow(Clustering.EMPTY, cell);
    }

    private static Row newRow(ColumnMetadata definition, List<ByteBuffer> values)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        long now = System.currentTimeMillis();
        if (values != null)
        {
            for (int i = 0, m = values.size(); i < m; i++)
            {
                TimeUUID uuid = TimeUUID.Generator.atUnixMillis(now, i);
                ByteBuffer key = uuid.toBytes();
                ByteBuffer value = values.get(i);
                BufferCell cell = new BufferCell(definition,
                                                 0L,
                                                 Cell.NO_TTL,
                                                 Cell.NO_DELETION_TIME,
                                                 value,
                                                 CellPath.create(key));
                builder.addCell(cell);
            }
        }
        return builder.build();
    }

    private static Row newRow(ColumnMetadata definition, SortedSet<ByteBuffer> values)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        if (values != null)
        {
            for (ByteBuffer value : values)
            {
                BufferCell cell = new BufferCell(definition,
                                                 0L,
                                                 Cell.NO_TTL,
                                                 Cell.NO_DELETION_TIME,
                                                 ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                 CellPath.create(value));
                builder.addCell(cell);
            }
        }
        return builder.build();
    }

    private static Row newRow(ColumnMetadata definition, Map<ByteBuffer, ByteBuffer> values)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        if (values != null)
        {
            for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet())
            {
                BufferCell cell = new BufferCell(definition,
                                                 0L,
                                                 Cell.NO_TTL,
                                                 Cell.NO_DELETION_TIME,
                                                 entry.getValue(),
                                                 CellPath.create(entry.getKey()));
                builder.addCell(cell);
            }
        }
        return builder.build();
    }

    private static boolean appliesSimpleCondition(ByteBuffer rowValue, Operator op, ByteBuffer conditionValue)
    {
        ColumnMetadata definition = ColumnMetadata.regularColumn("ks", "cf", "c", Int32Type.instance);
        ColumnsExpression column = ColumnsExpression.singleColumn(definition);
        Terms terms = Terms.of(new Constants.Value(conditionValue));
        ColumnCondition condition = new ColumnCondition(column, op, terms);
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return bound.appliesTo(newRow(definition, rowValue));
    }

    private static boolean appliesListCondition(List<ByteBuffer> rowValue, Operator op, List<ByteBuffer> conditionValue)
    {
        ListType<Integer> type = ListType.getInstance(Int32Type.instance, true);
        ColumnMetadata definition = ColumnMetadata.regularColumn("ks", "cf", "c", type);
        ColumnsExpression column = ColumnsExpression.singleColumn(definition);
        Term term = conditionValue == null ? Constants.NULL_VALUE : new MultiElements.Value(type, conditionValue);
        ColumnCondition condition = new ColumnCondition(column, op, Terms.of(term));
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return bound.appliesTo(newRow(definition, rowValue));
    }

    private static boolean conditionContainsApplies(List<ByteBuffer> rowValue, Operator op, ByteBuffer conditionValue)
    {
        ColumnMetadata definition = ColumnMetadata.regularColumn("ks", "cf", "c", ListType.getInstance(Int32Type.instance, true));
        ColumnsExpression column = ColumnsExpression.singleColumn(definition);
        Terms terms = Terms.of(new Constants.Value(conditionValue));
        ColumnCondition condition = new ColumnCondition(column, op, terms);
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return bound.appliesTo(newRow(definition, rowValue));
    }

    private static boolean conditionContainsApplies(Map<ByteBuffer, ByteBuffer> rowValue, Operator op, ByteBuffer conditionValue)
    {
        ColumnMetadata definition = ColumnMetadata.regularColumn("ks", "cf", "c", MapType.getInstance(Int32Type.instance, Int32Type.instance, true));
        ColumnsExpression column = ColumnsExpression.singleColumn(definition);
        Terms terms = Terms.of(new Constants.Value(conditionValue));
        ColumnCondition condition = new ColumnCondition(column, op, terms);
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return bound.appliesTo(newRow(definition, rowValue));
    }

    private static boolean appliesSetCondition(SortedSet<ByteBuffer> rowValue, Operator op, SortedSet<ByteBuffer> conditionValue)
    {
        SetType<Integer> type = SetType.getInstance(Int32Type.instance, true);
        ColumnMetadata definition = ColumnMetadata.regularColumn("ks", "cf", "c", type);
        ColumnsExpression column = ColumnsExpression.singleColumn(definition);
        Term term = conditionValue == null ? Constants.NULL_VALUE : new MultiElements.Value(type, new ArrayList<>(conditionValue));
        ColumnCondition condition = new ColumnCondition(column, op, Terms.of(term));
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return bound.appliesTo(newRow(definition, rowValue));
    }

    private static boolean conditionContainsApplies(SortedSet<ByteBuffer> rowValue, Operator op, ByteBuffer conditionValue)
    {
        ColumnMetadata definition = ColumnMetadata.regularColumn("ks", "cf", "c", SetType.getInstance(Int32Type.instance, true));
        ColumnsExpression column = ColumnsExpression.singleColumn(definition);
        Terms terms = Terms.of(new Constants.Value(conditionValue));
        ColumnCondition condition = new ColumnCondition(column, op, terms);

        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return bound.appliesTo(newRow(definition, rowValue));
    }

    private boolean conditionUDTApplies(ByteBuffer rowValue, Operator op, ByteBuffer conditionValue)
    {
        boolean frozen = conditionUDTApplies(UDT_FROZEN, rowValue, op, conditionValue);
        boolean multi = conditionUDTApplies(UDT_MULTI_CELL, rowValue, op, conditionValue);
        Assertions.assertThat(frozen).isEqualTo(multi);
        return frozen;
    }

    private boolean conditionUDTApplies(UserType ut, ByteBuffer rowValue, Operator op, ByteBuffer conditionValue)
    {
        ColumnMetadata column = ColumnMetadata.regularColumn(KEYSPACE, "tbl", "c", ut);
        ColumnCondition.ElementOrFieldAccessBound bounds = new ColumnCondition.ElementOrFieldAccessBound(column, UDT_FIELD_A.bytes, op, conditionValue);
        Row row;
        if (ut.isMultiCell())
        {
            Row.Builder builder = BTreeRow.sortedBuilder();
            builder.newRow(Clustering.EMPTY);
            if (rowValue != null)
            {
                builder.addCell(new BufferCell(column,
                                               0L,
                                               Cell.NO_TTL,
                                               Cell.NO_DELETION_TIME,
                                               rowValue,
                                               ut.cellPathForField(UDT_FIELD_A)));
                builder.addCell(new BufferCell(column,
                                               0L,
                                               Cell.NO_TTL,
                                               Cell.NO_DELETION_TIME,
                                               EMPTY_BYTE_BUFFER,
                                               ut.cellPathForField(UDT_FIELD_B)));
            }
            row = builder.build();
        }
        else
        {
            row = newRow(column, ut.pack(rowValue, EMPTY_BYTE_BUFFER));
        }
        return bounds.appliesTo(row);
    }

    private static boolean appliesMapCondition(Map<ByteBuffer, ByteBuffer> rowValue, Operator op, SortedMap<ByteBuffer, ByteBuffer> conditionValue)
    {
        MapType<Integer, Integer> type = MapType.getInstance(Int32Type.instance, Int32Type.instance, true);
        ColumnMetadata definition = ColumnMetadata.regularColumn("ks", "cf", "c", type);
        Term term;
        if (conditionValue == null)
        {
            term = Constants.NULL_VALUE;
        }
        else
        {
            List<ByteBuffer> value = new ArrayList<>(conditionValue.size() * 2);
            for (Map.Entry<ByteBuffer, ByteBuffer> entry : conditionValue.entrySet())
            {
                value.add(entry.getKey());
                value.add(entry.getValue());
            }
            term = new MultiElements.Value(type, value);
        }
        ColumnsExpression column = ColumnsExpression.singleColumn(definition);
        ColumnCondition condition = new ColumnCondition(column, op, Terms.of(term));
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return bound.appliesTo(newRow(definition, rowValue));
    }

    @FunctionalInterface
    public interface CheckedFunction {
        void apply();
    }

    private static void assertThrowsIRE(CheckedFunction runnable, String errorMessage)
    {
        try
        {
            runnable.apply();
            fail("Expected InvalidRequestException was not thrown");
        } catch (InvalidRequestException e)
        {
            Assert.assertTrue("Expected error message to contain '" + errorMessage + "', but got '" + e.getMessage() + '\'',
                              e.getMessage().contains(errorMessage));
        }
    }

    @Test
    public void testSimpleBoundIsSatisfiedByValue() throws InvalidRequestException
    {
        // EQ
        assertTrue(appliesSimpleCondition(ONE, EQ, ONE));
        assertFalse(appliesSimpleCondition(TWO, EQ, ONE));
        assertFalse(appliesSimpleCondition(ONE, EQ, TWO));
        assertFalse(appliesSimpleCondition(ONE, EQ, EMPTY_BYTE_BUFFER));
        assertFalse(appliesSimpleCondition(EMPTY_BYTE_BUFFER, EQ, ONE));
        assertTrue(appliesSimpleCondition(EMPTY_BYTE_BUFFER, EQ, EMPTY_BYTE_BUFFER));
        assertFalse(appliesSimpleCondition(ONE, EQ, null));
        assertFalse(appliesSimpleCondition(null, EQ, ONE));
        assertTrue(appliesSimpleCondition(null, EQ, null));

        // NEQ
        assertFalse(appliesSimpleCondition(ONE, NEQ, ONE));
        assertTrue(appliesSimpleCondition(TWO, NEQ, ONE));
        assertTrue(appliesSimpleCondition(ONE, NEQ, TWO));
        assertTrue(appliesSimpleCondition(ONE, NEQ, EMPTY_BYTE_BUFFER));
        assertTrue(appliesSimpleCondition(EMPTY_BYTE_BUFFER, NEQ, ONE));
        assertFalse(appliesSimpleCondition(EMPTY_BYTE_BUFFER, NEQ, EMPTY_BYTE_BUFFER));
        assertTrue(appliesSimpleCondition(ONE, NEQ, null));
        assertTrue(appliesSimpleCondition(null, NEQ, ONE));
        assertFalse(appliesSimpleCondition(null, NEQ, null));

        // LT
        assertFalse(appliesSimpleCondition(ONE, LT, ONE));
        assertFalse(appliesSimpleCondition(TWO, LT, ONE));
        assertTrue(appliesSimpleCondition(ONE, LT, TWO));
        assertFalse(appliesSimpleCondition(ONE, LT, EMPTY_BYTE_BUFFER));
        assertTrue(appliesSimpleCondition(EMPTY_BYTE_BUFFER, LT, ONE));
        assertFalse(appliesSimpleCondition(EMPTY_BYTE_BUFFER, LT, EMPTY_BYTE_BUFFER));
        assertThrowsIRE(() -> appliesSimpleCondition(ONE, LT, null), "Invalid comparison with null for operator \"<\"");
        assertFalse(appliesSimpleCondition(null, LT, ONE));

        // LTE
        assertTrue(appliesSimpleCondition(ONE, LTE, ONE));
        assertFalse(appliesSimpleCondition(TWO, LTE, ONE));
        assertTrue(appliesSimpleCondition(ONE, LTE, TWO));
        assertFalse(appliesSimpleCondition(ONE, LTE, EMPTY_BYTE_BUFFER));
        assertTrue(appliesSimpleCondition(EMPTY_BYTE_BUFFER, LTE, ONE));
        assertTrue(appliesSimpleCondition(EMPTY_BYTE_BUFFER, LTE, EMPTY_BYTE_BUFFER));
        assertThrowsIRE(() -> appliesSimpleCondition(ONE, LTE, null), "Invalid comparison with null for operator \"<=\"");
        assertFalse(appliesSimpleCondition(null, LTE, ONE));

        // GT
        assertFalse(appliesSimpleCondition(ONE, GT, ONE));
        assertTrue(appliesSimpleCondition(TWO, GT, ONE));
        assertFalse(appliesSimpleCondition(ONE, GT, TWO));
        assertTrue(appliesSimpleCondition(ONE, GT, EMPTY_BYTE_BUFFER));
        assertFalse(appliesSimpleCondition(EMPTY_BYTE_BUFFER, GT, ONE));
        assertFalse(appliesSimpleCondition(EMPTY_BYTE_BUFFER, GT, EMPTY_BYTE_BUFFER));
        assertThrowsIRE(() -> appliesSimpleCondition(ONE, GT, null), "Invalid comparison with null for operator \">\"");
        assertFalse(appliesSimpleCondition(null, GT, ONE));

        // GTE
        assertTrue(appliesSimpleCondition(ONE, GTE, ONE));
        assertTrue(appliesSimpleCondition(TWO, GTE, ONE));
        assertFalse(appliesSimpleCondition(ONE, GTE, TWO));
        assertTrue(appliesSimpleCondition(ONE, GTE, EMPTY_BYTE_BUFFER));
        assertFalse(appliesSimpleCondition(EMPTY_BYTE_BUFFER, GTE, ONE));
        assertTrue(appliesSimpleCondition(EMPTY_BYTE_BUFFER, GTE, EMPTY_BYTE_BUFFER));
        assertThrowsIRE(() -> appliesSimpleCondition(ONE, GTE, null), "Invalid comparison with null for operator \">=\"");
        assertFalse(appliesSimpleCondition(null, GTE, ONE));
    }

    private static List<ByteBuffer> list(ByteBuffer... values)
    {
        return asList(values);
    }

    @Test
    // sets use the same check as lists
    public void testListCollectionBoundAppliesTo() throws InvalidRequestException
    {
        // EQ
        assertTrue(appliesListCondition(list(ONE), EQ, list(ONE)));
        assertTrue(appliesListCondition(null, EQ, null));
        assertTrue(appliesListCondition(null, EQ, list()));
        assertFalse(appliesListCondition(list(ONE), EQ, list(ZERO)));
        assertFalse(appliesListCondition(list(ZERO), EQ, list(ONE)));
        assertFalse(appliesListCondition(list(ONE, ONE), EQ, list(ONE)));
        assertFalse(appliesListCondition(list(ONE), EQ, list(ONE, ONE)));
        assertFalse(appliesListCondition(list(ONE), EQ, null));
        assertFalse(appliesListCondition(list(ONE), EQ, list()));
        assertFalse(appliesListCondition(null, EQ, list(ONE)));

        assertFalse(appliesListCondition(list(ONE), EQ, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(appliesListCondition(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), EQ, list(ONE)));
        assertTrue(appliesListCondition(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), EQ, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // NEQ
        assertFalse(appliesListCondition(list(ONE), NEQ, list(ONE)));
        assertFalse(appliesListCondition(null, NEQ, null));
        assertFalse(appliesListCondition(null, NEQ, list()));
        assertTrue(appliesListCondition(list(ONE), NEQ, list(ZERO)));
        assertTrue(appliesListCondition(list(ZERO), NEQ, list(ONE)));
        assertTrue(appliesListCondition(list(ONE, ONE), NEQ, list(ONE)));
        assertTrue(appliesListCondition(list(ONE), NEQ, list(ONE, ONE)));
        assertTrue(appliesListCondition(list(ONE), NEQ, null));
        assertTrue(appliesListCondition(list(ONE), NEQ, list()));
        assertTrue(appliesListCondition(null, NEQ, list(ONE)));

        assertTrue(appliesListCondition(list(ONE), NEQ, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(appliesListCondition(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), NEQ, list(ONE)));
        assertFalse(appliesListCondition(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), NEQ, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LT
        assertFalse(appliesListCondition(list(ONE), LT, list(ONE)));
        assertThrowsIRE(() -> appliesListCondition(null, LT, null), "Invalid comparison with null for operator \"<\"");
        assertThrowsIRE(() -> appliesListCondition(null, LT, list()), "Invalid comparison with an empty list for operator \"<\"");
        assertFalse(appliesListCondition(list(ONE), LT, list(ZERO)));
        assertTrue(appliesListCondition(list(ZERO), LT, list(ONE)));
        assertFalse(appliesListCondition(list(ONE, ONE), LT, list(ONE)));
        assertTrue(appliesListCondition(list(ONE), LT, list(ONE, ONE)));
        assertThrowsIRE(() -> appliesListCondition(list(ONE), LT, null), "Invalid comparison with null for operator \"<\"");
        assertThrowsIRE(() -> appliesListCondition(list(ONE), LT, list()), "Invalid comparison with an empty list for operator \"<\"");
        assertFalse(appliesListCondition(null, LT, list(ONE)));

        assertFalse(appliesListCondition(list(ONE), LT, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(appliesListCondition(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), LT, list(ONE)));
        assertFalse(appliesListCondition(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), LT, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LTE
        assertTrue(appliesListCondition(list(ONE), LTE, list(ONE)));
        assertThrowsIRE(() -> appliesListCondition(null, LTE, null), "Invalid comparison with null for operator \"<=\"");
        assertThrowsIRE(() -> appliesListCondition(null, LTE, list()), "Invalid comparison with an empty list for operator \"<=\"");
        assertFalse(appliesListCondition(list(ONE), LTE, list(ZERO)));
        assertTrue(appliesListCondition(list(ZERO), LTE, list(ONE)));
        assertFalse(appliesListCondition(list(ONE, ONE), LTE, list(ONE)));
        assertTrue(appliesListCondition(list(ONE), LTE, list(ONE, ONE)));
        assertThrowsIRE(() -> appliesListCondition(list(ONE), LTE, null), "Invalid comparison with null for operator \"<=\"");
        assertThrowsIRE(() -> appliesListCondition(list(ONE), LTE, list()), "Invalid comparison with an empty list for operator \"<=\"");
        assertFalse(appliesListCondition(null, LTE, list(ONE)));

        assertFalse(appliesListCondition(list(ONE), LTE, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(appliesListCondition(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), LTE, list(ONE)));
        assertTrue(appliesListCondition(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), LTE, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GT
        assertFalse(appliesListCondition(list(ONE), GT, list(ONE)));
        assertThrowsIRE(() -> appliesListCondition(null, GT, null), "Invalid comparison with null for operator \">\"");
        assertThrowsIRE(() -> appliesListCondition(null, GT, list()), "Invalid comparison with an empty list for operator \">\"");
        assertTrue(appliesListCondition(list(ONE), GT, list(ZERO)));
        assertFalse(appliesListCondition(list(ZERO), GT, list(ONE)));
        assertTrue(appliesListCondition(list(ONE, ONE), GT, list(ONE)));
        assertFalse(appliesListCondition(list(ONE), GT, list(ONE, ONE)));
        assertThrowsIRE(() -> appliesListCondition(list(ONE), GT, null), "Invalid comparison with null for operator \">\"");
        assertThrowsIRE(() -> appliesListCondition(list(ONE), GT, list()), "Invalid comparison with an empty list for operator \">\"");
        assertFalse(appliesListCondition(null, GT, list(ONE)));

        assertTrue(appliesListCondition(list(ONE), GT, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(appliesListCondition(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), GT, list(ONE)));
        assertFalse(appliesListCondition(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), GT, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GTE
        assertTrue(appliesListCondition(list(ONE), GTE, list(ONE)));
        assertThrowsIRE(() -> appliesListCondition(null, GTE, null), "Invalid comparison with null for operator \">=\"");
        assertThrowsIRE(() -> appliesListCondition(null, GTE, list()), "Invalid comparison with an empty list for operator \">=\"");
        assertTrue(appliesListCondition(list(ONE), GTE, list(ZERO)));
        assertFalse(appliesListCondition(list(ZERO), GTE, list(ONE)));
        assertTrue(appliesListCondition(list(ONE, ONE), GTE, list(ONE)));
        assertFalse(appliesListCondition(list(ONE), GTE, list(ONE, ONE)));
        assertThrowsIRE(() -> appliesListCondition(list(ONE), GTE, null), "Invalid comparison with null for operator \">=\"");
        assertThrowsIRE(() -> appliesListCondition(list(ONE), GTE, list()), "Invalid comparison with an empty list for operator \">=\"");
        assertFalse(appliesListCondition(null, GTE, list(ONE)));

        assertTrue(appliesListCondition(list(ONE), GTE, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(appliesListCondition(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), GTE, list(ONE)));
        assertTrue(appliesListCondition(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), GTE, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        //CONTAINS
        assertTrue(conditionContainsApplies(list(ZERO, ONE, TWO), CONTAINS, ONE));
        assertFalse(conditionContainsApplies(list(ZERO, ONE), CONTAINS, TWO));

        assertFalse(conditionContainsApplies(list(ZERO, ONE, TWO), CONTAINS, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(conditionContainsApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), CONTAINS, ONE));
        assertTrue(conditionContainsApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), CONTAINS, ByteBufferUtil.EMPTY_BYTE_BUFFER));
    }

    private static SortedSet<ByteBuffer> set(ByteBuffer... values)
    {
        SortedSet<ByteBuffer> results = new TreeSet<>(Int32Type.instance);
        results.addAll(asList(values));
        return results;
    }

    @Test
    public void testSetCollectionBoundAppliesTo() throws InvalidRequestException
    {
        // EQ
        assertTrue(appliesSetCondition(set(ONE), EQ, set(ONE)));
        assertTrue(appliesSetCondition(null, EQ, null));
        assertTrue(appliesSetCondition(null, EQ, set()));
        assertFalse(appliesSetCondition(set(ONE), EQ, set(ZERO)));
        assertFalse(appliesSetCondition(set(ZERO), EQ, set(ONE)));
        assertFalse(appliesSetCondition(set(ONE, TWO), EQ, set(ONE)));
        assertFalse(appliesSetCondition(set(ONE), EQ, set(ONE, TWO)));
        assertFalse(appliesSetCondition(set(ONE), EQ, null));
        assertFalse(appliesSetCondition(set(ONE), EQ, set()));
        assertFalse(appliesSetCondition(null, EQ, set(ONE)));

        assertFalse(appliesSetCondition(set(ONE), EQ, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(appliesSetCondition(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), EQ, set(ONE)));
        assertTrue(appliesSetCondition(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), EQ, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // NEQ
        assertFalse(appliesSetCondition(set(ONE), NEQ, set(ONE)));
        assertFalse(appliesSetCondition(null, NEQ, null));
        assertFalse(appliesSetCondition(null, NEQ, set()));
        assertTrue(appliesSetCondition(set(ONE), NEQ, set(ZERO)));
        assertTrue(appliesSetCondition(set(ZERO), NEQ, set(ONE)));
        assertTrue(appliesSetCondition(set(ONE, TWO), NEQ, set(ONE)));
        assertTrue(appliesSetCondition(set(ONE), NEQ, set(ONE, TWO)));
        assertTrue(appliesSetCondition(set(ONE), NEQ, null));
        assertTrue(appliesSetCondition(set(ONE), NEQ, set()));
        assertTrue(appliesSetCondition(null, NEQ, set(ONE)));

        assertTrue(appliesSetCondition(set(ONE), NEQ, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(appliesSetCondition(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), NEQ, set(ONE)));
        assertFalse(appliesSetCondition(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), NEQ, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LT
        assertFalse(appliesSetCondition(set(ONE), LT, set(ONE)));
        assertThrowsIRE(() -> appliesSetCondition(null, LT, null), "Invalid comparison with null for operator \"<\"");
        assertThrowsIRE(() -> appliesSetCondition(null, LT, set()), "Invalid comparison with an empty set for operator \"<\"");
        assertFalse(appliesSetCondition(set(ONE), LT, set(ZERO)));
        assertTrue(appliesSetCondition(set(ZERO), LT, set(ONE)));
        assertFalse(appliesSetCondition(set(ONE, TWO), LT, set(ONE)));
        assertTrue(appliesSetCondition(set(ONE), LT, set(ONE, TWO)));
        assertThrowsIRE(() -> appliesSetCondition(set(ONE), LT, null), "Invalid comparison with null for operator \"<\"");
        assertThrowsIRE(() -> appliesSetCondition(set(ONE), LT, set()), "Invalid comparison with an empty set for operator \"<\"");
        assertFalse(appliesSetCondition(null, LT, set(ONE)));

        assertFalse(appliesSetCondition(set(ONE), LT, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(appliesSetCondition(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), LT, set(ONE)));
        assertFalse(appliesSetCondition(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), LT, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LTE
        assertTrue(appliesSetCondition(set(ONE), LTE, set(ONE)));
        assertThrowsIRE(() -> appliesSetCondition(null, LTE, null), "Invalid comparison with null for operator \"<=\"");
        assertThrowsIRE(() -> appliesSetCondition(null, LTE, set()), "Invalid comparison with an empty set for operator \"<=\"");
        assertFalse(appliesSetCondition(set(ONE), LTE, set(ZERO)));
        assertTrue(appliesSetCondition(set(ZERO), LTE, set(ONE)));
        assertFalse(appliesSetCondition(set(ONE, TWO), LTE, set(ONE)));
        assertTrue(appliesSetCondition(set(ONE), LTE, set(ONE, TWO)));
        assertThrowsIRE(() -> appliesSetCondition(set(ONE), LTE, null), "Invalid comparison with null for operator \"<=\"");
        assertThrowsIRE(() -> appliesSetCondition(set(ONE), LTE, set()), "Invalid comparison with an empty set for operator \"<=\"");
        assertFalse(appliesSetCondition(null, LTE, set(ONE)));

        assertFalse(appliesSetCondition(set(ONE), LTE, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(appliesSetCondition(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), LTE, set(ONE)));
        assertTrue(appliesSetCondition(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), LTE, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GT
        assertFalse(appliesSetCondition(set(ONE), GT, set(ONE)));
        assertThrowsIRE(() -> appliesSetCondition(null, GT, null), "Invalid comparison with null for operator \">\"");
        assertThrowsIRE(() -> appliesSetCondition(null, GT, set()), "Invalid comparison with an empty set for operator \">\"");
        assertTrue(appliesSetCondition(set(ONE), GT, set(ZERO)));
        assertFalse(appliesSetCondition(set(ZERO), GT, set(ONE)));
        assertTrue(appliesSetCondition(set(ONE, TWO), GT, set(ONE)));
        assertFalse(appliesSetCondition(set(ONE), GT, set(ONE, TWO)));
        assertThrowsIRE(() -> appliesSetCondition(set(ONE), GT, null), "Invalid comparison with null for operator \">\"");
        assertThrowsIRE(() -> appliesSetCondition(set(ONE), GT, set()), "Invalid comparison with an empty set for operator \">\"");
        assertFalse(appliesSetCondition(null, GT, set(ONE)));

        assertTrue(appliesSetCondition(set(ONE), GT, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(appliesSetCondition(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), GT, set(ONE)));
        assertFalse(appliesSetCondition(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), GT, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GTE
        assertTrue(appliesSetCondition(set(ONE), GTE, set(ONE)));
        assertThrowsIRE(() -> appliesSetCondition(null, GTE, null), "Invalid comparison with null for operator \">=\"");
        assertThrowsIRE(() -> appliesSetCondition(null, GTE, set()), "Invalid comparison with an empty set for operator \">=\"");
        assertTrue(appliesSetCondition(set(ONE), GTE, set(ZERO)));
        assertFalse(appliesSetCondition(set(ZERO), GTE, set(ONE)));
        assertTrue(appliesSetCondition(set(ONE, TWO), GTE, set(ONE)));
        assertFalse(appliesSetCondition(set(ONE), GTE, set(ONE, TWO)));
        assertThrowsIRE(() -> appliesSetCondition(set(ONE), GTE, null), "Invalid comparison with null for operator \">=\"");
        assertThrowsIRE(() -> appliesSetCondition(set(ONE), GTE, set()), "Invalid comparison with an empty set for operator \">=\"");
        assertFalse(appliesSetCondition(null, GTE, set(ONE)));

        assertTrue(appliesSetCondition(set(ONE), GTE, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(appliesSetCondition(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), GTE, set(ONE)));
        assertTrue(appliesSetCondition(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), GTE, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // CONTAINS
        assertTrue(conditionContainsApplies(set(ZERO, ONE, TWO), CONTAINS, ONE));
        assertFalse(conditionContainsApplies(set(ZERO, ONE), CONTAINS, TWO));

        assertFalse(conditionContainsApplies(set(ZERO, ONE, TWO), CONTAINS, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(conditionContainsApplies(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), CONTAINS, ONE));
        assertTrue(conditionContainsApplies(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), CONTAINS, ByteBufferUtil.EMPTY_BYTE_BUFFER));
    }

    // values should be a list of key, value, key, value, ...
    private static SortedMap<ByteBuffer, ByteBuffer> map(ByteBuffer... values)
    {
        SortedMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
        for (int i = 0; i < values.length; i += 2)
            map.put(values[i], values[i + 1]);

        return map;
    }

    @Test
    public void testMapCollectionBoundIsSatisfiedByValue() throws InvalidRequestException
    {
        // EQ
        assertTrue(appliesMapCondition(map(ONE, ONE), EQ, map(ONE, ONE)));
        assertTrue(appliesMapCondition(null, EQ, null));
        assertTrue(appliesMapCondition(null, EQ, map()));
        assertFalse(appliesMapCondition(map(ONE, ONE), EQ, map(ZERO, ONE)));
        assertFalse(appliesMapCondition(map(ZERO, ONE), EQ, map(ONE, ONE)));
        assertFalse(appliesMapCondition(map(ONE, ONE), EQ, map(ONE, ZERO)));
        assertFalse(appliesMapCondition(map(ONE, ZERO), EQ, map(ONE, ONE)));
        assertFalse(appliesMapCondition(map(ONE, ONE, TWO, ONE), EQ, map(ONE, ONE)));
        assertFalse(appliesMapCondition(map(ONE, ONE), EQ, map(ONE, ONE, TWO, ONE)));
        assertFalse(appliesMapCondition(map(ONE, ONE), EQ, null));
        assertFalse(appliesMapCondition(map(ONE, ONE), EQ, map()));
        assertFalse(appliesMapCondition(null, EQ, map(ONE, ONE)));

        assertFalse(appliesMapCondition(map(ONE, ONE), EQ, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(appliesMapCondition(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), EQ, map(ONE, ONE)));
        assertFalse(appliesMapCondition(map(ONE, ONE), EQ, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(appliesMapCondition(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), EQ, map(ONE, ONE)));
        assertTrue(appliesMapCondition(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), EQ, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(appliesMapCondition(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), EQ, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // NEQ
        assertFalse(appliesMapCondition(map(ONE, ONE), NEQ, map(ONE, ONE)));
        assertFalse(appliesMapCondition(null, NEQ, null));
        assertFalse(appliesMapCondition(null, NEQ, map()));
        assertTrue(appliesMapCondition(map(ONE, ONE), NEQ, map(ZERO, ONE)));
        assertTrue(appliesMapCondition(map(ZERO, ONE), NEQ, map(ONE, ONE)));
        assertTrue(appliesMapCondition(map(ONE, ONE), NEQ, map(ONE, ZERO)));
        assertTrue(appliesMapCondition(map(ONE, ZERO), NEQ, map(ONE, ONE)));
        assertTrue(appliesMapCondition(map(ONE, ONE, TWO, ONE), NEQ, map(ONE, ONE)));
        assertTrue(appliesMapCondition(map(ONE, ONE), NEQ, map(ONE, ONE, TWO, ONE)));
        assertTrue(appliesMapCondition(map(ONE, ONE), NEQ, null));
        assertTrue(appliesMapCondition(map(ONE, ONE), NEQ, map()));
        assertTrue(appliesMapCondition(null, NEQ, map(ONE, ONE)));

        assertTrue(appliesMapCondition(map(ONE, ONE), NEQ, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(appliesMapCondition(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), NEQ, map(ONE, ONE)));
        assertTrue(appliesMapCondition(map(ONE, ONE), NEQ, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(appliesMapCondition(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), NEQ, map(ONE, ONE)));
        assertFalse(appliesMapCondition(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), NEQ, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(appliesMapCondition(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), NEQ, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LT
        assertFalse(appliesMapCondition(map(ONE, ONE), LT, map(ONE, ONE)));
        assertThrowsIRE(() -> appliesMapCondition(null, LT, null), "Invalid comparison with null for operator \"<\"");
        assertThrowsIRE(() -> appliesMapCondition(null, LT, map()), "Invalid comparison with an empty map for operator \"<\"");
        assertFalse(appliesMapCondition(map(ONE, ONE), LT, map(ZERO, ONE)));
        assertTrue(appliesMapCondition(map(ZERO, ONE), LT, map(ONE, ONE)));
        assertFalse(appliesMapCondition(map(ONE, ONE), LT, map(ONE, ZERO)));
        assertTrue(appliesMapCondition(map(ONE, ZERO), LT, map(ONE, ONE)));
        assertFalse(appliesMapCondition(map(ONE, ONE, TWO, ONE), LT, map(ONE, ONE)));
        assertTrue(appliesMapCondition(map(ONE, ONE), LT, map(ONE, ONE, TWO, ONE)));
        assertThrowsIRE(() -> appliesMapCondition(map(ONE, ONE), LT, null), "Invalid comparison with null for operator \"<\"");
        assertThrowsIRE(() -> appliesMapCondition(map(ONE, ONE), LT, map()), "Invalid comparison with an empty map for operator \"<\"");
        assertFalse(appliesMapCondition(null, LT, map(ONE, ONE)));

        assertFalse(appliesMapCondition(map(ONE, ONE), LT, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(appliesMapCondition(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), LT, map(ONE, ONE)));
        assertFalse(appliesMapCondition(map(ONE, ONE), LT, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(appliesMapCondition(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), LT, map(ONE, ONE)));
        assertFalse(appliesMapCondition(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), LT, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(appliesMapCondition(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), LT, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LTE
        assertTrue(appliesMapCondition(map(ONE, ONE), LTE, map(ONE, ONE)));
        assertThrowsIRE(() -> appliesMapCondition(null, LTE, null), "Invalid comparison with null for operator \"<=\"");
        assertThrowsIRE(() -> appliesMapCondition(null, LTE, map()), "Invalid comparison with an empty map for operator \"<=\"");
        assertFalse(appliesMapCondition(map(ONE, ONE), LTE, map(ZERO, ONE)));
        assertTrue(appliesMapCondition(map(ZERO, ONE), LTE, map(ONE, ONE)));
        assertFalse(appliesMapCondition(map(ONE, ONE), LTE, map(ONE, ZERO)));
        assertTrue(appliesMapCondition(map(ONE, ZERO), LTE, map(ONE, ONE)));
        assertFalse(appliesMapCondition(map(ONE, ONE, TWO, ONE), LTE, map(ONE, ONE)));
        assertTrue(appliesMapCondition(map(ONE, ONE), LTE, map(ONE, ONE, TWO, ONE)));
        assertThrowsIRE(() -> appliesMapCondition(map(ONE, ONE), LTE, null), "Invalid comparison with null for operator \"<=\"");
        assertThrowsIRE(() -> appliesMapCondition(map(ONE, ONE), LTE, map()), "Invalid comparison with an empty map for operator \"<=\"");
        assertFalse(appliesMapCondition(null, LTE, map(ONE, ONE)));

        assertFalse(appliesMapCondition(map(ONE, ONE), LTE, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(appliesMapCondition(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), LTE, map(ONE, ONE)));
        assertFalse(appliesMapCondition(map(ONE, ONE), LTE, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(appliesMapCondition(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), LTE, map(ONE, ONE)));
        assertTrue(appliesMapCondition(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), LTE, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(appliesMapCondition(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), LTE, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GT
        assertFalse(appliesMapCondition(map(ONE, ONE), GT, map(ONE, ONE)));
        assertThrowsIRE(() -> appliesMapCondition(null, GT, null), "Invalid comparison with null for operator \">\"");
        assertThrowsIRE(() -> appliesMapCondition(null, GT, map()), "Invalid comparison with an empty map for operator \">\"");
        assertTrue(appliesMapCondition(map(ONE, ONE), GT, map(ZERO, ONE)));
        assertFalse(appliesMapCondition(map(ZERO, ONE), GT, map(ONE, ONE)));
        assertTrue(appliesMapCondition(map(ONE, ONE), GT, map(ONE, ZERO)));
        assertFalse(appliesMapCondition(map(ONE, ZERO), GT, map(ONE, ONE)));
        assertTrue(appliesMapCondition(map(ONE, ONE, TWO, ONE), GT, map(ONE, ONE)));
        assertFalse(appliesMapCondition(map(ONE, ONE), GT, map(ONE, ONE, TWO, ONE)));
        assertThrowsIRE(() -> appliesMapCondition(map(ONE, ONE), GT, null), "Invalid comparison with null for operator \">\"");
        assertThrowsIRE(() -> appliesMapCondition(map(ONE, ONE), GT, map()), "Invalid comparison with an empty map for operator \">\"");
        assertFalse(appliesMapCondition(null, GT, map(ONE, ONE)));

        assertTrue(appliesMapCondition(map(ONE, ONE), GT, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(appliesMapCondition(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), GT, map(ONE, ONE)));
        assertTrue(appliesMapCondition(map(ONE, ONE), GT, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(appliesMapCondition(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), GT, map(ONE, ONE)));
        assertFalse(appliesMapCondition(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), GT, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(appliesMapCondition(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), GT, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GTE
        assertTrue(appliesMapCondition(map(ONE, ONE), GTE, map(ONE, ONE)));
        assertThrowsIRE(() -> appliesMapCondition(null, GTE, null), "Invalid comparison with null for operator \">=\"");
        assertThrowsIRE(() -> appliesMapCondition(null, GTE, map()), "Invalid comparison with an empty map for operator \">=\"");
        assertTrue(appliesMapCondition(map(ONE, ONE), GTE, map(ZERO, ONE)));
        assertFalse(appliesMapCondition(map(ZERO, ONE), GTE, map(ONE, ONE)));
        assertTrue(appliesMapCondition(map(ONE, ONE), GTE, map(ONE, ZERO)));
        assertFalse(appliesMapCondition(map(ONE, ZERO), GTE, map(ONE, ONE)));
        assertTrue(appliesMapCondition(map(ONE, ONE, TWO, ONE), GTE, map(ONE, ONE)));
        assertFalse(appliesMapCondition(map(ONE, ONE), GTE, map(ONE, ONE, TWO, ONE)));
        assertThrowsIRE(() -> appliesMapCondition(map(ONE, ONE), GTE, null), "Invalid comparison with null for operator \">=\"");
        assertThrowsIRE(() -> appliesMapCondition(map(ONE, ONE), GTE, map()), "Invalid comparison with an empty map for operator \">=\"");
        assertFalse(appliesMapCondition(null, GTE, map(ONE, ONE)));

        assertTrue(appliesMapCondition(map(ONE, ONE), GTE, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(appliesMapCondition(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), GTE, map(ONE, ONE)));
        assertTrue(appliesMapCondition(map(ONE, ONE), GTE, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(appliesMapCondition(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), GTE, map(ONE, ONE)));
        assertTrue(appliesMapCondition(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), GTE, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(appliesMapCondition(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), GTE, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        //CONTAINS
        assertTrue(conditionContainsApplies(map(ZERO, ONE), CONTAINS, ONE));
        assertFalse(conditionContainsApplies(map(ZERO, ONE), CONTAINS, ZERO));

        assertFalse(conditionContainsApplies(map(ONE, ONE), CONTAINS, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertTrue(conditionContainsApplies(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), CONTAINS, ONE));
        assertFalse(conditionContainsApplies(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), CONTAINS, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(conditionContainsApplies(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), CONTAINS, ONE));
        assertTrue(conditionContainsApplies(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), CONTAINS, ByteBufferUtil.EMPTY_BYTE_BUFFER));

        //CONTAINS KEY
        assertTrue(conditionContainsApplies(map(ZERO, ONE), CONTAINS_KEY, ZERO));
        assertFalse(conditionContainsApplies(map(ZERO, ONE), CONTAINS_KEY, ONE));

        assertFalse(conditionContainsApplies(map(ONE, ONE), CONTAINS_KEY, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(conditionContainsApplies(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), CONTAINS_KEY, ONE));
        assertTrue(conditionContainsApplies(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), CONTAINS_KEY, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertTrue(conditionContainsApplies(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), CONTAINS_KEY, ONE));
        assertFalse(conditionContainsApplies(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), CONTAINS_KEY, ByteBufferUtil.EMPTY_BYTE_BUFFER));
    }

    @Test
    public void toCQLStringTest()
    {
        ColumnIdentifier col = new ColumnIdentifier("col", false);
        Marker.Raw marker = new Marker.Raw(0);
        InMarker.Raw inMarker = new InMarker.Raw(0);
        Term.Raw one = Constants.Literal.integer("1");
        Term.Raw two = Constants.Literal.integer("2");
        Terms.Raw oneTwo = Terms.Raw.of(asList(one, two));
        Term.Raw text = Constants.Literal.string("text");

        assertEquals("col = ?", simpleCondition(col, EQ, Terms.Raw.of(marker)).toCQLString());
        assertEquals("col = 2", simpleCondition(col, EQ, Terms.Raw.of(two)).toCQLString());
        assertEquals("col = 'text'", simpleCondition(col, EQ, Terms.Raw.of(text)).toCQLString());
        assertEquals("col >= ?", simpleCondition(col, Operator.GTE, Terms.Raw.of(marker)).toCQLString());
        assertEquals("col IN ?", simpleCondition(col, Operator.IN, inMarker).toCQLString());
        assertEquals("col IN (1, 2)", simpleCondition(col, Operator.IN, oneTwo).toCQLString());
        assertEquals("col IN (1)", simpleCondition(col, Operator.IN, Terms.Raw.of(List.of(one))).toCQLString());
        assertEquals("col BETWEEN 1 AND 2", simpleCondition(col, Operator.BETWEEN, oneTwo).toCQLString());

        assertEquals("col CONTAINS 1", simpleCondition(col, CONTAINS, Terms.Raw.of(one)).toCQLString());
        assertEquals("col CONTAINS KEY 1", simpleCondition(col, CONTAINS_KEY, Terms.Raw.of(one)).toCQLString());
        assertEquals("col CONTAINS KEY ?", simpleCondition(col, CONTAINS_KEY, Terms.Raw.of(marker)).toCQLString());
        assertEquals("col CONTAINS ?", simpleCondition(col, CONTAINS, Terms.Raw.of(marker)).toCQLString());
        assertEquals("col CONTAINS ?", simpleCondition(col, CONTAINS, Terms.Raw.of(marker)).toCQLString());

        Term.Raw set = new Sets.Literal(asList(one, two));
        Term.Raw set2 = new Sets.Literal(List.of(Constants.Literal.string("baz")));

        assertEquals("col = {1, 2}", simpleCondition(col, EQ, Terms.Raw.of(set)).toCQLString());
        assertEquals("col != {'baz'}", simpleCondition(col, Operator.NEQ, Terms.Raw.of(set2)).toCQLString());

        assertEquals("col['text'] = ?", collectionElementCondition(col, text, EQ, Terms.Raw.of(marker)).toCQLString());
        assertEquals("col[?] = ?", collectionElementCondition(col, marker, EQ, Terms.Raw.of(marker)).toCQLString());

        // element access is not allowed for sets

        FieldIdentifier f = FieldIdentifier.forQuoted("f1");
        assertEquals("col.f1 = ?", udtFieldCondition(col, f, EQ, Terms.Raw.of(marker)).toCQLString());
        assertEquals("col.f1 = 1", udtFieldCondition(col, f, EQ, Terms.Raw.of(one)).toCQLString());
    }

    @Test
    public void testUDTBound() throws InvalidRequestException
    {
        // EQ
        assertTrue(conditionUDTApplies(ONE, EQ, ONE));
        assertFalse(conditionUDTApplies(ONE, EQ, ZERO));
        assertFalse(conditionUDTApplies(ZERO, EQ, ONE));
        assertFalse(conditionUDTApplies(ONE, EQ, null));

        assertFalse(conditionUDTApplies(ONE, EQ, null));
        assertFalse(conditionUDTApplies(null, EQ, ONE));
        assertTrue(conditionUDTApplies(null, EQ, null));

        assertFalse(conditionUDTApplies(ONE, EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(conditionUDTApplies(ByteBufferUtil.EMPTY_BYTE_BUFFER, EQ, ONE));
        assertTrue(conditionUDTApplies(ByteBufferUtil.EMPTY_BYTE_BUFFER, EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER));

        // NEQ
        assertFalse(conditionUDTApplies(ONE, NEQ, ONE));
        assertTrue(conditionUDTApplies(ONE, NEQ, ZERO));
        assertTrue(conditionUDTApplies(ZERO, NEQ, ONE));
        assertTrue(conditionUDTApplies(ONE, NEQ, null));
        assertTrue(conditionUDTApplies(null, NEQ, ONE));

        assertTrue(conditionUDTApplies(ONE, NEQ, null));
        assertTrue(conditionUDTApplies(null, NEQ, ONE));
        assertFalse(conditionUDTApplies(null, NEQ, null));

        assertTrue(conditionUDTApplies(ONE, NEQ, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertTrue(conditionUDTApplies(ByteBufferUtil.EMPTY_BYTE_BUFFER, NEQ, ONE));
        assertFalse(conditionUDTApplies(ByteBufferUtil.EMPTY_BYTE_BUFFER, NEQ, ByteBufferUtil.EMPTY_BYTE_BUFFER));

        // LT
        assertFalse(conditionUDTApplies(ONE, LT, ONE));
        assertThatThrownBy(() -> conditionUDTApplies(null, LT, null)).isInstanceOf(InvalidRequestException.class);
        assertFalse(conditionUDTApplies(ONE, LT, ZERO));
        assertTrue(conditionUDTApplies(ZERO, LT, ONE));
        assertThatThrownBy(() -> conditionUDTApplies(ONE, LT, null)).isInstanceOf(InvalidRequestException.class);

        assertFalse(conditionUDTApplies(ONE, LT, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertTrue(conditionUDTApplies(ByteBufferUtil.EMPTY_BYTE_BUFFER, LT, ONE));
        assertFalse(conditionUDTApplies(ByteBufferUtil.EMPTY_BYTE_BUFFER, LT, ByteBufferUtil.EMPTY_BYTE_BUFFER));

        // LTE
        assertTrue(conditionUDTApplies(ONE, LTE, ONE));
        assertFalse(conditionUDTApplies(ONE, LTE, ZERO));
        assertTrue(conditionUDTApplies(ZERO, LTE, ONE));
        assertThatThrownBy(() -> conditionUDTApplies(ONE, LTE, null)).isInstanceOf(InvalidRequestException.class);

        assertFalse(conditionUDTApplies(ONE, LTE, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertTrue(conditionUDTApplies(ByteBufferUtil.EMPTY_BYTE_BUFFER, LTE, ONE));
        assertTrue(conditionUDTApplies(ByteBufferUtil.EMPTY_BYTE_BUFFER, LTE, ByteBufferUtil.EMPTY_BYTE_BUFFER));

        // GT
        assertFalse(conditionUDTApplies(ONE, GT, ONE));
        assertTrue(conditionUDTApplies(ONE, GT, ZERO));
        assertFalse(conditionUDTApplies(ZERO, GT, ONE));
        assertThatThrownBy(() -> conditionUDTApplies(ONE, GT, null)).isInstanceOf(InvalidRequestException.class);

        assertTrue(conditionUDTApplies(ONE, GT, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(conditionUDTApplies(ByteBufferUtil.EMPTY_BYTE_BUFFER, GT, ONE));
        assertFalse(conditionUDTApplies(ByteBufferUtil.EMPTY_BYTE_BUFFER, GT, ByteBufferUtil.EMPTY_BYTE_BUFFER));

        // GTE
        assertTrue(conditionUDTApplies(ONE, GTE, ONE));
        assertTrue(conditionUDTApplies(ONE, GTE, ZERO));
        assertFalse(conditionUDTApplies(ZERO, GTE, ONE));
        assertTrue(conditionUDTApplies(ONE, GTE, ONE));
        assertThatThrownBy(() -> conditionUDTApplies(ONE, GTE, null)).isInstanceOf(InvalidRequestException.class);

        assertTrue(conditionUDTApplies(ONE, GTE, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(conditionUDTApplies(ByteBufferUtil.EMPTY_BYTE_BUFFER, GTE, ONE));
        assertTrue(conditionUDTApplies(ByteBufferUtil.EMPTY_BYTE_BUFFER, GTE, ByteBufferUtil.EMPTY_BYTE_BUFFER));
    }

    @Test
    public void serde()
    {
        DataOutputBuffer out = new DataOutputBuffer();
        qt().forAll(boundGen()).check(bounds -> {
            Schema.instance = Mockito.mock(SchemaProvider.class);
            Mockito.when(Schema.instance.getColumnMetadata(Mockito.eq(bounds.column.ksName), Mockito.eq(bounds.column.cfName), Mockito.eq(bounds.column.name.bytes))).thenReturn(bounds.column);
            for (MessagingService.Version version : MessagingService.Version.MIN_ACCORD_VERSION.greaterThanOrEqual())
                IVersionedSerializers.testSerde(out, ColumnCondition.Bound.serializer, bounds, version.value);
        });
    }

    private static Gen<ColumnMetadata> columnMetadataGen(ColumnCondition.BoundKind kind)
    {
        var typeGen = selectTypes(kind);
        var columnKindGen = selectColumnKinds(kind);
        return Generators.toGen(CassandraGenerators.columnMetadataGen(columnKindGen, typeGen));
    }

    private static org.quicktheories.core.Gen<ColumnMetadata.Kind> selectColumnKinds(ColumnCondition.BoundKind kind)
    {
        if (kind == ColumnCondition.BoundKind.MultiCell || kind == ColumnCondition.BoundKind.ElementOrFieldAccess)
            return SourceDSL.arbitrary().pick(ColumnMetadata.Kind.STATIC, ColumnMetadata.Kind.REGULAR);
        return SourceDSL.arbitrary().enumValues(ColumnMetadata.Kind.class);
    }

    private static ColumnMetadata createColumnMetadata(RandomSource rs, ColumnCondition.BoundKind kind)
    {
        return columnMetadataGen(kind).next(rs);
    }

    private static org.quicktheories.core.Gen<AbstractType<?>> selectTypes(ColumnCondition.BoundKind kind)
    {
        switch (kind)
        {
            // A condition on a single non-collection column.
            case Simple:
                return new AbstractTypeGenerators.TypeGenBuilder().build();
            // A condition on a multicell column.
            // assert column.type.isMultiCell();
            case MultiCell:
                return new AbstractTypeGenerators.TypeGenBuilder().withTypeKinds(TypeKind.UDT, TypeKind.LIST, TypeKind.MAP, TypeKind.SET).withMultiCell(true).build();
            // The map key, list index or UDT fieldname.
            case ElementOrFieldAccess:
                return new AbstractTypeGenerators.TypeGenBuilder().withTypeKinds(TypeKind.UDT, TypeKind.LIST, TypeKind.MAP).withMultiCell(true).build();
            default: throw new UnsupportedOperationException(kind.name());
        }
    }

    private static Gen<ColumnCondition.Bound> boundGen()
    {
        Gen<ColumnCondition.BoundKind> kindGen = Gens.enums().all(ColumnCondition.BoundKind.class);
        Gen<Operator> operatorGen = Gens.enums().all(Operator.class);
        Gen<ByteBuffer> nonNullValuesGen = Generators.toGen(Generators.bytes(1, 100));
        Gen<ByteBuffer> valueGen = rs -> {
            if (rs.decide(.2)) return null;
            return nonNullValuesGen.next(rs);
        };

        return rs -> {
            ColumnCondition.BoundKind kind = kindGen.next(rs);
            ColumnMetadata metadata = createColumnMetadata(rs, kind);
            Operator operator = operatorGen.next(rs);
            ByteBuffer value = valueGen.next(rs);
            switch (kind)
            {
                // A condition on a single non-collection column.
                case Simple: return new ColumnCondition.SimpleBound(metadata, operator, value);
                // A condition on a multicell column.
                // assert column.type.isMultiCell();
                case MultiCell: return new ColumnCondition.MultiCellBound(metadata, operator, value);
                // The map key, list index or UDT fieldname.
                case ElementOrFieldAccess: return new ColumnCondition.ElementOrFieldAccessBound(metadata, Generators.toGen(AbstractTypeGenerators.elementAccess(metadata.type).bytesGen()).next(rs), operator, value);
                default: throw new UnsupportedOperationException(kind.name());
            }
        };
    }
}
