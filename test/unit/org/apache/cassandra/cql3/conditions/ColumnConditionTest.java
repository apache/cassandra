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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Terms;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.serializers.SetSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.cql3.Operator.CONTAINS;
import static org.apache.cassandra.cql3.Operator.CONTAINS_KEY;
import static org.apache.cassandra.cql3.Operator.EQ;
import static org.apache.cassandra.cql3.Operator.GT;
import static org.apache.cassandra.cql3.Operator.GTE;
import static org.apache.cassandra.cql3.Operator.LT;
import static org.apache.cassandra.cql3.Operator.LTE;
import static org.apache.cassandra.cql3.Operator.NEQ;
import static org.apache.cassandra.transport.ProtocolVersion.CURRENT;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class ColumnConditionTest extends CQLTester
{
    public static final ByteBuffer ZERO = Int32Type.instance.fromString("0");
    public static final ByteBuffer ONE = Int32Type.instance.fromString("1");
    public static final ByteBuffer TWO = Int32Type.instance.fromString("2");

    private static final ListType<Integer> listType = ListType.getInstance(Int32Type.instance, true);
    private static final MapType<Integer, Integer> mapType = MapType.getInstance(Int32Type.instance, Int32Type.instance, true);
    private static final SetType<Integer> setType = SetType.getInstance(Int32Type.instance, true);

    private Row newRow(ColumnMetadata definition, ByteBuffer value)
    {
        BufferCell cell;
        if (definition.type.isUDT() )
        {
            if (definition.type.isMultiCell()) {
                CellPath cellPath = udtType.cellPathForField(udtType.fieldName(0));
                cell = new BufferCell(definition, 0L, Cell.NO_TTL, Cell.NO_DELETION_TIME, value, cellPath);
            }
            else
            {
                ByteBuffer udtValue = UserType.buildValue(value, TWO);
                cell = new BufferCell(definition, 0L, Cell.NO_TTL, Cell.NO_DELETION_TIME, udtValue, null);
            }
        }
        else
        {
            cell = new BufferCell(definition, 0L, Cell.NO_TTL, Cell.NO_DELETION_TIME, value, null);
        }
        return BTreeRow.singleCellRow(Clustering.EMPTY, cell);
    }

    private static Row newRow(ColumnMetadata definition, List<ByteBuffer> values)
    {
        AbstractType<?> type = definition.type;
        if (type.isFrozenCollection())
        {
            ByteBuffer cellValue = ListSerializer.pack(values, values.size(), CURRENT);
            Cell<ByteBuffer> cell = new BufferCell(definition, 0L, Cell.NO_TTL, Cell.NO_DELETION_TIME, cellValue, null);
            return BTreeRow.singleCellRow(Clustering.EMPTY, cell);
        }
        else
        {
            Row.Builder builder = BTreeRow.sortedBuilder();
            builder.newRow(Clustering.EMPTY);
            long now = System.currentTimeMillis();
            if (values != null)
            {
                for (int i = 0, m = values.size(); i < m; i++)
                {
                    BufferCell cell;
                    if (type.isUDT())
                    {
                        cell = new BufferCell(definition,
                                              0L,
                                              Cell.NO_TTL,
                                              Cell.NO_DELETION_TIME,
                                              values.get(i),
                                              ((UserType)type).cellPathForField(((UserType) type).fieldName(i)));
                    }
                    else
                    {
                        TimeUUID uuid = TimeUUID.Generator.atUnixMillis(now, i);
                        ByteBuffer key = uuid.toBytes();
                        ByteBuffer value = values.get(i);
                        cell = new BufferCell(definition,
                                              0L,
                                              Cell.NO_TTL,
                                              Cell.NO_DELETION_TIME,
                                              value,
                                              CellPath.create(key));
                    }
                    builder.addCell(cell);
                }
            }
            return builder.build();
        }
    }

    private static Row newRow(ColumnMetadata definition, SortedSet<ByteBuffer> values)
    {
        if (definition.type.isFrozenCollection())
        {
            ByteBuffer cellValue = SetSerializer.pack(values, values.size(), CURRENT);
            Cell<ByteBuffer> cell = new BufferCell(definition, 0L, Cell.NO_TTL, Cell.NO_DELETION_TIME, cellValue, null);
            return BTreeRow.singleCellRow(Clustering.EMPTY, cell);
        }
        else
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
    }

    private static Row newRow(ColumnMetadata definition, SortedMap<ByteBuffer, ByteBuffer> values)
    {
        if (definition.type.isFrozenCollection())
        {
            List<ByteBuffer> packableValues = values.entrySet().stream().flatMap(entry -> Stream.of(entry.getKey(), entry.getValue())).collect(Collectors.toList());
            ByteBuffer cellValue = MapSerializer.pack(packableValues, values.size(), CURRENT);
            Cell<ByteBuffer> cell = new BufferCell(definition, 0L, Cell.NO_TTL, Cell.NO_DELETION_TIME, cellValue, null);
            return BTreeRow.singleCellRow(Clustering.EMPTY, cell);
        }
        else
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
    }

    private static boolean testRoundtripped(ColumnCondition.Bound bound, Row row)
    {
        DataOutputBuffer dab = new DataOutputBuffer();
        IVersionedSerializer<ColumnCondition.Bound> serializer = ColumnCondition.Bound.serializer;
        int version = MessagingService.current_version;
        try
        {
            serializer.serialize(bound, dab, version);
            assertEquals(serializer.serializedSize(bound, version), dab.position());
            ColumnCondition.Bound deserializedBound = serializer.deserialize(new DataInputBuffer(dab.buffer(), false), version);
            boolean originalResult = bound.appliesTo(row);
            assertEquals(originalResult, deserializedBound.appliesTo(row));
            return originalResult;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private boolean conditionApplies(ByteBuffer rowValue, Operator op, ByteBuffer conditionValue)
    {
        AbstractType<?> columnType = Int32Type.instance;
        ColumnMetadata definition = ColumnMetadata.regularColumn(KEYSPACE, maybeCreateTable(columnType, false), "c", columnType);
        ColumnCondition condition = ColumnCondition.condition(definition, op, Terms.of(new Constants.Value(conditionValue)));
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        boolean regularColumnResult = testRoundtripped(bound, newRow(definition, rowValue));
        // Every simple bound test is also a valid test of the UDT access path
        boolean conditionUDTApplies = conditionUDTApplies(rowValue, op, conditionValue);
        assertEquals(regularColumnResult, conditionUDTApplies);
        return regularColumnResult;
    }

    private boolean conditionApplies(List<ByteBuffer> rowValue, Operator op, List<ByteBuffer> conditionValue)
    {
        boolean nonFrozenResult = conditionApplies(rowValue, op, conditionValue, listType);
        boolean frozenResult = conditionApplies(rowValue, op, conditionValue, listType.freeze());
        assertEquals(nonFrozenResult, frozenResult);
        return nonFrozenResult;
    }

    private boolean conditionApplies(List<ByteBuffer> rowValue, Operator op, List<ByteBuffer> conditionValue, ListType<Integer> columnType)
    {
        ColumnMetadata definition = ColumnMetadata.regularColumn(KEYSPACE, maybeCreateTable(columnType, columnType.isFrozenCollection()), "c", columnType);
        ColumnCondition condition = ColumnCondition.condition(definition, op, Terms.of(new Lists.Value(conditionValue)));
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return testRoundtripped(bound, newRow(definition, rowValue));
    }

    private boolean conditionContainsApplies(List<ByteBuffer> rowValue, Operator op, ByteBuffer conditionValue)
    {
        boolean nonFrozenResult = conditionContainsApplies(rowValue, op, conditionValue, listType);
        boolean frozenResult = conditionContainsApplies(rowValue, op, conditionValue, listType.freeze());
        assertEquals(nonFrozenResult, frozenResult);
        return frozenResult;
    }

    private boolean conditionContainsApplies(List<ByteBuffer> rowValue, Operator op, ByteBuffer conditionValue, ListType<Integer> columnType)
    {
        ColumnMetadata definition = ColumnMetadata.regularColumn(KEYSPACE, maybeCreateTable(columnType, columnType.isFrozenCollection()), "c", columnType);
        ColumnCondition condition = ColumnCondition.condition(definition, op, Terms.of(new Constants.Value(conditionValue)));
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return testRoundtripped(bound, newRow(definition, rowValue));
    }

    private boolean conditionContainsApplies(SortedMap<ByteBuffer, ByteBuffer> rowValue, Operator op, ByteBuffer conditionValue)
    {
        boolean nonFrozenResult = conditionContainsApplies(rowValue, op, conditionValue, mapType);
        boolean frozenResult = conditionContainsApplies(rowValue, op, conditionValue, mapType.freeze());
        assertEquals(nonFrozenResult, frozenResult);
        return frozenResult;
    }

    private boolean conditionContainsApplies(SortedMap<ByteBuffer, ByteBuffer> rowValue, Operator op, ByteBuffer conditionValue, MapType<Integer, Integer> columnType)
    {
        ColumnMetadata definition = ColumnMetadata.regularColumn(KEYSPACE, maybeCreateTable(columnType, columnType.isFrozenCollection()), "c", columnType);
        ColumnCondition condition = ColumnCondition.condition(definition, op, Terms.of(new Constants.Value(conditionValue)));
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return testRoundtripped(bound, newRow(definition, rowValue));
    }

    private boolean conditionApplies(SortedSet<ByteBuffer> rowValue, Operator op, SortedSet<ByteBuffer> conditionValue)
    {
        boolean nonFrozenResult = conditionApplies(rowValue, op, conditionValue, setType);
        boolean frozenResult = conditionApplies(rowValue, op, conditionValue, setType.freeze());
        assertEquals(nonFrozenResult, frozenResult);
        return nonFrozenResult;
    }

    private boolean conditionApplies(SortedSet<ByteBuffer> rowValue, Operator op, SortedSet<ByteBuffer> conditionValue, SetType<Integer> columnType)
    {
        ColumnMetadata definition = ColumnMetadata.regularColumn(KEYSPACE, maybeCreateTable(columnType, columnType.isFrozenCollection()), "c", columnType);
        ColumnCondition condition = ColumnCondition.condition(definition, op, Terms.of(new Sets.Value(conditionValue)));
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return testRoundtripped(bound, newRow(definition, rowValue));
    }

    private boolean conditionContainsApplies(SortedSet<ByteBuffer> rowValue, Operator op, ByteBuffer conditionValue)
    {
        boolean nonFrozenResult = conditionContainsApplies(rowValue, op, conditionValue, setType);
        boolean frozenResult = conditionContainsApplies(rowValue, op, conditionValue, setType.freeze());
        assertEquals(nonFrozenResult, frozenResult);
        return frozenResult;
    }

    private boolean conditionContainsApplies(SortedSet<ByteBuffer> rowValue, Operator op, ByteBuffer conditionValue, SetType<Integer> columnType)
    {
        ColumnMetadata definition = ColumnMetadata.regularColumn(KEYSPACE, maybeCreateTable(columnType, columnType.isFrozenCollection()), "c", columnType);
        ColumnCondition condition = ColumnCondition.condition(definition, op, Terms.of(new Constants.Value(conditionValue)));
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return testRoundtripped(bound, newRow(definition, rowValue));
    }

    private boolean conditionApplies(SortedMap<ByteBuffer, ByteBuffer> rowValue, Operator op, SortedMap<ByteBuffer, ByteBuffer> conditionValue)
    {
        boolean nonFrozenResult = conditionApplies(rowValue, op, conditionValue, mapType);
        boolean frozenResult = conditionApplies(rowValue, op, conditionValue, mapType.freeze());
        assertEquals(nonFrozenResult, frozenResult);
        return nonFrozenResult;
    }

    private boolean conditionApplies(SortedMap<ByteBuffer, ByteBuffer> rowValue, Operator op, SortedMap<ByteBuffer, ByteBuffer> conditionValue, MapType<Integer, Integer> columnType)
    {
        ColumnMetadata definition = ColumnMetadata.regularColumn(KEYSPACE, maybeCreateTable(columnType, columnType.isFrozenCollection()), "c", columnType);
        ColumnCondition condition = ColumnCondition.condition(definition, op, Terms.of(new Maps.Value(conditionValue)));
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return testRoundtripped(bound, newRow(definition, rowValue));
    }

    private boolean conditionElementApplies(List<ByteBuffer> values, ByteBuffer elementIndex, Operator op, ByteBuffer elementValue)
    {
        boolean nonFrozenResult = conditionElementApplies(values, elementIndex, op, elementValue, listType);
        boolean frozenResult = conditionElementApplies(values, elementIndex, op, elementValue, listType.freeze());
        assertEquals(nonFrozenResult, frozenResult);
        return nonFrozenResult;
    }

    private boolean conditionElementApplies(List<ByteBuffer> values, ByteBuffer elementIndex, Operator op, ByteBuffer elementValue, ListType<Integer> columnType)
    {
        ColumnMetadata definition = ColumnMetadata.regularColumn(KEYSPACE, maybeCreateTable(columnType, columnType.isFrozenCollection()), "c", columnType);
        ColumnCondition condition = ColumnCondition.condition(definition, new Constants.Value(elementIndex), op, Terms.of(new Constants.Value(elementValue)));
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return testRoundtripped(bound, newRow(definition, values));
    }

    private boolean conditionElementApplies(SortedMap<ByteBuffer, ByteBuffer> rowValue, ByteBuffer elementKey, Operator op, ByteBuffer elementValue)
    {
        boolean nonFrozenResult = conditionElementApplies(rowValue, elementKey, op, elementValue, mapType);
        boolean frozenResult = conditionElementApplies(rowValue, elementKey, op, elementValue, mapType.freeze());
        assertEquals(nonFrozenResult, frozenResult);
        return nonFrozenResult;
    }

    private boolean conditionElementApplies(SortedMap<ByteBuffer, ByteBuffer> rowValue, ByteBuffer elementKey, Operator op, ByteBuffer elementValue, MapType<Integer, Integer> columnType)
    {
        ColumnMetadata definition = ColumnMetadata.regularColumn(KEYSPACE, maybeCreateTable(columnType, columnType.isFrozenCollection()), "c", columnType);
        ColumnCondition condition = ColumnCondition.condition(definition, new Constants.Value(elementKey), op, Terms.of(new Constants.Value(elementValue)));
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return testRoundtripped(bound, newRow(definition, rowValue));
    }

    private boolean conditionUDTApplies(ByteBuffer fieldValue, Operator op, ByteBuffer elementValue)
    {
        boolean nonFrozenResult = conditionUDTApplies(fieldValue, op, elementValue, udtType);
        boolean frozenResult = conditionUDTApplies(fieldValue, op, elementValue, udtType.freeze());
        assertEquals(nonFrozenResult, frozenResult);
        return nonFrozenResult;
    }

    private boolean conditionUDTApplies(ByteBuffer fieldValue, Operator op, ByteBuffer elementValue, UserType type)
    {
        ColumnMetadata definition = ColumnMetadata.regularColumn(KEYSPACE, maybeCreateTable(type, !type.isMultiCell()), "c", type);
        ColumnCondition condition = ColumnCondition.condition(definition, type.fieldName(0), op, Terms.of(new Constants.Value(elementValue)));
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return testRoundtripped(bound, newRow(definition, fieldValue));
    }

    private boolean conditionUDTApplies(List<ByteBuffer> rowValue, Operator op, List<ByteBuffer> conditionValue)
    {
        ColumnMetadata definition = ColumnMetadata.regularColumn(KEYSPACE, maybeCreateTable(udtType, false), "c", udtType);
        Term term = conditionValue == null ? Constants.NULL_VALUE : new UserTypes.Value(udtType, conditionValue.toArray(new ByteBuffer[0]));
        ColumnCondition condition = ColumnCondition.condition(definition, op, Terms.of(term));
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        return testRoundtripped(bound, newRow(definition, rowValue));
    }

    @Override
    public void beforeTest() throws Throwable
    {
        super.beforeTest();
        String typeName = createType("CREATE TYPE %s (a int, b int);");
        udtType = Schema.instance.getKeyspaceMetadata(KEYSPACE).types.get(ByteBufferUtil.bytes(typeName)).get();
    }
    @Override
    public void afterTest() throws Throwable
    {
        super.afterTest();
        typeToTable.clear();
        udtType = null;
    }

    // Is this useful enough to have in CQL tester?
    private UserType udtType;
    private final Map<Pair<AbstractType<?>, Boolean>, String> typeToTable = new HashMap<>();

    private String maybeCreateTable(AbstractType<?> columnType, boolean frozen)
    {
        String columnTypeCQL = columnType.asCQL3Type().toString();
        String maybeFrozenColumnTypeCQL = frozen ? columnTypeCQL : "frozen<%s>".format(columnTypeCQL);
        return typeToTable.computeIfAbsent(Pair.create(columnType, frozen), type -> createTable("CREATE TABLE %s (id int primary key, c " + maybeFrozenColumnTypeCQL + ")"));
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
            Assert.assertTrue("Expected error message to contain '" + errorMessage + "', but got '" + e.getMessage() + "'",
                              e.getMessage().contains(errorMessage));
        }
    }

    @Test
    public void testSimpleAndUDTBoundIsSatisfiedByValue() throws InvalidRequestException
    {
        // EQ
        assertTrue(conditionApplies(ONE, EQ, ONE));
        assertFalse(conditionApplies(TWO, EQ, ONE));
        assertFalse(conditionApplies(ONE, EQ, TWO));
        assertFalse(conditionApplies(ONE, EQ, EMPTY_BYTE_BUFFER));
        assertFalse(conditionApplies(EMPTY_BYTE_BUFFER, EQ, ONE));
        assertTrue(conditionApplies(EMPTY_BYTE_BUFFER, EQ, EMPTY_BYTE_BUFFER));
        assertFalse(conditionApplies(ONE, EQ, null));
        assertFalse(conditionApplies(null, EQ, ONE));
        assertTrue(conditionApplies(null, EQ, (ByteBuffer) null));

        // NEQ
        assertFalse(conditionApplies(ONE, NEQ, ONE));
        assertTrue(conditionApplies(TWO, NEQ, ONE));
        assertTrue(conditionApplies(ONE, NEQ, TWO));
        assertTrue(conditionApplies(ONE, NEQ, EMPTY_BYTE_BUFFER));
        assertTrue(conditionApplies(EMPTY_BYTE_BUFFER, NEQ, ONE));
        assertFalse(conditionApplies(EMPTY_BYTE_BUFFER, NEQ, EMPTY_BYTE_BUFFER));
        assertTrue(conditionApplies(ONE, NEQ, null));
        assertTrue(conditionApplies(null, NEQ, ONE));
        assertFalse(conditionApplies(null, NEQ, (ByteBuffer) null));

        // LT
        assertFalse(conditionApplies(ONE, LT, ONE));
        assertFalse(conditionApplies(TWO, LT, ONE));
        assertTrue(conditionApplies(ONE, LT, TWO));
        assertFalse(conditionApplies(ONE, LT, EMPTY_BYTE_BUFFER));
        assertTrue(conditionApplies(EMPTY_BYTE_BUFFER, LT, ONE));
        assertFalse(conditionApplies(EMPTY_BYTE_BUFFER, LT, EMPTY_BYTE_BUFFER));
        assertThrowsIRE(() -> conditionApplies(ONE, LT, null), "Invalid comparison with null for operator \"<\"");
        assertFalse(conditionApplies(null, LT, ONE));

        // LTE
        assertTrue(conditionApplies(ONE, LTE, ONE));
        assertFalse(conditionApplies(TWO, LTE, ONE));
        assertTrue(conditionApplies(ONE, LTE, TWO));
        assertFalse(conditionApplies(ONE, LTE, EMPTY_BYTE_BUFFER));
        assertTrue(conditionApplies(EMPTY_BYTE_BUFFER, LTE, ONE));
        assertTrue(conditionApplies(EMPTY_BYTE_BUFFER, LTE, EMPTY_BYTE_BUFFER));
        assertThrowsIRE(() -> conditionApplies(ONE, LTE, null), "Invalid comparison with null for operator \"<=\"");
        assertFalse(conditionApplies(null, LTE, ONE));

        // GT
        assertFalse(conditionApplies(ONE, GT, ONE));
        assertTrue(conditionApplies(TWO, GT, ONE));
        assertFalse(conditionApplies(ONE, GT, TWO));
        assertTrue(conditionApplies(ONE, GT, EMPTY_BYTE_BUFFER));
        assertFalse(conditionApplies(EMPTY_BYTE_BUFFER, GT, ONE));
        assertFalse(conditionApplies(EMPTY_BYTE_BUFFER, GT, EMPTY_BYTE_BUFFER));
        assertThrowsIRE(() -> conditionApplies(ONE, GT, null), "Invalid comparison with null for operator \">\"");
        assertFalse(conditionApplies(null, GT, ONE));

        // GTE
        assertTrue(conditionApplies(ONE, GTE, ONE));
        assertTrue(conditionApplies(TWO, GTE, ONE));
        assertFalse(conditionApplies(ONE, GTE, TWO));
        assertTrue(conditionApplies(ONE, GTE, EMPTY_BYTE_BUFFER));
        assertFalse(conditionApplies(EMPTY_BYTE_BUFFER, GTE, ONE));
        assertTrue(conditionApplies(EMPTY_BYTE_BUFFER, GTE, EMPTY_BYTE_BUFFER));
        assertThrowsIRE(() -> conditionApplies(ONE, GTE, null), "Invalid comparison with null for operator \">=\"");
        assertFalse(conditionApplies(null, GTE, ONE));
    }

    private static List<ByteBuffer> list(ByteBuffer... values)
    {
        return Arrays.asList(values);
    }

    @Test
    // sets use the same check as lists
    public void testListCollectionBoundAppliesTo() throws InvalidRequestException
    {
        // EQ
        assertTrue(conditionApplies(list(ONE), EQ, list(ONE)));
        assertTrue(conditionApplies(list(), EQ, list()));
        assertFalse(conditionApplies(list(ONE), EQ, list(ZERO)));
        assertFalse(conditionApplies(list(ZERO), EQ, list(ONE)));
        assertFalse(conditionApplies(list(ONE, ONE), EQ, list(ONE)));
        assertFalse(conditionApplies(list(ONE), EQ, list(ONE, ONE)));
        assertFalse(conditionApplies(list(ONE), EQ, list()));
        assertFalse(conditionApplies(list(), EQ, list(ONE)));

        assertFalse(conditionApplies(list(ONE), EQ, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(conditionApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), EQ, list(ONE)));
        assertTrue(conditionApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), EQ, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // NEQ
        assertFalse(conditionApplies(list(ONE), NEQ, list(ONE)));
        assertFalse(conditionApplies(list(), NEQ, list()));
        assertTrue(conditionApplies(list(ONE), NEQ, list(ZERO)));
        assertTrue(conditionApplies(list(ZERO), NEQ, list(ONE)));
        assertTrue(conditionApplies(list(ONE, ONE), NEQ, list(ONE)));
        assertTrue(conditionApplies(list(ONE), NEQ, list(ONE, ONE)));
        assertTrue(conditionApplies(list(ONE), NEQ, list()));
        assertTrue(conditionApplies(list(), NEQ, list(ONE)));

        assertTrue(conditionApplies(list(ONE), NEQ, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(conditionApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), NEQ, list(ONE)));
        assertFalse(conditionApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), NEQ, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LT
        assertFalse(conditionApplies(list(ONE), LT, list(ONE)));
        assertFalse(conditionApplies(list(), LT, list()));
        assertFalse(conditionApplies(list(ONE), LT, list(ZERO)));
        assertTrue(conditionApplies(list(ZERO), LT, list(ONE)));
        assertFalse(conditionApplies(list(ONE, ONE), LT, list(ONE)));
        assertTrue(conditionApplies(list(ONE), LT, list(ONE, ONE)));
        assertFalse(conditionApplies(list(ONE), LT, list()));
        assertTrue(conditionApplies(list(), LT, list(ONE)));

        assertFalse(conditionApplies(list(ONE), LT, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(conditionApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), LT, list(ONE)));
        assertFalse(conditionApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), LT, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LTE
        assertTrue(conditionApplies(list(ONE), LTE, list(ONE)));
        assertTrue(conditionApplies(list(), LTE, list()));
        assertFalse(conditionApplies(list(ONE), LTE, list(ZERO)));
        assertTrue(conditionApplies(list(ZERO), LTE, list(ONE)));
        assertFalse(conditionApplies(list(ONE, ONE), LTE, list(ONE)));
        assertTrue(conditionApplies(list(ONE), LTE, list(ONE, ONE)));
        assertFalse(conditionApplies(list(ONE), LTE, list()));
        assertTrue(conditionApplies(list(), LTE, list(ONE)));

        assertFalse(conditionApplies(list(ONE), LTE, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(conditionApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), LTE, list(ONE)));
        assertTrue(conditionApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), LTE, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GT
        assertFalse(conditionApplies(list(ONE), GT, list(ONE)));
        assertFalse(conditionApplies(list(), GT, list()));
        assertTrue(conditionApplies(list(ONE), GT, list(ZERO)));
        assertFalse(conditionApplies(list(ZERO), GT, list(ONE)));
        assertTrue(conditionApplies(list(ONE, ONE), GT, list(ONE)));
        assertFalse(conditionApplies(list(ONE), GT, list(ONE, ONE)));
        assertTrue(conditionApplies(list(ONE), GT, list()));
        assertFalse(conditionApplies(list(), GT, list(ONE)));

        assertTrue(conditionApplies(list(ONE), GT, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(conditionApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), GT, list(ONE)));
        assertFalse(conditionApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), GT, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GTE
        assertTrue(conditionApplies(list(ONE), GTE, list(ONE)));
        assertTrue(conditionApplies(list(), GTE, list()));
        assertTrue(conditionApplies(list(ONE), GTE, list(ZERO)));
        assertFalse(conditionApplies(list(ZERO), GTE, list(ONE)));
        assertTrue(conditionApplies(list(ONE, ONE), GTE, list(ONE)));
        assertFalse(conditionApplies(list(ONE), GTE, list(ONE, ONE)));
        assertTrue(conditionApplies(list(ONE), GTE, list()));
        assertFalse(conditionApplies(list(), GTE, list(ONE)));

        assertTrue(conditionApplies(list(ONE), GTE, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(conditionApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), GTE, list(ONE)));
        assertTrue(conditionApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), GTE, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        //CONTAINS
        assertTrue(conditionContainsApplies(list(ZERO, ONE, TWO), CONTAINS, ONE));
        assertFalse(conditionContainsApplies(list(ZERO, ONE), CONTAINS, TWO));

        assertFalse(conditionContainsApplies(list(ZERO, ONE, TWO), CONTAINS, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(conditionContainsApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), CONTAINS, ONE));
        assertTrue(conditionContainsApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), CONTAINS, ByteBufferUtil.EMPTY_BYTE_BUFFER));

        //ELEMENT
        assertTrue(conditionElementApplies(list(ZERO, ONE, TWO), ZERO, EQ, ZERO));
        assertTrue(conditionElementApplies(list(ZERO, ONE, TWO), ONE, EQ, ONE));
        assertTrue(conditionElementApplies(list(ZERO, ONE, TWO), TWO, EQ, TWO));

        assertFalse(conditionElementApplies(list(ZERO, ONE, TWO), ZERO, EQ, ONE));
        assertFalse(conditionElementApplies(list(ZERO, ONE, TWO), ZERO, EQ, TWO));
        assertFalse(conditionElementApplies(list(ZERO, ONE, TWO), ONE, EQ, ZERO));
        assertFalse(conditionElementApplies(list(ZERO, ONE, TWO), ONE, EQ, TWO));
        assertFalse(conditionElementApplies(list(ZERO, ONE, TWO), TWO, EQ, ZERO));
        assertFalse(conditionElementApplies(list(ZERO, ONE, TWO), TWO, EQ, ONE));
    }

    private static SortedSet<ByteBuffer> set(ByteBuffer... values)
    {
        SortedSet<ByteBuffer> results = new TreeSet<>(Int32Type.instance);
        results.addAll(Arrays.asList(values));
        return results;
    }

    @Test
    public void testSetCollectionBoundAppliesTo() throws InvalidRequestException
    {
        // EQ
        assertTrue(conditionApplies(set(ONE), EQ, set(ONE)));
        assertTrue(conditionApplies(set(), EQ, set()));
        assertFalse(conditionApplies(set(ONE), EQ, set(ZERO)));
        assertFalse(conditionApplies(set(ZERO), EQ, set(ONE)));
        assertFalse(conditionApplies(set(ONE, TWO), EQ, set(ONE)));
        assertFalse(conditionApplies(set(ONE), EQ, set(ONE, TWO)));
        assertFalse(conditionApplies(set(ONE), EQ, set()));
        assertFalse(conditionApplies(set(), EQ, set(ONE)));

        assertFalse(conditionApplies(set(ONE), EQ, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(conditionApplies(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), EQ, set(ONE)));
        assertTrue(conditionApplies(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), EQ, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // NEQ
        assertFalse(conditionApplies(set(ONE), NEQ, set(ONE)));
        assertFalse(conditionApplies(set(), NEQ, set()));
        assertTrue(conditionApplies(set(ONE), NEQ, set(ZERO)));
        assertTrue(conditionApplies(set(ZERO), NEQ, set(ONE)));
        assertTrue(conditionApplies(set(ONE, TWO), NEQ, set(ONE)));
        assertTrue(conditionApplies(set(ONE), NEQ, set(ONE, TWO)));
        assertTrue(conditionApplies(set(ONE), NEQ, set()));
        assertTrue(conditionApplies(set(), NEQ, set(ONE)));

        assertTrue(conditionApplies(set(ONE), NEQ, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(conditionApplies(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), NEQ, set(ONE)));
        assertFalse(conditionApplies(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), NEQ, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LT
        assertFalse(conditionApplies(set(ONE), LT, set(ONE)));
        assertFalse(conditionApplies(set(), LT, set()));
        assertFalse(conditionApplies(set(ONE), LT, set(ZERO)));
        assertTrue(conditionApplies(set(ZERO), LT, set(ONE)));
        assertFalse(conditionApplies(set(ONE, TWO), LT, set(ONE)));
        assertTrue(conditionApplies(set(ONE), LT, set(ONE, TWO)));
        assertFalse(conditionApplies(set(ONE), LT, set()));
        assertTrue(conditionApplies(set(), LT, set(ONE)));

        assertFalse(conditionApplies(set(ONE), LT, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(conditionApplies(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), LT, set(ONE)));
        assertFalse(conditionApplies(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), LT, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LTE
        assertTrue(conditionApplies(set(ONE), LTE, set(ONE)));
        assertTrue(conditionApplies(set(), LTE, set()));
        assertFalse(conditionApplies(set(ONE), LTE, set(ZERO)));
        assertTrue(conditionApplies(set(ZERO), LTE, set(ONE)));
        assertFalse(conditionApplies(set(ONE, TWO), LTE, set(ONE)));
        assertTrue(conditionApplies(set(ONE), LTE, set(ONE, TWO)));
        assertFalse(conditionApplies(set(ONE), LTE, set()));
        assertTrue(conditionApplies(set(), LTE, set(ONE)));

        assertFalse(conditionApplies(set(ONE), LTE, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(conditionApplies(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), LTE, set(ONE)));
        assertTrue(conditionApplies(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), LTE, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GT
        assertFalse(conditionApplies(set(ONE), GT, set(ONE)));
        assertFalse(conditionApplies(set(), GT, set()));
        assertTrue(conditionApplies(set(ONE), GT, set(ZERO)));
        assertFalse(conditionApplies(set(ZERO), GT, set(ONE)));
        assertTrue(conditionApplies(set(ONE, TWO), GT, set(ONE)));
        assertFalse(conditionApplies(set(ONE), GT, set(ONE, TWO)));
        assertTrue(conditionApplies(set(ONE), GT, set()));
        assertFalse(conditionApplies(set(), GT, set(ONE)));

        assertTrue(conditionApplies(set(ONE), GT, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(conditionApplies(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), GT, set(ONE)));
        assertFalse(conditionApplies(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), GT, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GTE
        assertTrue(conditionApplies(set(ONE), GTE, set(ONE)));
        assertTrue(conditionApplies(set(), GTE, set()));
        assertTrue(conditionApplies(set(ONE), GTE, set(ZERO)));
        assertFalse(conditionApplies(set(ZERO), GTE, set(ONE)));
        assertTrue(conditionApplies(set(ONE, TWO), GTE, set(ONE)));
        assertFalse(conditionApplies(set(ONE), GTE, set(ONE, TWO)));
        assertTrue(conditionApplies(set(ONE), GTE, set()));
        assertFalse(conditionApplies(set(), GTE, set(ONE)));

        assertTrue(conditionApplies(set(ONE), GTE, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(conditionApplies(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), GTE, set(ONE)));
        assertTrue(conditionApplies(set(ByteBufferUtil.EMPTY_BYTE_BUFFER), GTE, set(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

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
        assertTrue(conditionApplies(map(ONE, ONE), EQ, map(ONE, ONE)));
        assertTrue(conditionApplies(map(), EQ, map()));
        assertFalse(conditionApplies(map(ONE, ONE), EQ, map(ZERO, ONE)));
        assertFalse(conditionApplies(map(ZERO, ONE), EQ, map(ONE, ONE)));
        assertFalse(conditionApplies(map(ONE, ONE), EQ, map(ONE, ZERO)));
        assertFalse(conditionApplies(map(ONE, ZERO), EQ, map(ONE, ONE)));
        assertFalse(conditionApplies(map(ONE, ONE, TWO, ONE), EQ, map(ONE, ONE)));
        assertFalse(conditionApplies(map(ONE, ONE), EQ, map(ONE, ONE, TWO, ONE)));
        assertFalse(conditionApplies(map(ONE, ONE), EQ, map()));
        assertFalse(conditionApplies(map(), EQ, map(ONE, ONE)));

        assertFalse(conditionApplies(map(ONE, ONE), EQ, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(conditionApplies(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), EQ, map(ONE, ONE)));
        assertFalse(conditionApplies(map(ONE, ONE), EQ, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(conditionApplies(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), EQ, map(ONE, ONE)));
        assertTrue(conditionApplies(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), EQ, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(conditionApplies(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), EQ, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // NEQ
        assertFalse(conditionApplies(map(ONE, ONE), NEQ, map(ONE, ONE)));
        assertFalse(conditionApplies(map(), NEQ, map()));
        assertTrue(conditionApplies(map(ONE, ONE), NEQ, map(ZERO, ONE)));
        assertTrue(conditionApplies(map(ZERO, ONE), NEQ, map(ONE, ONE)));
        assertTrue(conditionApplies(map(ONE, ONE), NEQ, map(ONE, ZERO)));
        assertTrue(conditionApplies(map(ONE, ZERO), NEQ, map(ONE, ONE)));
        assertTrue(conditionApplies(map(ONE, ONE, TWO, ONE), NEQ, map(ONE, ONE)));
        assertTrue(conditionApplies(map(ONE, ONE), NEQ, map(ONE, ONE, TWO, ONE)));
        assertTrue(conditionApplies(map(ONE, ONE), NEQ, map()));
        assertTrue(conditionApplies(map(), NEQ, map(ONE, ONE)));

        assertTrue(conditionApplies(map(ONE, ONE), NEQ, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(conditionApplies(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), NEQ, map(ONE, ONE)));
        assertTrue(conditionApplies(map(ONE, ONE), NEQ, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(conditionApplies(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), NEQ, map(ONE, ONE)));
        assertFalse(conditionApplies(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), NEQ, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(conditionApplies(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), NEQ, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LT
        assertFalse(conditionApplies(map(ONE, ONE), LT, map(ONE, ONE)));
        assertFalse(conditionApplies(map(), LT, map()));
        assertFalse(conditionApplies(map(ONE, ONE), LT, map(ZERO, ONE)));
        assertTrue(conditionApplies(map(ZERO, ONE), LT, map(ONE, ONE)));
        assertFalse(conditionApplies(map(ONE, ONE), LT, map(ONE, ZERO)));
        assertTrue(conditionApplies(map(ONE, ZERO), LT, map(ONE, ONE)));
        assertFalse(conditionApplies(map(ONE, ONE, TWO, ONE), LT, map(ONE, ONE)));
        assertTrue(conditionApplies(map(ONE, ONE), LT, map(ONE, ONE, TWO, ONE)));
        assertFalse(conditionApplies(map(ONE, ONE), LT, map()));
        assertTrue(conditionApplies(map(), LT, map(ONE, ONE)));

        assertFalse(conditionApplies(map(ONE, ONE), LT, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(conditionApplies(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), LT, map(ONE, ONE)));
        assertFalse(conditionApplies(map(ONE, ONE), LT, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(conditionApplies(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), LT, map(ONE, ONE)));
        assertFalse(conditionApplies(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), LT, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(conditionApplies(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), LT, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LTE
        assertTrue(conditionApplies(map(ONE, ONE), LTE, map(ONE, ONE)));
        assertTrue(conditionApplies(map(), LTE, map()));
        assertFalse(conditionApplies(map(ONE, ONE), LTE, map(ZERO, ONE)));
        assertTrue(conditionApplies(map(ZERO, ONE), LTE, map(ONE, ONE)));
        assertFalse(conditionApplies(map(ONE, ONE), LTE, map(ONE, ZERO)));
        assertTrue(conditionApplies(map(ONE, ZERO), LTE, map(ONE, ONE)));
        assertFalse(conditionApplies(map(ONE, ONE, TWO, ONE), LTE, map(ONE, ONE)));
        assertTrue(conditionApplies(map(ONE, ONE), LTE, map(ONE, ONE, TWO, ONE)));
        assertFalse(conditionApplies(map(ONE, ONE), LTE, map()));
        assertTrue(conditionApplies(map(), LTE, map(ONE, ONE)));

        assertFalse(conditionApplies(map(ONE, ONE), LTE, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(conditionApplies(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), LTE, map(ONE, ONE)));
        assertFalse(conditionApplies(map(ONE, ONE), LTE, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(conditionApplies(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), LTE, map(ONE, ONE)));
        assertTrue(conditionApplies(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), LTE, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(conditionApplies(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), LTE, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GT
        assertFalse(conditionApplies(map(ONE, ONE), GT, map(ONE, ONE)));
        assertFalse(conditionApplies(map(), GT, map()));
        assertTrue(conditionApplies(map(ONE, ONE), GT, map(ZERO, ONE)));
        assertFalse(conditionApplies(map(ZERO, ONE), GT, map(ONE, ONE)));
        assertTrue(conditionApplies(map(ONE, ONE), GT, map(ONE, ZERO)));
        assertFalse(conditionApplies(map(ONE, ZERO), GT, map(ONE, ONE)));
        assertTrue(conditionApplies(map(ONE, ONE, TWO, ONE), GT, map(ONE, ONE)));
        assertFalse(conditionApplies(map(ONE, ONE), GT, map(ONE, ONE, TWO, ONE)));
        assertTrue(conditionApplies(map(ONE, ONE), GT, map()));
        assertFalse(conditionApplies(map(), GT, map(ONE, ONE)));

        assertTrue(conditionApplies(map(ONE, ONE), GT, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(conditionApplies(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), GT, map(ONE, ONE)));
        assertTrue(conditionApplies(map(ONE, ONE), GT, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(conditionApplies(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), GT, map(ONE, ONE)));
        assertFalse(conditionApplies(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), GT, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(conditionApplies(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), GT, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GTE
        assertTrue(conditionApplies(map(ONE, ONE), GTE, map(ONE, ONE)));
        assertTrue(conditionApplies(map(), GTE, map()));
        assertTrue(conditionApplies(map(ONE, ONE), GTE, map(ZERO, ONE)));
        assertFalse(conditionApplies(map(ZERO, ONE), GTE, map(ONE, ONE)));
        assertTrue(conditionApplies(map(ONE, ONE), GTE, map(ONE, ZERO)));
        assertFalse(conditionApplies(map(ONE, ZERO), GTE, map(ONE, ONE)));
        assertTrue(conditionApplies(map(ONE, ONE, TWO, ONE), GTE, map(ONE, ONE)));
        assertFalse(conditionApplies(map(ONE, ONE), GTE, map(ONE, ONE, TWO, ONE)));
        assertTrue(conditionApplies(map(ONE, ONE), GTE, map()));
        assertFalse(conditionApplies(map(), GTE, map(ONE, ONE)));

        assertTrue(conditionApplies(map(ONE, ONE), GTE, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(conditionApplies(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), GTE, map(ONE, ONE)));
        assertTrue(conditionApplies(map(ONE, ONE), GTE, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(conditionApplies(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), GTE, map(ONE, ONE)));
        assertTrue(conditionApplies(map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), GTE, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(conditionApplies(map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), GTE, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

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

        // Element access
        assertTrue(conditionElementApplies(map(ZERO, ONE), ZERO, EQ, ONE));
        assertFalse(conditionElementApplies(map(ZERO, ONE), ZERO, EQ, TWO));
        assertFalse(conditionElementApplies(map(ZERO, ONE), ONE, EQ, TWO));
        assertFalse(conditionElementApplies(map(), ZERO, EQ, TWO));
    }

    @Test
    public void testMultiCellUDTBound() throws InvalidRequestException
    {
        // EQ
        assertTrue(conditionUDTApplies(list(ONE), EQ, list(ONE)));
        assertFalse(conditionUDTApplies(list(ONE), EQ, list(ZERO)));
        assertFalse(conditionUDTApplies(list(ZERO), EQ, list(ONE)));
        assertFalse(conditionUDTApplies(list(ONE, ONE), EQ, list(ONE)));
        assertFalse(conditionUDTApplies(list(ONE), EQ, list(ONE, ONE)));
        assertFalse(conditionUDTApplies(list(ONE), EQ, list()));

        assertFalse(conditionUDTApplies(list(ONE), EQ, null));
        assertFalse(conditionUDTApplies(null, EQ, list(ONE)));
        assertTrue(conditionUDTApplies(null, EQ, (List<ByteBuffer>) null));

        assertFalse(conditionUDTApplies(list(ONE), EQ, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(conditionUDTApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), EQ, list(ONE)));
        assertTrue(conditionUDTApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), EQ, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // NEQ
        assertFalse(conditionUDTApplies(list(ONE), NEQ, list(ONE)));
        assertTrue(conditionUDTApplies(list(ONE), NEQ, list(ZERO)));
        assertTrue(conditionUDTApplies(list(ZERO), NEQ, list(ONE)));
        assertTrue(conditionUDTApplies(list(ONE, ONE), NEQ, list(ONE)));
        assertTrue(conditionUDTApplies(list(ONE), NEQ, list(ONE, ONE)));
        assertTrue(conditionUDTApplies(list(ONE), NEQ, list()));
        assertTrue(conditionUDTApplies(list(), NEQ, list(ONE)));

        assertTrue(conditionUDTApplies(list(ONE), NEQ, null));
        assertTrue(conditionUDTApplies(null, NEQ, list(ONE)));
        assertFalse(conditionUDTApplies(null, NEQ, (List<ByteBuffer>) null));

        assertTrue(conditionUDTApplies(list(ONE), NEQ, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(conditionUDTApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), NEQ, list(ONE)));
        assertFalse(conditionUDTApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), NEQ, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LT
        assertFalse(conditionUDTApplies(list(ONE), LT, list(ONE)));
        assertFalse(conditionUDTApplies(list(), LT, list()));
        assertFalse(conditionUDTApplies(list(ONE), LT, list(ZERO)));
        assertTrue(conditionUDTApplies(list(ZERO), LT, list(ONE)));
        assertFalse(conditionUDTApplies(list(ONE, ONE), LT, list(ONE)));
        assertTrue(conditionUDTApplies(list(ONE), LT, list(ONE, ONE)));
        assertFalse(conditionUDTApplies(list(ONE), LT, list()));

        assertFalse(conditionUDTApplies(list(ONE), LT, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(conditionUDTApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), LT, list(ONE)));
        assertFalse(conditionUDTApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), LT, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LTE
        assertTrue(conditionUDTApplies(list(ONE), LTE, list(ONE)));
        assertFalse(conditionUDTApplies(list(ONE), LTE, list(ZERO)));
        assertTrue(conditionUDTApplies(list(ZERO), LTE, list(ONE)));
        assertFalse(conditionUDTApplies(list(ONE, ONE), LTE, list(ONE)));
        assertTrue(conditionUDTApplies(list(ONE), LTE, list(ONE, ONE)));
        assertFalse(conditionUDTApplies(list(ONE), LTE, list()));

        assertFalse(conditionUDTApplies(list(ONE), LTE, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(conditionUDTApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), LTE, list(ONE)));
        assertTrue(conditionUDTApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), LTE, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GT
        assertFalse(conditionUDTApplies(list(ONE), GT, list(ONE)));
        assertTrue(conditionUDTApplies(list(ONE), GT, list(ZERO)));
        assertFalse(conditionUDTApplies(list(ZERO), GT, list(ONE)));
        assertTrue(conditionUDTApplies(list(ONE, ONE), GT, list(ONE)));
        assertFalse(conditionUDTApplies(list(ONE), GT, list(ONE, ONE)));
        assertTrue(conditionUDTApplies(list(ONE), GT, list()));

        assertTrue(conditionUDTApplies(list(ONE), GT, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(conditionUDTApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), GT, list(ONE)));
        assertFalse(conditionUDTApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), GT, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GTE
        assertTrue(conditionUDTApplies(list(ONE), GTE, list(ONE)));
        assertTrue(conditionUDTApplies(list(ONE), GTE, list(ZERO)));
        assertFalse(conditionUDTApplies(list(ZERO), GTE, list(ONE)));
        assertTrue(conditionUDTApplies(list(ONE, ONE), GTE, list(ONE)));
        assertFalse(conditionUDTApplies(list(ONE), GTE, list(ONE, ONE)));
        assertTrue(conditionUDTApplies(list(ONE), GTE, list()));

        assertTrue(conditionUDTApplies(list(ONE), GTE, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(conditionUDTApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), GTE, list(ONE)));
        assertTrue(conditionUDTApplies(list(ByteBufferUtil.EMPTY_BYTE_BUFFER), GTE, list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
    }
}
