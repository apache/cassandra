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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.*;

import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.*;

public class ColumnConditionTest
{
    private static final CellPath LIST_PATH = CellPath.create(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes()));

    public static final ByteBuffer ZERO = Int32Type.instance.fromString("0");
    public static final ByteBuffer ONE = Int32Type.instance.fromString("1");
    public static final ByteBuffer TWO = Int32Type.instance.fromString("2");

    public static final ByteBuffer A = AsciiType.instance.fromString("a");
    public static final ByteBuffer B = AsciiType.instance.fromString("b");

    private static boolean isSatisfiedBy(ColumnCondition.Bound bound, ByteBuffer conditionValue, ByteBuffer columnValue) throws InvalidRequestException
    {
        Cell cell = null;
        if (columnValue != null)
        {
            ColumnDefinition definition = ColumnDefinition.regularDef("ks", "cf", "c", ListType.getInstance(Int32Type.instance, true));
            cell = testCell(definition, columnValue, LIST_PATH);
        }
        return bound.isSatisfiedByValue(conditionValue, cell, Int32Type.instance, bound.operator);
    }

    private static Cell testCell(ColumnDefinition column, ByteBuffer value, CellPath path)
    {
        return new BufferCell(column, 0L, Cell.NO_TTL, Cell.NO_DELETION_TIME, value, path);
    }

    private static void assertThrowsIRE(ColumnCondition.Bound bound, ByteBuffer conditionValue, ByteBuffer columnValue)
    {
        try
        {
            isSatisfiedBy(bound, conditionValue, columnValue);
            fail("Expected InvalidRequestException was not thrown");
        } catch (InvalidRequestException e) { }
    }

    @Test
    public void testSimpleBoundIsSatisfiedByValue() throws InvalidRequestException
    {
        ColumnDefinition definition = ColumnDefinition.regularDef("ks", "cf", "c", ListType.getInstance(Int32Type.instance, true));

        // EQ
        ColumnCondition condition = ColumnCondition.condition(definition, new Constants.Value(ONE), Operator.EQ);
        ColumnCondition.Bound bound = condition.bind(QueryOptions.DEFAULT);
        assertTrue(isSatisfiedBy(bound, ONE, ONE));
        assertFalse(isSatisfiedBy(bound, ZERO, ONE));
        assertFalse(isSatisfiedBy(bound, TWO, ONE));
        assertFalse(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE));
        assertFalse(isSatisfiedBy(bound, ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertTrue(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertTrue(isSatisfiedBy(bound, null, null));
        assertFalse(isSatisfiedBy(bound, ONE, null));
        assertFalse(isSatisfiedBy(bound, null, ONE));

        // NEQ
        condition = ColumnCondition.condition(definition, new Constants.Value(ONE), Operator.NEQ);
        bound = condition.bind(QueryOptions.DEFAULT);
        assertFalse(isSatisfiedBy(bound, ONE, ONE));
        assertTrue(isSatisfiedBy(bound, ZERO, ONE));
        assertTrue(isSatisfiedBy(bound, TWO, ONE));
        assertTrue(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE));
        assertTrue(isSatisfiedBy(bound, ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(isSatisfiedBy(bound, null, null));
        assertTrue(isSatisfiedBy(bound, ONE, null));
        assertTrue(isSatisfiedBy(bound, null, ONE));

        // LT
        condition = ColumnCondition.condition(definition, new Constants.Value(ONE), Operator.LT);
        bound = condition.bind(QueryOptions.DEFAULT);
        assertFalse(isSatisfiedBy(bound, ONE, ONE));
        assertFalse(isSatisfiedBy(bound, ZERO, ONE));
        assertTrue(isSatisfiedBy(bound, TWO, ONE));
        assertFalse(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE));
        assertTrue(isSatisfiedBy(bound, ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertThrowsIRE(bound, null, ONE);
        assertFalse(isSatisfiedBy(bound, ONE, null));

        // LTE
        condition = ColumnCondition.condition(definition, new Constants.Value(ONE), Operator.LTE);
        bound = condition.bind(QueryOptions.DEFAULT);
        assertTrue(isSatisfiedBy(bound, ONE, ONE));
        assertFalse(isSatisfiedBy(bound, ZERO, ONE));
        assertTrue(isSatisfiedBy(bound, TWO, ONE));
        assertFalse(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE));
        assertTrue(isSatisfiedBy(bound, ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertTrue(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertThrowsIRE(bound, null, ONE);
        assertFalse(isSatisfiedBy(bound, ONE, null));

        // GT
        condition = ColumnCondition.condition(definition, new Constants.Value(ONE), Operator.GT);
        bound = condition.bind(QueryOptions.DEFAULT);
        assertFalse(isSatisfiedBy(bound, ONE, ONE));
        assertTrue(isSatisfiedBy(bound, ZERO, ONE));
        assertFalse(isSatisfiedBy(bound, TWO, ONE));
        assertTrue(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE));
        assertFalse(isSatisfiedBy(bound, ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertThrowsIRE(bound, null, ONE);
        assertFalse(isSatisfiedBy(bound, ONE, null));

        // GT
        condition = ColumnCondition.condition(definition, new Constants.Value(ONE), Operator.GTE);
        bound = condition.bind(QueryOptions.DEFAULT);
        assertTrue(isSatisfiedBy(bound, ONE, ONE));
        assertTrue(isSatisfiedBy(bound, ZERO, ONE));
        assertFalse(isSatisfiedBy(bound, TWO, ONE));
        assertTrue(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE));
        assertFalse(isSatisfiedBy(bound, ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertTrue(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertThrowsIRE(bound, null, ONE);
        assertFalse(isSatisfiedBy(bound, ONE, null));
    }

    private static List<ByteBuffer> list(ByteBuffer... values)
    {
        return Arrays.asList(values);
    }

    private static boolean listAppliesTo(ColumnCondition.CollectionBound bound, List<ByteBuffer> conditionValues, List<ByteBuffer> columnValues)
    {
        CFMetaData cfm = CFMetaData.compile("create table foo(a int PRIMARY KEY, b int, c list<int>)", "ks");
        Map<ByteBuffer, CollectionType> typeMap = new HashMap<>();
        typeMap.put(ByteBufferUtil.bytes("c"), ListType.getInstance(Int32Type.instance, true));

        ColumnDefinition definition = ColumnDefinition.regularDef(cfm, ByteBufferUtil.bytes("c"), ListType.getInstance(Int32Type.instance, true));

        List<Cell> cells = new ArrayList<>(columnValues.size());
        if (columnValues != null)
        {
            for (int i = 0; i < columnValues.size(); i++)
            {
                ByteBuffer key = Int32Serializer.instance.serialize(i);
                ByteBuffer value = columnValues.get(i);
                cells.add(testCell(definition, value, CellPath.create(key)));
            };
        }

        return bound.listAppliesTo(ListType.getInstance(Int32Type.instance, true), cells == null ? null : cells.iterator(), conditionValues, bound.operator);
    }

    @Test
    // sets use the same check as lists
    public void testListCollectionBoundAppliesTo() throws InvalidRequestException
    {
        ColumnDefinition definition = ColumnDefinition.regularDef("ks", "cf", "c", ListType.getInstance(Int32Type.instance, true));

        // EQ
        ColumnCondition condition = ColumnCondition.condition(definition, new Lists.Value(Arrays.asList(ONE)), Operator.EQ);
        ColumnCondition.CollectionBound bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);
        assertTrue(listAppliesTo(bound, list(ONE), list(ONE)));
        assertTrue(listAppliesTo(bound, list(), list()));
        assertFalse(listAppliesTo(bound, list(ZERO), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ZERO)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ONE, ONE)));
        assertFalse(listAppliesTo(bound, list(ONE, ONE), list(ONE)));
        assertFalse(listAppliesTo(bound, list(), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list()));

        assertFalse(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // NEQ
        condition = ColumnCondition.condition(definition, new Lists.Value(Arrays.asList(ONE)), Operator.NEQ);
        bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);
        assertFalse(listAppliesTo(bound, list(ONE), list(ONE)));
        assertFalse(listAppliesTo(bound, list(), list()));
        assertTrue(listAppliesTo(bound, list(ZERO), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ZERO)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ONE, ONE)));
        assertTrue(listAppliesTo(bound, list(ONE, ONE), list(ONE)));
        assertTrue(listAppliesTo(bound, list(), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list()));

        assertTrue(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LT
        condition = ColumnCondition.condition(definition, new Lists.Value(Arrays.asList(ONE)), Operator.LT);
        bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);
        assertFalse(listAppliesTo(bound, list(ONE), list(ONE)));
        assertFalse(listAppliesTo(bound, list(), list()));
        assertFalse(listAppliesTo(bound, list(ZERO), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ZERO)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ONE, ONE)));
        assertTrue(listAppliesTo(bound, list(ONE, ONE), list(ONE)));
        assertFalse(listAppliesTo(bound, list(), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list()));

        assertFalse(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LTE
        condition = ColumnCondition.condition(definition, new Lists.Value(Arrays.asList(ONE)), Operator.LTE);
        bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);
        assertTrue(listAppliesTo(bound, list(ONE), list(ONE)));
        assertTrue(listAppliesTo(bound, list(), list()));
        assertFalse(listAppliesTo(bound, list(ZERO), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ZERO)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ONE, ONE)));
        assertTrue(listAppliesTo(bound, list(ONE, ONE), list(ONE)));
        assertFalse(listAppliesTo(bound, list(), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list()));

        assertFalse(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GT
        condition = ColumnCondition.condition(definition, new Lists.Value(Arrays.asList(ONE)), Operator.GT);
        bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);
        assertFalse(listAppliesTo(bound, list(ONE), list(ONE)));
        assertFalse(listAppliesTo(bound, list(), list()));
        assertTrue(listAppliesTo(bound, list(ZERO), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ZERO)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ONE, ONE)));
        assertFalse(listAppliesTo(bound, list(ONE, ONE), list(ONE)));
        assertTrue(listAppliesTo(bound, list(), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list()));

        assertTrue(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GTE
        condition = ColumnCondition.condition(definition, new Lists.Value(Arrays.asList(ONE)), Operator.GTE);
        bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);
        assertTrue(listAppliesTo(bound, list(ONE), list(ONE)));
        assertTrue(listAppliesTo(bound, list(), list()));
        assertTrue(listAppliesTo(bound, list(ZERO), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ZERO)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ONE, ONE)));
        assertFalse(listAppliesTo(bound, list(ONE, ONE), list(ONE)));
        assertTrue(listAppliesTo(bound, list(), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list()));

        assertTrue(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
    }

    private static SortedSet<ByteBuffer> set(ByteBuffer... values)
    {
        SortedSet<ByteBuffer> results = new TreeSet<>(Int32Type.instance);
        results.addAll(Arrays.asList(values));
        return results;
    }

    private static boolean setAppliesTo(ColumnCondition.CollectionBound bound, Set<ByteBuffer> conditionValues, List<ByteBuffer> columnValues)
    {
        CFMetaData cfm = CFMetaData.compile("create table foo(a int PRIMARY KEY, b int, c set<int>)", "ks");
        Map<ByteBuffer, CollectionType> typeMap = new HashMap<>();
        typeMap.put(ByteBufferUtil.bytes("c"), SetType.getInstance(Int32Type.instance, true));
        ColumnDefinition definition = ColumnDefinition.regularDef(cfm, ByteBufferUtil.bytes("c"), SetType.getInstance(Int32Type.instance, true));

        List<Cell> cells = new ArrayList<>(columnValues.size());
        if (columnValues != null)
        {
            for (int i = 0; i < columnValues.size(); i++)
            {
                ByteBuffer key = columnValues.get(i);
                cells.add(testCell(definition, ByteBufferUtil.EMPTY_BYTE_BUFFER, CellPath.create(key)));
            };
        }

        return bound.setAppliesTo(SetType.getInstance(Int32Type.instance, true), cells == null ? null : cells.iterator(), conditionValues, bound.operator);
    }

    @Test
    public void testSetCollectionBoundAppliesTo() throws InvalidRequestException
    {
        ColumnDefinition definition = ColumnDefinition.regularDef("ks", "cf", "c", ListType.getInstance(Int32Type.instance, true));

        // EQ
        ColumnCondition condition = ColumnCondition.condition(definition, new Sets.Value(set(ONE)), Operator.EQ);
        ColumnCondition.CollectionBound bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);
        assertTrue(setAppliesTo(bound, set(ONE), list(ONE)));
        assertTrue(setAppliesTo(bound, set(), list()));
        assertFalse(setAppliesTo(bound, set(ZERO), list(ONE)));
        assertFalse(setAppliesTo(bound, set(ONE), list(ZERO)));
        assertFalse(setAppliesTo(bound, set(ONE), list(ONE, TWO)));
        assertFalse(setAppliesTo(bound, set(ONE, TWO), list(ONE)));
        assertFalse(setAppliesTo(bound, set(), list(ONE)));
        assertFalse(setAppliesTo(bound, set(ONE), list()));

        assertFalse(setAppliesTo(bound, set(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertFalse(setAppliesTo(bound, set(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(setAppliesTo(bound, set(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // NEQ
        condition = ColumnCondition.condition(definition, new Sets.Value(set(ONE)), Operator.NEQ);
        bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);
        assertFalse(setAppliesTo(bound, set(ONE), list(ONE)));
        assertFalse(setAppliesTo(bound, set(), list()));
        assertTrue(setAppliesTo(bound, set(ZERO), list(ONE)));
        assertTrue(setAppliesTo(bound, set(ONE), list(ZERO)));
        assertTrue(setAppliesTo(bound, set(ONE), list(ONE, TWO)));
        assertTrue(setAppliesTo(bound, set(ONE, TWO), list(ONE)));
        assertTrue(setAppliesTo(bound, set(), list(ONE)));
        assertTrue(setAppliesTo(bound, set(ONE), list()));

        assertTrue(setAppliesTo(bound, set(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertTrue(setAppliesTo(bound, set(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(setAppliesTo(bound, set(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LT
        condition = ColumnCondition.condition(definition, new Lists.Value(Arrays.asList(ONE)), Operator.LT);
        bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);
        assertFalse(setAppliesTo(bound, set(ONE), list(ONE)));
        assertFalse(setAppliesTo(bound, set(), list()));
        assertFalse(setAppliesTo(bound, set(ZERO), list(ONE)));
        assertTrue(setAppliesTo(bound, set(ONE), list(ZERO)));
        assertFalse(setAppliesTo(bound, set(ONE), list(ONE, TWO)));
        assertTrue(setAppliesTo(bound, set(ONE, TWO), list(ONE)));
        assertFalse(setAppliesTo(bound, set(), list(ONE)));
        assertTrue(setAppliesTo(bound, set(ONE), list()));

        assertFalse(setAppliesTo(bound, set(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertTrue(setAppliesTo(bound, set(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(setAppliesTo(bound, set(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LTE
        condition = ColumnCondition.condition(definition, new Lists.Value(Arrays.asList(ONE)), Operator.LTE);
        bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);
        assertTrue(setAppliesTo(bound, set(ONE), list(ONE)));
        assertTrue(setAppliesTo(bound, set(), list()));
        assertFalse(setAppliesTo(bound, set(ZERO), list(ONE)));
        assertTrue(setAppliesTo(bound, set(ONE), list(ZERO)));
        assertFalse(setAppliesTo(bound, set(ONE), list(ONE, TWO)));
        assertTrue(setAppliesTo(bound, set(ONE, TWO), list(ONE)));
        assertFalse(setAppliesTo(bound, set(), list(ONE)));
        assertTrue(setAppliesTo(bound, set(ONE), list()));

        assertFalse(setAppliesTo(bound, set(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertTrue(setAppliesTo(bound, set(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(setAppliesTo(bound, set(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GT
        condition = ColumnCondition.condition(definition, new Lists.Value(Arrays.asList(ONE)), Operator.GT);
        bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);
        assertFalse(setAppliesTo(bound, set(ONE), list(ONE)));
        assertFalse(setAppliesTo(bound, set(), list()));
        assertTrue(setAppliesTo(bound, set(ZERO), list(ONE)));
        assertFalse(setAppliesTo(bound, set(ONE), list(ZERO)));
        assertTrue(setAppliesTo(bound, set(ONE), list(ONE, TWO)));
        assertFalse(setAppliesTo(bound, set(ONE, TWO), list(ONE)));
        assertTrue(setAppliesTo(bound, set(), list(ONE)));
        assertFalse(setAppliesTo(bound, set(ONE), list()));

        assertTrue(setAppliesTo(bound, set(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertFalse(setAppliesTo(bound, set(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(setAppliesTo(bound, set(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GTE
        condition = ColumnCondition.condition(definition, new Lists.Value(Arrays.asList(ONE)), Operator.GTE);
        bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);
        assertTrue(setAppliesTo(bound, set(ONE), list(ONE)));
        assertTrue(setAppliesTo(bound, set(), list()));
        assertTrue(setAppliesTo(bound, set(ZERO), list(ONE)));
        assertFalse(setAppliesTo(bound, set(ONE), list(ZERO)));
        assertTrue(setAppliesTo(bound, set(ONE), list(ONE, TWO)));
        assertFalse(setAppliesTo(bound, set(ONE, TWO), list(ONE)));
        assertTrue(setAppliesTo(bound, set(), list(ONE)));
        assertFalse(setAppliesTo(bound, set(ONE), list()));

        assertTrue(setAppliesTo(bound, set(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertFalse(setAppliesTo(bound, set(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(setAppliesTo(bound, set(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
    }

    // values should be a list of key, value, key, value, ...
    private static Map<ByteBuffer, ByteBuffer> map(ByteBuffer... values)
    {
        Map<ByteBuffer, ByteBuffer> map = new TreeMap<>();
        for (int i = 0; i < values.length; i += 2)
            map.put(values[i], values[i + 1]);

        return map;
    }

    private static boolean mapAppliesTo(ColumnCondition.CollectionBound bound, Map<ByteBuffer, ByteBuffer> conditionValues, Map<ByteBuffer, ByteBuffer> columnValues)
    {
        CFMetaData cfm = CFMetaData.compile("create table foo(a int PRIMARY KEY, b map<int, int>)", "ks");
        Map<ByteBuffer, CollectionType> typeMap = new HashMap<>();
        typeMap.put(ByteBufferUtil.bytes("b"), MapType.getInstance(Int32Type.instance, Int32Type.instance, true));
        ColumnDefinition definition = ColumnDefinition.regularDef(cfm, ByteBufferUtil.bytes("b"), MapType.getInstance(Int32Type.instance, Int32Type.instance, true));

        List<Cell> cells = new ArrayList<>(columnValues.size());
        if (columnValues != null)
        {
            for (Map.Entry<ByteBuffer, ByteBuffer> entry : columnValues.entrySet())
                cells.add(testCell(definition, entry.getValue(), CellPath.create(entry.getKey())));
        }

        return bound.mapAppliesTo(MapType.getInstance(Int32Type.instance, Int32Type.instance, true), cells.iterator(), conditionValues, bound.operator);
    }

    @Test
    public void testMapCollectionBoundIsSatisfiedByValue() throws InvalidRequestException
    {
        ColumnDefinition definition = ColumnDefinition.regularDef("ks", "cf", "c", ListType.getInstance(Int32Type.instance, true));

        Map<ByteBuffer, ByteBuffer> placeholderMap = new TreeMap<>();
        placeholderMap.put(ONE, ONE);
        Maps.Value placeholder = new Maps.Value(placeholderMap);

        // EQ
        ColumnCondition condition = ColumnCondition.condition(definition, placeholder, Operator.EQ);
        ColumnCondition.CollectionBound bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);

        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(), map()));
        assertFalse(mapAppliesTo(bound, map(ZERO, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ZERO, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ZERO), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ZERO)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE, TWO, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE, TWO, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map()));

        assertFalse(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // NEQ
        condition = ColumnCondition.condition(definition, placeholder, Operator.NEQ);
        bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);

        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(), map()));
        assertTrue(mapAppliesTo(bound, map(ZERO, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ZERO, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ZERO), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ZERO)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE, TWO, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE, TWO, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map()));

        assertTrue(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LT
        condition = ColumnCondition.condition(definition, placeholder, Operator.LT);
        bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);

        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(), map()));
        assertFalse(mapAppliesTo(bound, map(ZERO, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ZERO, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ZERO), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ZERO)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE, TWO, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE, TWO, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map()));

        assertFalse(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LTE
        condition = ColumnCondition.condition(definition, placeholder, Operator.LTE);
        bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);

        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(), map()));
        assertFalse(mapAppliesTo(bound, map(ZERO, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ZERO, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ZERO), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ZERO)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE, TWO, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE, TWO, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map()));

        assertFalse(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GT
        condition = ColumnCondition.condition(definition, placeholder, Operator.GT);
        bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);

        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(), map()));
        assertTrue(mapAppliesTo(bound, map(ZERO, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ZERO, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ZERO), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ZERO)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE, TWO, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE, TWO, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map()));

        assertTrue(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GTE
        condition = ColumnCondition.condition(definition, placeholder, Operator.GTE);
        bound = (ColumnCondition.CollectionBound) condition.bind(QueryOptions.DEFAULT);

        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(), map()));
        assertTrue(mapAppliesTo(bound, map(ZERO, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ZERO, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ZERO), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ZERO)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE, TWO, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE, TWO, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map()));

        assertTrue(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
    }
}
