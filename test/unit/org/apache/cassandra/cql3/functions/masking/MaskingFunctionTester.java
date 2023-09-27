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

package org.apache.cassandra.cql3.functions.masking;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static java.lang.String.format;

/**
 * Abstract class for testing a specific implementation of {@link MaskingFunction}.
 * <p>
 * It tests the application of the function as defined by {@link #testMaskingOnColumn(String, CQL3Type, Object)}
 * on all CQL data types on all the possible positions allowed for that type (primary key, regular and static columns).
 */
public abstract class MaskingFunctionTester extends CQLTester
{
    /**
     * Tests the native masking function for all CQL native data types.
     */
    @Test
    public void testMaskingOnNative() throws Throwable
    {
        for (CQL3Type.Native type : CQL3Type.Native.values())
        {
            switch (type)
            {
                case EMPTY:
                    break;
                case COUNTER:
                    testMaskingOnCounterColumn(0L, -1L, 1L);
                    break;
                case TEXT:
                case ASCII:
                case VARCHAR:
                    testMaskingOnAllColumns(type, "confidential");
                    break;
                case BOOLEAN:
                    testMaskingOnAllColumns(type, true, false);
                    break;
                case TINYINT:
                    testMaskingOnAllColumns(type, (byte) 0, (byte) 2);
                    break;
                case SMALLINT:
                    testMaskingOnAllColumns(type, (short) 0, (short) 2);
                    break;
                case INT:
                    testMaskingOnAllColumns(type, 2, Integer.MIN_VALUE, Integer.MAX_VALUE);
                    break;
                case BIGINT:
                    testMaskingOnAllColumns(type, 2L, Long.MIN_VALUE, Long.MAX_VALUE);
                    break;
                case FLOAT:
                    testMaskingOnAllColumns(type, 2.3f, Float.MIN_VALUE, Float.MAX_VALUE);
                    break;
                case DOUBLE:
                    testMaskingOnAllColumns(type, 2.3d, Double.MIN_VALUE, Double.MAX_VALUE);
                    break;
                case VARINT:
                    testMaskingOnAllColumns(type, BigInteger.valueOf(-1), BigInteger.valueOf(0), BigInteger.valueOf(1));
                    break;
                case DECIMAL:
                    testMaskingOnAllColumns(type, BigDecimal.valueOf(2.3), BigDecimal.valueOf(0), BigDecimal.valueOf(-2.3));
                    break;
                case DATE:
                    testMaskingOnAllColumns(type,
                                            SimpleDateSerializer.timeInMillisToDay(2),
                                            SimpleDateSerializer.timeInMillisToDay(Long.MAX_VALUE));
                    break;
                case DURATION:
                    testMaskingOnNotKeyColumns(type, Duration.newInstance(1, 2, 3), Duration.newInstance(3, 2, 1));
                    break;
                case TIME:
                    testMaskingOnAllColumns(CQL3Type.Native.TIME, 2L, (long) Integer.MAX_VALUE);
                    break;
                case TIMESTAMP:
                    testMaskingOnAllColumns(CQL3Type.Native.TIMESTAMP, new Date(2), new Date(Integer.MAX_VALUE));
                    break;
                case UUID:
                    testMaskingOnAllColumns(type, UUID.randomUUID());
                    break;
                case TIMEUUID:
                    testMaskingOnAllColumns(type, TimeUUID.minAtUnixMillis(2), TimeUUID.minAtUnixMillis(Long.MAX_VALUE));
                    break;
                case INET:
                    testMaskingOnAllColumns(type, new InetSocketAddress(0).getAddress());
                    break;
                case BLOB:
                    testMaskingOnAllColumns(type, UTF8Type.instance.decompose("confidential"));
                    break;
                default:
                    throw new AssertionError("Type " + type + " should be tested for masking functions");
            }
        }
    }

    /**
     * Tests the native masking function for collections.
     */
    @Test
    public void testMaskingOnCollection() throws Throwable
    {
        // set
        Object[] values = new Object[]{ set(), set(1, 2, 3) };
        testMaskingOnAllColumns(SetType.getInstance(Int32Type.instance, false).asCQL3Type(), values);
        testMaskingOnNotKeyColumns(SetType.getInstance(Int32Type.instance, true).asCQL3Type(), values);

        // list
        values = new Object[]{ list(), list(1, 2, 3) };
        testMaskingOnAllColumns(ListType.getInstance(Int32Type.instance, false).asCQL3Type(), values);
        testMaskingOnNotKeyColumns(ListType.getInstance(Int32Type.instance, true).asCQL3Type(), values);

        // map
        values = new Object[]{ map(), map(1, 10, 2, 20, 3, 30) };
        testMaskingOnAllColumns(MapType.getInstance(Int32Type.instance, Int32Type.instance, false).asCQL3Type(), values);
        testMaskingOnNotKeyColumns(MapType.getInstance(Int32Type.instance, Int32Type.instance, true).asCQL3Type(), values);
    }

    /**
     * Tests the native masking function for vectors.
     */
    @Test
    public void testMaskingOnVector() throws Throwable
    {
        testMaskingOnAllColumns(VectorType.getInstance(Int32Type.instance, 2).asCQL3Type(),
                                vector(1, 10), vector(2, 20));
        testMaskingOnAllColumns(VectorType.getInstance(FloatType.instance, 2).asCQL3Type(),
                                vector(1.1f, 10.1f), vector(2.2f, 20.2f));
        testMaskingOnAllColumns(VectorType.getInstance(UTF8Type.instance, 2).asCQL3Type(),
                                vector("a1", "a2"), vector("b1", "b2"));
    }

    /**
     * Tests the native masking function for tuples.
     */
    @Test
    public void testMaskingOnTuple() throws Throwable
    {
        testMaskingOnAllColumns(new TupleType(ImmutableList.of(Int32Type.instance, Int32Type.instance)).asCQL3Type(),
                                tuple(1, 10), tuple(2, 20));
    }

    /**
     * Tests the native masking function for UDTs.
     */
    @Test
    public void testMaskingOnUDT() throws Throwable
    {
        String name = createType("CREATE TYPE %s (a int, b text)");

        KeyspaceMetadata ks = Schema.instance.getKeyspaceMetadata(keyspace());
        Assert.assertNotNull(ks);

        UserType udt = ks.types.get(ByteBufferUtil.bytes(name)).orElseThrow(AssertionError::new);
        Assert.assertNotNull(udt);

        Object[] values = new Object[]{ userType("a", 1, "b", "Alice"), userType("a", 2, "b", "Bob") };
        testMaskingOnNotKeyColumns(udt.asCQL3Type(), values);
        testMaskingOnAllColumns(udt.freeze().asCQL3Type(), values);
    }

    /**
     * Tests the native masking function for the specified column type and values on all possible types of column.
     * That is, when the column is part of the primary key, or a regular column, or a static column.
     *
     * @param type the type of the tested column
     * @param values the values of the tested column
     */
    private void testMaskingOnAllColumns(CQL3Type type, Object... values) throws Throwable
    {
        createTable(format("CREATE TABLE %%s (pk %s, ck %<s, s %<s static, v %<s, PRIMARY KEY (pk, ck))", type));

        for (Object value : values)
        {
            // Test null values
            execute("INSERT INTO %s(pk, ck) VALUES (?, ?)", value, value);
            testMaskingOnColumn("s", type, null);
            testMaskingOnColumn("v", type, null);

            // Test not-null values
            execute("INSERT INTO %s(pk, ck, s, v) VALUES (?, ?, ?, ?)", value, value, value, value);
            testMaskingOnColumn("pk", type, value);
            testMaskingOnColumn("ck", type, value);
            testMaskingOnColumn("s", type, value);
            testMaskingOnColumn("v", type, value);

            // Cleanup
            execute("DELETE FROM %s WHERE pk=?", value);
        }
    }

    /**
     * Tests the native masking function for the specified column type and values when the column isn't part of the
     * primary key. That is, when the column is either a regular column or a static column.
     *
     * @param type the type of the tested column
     * @param values the values of the tested column
     */
    private void testMaskingOnNotKeyColumns(CQL3Type type, Object... values) throws Throwable
    {
        createTable(format("CREATE TABLE %%s (pk int, ck int, s %s static, v %<s, PRIMARY KEY (pk, ck))", type));

        // Test null values
        execute("INSERT INTO %s(pk, ck) VALUES (0, 0)");
        testMaskingOnColumn("s", type, null);
        testMaskingOnColumn("v", type, null);

        // Test not-null values
        for (Object value : values)
        {
            execute("INSERT INTO %s(pk, ck, s, v) VALUES (0, 0, ?, ?)", value, value);
            testMaskingOnColumn("s", type, value);
            testMaskingOnColumn("v", type, value);
        }
    }

    /**
     * Tests the native masking function on the specified counter column values in all the positions where it can appear.
     * That is, when the counter column is either a regular column or a static column.
     *
     * @param values the values of the tested counter column
     */
    private void testMaskingOnCounterColumn(Object... values) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s counter static, v counter, PRIMARY KEY (pk, ck))");
        for (Object value : values)
        {
            execute("UPDATE %s SET v = v + ?, s = s + ? WHERE pk = 0 AND ck = 0", value, value);
            testMaskingOnColumn("s", CQL3Type.Native.COUNTER, value);
            testMaskingOnColumn("v", CQL3Type.Native.COUNTER, value);
            execute("TRUNCATE %s");
        }
    }

    /**
     * Tests the native masking function for the specified column type and value.
     * This assumes that the table is already created.
     *
     * @param name the name of the tested column
     * @param type the type of the tested column
     * @param value the value of the tested column
     */
    protected abstract void testMaskingOnColumn(String name, CQL3Type type, Object value) throws Throwable;

    protected boolean isNullOrEmptyMultiCell(CQL3Type type, Object value)
    {
        if (value == null)
            return true;

        AbstractType<?> dataType = type.getType();
        if (dataType.isMultiCell() && dataType.isCollection())
        {
            return (((CollectionType<?>) dataType).kind == CollectionType.Kind.MAP)
                   ? ((Map<?, ?>) value).isEmpty()
                   : ((Collection<?>) value).isEmpty();
        }

        return false;
    }
}
