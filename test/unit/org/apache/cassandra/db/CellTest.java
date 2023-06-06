/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert;

import static java.util.Arrays.asList;

public class CellTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static final String KEYSPACE1 = "CellTest";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_COLLECTION = "Collection1";

    private static final TableMetadata cfm = SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1).build();
    private static final TableMetadata cfm2 =
        TableMetadata.builder(KEYSPACE1, CF_COLLECTION)
                     .addPartitionKeyColumn("k", IntegerType.instance)
                     .addClusteringColumn("c", IntegerType.instance)
                     .addRegularColumn("v", IntegerType.instance)
                     .addRegularColumn("m", MapType.getInstance(IntegerType.instance, IntegerType.instance, true))
                     .build();

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1), cfm, cfm2);
    }

    private static ColumnMetadata fakeColumn(String name, AbstractType<?> type)
    {
        return new ColumnMetadata("fakeKs",
                                  "fakeTable",
                                  ColumnIdentifier.getInterned(name, false),
                                  type,
                                  ColumnMetadata.NO_POSITION,
                                  ColumnMetadata.Kind.REGULAR,
                                  null);
    }

    @Test
    public void testConflictingTypeEquality()
    {
        boolean[] tf = new boolean[]{ true, false };
        for (boolean lhs : tf)
        {
            for (boolean rhs : tf)
            {
                // don't test equality for both sides native, as this is based on CellName resolution
                if (lhs && rhs)
                    continue;
                Cell<?> a = expiring(cfm, "val", "a", 1, 1);
                Cell<?> b = regular(cfm, "val", "a", 1);
                Assert.assertNotSame(a, b);
                Assert.assertNotSame(b, a);

                a = deleted(cfm, "val", 1, 1);
                Assert.assertNotSame(a, b);
                Assert.assertNotSame(b, a);
            }
        }
    }

    @Test
    public void testUnmarshallableInMulticellCollection()
    {
        List<CQL3Type.Native> unmarshallableTypes = new ArrayList<>();
        for (CQL3Type.Native nativeType : CQL3Type.Native.values())
        {
            ColumnMetadata c = fakeColumn("c", MapType.getInstance(Int32Type.instance, nativeType.getType(), true));
            BufferCell cell = BufferCell.tombstone(c, 0, 4, CellPath.create(ByteBufferUtil.bytes(4)));
            try
            {
                Assert.assertEquals("expected #toString failed for type " + nativeType, "[c[4]=<tombstone> ts=0 ldt=4]", cell.toString());
            }
            catch (MarshalException m)
            {
                unmarshallableTypes.add(nativeType);
            }
        }
        Assert.assertTrue(unmarshallableTypes.isEmpty());
    }

    private void assertValid(Cell<?> cell)
    {
        try
        {
            cell.validate();
        }
        catch (Exception e)
        {
            Assert.fail("Cell should be valid but got error: " + e);
        }
    }

    private void assertInvalid(Cell<?> cell)
    {
        try
        {
            cell.validate();
            Assert.fail("Cell " + cell + " should be invalid");
        }
        catch (MarshalException e)
        {
            // Note that we shouldn't get anything else than a MarshalException so let other escape and fail the test
        }
    }

    @Test
    public void testValidate()
    {
        ColumnMetadata c;

        // Valid cells
        c = fakeColumn("c", Int32Type.instance);
        assertValid(BufferCell.live(c, 0, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertValid(BufferCell.live(c, 0, ByteBufferUtil.bytes(4)));

        assertValid(BufferCell.expiring(c, 0, 4, 4, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertValid(BufferCell.expiring(c, 0, 4, 4, ByteBufferUtil.bytes(4)));

        assertValid(BufferCell.tombstone(c, 0, 4));

        // Invalid value (we don't all empty values for smallint)
        c = fakeColumn("c", ShortType.instance);
        assertInvalid(BufferCell.live(c, 0, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        // But this should be valid even though the underlying value is an empty BB (catches bug #11618)
        assertValid(BufferCell.tombstone(c, 0, 4));
        // And of course, this should be valid with a proper value
        assertValid(BufferCell.live(c, 0, bbs(4)));

        // Invalid ttl
        assertInvalid(BufferCell.expiring(c, 0, -4, 4, bbs(4)));
        ColumnMetadata f = c;
        assertThrowsOnInvalidDeletionTime(() -> BufferCell.expiring(f, 0, 4, -5, bbs(4)));
        assertThrowsOnInvalidDeletionTime(() -> BufferCell.expiring(f, 0, 4, Cell.NO_DELETION_TIME, bbs(4)));

        c = fakeColumn("c", MapType.getInstance(Int32Type.instance, Int32Type.instance, true));
        // Valid cell path
        assertValid(BufferCell.live(c, 0, ByteBufferUtil.bytes(4), CellPath.create(ByteBufferUtil.bytes(4))));
        // Invalid cell path (int values should be 0 or 4 bytes)
        assertInvalid(BufferCell.live(c, 0, ByteBufferUtil.bytes(4), CellPath.create(ByteBufferUtil.bytes((long)4))));
    }

    @Test
    public void testValidateNonFrozenUDT()
    {
        FieldIdentifier f1 = field("f1");  // has field position 0
        FieldIdentifier f2 = field("f2");  // has field position 1
        UserType udt = new UserType("ks",
                                    bb("myType"),
                                    asList(f1, f2),
                                    asList(Int32Type.instance, UTF8Type.instance),
                                    true);
        ColumnMetadata c;

        // Valid cells
        c = fakeColumn("c", udt);
        assertValid(BufferCell.live(c, 0, bb(1), CellPath.create(bbs(0))));
        assertValid(BufferCell.live(c, 0, bb("foo"), CellPath.create(bbs(1))));
        assertValid(BufferCell.expiring(c, 0, 4, 4, bb(1), CellPath.create(bbs(0))));
        assertValid(BufferCell.expiring(c, 0, 4, 4, bb("foo"), CellPath.create(bbs(1))));
        assertValid(BufferCell.tombstone(c, 0, 4, CellPath.create(bbs(0))));

        // Invalid value (text in an int field)
        assertInvalid(BufferCell.live(c, 0, bb("foo"), CellPath.create(bbs(0))));

        // Invalid ttl
        assertInvalid(BufferCell.expiring(c, 0, -4, 4, bb(1), CellPath.create(bbs(0))));
        assertThrowsOnInvalidDeletionTime(() -> BufferCell.expiring(c, 0, 4, -5, bb(1), CellPath.create(bbs(0))));
        assertThrowsOnInvalidDeletionTime(() -> BufferCell.expiring(c, 0, 4, Cell.NO_DELETION_TIME, bb(1), CellPath.create(bbs(0))));

        // Invalid cell path (int values should be 0 or 2 bytes)
        assertInvalid(BufferCell.live(c, 0, bb(1), CellPath.create(ByteBufferUtil.bytes((long)4))));
    }

    @Test
    public void testValidateFrozenUDT()
    {
        FieldIdentifier f1 = field("f1");  // has field position 0
        FieldIdentifier f2 = field("f2");  // has field position 1
        UserType udt = new UserType("ks",
                                    bb("myType"),
                                    asList(f1, f2),
                                    asList(Int32Type.instance, UTF8Type.instance),
                                    false);

        ColumnMetadata c = fakeColumn("c", udt);
        ByteBuffer val = udt(bb(1), bb("foo"));

        // Valid cells
        assertValid(BufferCell.live(c, 0, val));
        assertValid(BufferCell.live(c, 0, val));
        assertValid(BufferCell.expiring(c, 0, 4, 4, val));
        assertValid(BufferCell.expiring(c, 0, 4, 4, val));
        assertValid(BufferCell.tombstone(c, 0, 4));
        // fewer values than types is accepted
        assertValid(BufferCell.live(c, 0, udt(bb(1))));

        // Invalid values
        // invalid types
        assertInvalid(BufferCell.live(c, 0, udt(bb("foo"), bb(1))));
        // too many types
        assertInvalid(BufferCell.live(c, 0, udt(bb(1), bb("foo"), bb("bar"))));

        // Invalid ttl
        assertInvalid(BufferCell.expiring(c, 0, -4, 4, val));
        assertThrowsOnInvalidDeletionTime(() -> BufferCell.expiring(c, 0, 4, -5, val));
        assertThrowsOnInvalidDeletionTime(() -> BufferCell.expiring(c, 0, 4, Cell.NO_DELETION_TIME, val));
    }

    @Test
    public void testExpiringCellReconile()
    {
        // equal
        Assert.assertEquals(0, testExpiring("val", "a", 1, 1, null, null, null, null));

        // newer timestamp
        Assert.assertEquals(-1, testExpiring("val", "a", 2, 1, null, null, 1L, null));
        Assert.assertEquals(-1, testExpiring("val", "a", 2, 1, null, "val", 1L, 2));

        Assert.assertEquals(-1, testExpiring("val", "a", 1, 2, null, null, null, 1));
        Assert.assertEquals(-1, testExpiring("val", "a", 1, 2, null, "val", null, 1));
        Assert.assertEquals(1, testExpiring("val", "a", 1, 1, null, "val", null, 2));
        Assert.assertEquals(1, testExpiring("val", "a", 1, 1, null, "val", null, 1));

        // newer value
        Assert.assertEquals(-1, testExpiring("val", "b", 2, 1, null, "a", null, null));
        Assert.assertEquals(-1, testExpiring("val", "b", 2, 1, null, "a", null, 1));
    }

    class SimplePurger implements DeletionPurger
    {
        private final long gcBefore;

        public SimplePurger(long gcBefore)
        {
            this.gcBefore = gcBefore;
        }

        public boolean shouldPurge(long timestamp, long localDeletionTime)
        {
            return localDeletionTime < gcBefore;
        }
    }

    /**
     * tombstones shouldn't be purged if localDeletionTime is greater than gcBefore
     */
    @Test
    public void testNonPurgableTombstone()
    {
        int now = 100;
        Cell<?> cell = deleted(cfm, "val", now, now);
        Cell<?> purged = cell.purge(new SimplePurger(now - 1), now + 1);
        Assert.assertEquals(cell, purged);
    }

    @Test
    public void testPurgeableTombstone()
    {
        int now = 100;
        Cell<?> cell = deleted(cfm, "val", now, now);
        Cell<?> purged = cell.purge(new SimplePurger(now + 1), now + 1);
        Assert.assertNull(purged);
    }

    @Test
    public void testLiveExpiringCell()
    {
        int now = 100;
        Cell<?> cell = expiring(cfm, "val", "a", now, now + 10);
        Cell<?> purged = cell.purge(new SimplePurger(now), now + 1);
        Assert.assertEquals(cell, purged);
    }

    /**
     * cells that have expired should be converted to tombstones with an local deletion time
     * of the cell's local expiration time, minus it's ttl
     */
    @Test
    public void testExpiredTombstoneConversion()
    {
        int now = 100;
        Cell<?> cell = expiring(cfm, "val", "a", now, 10, now + 10);
        Cell<?> purged = cell.purge(new SimplePurger(now), now + 11);
        Assert.assertEquals(deleted(cfm, "val", now, now), purged);
    }

    /**
     * if the tombstone created by an expiring cell has a local deletion time less than gcBefore,
     * it should be purged
     */
    @Test
    public void testPurgeableExpiringCell()
    {
        int now = 100;
        Cell<?> cell = expiring(cfm, "val", "a", now, 10, now + 10);
        Cell<?> purged = cell.purge(new SimplePurger(now + 1), now + 11);
        Assert.assertNull(purged);
    }

    private static ByteBuffer bb(int i)
    {
        return ByteBufferUtil.bytes(i);
    }

    private static ByteBuffer bbs(int s)
    {
        return ByteBufferUtil.bytes((short) s);
    }

    private static ByteBuffer bb(String str)
    {
        return UTF8Type.instance.decompose(str);
    }

    private static ByteBuffer udt(ByteBuffer...buffers)
    {
        return UserType.buildValue(buffers);
    }

    private static FieldIdentifier field(String field)
    {
        return FieldIdentifier.forQuoted(field);
    }

//    @Test
//    public void testComplexCellReconcile()
//    {
//        ColumnMetadata m = cfm2.getColumn(new ColumnIdentifier("m", false));
//        int now1 = FBUtilities.nowInSeconds();
//        long ts1 = now1*1000000L;
//
//
//        Cell<?> r1m1 = BufferCell.live(m, ts1, bb(1), CellPath.create(bb(1)));
//        Cell<?> r1m2 = BufferCell.live(m, ts1, bb(2), CellPath.create(bb(2)));
//        List<Cell<?>> cells1 = Lists.newArrayList(r1m1, r1m2);
//
//        int now2 = now1 + 1;
//        long ts2 = now2*1000000L;
//        Cell<?> r2m2 = BufferCell.live(m, ts2, bb(1), CellPath.create(bb(2)));
//        Cell<?> r2m3 = BufferCell.live(m, ts2, bb(2), CellPath.create(bb(3)));
//        Cell<?> r2m4 = BufferCell.live(m, ts2, bb(3), CellPath.create(bb(4)));
//        List<Cell<?>> cells2 = Lists.newArrayList(r2m2, r2m3, r2m4);
//
//        RowBuilder builder = new RowBuilder();
//        Cells.reconcileComplex(m, cells1.iterator(), cells2.iterator(), DeletionTime.LIVE, builder);
//        Assert.assertEquals(Lists.newArrayList(r1m1, r2m2, r2m3, r2m4), builder.cells);
//    }

    private int testExpiring(String n1, String v1, long t1, int et1, String n2, String v2, Long t2, Integer et2)
    {
        if (n2 == null)
            n2 = n1;
        if (v2 == null)
            v2 = v1;
        if (t2 == null)
            t2 = t1;
        if (et2 == null)
            et2 = et1;
        Cell<?> c1 = expiring(cfm, n1, v1, t1, et1);
        Cell<?> c2 = expiring(cfm, n2, v2, t2, et2);

        if (Cells.reconcile(c1, c2) == c1)
            return Cells.reconcile(c2, c1) == c1 ? -1 : 0;
        return Cells.reconcile(c2, c1) == c2 ? 1 : 0;
    }

    private Cell<?> regular(TableMetadata cfm, String columnName, String value, long timestamp)
    {
        ColumnMetadata cdef = cfm.getColumn(ByteBufferUtil.bytes(columnName));
        return BufferCell.live(cdef, timestamp, ByteBufferUtil.bytes(value));
    }

    private Cell<?> expiring(TableMetadata cfm, String columnName, String value, long timestamp, long localExpirationTime)
    {
        return expiring(cfm, columnName, value, timestamp, 1, localExpirationTime);
    }

    private Cell<?> expiring(TableMetadata cfm, String columnName, String value, long timestamp, int ttl, long localExpirationTime)
    {
        ColumnMetadata cdef = cfm.getColumn(ByteBufferUtil.bytes(columnName));
        return new BufferCell(cdef, timestamp, ttl, localExpirationTime, ByteBufferUtil.bytes(value), null);
    }

    private Cell<?> deleted(TableMetadata cfm, String columnName, long localDeletionTime, long timestamp)
    {
        ColumnMetadata cdef = cfm.getColumn(ByteBufferUtil.bytes(columnName));
        return BufferCell.tombstone(cdef, timestamp, localDeletionTime);
    }

    private static void assertThrowsOnInvalidDeletionTime(ThrowableAssert.ThrowingCallable runnable)
    {
        Assertions.assertThatThrownBy(runnable)
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("out of range");
    }
}
