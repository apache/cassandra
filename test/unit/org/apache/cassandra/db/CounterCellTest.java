/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
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
import java.security.MessageDigest;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.serializers.AsciiSerializer;
import org.apache.cassandra.utils.*;

import static org.junit.Assert.*;
import static org.apache.cassandra.db.context.CounterContext.ContextState;

public class CounterCellTest
{
    private static final CounterContext cc = new CounterContext();

    private static final int idLength;
    private static final int clockLength;
    private static final int countLength;

    private static final int stepLength;

    private static final String KEYSPACE1 = "CounterCacheTest";
    private static final String COUNTER1 = "Counter1";
    private static final String STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD1),
                                    SchemaLoader.counterCFMD(KEYSPACE1, COUNTER1));
    }

    @AfterClass
    public static void cleanup()
    {
        SchemaLoader.cleanupSavedCaches();
    }

    static
    {
        idLength      = CounterId.LENGTH;
        clockLength   = 8; // size of long
        countLength   = 8; // size of long

        stepLength    = idLength + clockLength + countLength;
    }

    @Test
    public void testCreate()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(COUNTER1);
        long delta = 3L;

        Cell cell = createLegacyCounterCell(cfs, ByteBufferUtil.bytes("val"), delta, 1);

        assertEquals(delta, CounterContext.instance().total(cell.value()));
        assertEquals(1, cell.value().getShort(0));
        assertEquals(0, cell.value().getShort(2));
        Assert.assertTrue(CounterId.wrap(cell.value(), 4).isLocalId());
        assertEquals(1L, cell.value().getLong(4 + idLength));
        assertEquals(delta, cell.value().getLong(4 + idLength + clockLength));

    }

    private Cell createLegacyCounterCell(ColumnFamilyStore cfs, ByteBuffer colName, long count, long ts)
    {
        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(colName);
        ByteBuffer val = CounterContext.instance().createLocal(count);
        return BufferCell.live(cDef, ts, val);
    }

    private Cell createCounterCell(ColumnFamilyStore cfs, ByteBuffer colName, CounterId id, long count, long ts)
    {
        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(colName);
        ByteBuffer val = CounterContext.instance().createGlobal(id, ts, count);
        return BufferCell.live(cDef, ts, val);
    }

    private Cell createCounterCellFromContext(ColumnFamilyStore cfs, ByteBuffer colName, ContextState context, long ts)
    {
        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(colName);
        return BufferCell.live(cDef, ts, context.context);
    }

    private Cell createDeleted(ColumnFamilyStore cfs, ByteBuffer colName, long ts, int localDeletionTime)
    {
        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(colName);
        return BufferCell.tombstone(cDef, ts, localDeletionTime);
    }

    @Test
    public void testReconcile()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(COUNTER1);
        ByteBuffer col = ByteBufferUtil.bytes("val");

        Cell left;
        Cell right;

        // both deleted, diff deletion time, same ts
        left = createDeleted(cfs, col, 2, 5);
        right = createDeleted(cfs, col, 2, 10);
        assert Cells.reconcile(left, right, 10) == right;

        // diff ts
        right = createLegacyCounterCell(cfs, col, 1, 10);
        assert Cells.reconcile(left, right, 10) == left;

        // < tombstone
        left = createDeleted(cfs, col, 6, 6);
        right = createLegacyCounterCell(cfs, col, 1, 5);
        assert Cells.reconcile(left, right, 10) == left;

        // > tombstone
        left = createDeleted(cfs, col, 1, 1);
        right = createLegacyCounterCell(cfs, col, 1, 5);
        assert Cells.reconcile(left, right, 10) == left;

        // == tombstone
        left = createDeleted(cfs, col, 8, 8);
        right = createLegacyCounterCell(cfs, col, 1, 8);
        assert Cells.reconcile(left, right, 10) == left;

        // live + live
        left = createLegacyCounterCell(cfs, col, 1, 2);
        right = createLegacyCounterCell(cfs, col, 3, 5);
        Cell reconciled = Cells.reconcile(left, right, 10);
        assertEquals(CounterContext.instance().total(reconciled.value()), 4);
        assertEquals(reconciled.timestamp(), 5L);

        // Add, don't change TS
        Cell addTen = createLegacyCounterCell(cfs, col, 10, 4);
        reconciled = Cells.reconcile(reconciled, addTen, 10);
        assertEquals(CounterContext.instance().total(reconciled.value()), 14);
        assertEquals(reconciled.timestamp(), 5L);

        // Add w/new TS
        Cell addThree = createLegacyCounterCell(cfs, col, 3, 7);
        reconciled = Cells.reconcile(reconciled, addThree, 10);
        assertEquals(CounterContext.instance().total(reconciled.value()), 17);
        assertEquals(reconciled.timestamp(), 7L);

        // Confirm no deletion time
        assert reconciled.localDeletionTime() == Integer.MAX_VALUE;

        Cell deleted = createDeleted(cfs, col, 8, 8);
        reconciled = Cells.reconcile(reconciled, deleted, 10);
        assert reconciled.localDeletionTime() == 8;
    }

    @Test
    public void testDiff()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(COUNTER1);
        ByteBuffer col = ByteBufferUtil.bytes("val");

        Cell leftCell;
        Cell rightCell;

        // Equal count
        leftCell = createLegacyCounterCell(cfs, col, 2, 2);
        rightCell = createLegacyCounterCell(cfs, col, 2, 1);
        assertEquals(CounterContext.Relationship.EQUAL, CounterContext.instance().diff(leftCell.value(), rightCell.value()));

        // Non-equal count
        leftCell = createLegacyCounterCell(cfs, col, 1, 2);
        rightCell = createLegacyCounterCell(cfs, col, 2, 1);
        assertEquals(CounterContext.Relationship.DISJOINT, CounterContext.instance().diff(leftCell.value(), rightCell.value()));

        // timestamp
        CounterId id = CounterId.generate();
        leftCell = createCounterCell(cfs, col, id, 2, 2);
        rightCell = createCounterCell(cfs, col, id, 2, 1);
        assertEquals(CounterContext.Relationship.GREATER_THAN, CounterContext.instance().diff(leftCell.value(), rightCell.value()));

        ContextState leftContext;
        ContextState rightContext;

        // Equal based on context w/shards etc
        leftContext = ContextState.allocate(0, 0, 3);
        leftContext.writeRemote(CounterId.fromInt(3), 3L, 0L);
        leftContext.writeRemote(CounterId.fromInt(6), 2L, 0L);
        leftContext.writeRemote(CounterId.fromInt(9), 1L, 0L);
        rightContext = ContextState.wrap(ByteBufferUtil.clone(leftContext.context));

        leftCell = createCounterCellFromContext(cfs, col, leftContext, 1);
        rightCell = createCounterCellFromContext(cfs, col, rightContext, 1);
        assertEquals(CounterContext.Relationship.EQUAL, CounterContext.instance().diff(leftCell.value(), rightCell.value()));

        // greater than: left has superset of nodes (counts equal)
        leftContext = ContextState.allocate(0, 0, 4);
        leftContext.writeRemote(CounterId.fromInt(3), 3L, 0L);
        leftContext.writeRemote(CounterId.fromInt(6), 2L, 0L);
        leftContext.writeRemote(CounterId.fromInt(9), 1L, 0L);
        leftContext.writeRemote(CounterId.fromInt(12), 0L, 0L);

        rightContext = ContextState.allocate(0, 0, 3);
        rightContext.writeRemote(CounterId.fromInt(3), 3L, 0L);
        rightContext.writeRemote(CounterId.fromInt(6), 2L, 0L);
        rightContext.writeRemote(CounterId.fromInt(9), 1L, 0L);

        leftCell = createCounterCellFromContext(cfs, col, leftContext, 1);
        rightCell = createCounterCellFromContext(cfs, col, rightContext, 1);
        assertEquals(CounterContext.Relationship.GREATER_THAN, CounterContext.instance().diff(leftCell.value(), rightCell.value()));
        assertEquals(CounterContext.Relationship.LESS_THAN, CounterContext.instance().diff(rightCell.value(), leftCell.value()));

        // disjoint: right and left have disjoint node sets
        leftContext = ContextState.allocate(0, 0, 3);
        leftContext.writeRemote(CounterId.fromInt(3), 1L, 0L);
        leftContext.writeRemote(CounterId.fromInt(4), 1L, 0L);
        leftContext.writeRemote(CounterId.fromInt(9), 1L, 0L);

        rightContext = ContextState.allocate(0, 0, 3);
        rightContext.writeRemote(CounterId.fromInt(3), 1L, 0L);
        rightContext.writeRemote(CounterId.fromInt(6), 1L, 0L);
        rightContext.writeRemote(CounterId.fromInt(9), 1L, 0L);

        leftCell = createCounterCellFromContext(cfs, col, leftContext, 1);
        rightCell = createCounterCellFromContext(cfs, col, rightContext, 1);
        assertEquals(CounterContext.Relationship.DISJOINT, CounterContext.instance().diff(leftCell.value(), rightCell.value()));
        assertEquals(CounterContext.Relationship.DISJOINT, CounterContext.instance().diff(rightCell.value(), leftCell.value()));
    }

    @Test
    public void testUpdateDigest() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(COUNTER1);
        ByteBuffer col = ByteBufferUtil.bytes("val");

        MessageDigest digest1 = MessageDigest.getInstance("md5");
        MessageDigest digest2 = MessageDigest.getInstance("md5");

        CounterContext.ContextState state = CounterContext.ContextState.allocate(0, 2, 2);
        state.writeRemote(CounterId.fromInt(1), 4L, 4L);
        state.writeLocal(CounterId.fromInt(2), 4L, 4L);
        state.writeRemote(CounterId.fromInt(3), 4L, 4L);
        state.writeLocal(CounterId.fromInt(4), 4L, 4L);

        Cell original = createCounterCellFromContext(cfs, col, state, 5);

        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(col);
        Cell cleared = BufferCell.live(cDef, 5, CounterContext.instance().clearAllLocal(state.context));

        original.digest(digest1);
        cleared.digest(digest2);

        assert Arrays.equals(digest1.digest(), digest2.digest());
    }

    @Test
    public void testDigestWithEmptyCells() throws Exception
    {
        // For DB-1881
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(COUNTER1);

        ColumnDefinition emptyColDef = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("val2"));
        BufferCell emptyCell = BufferCell.live(emptyColDef, 0, ByteBuffer.allocate(0));

        Row.Builder builder = BTreeRow.unsortedBuilder(0);
        builder.newRow(Clustering.make(AsciiSerializer.instance.serialize("test")));
        builder.addCell(emptyCell);
        Row row = builder.build();

        MessageDigest digest = MessageDigest.getInstance("md5");
        row.digest(digest);
        assertNotNull(digest.digest());
    }

}
