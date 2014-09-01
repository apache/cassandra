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
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.rows.AbstractCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
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
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
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

    private class TestCounterCell extends AbstractCell
    {
        private final ColumnDefinition column;
        private final ByteBuffer value;
        private final LivenessInfo info;

        private TestCounterCell(ColumnDefinition column, ByteBuffer value, LivenessInfo info)
        {
            this.column = column;
            this.value = value;
            this.info = info.takeAlias();
        }

        public ColumnDefinition column()
        {
            return column;
        }

        public boolean isCounterCell()
        {
            return true;
        }

        public ByteBuffer value()
        {
            return value;
        }

        public LivenessInfo livenessInfo()
        {
            return info;
        }

        public CellPath path()
        {
            return null;
        }
    }

    @Test
    public void testCreate()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(COUNTER1);
        long delta = 3L;

        TestCounterCell cell = createLegacyCounterCell(cfs, ByteBufferUtil.bytes("c1"), delta, 1, 0, 0);

        assertEquals(delta, CounterContext.instance().total(cell.value()));
        assertEquals(1, cell.value().getShort(0));
        assertEquals(0, cell.value().getShort(2));
        Assert.assertTrue(CounterId.wrap(cell.value(), 4).isLocalId());
        assertEquals(1L, cell.value().getLong(4 + idLength));
        assertEquals(delta, cell.value().getLong(4 + idLength + clockLength));

    }

    private TestCounterCell createLegacyCounterCell(ColumnFamilyStore cfs,
                                                    ByteBuffer colName,
                                                    long count,
                                                    long ts,
                                                    int ttl,
                                                    int localDeletion)
    {
        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(colName);
        ByteBuffer val = CounterContext.instance().createLocal(count);
        LivenessInfo li = new SimpleLivenessInfo(ts, ttl, localDeletion);
        return new TestCounterCell(cDef, val, li);
    }

    private TestCounterCell createCounterCell(ColumnFamilyStore cfs,
                                              ByteBuffer colName,
                                              CounterId id,
                                              long count,
                                              long ts,
                                              int ttl,
                                              int localDeletion)
    {
        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(colName);
        ByteBuffer val = CounterContext.instance().createGlobal(id, ts, count);
        LivenessInfo li = new SimpleLivenessInfo(ts, ttl, localDeletion);
        return new TestCounterCell(cDef, val, li);
    }

    private TestCounterCell createCounterCellFromContext(ColumnFamilyStore cfs,
                                                         ByteBuffer colName,
                                                         ContextState context,
                                                         long ts,
                                                         int ttl,
                                                         int localDeletion)
    {
        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(colName);
        LivenessInfo li = new SimpleLivenessInfo(ts, ttl, localDeletion);
        return new TestCounterCell(cDef, context.context, li);
    }

    @Test
    public void testReconcile()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(COUNTER1);
        ColumnDefinition cDef = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1).metadata.getColumnDefinition(ByteBufferUtil.bytes("val"));
        ByteBuffer col = ByteBufferUtil.bytes("c1");

        AbstractCell left;
        AbstractCell right;

        // both deleted, diff deletion time, same ts
        left = createLegacyCounterCell(cfs, col, 1, 2, 0, 5);
        right = createLegacyCounterCell(cfs, col, 1, 2, 0, 10);
        assert Cells.reconcile(left, right, 10) == right;

        // diff ts
        right = createLegacyCounterCell(cfs, col, 1, 1, 0, 10);
        assert Cells.reconcile(left, right, 10) == left;

        // < tombstone
        left = new CellTest.TestCell(cDef, ByteBufferUtil.bytes(5L), SimpleLivenessInfo.forDeletion(6, 6));
        right = createLegacyCounterCell(cfs, col, 1, 5, 0, 5);
        assert Cells.reconcile(left, right, 10) == left;

        // > tombstone
        left = new CellTest.TestCell(cDef, ByteBufferUtil.bytes(5L), SimpleLivenessInfo.forDeletion(1, 1));
        right = createLegacyCounterCell(cfs, col, 1, 5, 0, Integer.MAX_VALUE);
        assert Cells.reconcile(left, right, 10) == left;

        // == tombstone
        left = new CellTest.TestCell(cDef, ByteBufferUtil.bytes(5L), SimpleLivenessInfo.forDeletion(8, 8));
        right = createLegacyCounterCell(cfs, col, 1, 8, 0, Integer.MAX_VALUE);
        assert Cells.reconcile(left, right, 10) == left;

        // live + live
        left = createLegacyCounterCell(cfs, col, 1, 2, 0, Integer.MAX_VALUE);
        right = createLegacyCounterCell(cfs, col, 3, 5, 0, Integer.MAX_VALUE);
        Cell reconciled = Cells.reconcile(left, right, 10);
        assertEquals(CounterContext.instance().total(reconciled.value()), 4);
        assertEquals(reconciled.livenessInfo().timestamp(), 5L);

        // Add, don't change TS
        Cell addTen = createLegacyCounterCell(cfs, col, 10, 4, 0, Integer.MAX_VALUE);
        reconciled = Cells.reconcile(reconciled, addTen, 10);
        assertEquals(CounterContext.instance().total(reconciled.value()), 14);
        assertEquals(reconciled.livenessInfo().timestamp(), 5L);

        // Add w/new TS
        Cell addThree = createLegacyCounterCell(cfs, col, 3, 7, 0, Integer.MAX_VALUE);
        reconciled = Cells.reconcile(reconciled, addThree, 10);
        assertEquals(CounterContext.instance().total(reconciled.value()), 17);
        assertEquals(reconciled.livenessInfo().timestamp(), 7L);

        // Confirm no deletion time
        assert reconciled.livenessInfo().localDeletionTime() == Integer.MAX_VALUE;

        Cell deleted = createLegacyCounterCell(cfs, col, 2, 8, 0, 8);
        reconciled = Cells.reconcile(reconciled, deleted, 10);
        assertEquals(2, CounterContext.instance().total(reconciled.value()));
        assertEquals(reconciled.livenessInfo().timestamp(), 8L);
        assert reconciled.livenessInfo().localDeletionTime() == 8;
    }

    @Test
    public void testDiff()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(COUNTER1);
        ByteBuffer col = ByteBufferUtil.bytes("c1");

        TestCounterCell leftCell;
        TestCounterCell rightCell;

        // Equal count
        leftCell = createLegacyCounterCell(cfs, col, 2, 2, 0, Integer.MAX_VALUE);
        rightCell = createLegacyCounterCell(cfs, col, 2, 1, 0, Integer.MAX_VALUE);
        assertEquals(CounterContext.Relationship.EQUAL, CounterContext.instance().diff(leftCell.value(), rightCell.value()));

        // Non-equal count
        leftCell = createLegacyCounterCell(cfs, col, 1, 2, 0, Integer.MAX_VALUE);
        rightCell = createLegacyCounterCell(cfs, col, 2, 1, 0, Integer.MAX_VALUE);
        assertEquals(CounterContext.Relationship.DISJOINT, CounterContext.instance().diff(leftCell.value(), rightCell.value()));

        // timestamp
        CounterId id = CounterId.generate();
        leftCell = createCounterCell(cfs, col, id, 2, 2, 0, Integer.MAX_VALUE);
        rightCell = createCounterCell(cfs, col, id, 2, 1, 0, Integer.MAX_VALUE);
        assertEquals(CounterContext.Relationship.GREATER_THAN, CounterContext.instance().diff(leftCell.value(), rightCell.value()));

        ContextState leftContext;
        ContextState rightContext;

        // Equal based on context w/shards etc
        leftContext = ContextState.allocate(0, 0, 3);
        leftContext.writeRemote(CounterId.fromInt(3), 3L, 0L);
        leftContext.writeRemote(CounterId.fromInt(6), 2L, 0L);
        leftContext.writeRemote(CounterId.fromInt(9), 1L, 0L);
        rightContext = ContextState.wrap(ByteBufferUtil.clone(leftContext.context));

        leftCell = createCounterCellFromContext(cfs, col, leftContext, 1, 0, Integer.MAX_VALUE);
        rightCell = createCounterCellFromContext(cfs, col, rightContext, 1, 0, Integer.MAX_VALUE);
        assertEquals(CounterContext.Relationship.EQUAL, CounterContext.instance().diff(leftCell.value, rightCell.value));

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

        leftCell = createCounterCellFromContext(cfs, col, leftContext, 1, 0, Integer.MAX_VALUE);
        rightCell = createCounterCellFromContext(cfs, col, rightContext, 1, 0, Integer.MAX_VALUE);
        assertEquals(CounterContext.Relationship.GREATER_THAN, CounterContext.instance().diff(leftCell.value, rightCell.value));
        assertEquals(CounterContext.Relationship.LESS_THAN, CounterContext.instance().diff(rightCell.value, leftCell.value));

        // disjoint: right and left have disjoint node sets
        leftContext = ContextState.allocate(0, 0, 3);
        leftContext.writeRemote(CounterId.fromInt(3), 1L, 0L);
        leftContext.writeRemote(CounterId.fromInt(4), 1L, 0L);
        leftContext.writeRemote(CounterId.fromInt(9), 1L, 0L);

        rightContext = ContextState.allocate(0, 0, 3);
        rightContext.writeRemote(CounterId.fromInt(3), 1L, 0L);
        rightContext.writeRemote(CounterId.fromInt(6), 1L, 0L);
        rightContext.writeRemote(CounterId.fromInt(9), 1L, 0L);

        leftCell = createCounterCellFromContext(cfs, col, leftContext, 1, 0, Integer.MAX_VALUE);
        rightCell = createCounterCellFromContext(cfs, col, rightContext, 1, 0, Integer.MAX_VALUE);
        assertEquals(CounterContext.Relationship.DISJOINT, CounterContext.instance().diff(leftCell.value, rightCell.value));
        assertEquals(CounterContext.Relationship.DISJOINT, CounterContext.instance().diff(rightCell.value, leftCell.value));
    }

    @Test
    public void testUpdateDigest() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(COUNTER1);
        ByteBuffer col = ByteBufferUtil.bytes("c1");

        MessageDigest digest1 = MessageDigest.getInstance("md5");
        MessageDigest digest2 = MessageDigest.getInstance("md5");

        CounterContext.ContextState state = CounterContext.ContextState.allocate(0, 2, 2);
        state.writeRemote(CounterId.fromInt(1), 4L, 4L);
        state.writeLocal(CounterId.fromInt(2), 4L, 4L);
        state.writeRemote(CounterId.fromInt(3), 4L, 4L);
        state.writeLocal(CounterId.fromInt(4), 4L, 4L);

        TestCounterCell original = createCounterCellFromContext(cfs, col, state, 5, 0, Integer.MAX_VALUE);

        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(col);
        LivenessInfo li = new SimpleLivenessInfo(5, 0, Integer.MAX_VALUE);
        TestCounterCell cleared = new TestCounterCell(cDef, CounterContext.instance().clearAllLocal(state.context), li);

        CounterContext.instance().updateDigest(digest1, original.value);
        CounterContext.instance().updateDigest(digest2, cleared.value);

        assert Arrays.equals(digest1.digest(), digest2.digest());
    }
}
