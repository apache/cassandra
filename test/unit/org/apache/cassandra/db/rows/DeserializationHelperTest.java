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

package org.apache.cassandra.db.rows;


import java.io.DataInput;

import org.junit.Test;

import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.rows.DeserializationHelper.NO_DROP_HORIZON;
import static org.apache.cassandra.db.rows.DeserializationHelper.isDroppedAtHorizon;
import static org.apache.cassandra.net.MessagingService.Version.VERSION_60;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class DeserializationHelperTest
{
    private static final long DROP_TIME = 20_000L;

    static TableMetadata metadata =
    TableMetadata.builder("DeserializationHelperTest", "Test")
                 .addPartitionKeyColumn("key", AsciiType.instance)
                 .addClusteringColumn("clustering", Int32Type.instance)
                 .addRegularColumn("data", Int32Type.instance)
                 .build();

    @Test
    public void testTrackedDataInputPlusIsReusable()
    {
        DeserializationHelper helper = new DeserializationHelper(metadata, VERSION_60.value, DeserializationHelper.Flag.LOCAL);
        TrackedDataInputPlus trackedDataInputPlus = helper.trackedDataInputPlus(mock(DataInput.class), 0);
        assertSame(trackedDataInputPlus, helper.trackedDataInputPlus(mock(DataInput.class), 1));
    }

    /**
     * The horizon array the cursor reader precomputes must decide exactly what
     * {@code isDropped(column, timestamp, false)} decides. The two forms part company on one input,
     * and only because {@link DeserializationHelper#NO_DROP_HORIZON} is {@code Long.MIN_VALUE}: a
     * bare {@code timestamp <= horizon} discards a cell timestamped {@link LivenessInfo#NO_TIMESTAMP}
     * on a never-dropped column, which the iterator path keeps.
     *
     * This pins the RULE, in isolation. The end-to-end pin is
     * {@code DroppedColumnDifferentialCompactionTest.noTimestampCellSurvivesBothSentinelCollisions}, which
     * cannot reach the input through CQL: {@code cql3/RowUpdateBuilder}'s constructor rejects
     * {@code Long.MIN_VALUE} for every modification statement, and {@code QueryOptions} rejects it again at
     * the native protocol, both because the engine uses that value for "no timestamp". It writes through
     * {@code PartitionUpdate.simpleBuilder}, which has no such guard. Nothing about the encoding stands in
     * the way — a {@code Long.MIN_VALUE} cell timestamp round-trips through an sstable exactly, since
     * {@code SerializationHeader} writes it as an unsigned vint delta from {@code minTimestamp} and reads it
     * back by adding, inverses mod 2^64 for any base.
     *
     * That scenario walks three {@code Long.MIN_VALUE} collisions on one input, so it cannot say which of
     * them a failure belongs to. That is what this test is for, and it is why the two are not redundant.
     */
    @Test
    public void droppedHorizonAgreesWithTheMapLookup()
    {
        ColumnMetadata dropped = ColumnMetadata.regularColumn("DeserializationHelperTest", "Test", "gone",
                                                             Int32Type.instance, ColumnMetadata.NO_UNIQUE_ID);
        TableMetadata withDrop = TableMetadata.builder("DeserializationHelperTest", "Test")
                                              .addPartitionKeyColumn("key", AsciiType.instance)
                                              .addClusteringColumn("clustering", Int32Type.instance)
                                              .addRegularColumn("data", Int32Type.instance)
                                              .recordColumnDrop(dropped, DROP_TIME)
                                              .build();
        DeserializationHelper helper = new DeserializationHelper(withDrop, VERSION_60.value, DeserializationHelper.Flag.LOCAL);
        ColumnMetadata live = withDrop.getColumn(ByteBufferUtil.bytes("data"));

        // precondition: this helper is in the state the cursor reader's gate lets through at all
        assertTrue(helper.hasDroppedColumns());
        assertEquals(DROP_TIME, helper.droppedTimeOrMin(dropped));
        assertEquals(NO_DROP_HORIZON, helper.droppedTimeOrMin(live));

        // the one input the two forms disagree on unless the sentinel is tested first
        assertFalse(isDroppedAtHorizon(LivenessInfo.NO_TIMESTAMP, helper.droppedTimeOrMin(live)));
        assertFalse(helper.isDropped(live, LivenessInfo.NO_TIMESTAMP, false));

        // and the rule itself, at and either side of the horizon
        assertTrue(isDroppedAtHorizon(DROP_TIME, DROP_TIME));
        assertTrue(isDroppedAtHorizon(DROP_TIME - 1, DROP_TIME));
        assertFalse(isDroppedAtHorizon(DROP_TIME + 1, DROP_TIME));
        assertEquals(helper.isDropped(dropped, DROP_TIME, false), isDroppedAtHorizon(DROP_TIME, DROP_TIME));
        assertEquals(helper.isDropped(dropped, DROP_TIME + 1, false), isDroppedAtHorizon(DROP_TIME + 1, DROP_TIME));
    }
}
