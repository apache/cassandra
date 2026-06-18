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

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class BTreeRowHasLiveDataTest
{
    private static final TableMetadata metadata;
    private static final ColumnMetadata intColumn;
    private static final ColumnMetadata mapColumn;
    private static final Clustering<?> clusteringKey;

    static
    {
        metadata = TableMetadata.builder("ks", "tbl")
                     .addPartitionKeyColumn("k", IntegerType.instance)
                     .addClusteringColumn("c", IntegerType.instance)
                     .addRegularColumn("v", IntegerType.instance)
                     .addRegularColumn("m", MapType.getInstance(IntegerType.instance, IntegerType.instance, true))
                     .build();
        intColumn = metadata.getColumn(new ColumnIdentifier("v", false));
        mapColumn = metadata.getColumn(new ColumnIdentifier("m", false));
        clusteringKey = metadata.comparator.make(BigInteger.valueOf(1));
    }

    private static final ByteBuffer KEY1 = ByteBufferUtil.bytes(1);
    private static final ByteBuffer KEY2 = ByteBufferUtil.bytes(2);

    private static final ByteBuffer VAL = ByteBufferUtil.bytes(10);

    private static Row.Builder newBuilder()
    {
        Row.Builder b = BTreeRow.unsortedBuilder();
        b.newRow(clusteringKey);
        return b;
    }

    // TRIGGER: row with one live regular cell and an EMPTY primary key liveness.
    @Test
    public void rowWithOneLiveRegularCellAndEmptyPrimaryKeyLiveness_returnsTrue()
    {
        long nowInSec = nowInSec();
        long ts = timestampMicro(nowInSec);

        Row.Builder b = newBuilder();
        // No addPrimaryKeyLivenessInfo => LivenessInfo.EMPTY
        b.addCell(BufferCell.live(intColumn, ts, VAL));
        Row row = b.build();

        Assert.assertTrue(row.hasLiveData(nowInSec, false));
    }

    // TRIGGER: same shape as above, but enforceStrictLiveness = true.
    @Test
    public void rowWithOneLiveRegularCellAndEmptyPrimaryKeyLivenessEnforcedStrictLiveness_returnsFalse()
    {
        long nowInSec = nowInSec();
        long ts = timestampMicro(nowInSec);

        Row.Builder b = newBuilder();
        b.addCell(BufferCell.live(intColumn, ts, VAL));
        Row row = b.build();

        Assert.assertFalse(row.hasLiveData(nowInSec, true));
    }

    // TRIGGER: row with a TTL'd-but-not-yet-expired cell, empty PK liveness.
    @Test
    public void unexpiredTtlCell_returnsTrue()
    {
        long nowInSec = nowInSec();
        long ts = timestampMicro(nowInSec);

        Row.Builder b = newBuilder();
        b.addCell(BufferCell.expiring(intColumn, ts, /*ttl*/ 3600, /*writeNow*/ nowInSec, VAL));
        Row row = b.build();

        Assert.assertTrue(row.hasLiveData(nowInSec, false));
    }

    // TRIGGER: row whose only cell is an expired TTL cell, empty PK liveness.
    @Test
    public void expiredTtlCell_returnsFalse()
    {
        long writeTime = nowInSec();
        long ts = timestampMicro(writeTime);
        long nowInSec = writeTime + 7200; // well past the TTL

        Row.Builder b = newBuilder();
        b.addCell(BufferCell.expiring(intColumn, ts, /*ttl*/ 3600, /*writeNow*/ writeTime, VAL));
        Row row = b.build();

        Assert.assertFalse(row.hasLiveData(nowInSec, false));
    }

    // TRIGGER: row whose only cell is a tombstone, empty PK liveness.
    @Test
    public void tombstoneCell_returnsFalse()
    {
        long nowInSec = nowInSec();
        long ts = timestampMicro(nowInSec);

        Row.Builder b = newBuilder();
        b.addCell(BufferCell.tombstone(intColumn, ts, nowInSec));
        Row row = b.build();

        Assert.assertFalse(row.hasLiveData(nowInSec, false));
    }

    // TRIGGER: row with a live PK liveness info (not strict).
    @Test
    public void livePK_returnsTrue()
    {
        long nowInSec = nowInSec();
        long ts = timestampMicro(nowInSec);

        Row.Builder b = newBuilder();
        b.addPrimaryKeyLivenessInfo(LivenessInfo.create(ts));
        // No cells.
        Row row = b.build();

        Assert.assertTrue(row.hasLiveData(nowInSec, false));
        Assert.assertTrue(row.hasLiveData(nowInSec, true));
    }

    // TRIGGER: row whose only data is a row deletion (no PK liveness, no cells).
    @Test
    public void rowDeletionOnly_returnsFalse()
    {
        long nowInSec = nowInSec();
        long ts = timestampMicro(nowInSec);

        Row.Builder b = newBuilder();
        b.addRowDeletion(new Row.Deletion(DeletionTime.build(ts, nowInSec), false));
        Row row = b.build();

        Assert.assertFalse(row.hasLiveData(nowInSec, false));
    }

    // TRIGGER: row with a live cell inside a complex (collection) column, empty PK liveness
    @Test
    public void liveComplexCell_returnsTrue()
    {
        long nowInSec = nowInSec();
        long ts = timestampMicro(nowInSec);

        Row.Builder b = newBuilder();
        b.addCell(BufferCell.live(mapColumn, ts, VAL, org.apache.cassandra.db.rows.CellPath.create(KEY1)));
        Row row = b.build();

        Assert.assertTrue(row.hasLiveData(nowInSec, false));
    }

    // TRIGGER: row with a tombstone alongside a live regular cell.
    @Test
    public void mixedTombstoneAndLiveCell_returnsTrue()
    {
        long nowInSec = nowInSec();
        long ts = timestampMicro(nowInSec);

        Row.Builder b = newBuilder();
        b.addCell(BufferCell.live(intColumn, ts, VAL));
        b.addCell(BufferCell.tombstone(mapColumn, ts, nowInSec, org.apache.cassandra.db.rows.CellPath.create(KEY1)));
        Row row = b.build();

        Assert.assertTrue(row.hasLiveData(nowInSec, false));
    }

    // TRIGGER: row with a tombstone alongside a live regular cell.
    @Test
    public void mixedTombstoneAndLiveComplexCells_returnsTrue()
    {
        long nowInSec = nowInSec();
        long ts = timestampMicro(nowInSec);

        Row.Builder b = newBuilder();
        b.addCell(BufferCell.live(mapColumn, ts, VAL, org.apache.cassandra.db.rows.CellPath.create(KEY1)));
        b.addCell(BufferCell.tombstone(mapColumn, ts, nowInSec, org.apache.cassandra.db.rows.CellPath.create(KEY2)));
        Row row = b.build();

        Assert.assertTrue(row.hasLiveData(nowInSec, false));
    }

    private static long nowInSec()
    {
        return 1_000_000L;
    }

    private static long timestampMicro(long nowInSec)
    {
        return nowInSec * 1_000_000L;
    }
}
