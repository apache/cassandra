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
package org.apache.cassandra.service.paxos;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.zip.CRC32C;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTree;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.apache.cassandra.service.paxos.PaxosPartitionUpdateBlob.EMPTY_UPDATE_MESSAGING_VERSION;
import static org.apache.cassandra.service.paxos.PaxosPartitionUpdateBlob.VERSION_ABSENT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaxosPartitionUpdateBlobTest extends CQLTester
{
    private static final int[] VERSIONS =
        MessagingService.Version.supportedVersions().stream().mapToInt(version -> version.value).toArray();
    private static final Ballot BALLOT = Ballot.atUnixMicrosWithLsb(1_700_000_000_000_000L, 42L, Ballot.Flag.LOCAL);

    private TableMetadata metadata;
    private UUID cfId;
    private ByteBuffer rowKey;
    private ListAppender<ILoggingEvent> appender;
    private ch.qos.logback.classic.Logger rootLogger;

    @Before
    public void setUpTable()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        metadata = currentTableMetadata();
        cfId = metadata.id.asUUID();
        rowKey = ByteBufferUtil.bytes(1);

        appender = new ListAppender<>();
        appender.start();
        rootLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(appender);
    }

    @After
    public void detachAppender()
    {
        rootLogger.detachAppender(appender);
    }

    private PartitionUpdate update(int value)
    {
        return UpdateBuilder.create(metadata, 1).newRow().add("v", value).build();
    }

    private PartitionUpdate emptyUpdate()
    {
        return PartitionUpdate.emptyUpdate(metadata, metadata.partitioner.decorateKey(rowKey));
    }

    private static void assertSameUpdate(PartitionUpdate expected, PartitionUpdate actual, int version)
    {
        Assert.assertEquals(PartitionUpdate.toBytes(expected, version), PartitionUpdate.toBytes(actual, version));
    }

    private List<ILoggingEvent> eventsAtOrAbove(Level level)
    {
        List<ILoggingEvent> result = new ArrayList<>();
        for (ILoggingEvent event : new ArrayList<>(appender.list))
            if (event.getLevel().isGreaterOrEqual(level))
                result.add(event);
        return result;
    }

    @Test
    public void blobRoundTripsAtEveryVersionIgnoringTheColumnVersion()
    {
        PartitionUpdate update = update(7);
        for (int version : VERSIONS)
        {
            ByteBuffer blob = PaxosPartitionUpdateBlob.encode(update, version, rowKey, cfId, BALLOT);
            for (int columnVersion : new int[]{ VERSION_ABSENT, MessagingService.VERSION_40, MessagingService.current_version })
            {
                PaxosPartitionUpdateBlob.Decoded decoded = PaxosPartitionUpdateBlob.decode(blob, columnVersion, rowKey, cfId);
                assertSameUpdate(update, decoded.update, version);
                Assert.assertEquals(BALLOT, decoded.ballot);
            }
        }
        Assert.assertTrue(eventsAtOrAbove(Level.WARN).isEmpty());
    }

    @Test
    public void bytesMatchTheDocumentedLayout()
    {
        PartitionUpdate update = update(7);
        ByteBuffer blob = PaxosPartitionUpdateBlob.encode(update, MessagingService.current_version, rowKey, cfId, BALLOT);
        ByteBuffer legacy = PartitionUpdate.toBytes(update, MessagingService.current_version);
        int updateLength = legacy.remaining();
        int totalLength = blob.remaining();
        int start = blob.position();

        Assert.assertEquals(legacy, blob.duplicate().limit(start + updateLength));
        Assert.assertEquals(BALLOT.msb(), blob.getLong(start + updateLength));
        Assert.assertEquals(BALLOT.lsb(), blob.getLong(start + updateLength + 8));
        Assert.assertEquals(MessagingService.current_version, blob.get(start + updateLength + 16));
        Assert.assertEquals((2 << 28) | (totalLength >>> 4), blob.getInt(start + totalLength - 8));

        // The salt is the row key, then the table id as two little-endian longs; the ballot is not in it.
        ByteBuffer tableId = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        tableId.putLong(cfId.getMostSignificantBits()).putLong(cfId.getLeastSignificantBits()).flip();
        CRC32C crc = new CRC32C();
        crc.update(rowKey.duplicate());
        crc.update(tableId);
        crc.update(blob.duplicate().limit(start + totalLength - 4));
        Assert.assertEquals((int) crc.getValue(), blob.getInt(start + totalLength - 4));
    }

    @Test
    public void footerSizeFollowsTheBytesWrittenAndTheBlobDecodesThroughTheFooter()
    {
        TableMetadata nullEpoch = metadata.unbuild().epoch(null).build();
        Row nullEpochRow = BTreeRow.create(Clustering.EMPTY, LivenessInfo.EMPTY, Row.Deletion.LIVE,
                                           BTree.singleton(new BufferCell(nullEpoch.getColumn(ByteBufferUtil.bytes("v")), 1L, Cell.NO_TTL,
                                                                          Cell.NO_DELETION_TIME, ByteBufferUtil.bytes(7), null)));
        PartitionUpdate[] updates = { update(0), update(7), update(Integer.MAX_VALUE), emptyUpdate(),
                                      PartitionUpdate.singleRowUpdate(nullEpoch, metadata.partitioner.decorateKey(rowKey), nullEpochRow) };

        for (PartitionUpdate update : updates)
        {
            for (int version : VERSIONS)
            {
                for (boolean permitEmpty : new boolean[]{ true, false })
                {
                    ByteBuffer blob = PaxosPartitionUpdateBlob.encode(update, version, rowKey, cfId, BALLOT, permitEmpty);
                    int start = blob.position();
                    int total = blob.remaining();
                    int footerWord = blob.getInt(start + total - 8);

                    Assert.assertEquals(2, footerWord >>> 28);
                    Assert.assertEquals(total >>> 4, footerWord & 0x0FFFFFFF);

                    // A ballot comes back only when the footer path accepted the size and the CRC;
                    // the raw fallback returns a null ballot.
                    PaxosPartitionUpdateBlob.Decoded decoded = PaxosPartitionUpdateBlob.decode(blob, VERSION_ABSENT, rowKey, cfId);
                    Assert.assertEquals(BALLOT, decoded.ballot);
                }
            }
        }
        Assert.assertTrue(eventsAtOrAbove(Level.WARN).isEmpty());
    }

    @Test
    public void emptySentinelDecodesToAnEmptyUpdate()
    {
        ByteBuffer blob = PaxosPartitionUpdateBlob.encode(emptyUpdate(), MessagingService.current_version, rowKey, cfId, BALLOT, true);
        PaxosPartitionUpdateBlob.Decoded decoded = PaxosPartitionUpdateBlob.decode(blob, VERSION_ABSENT, rowKey, cfId);
        Assert.assertTrue(decoded.update.isEmpty());
        Assert.assertEquals(rowKey, decoded.update.partitionKey().getKey());
        Assert.assertEquals(BALLOT, decoded.ballot);
        Assert.assertTrue(PaxosPartitionUpdateBlob.updateIsEmpty(blob, VERSION_ABSENT, rowKey, cfId));
        Assert.assertEquals(EMPTY_UPDATE_MESSAGING_VERSION, blob.get(blob.limit() - 9));
    }

    @Test
    public void anEmptyUpdateIsWrittenWithoutBytesAtTheCurrentVersionAndInFullBelowIt()
    {
        ByteBuffer newest = PaxosPartitionUpdateBlob.encode(emptyUpdate(), MessagingService.current_version, rowKey, cfId, BALLOT);
        Assert.assertEquals(EMPTY_UPDATE_MESSAGING_VERSION, newest.get(newest.limit() - 9));
        Assert.assertEquals(16 + 9, newest.remaining());

        for (int version : VERSIONS)
        {
            if (version == MessagingService.current_version)
                continue;
            ByteBuffer full = PaxosPartitionUpdateBlob.encode(emptyUpdate(), version, rowKey, cfId, BALLOT);
            Assert.assertEquals(version, full.get(full.limit() - 9));
            Assert.assertTrue(full.remaining() > 16 + 9);
            PaxosPartitionUpdateBlob.Decoded decoded = PaxosPartitionUpdateBlob.decode(full, VERSION_ABSENT, rowKey, cfId);
            Assert.assertTrue(decoded.update.isEmpty());
            Assert.assertEquals(BALLOT, decoded.ballot);
        }
    }

    @Test
    public void legacyBlobDecodesAtItsColumnVersionWithoutABallot()
    {
        PartitionUpdate update = update(7);
        for (int version : VERSIONS)
        {
            ByteBuffer legacy = PartitionUpdate.toBytes(update, version);
            PaxosPartitionUpdateBlob.Decoded decoded = PaxosPartitionUpdateBlob.decode(legacy, version, rowKey, cfId);
            assertSameUpdate(update, decoded.update, version);
            Assert.assertNull(decoded.ballot);
        }
    }

    @Test
    public void legacyBlobDecodesWhenTheColumnVersionIsNewerOrAbsent()
    {
        PartitionUpdate update = update(7);
        ByteBuffer oldest = PartitionUpdate.toBytes(update, MessagingService.VERSION_40);

        for (int columnVersion : VERSIONS)
            if (columnVersion > MessagingService.VERSION_40)
                assertSameUpdate(update, PaxosPartitionUpdateBlob.decode(oldest, columnVersion, rowKey, cfId).update, MessagingService.VERSION_40);
        assertSameUpdate(update, PaxosPartitionUpdateBlob.decode(oldest, VERSION_ABSENT, rowKey, cfId).update, MessagingService.VERSION_40);
        Assert.assertTrue(eventsAtOrAbove(Level.WARN).isEmpty());
    }

    @Test
    public void aLegacyBlobIsNeverReadAboveItsColumnVersion()
    {
        ByteBuffer newest = PartitionUpdate.toBytes(update(7), MessagingService.current_version);

        assertThatThrownBy(() -> PaxosPartitionUpdateBlob.decode(newest, MessagingService.VERSION_40, rowKey, cfId)).isInstanceOf(MarshalException.class);
        assertThatThrownBy(() -> PaxosPartitionUpdateBlob.decode(newest, VERSION_ABSENT, rowKey, cfId)).isInstanceOf(MarshalException.class);
    }

    @Test
    public void aFlippedByteInABlobFailsTheCrcAndLogsAtError()
    {
        ByteBuffer blob = PaxosPartitionUpdateBlob.encode(update(7), MessagingService.current_version, rowKey, cfId, BALLOT);
        byte[] bytes = ByteBufferUtil.getArray(blob);
        bytes[2] ^= (byte) 0xff;

        assertThatThrownBy(() -> PaxosPartitionUpdateBlob.decode(ByteBuffer.wrap(bytes), VERSION_ABSENT, rowKey, cfId))
        .isInstanceOf(MarshalException.class);

        List<ILoggingEvent> errors = eventsAtOrAbove(Level.ERROR);
        Assert.assertEquals(1, errors.size());
        Assert.assertTrue(errors.get(0).getFormattedMessage().contains(ByteBufferUtil.bytesToHex(rowKey)));
        Assert.assertTrue(errors.get(0).getFormattedMessage().contains(cfId.toString()));
        Assert.assertTrue(errors.get(0).getFormattedMessage().contains("footer CRC-32C mismatch"));
    }

    @Test
    public void serializedSizeMatchesSerializeWhenTheTableEpochIsNull() throws Exception
    {
        TableMetadata nullEpoch = metadata.unbuild().epoch(null).build();
        Assert.assertNull(nullEpoch.epoch);
        // UpdateBuilder resolves the current table metadata, so build the update directly.
        Row row = BTreeRow.create(Clustering.EMPTY, LivenessInfo.EMPTY, Row.Deletion.LIVE,
                                  BTree.singleton(new BufferCell(nullEpoch.getColumn(ByteBufferUtil.bytes("v")), 1L, Cell.NO_TTL,
                                                                 Cell.NO_DELETION_TIME, ByteBufferUtil.bytes(7), null)));
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(nullEpoch, metadata.partitioner.decorateKey(rowKey), row);
        Assert.assertNull(update.metadata().epoch);
        Assert.assertTrue(MessagingService.current_version >= MessagingService.VERSION_60);

        for (int version : new int[]{ MessagingService.VERSION_60, MessagingService.current_version })
        {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                PartitionUpdate.serializer.serialize(update, out, version);
                Assert.assertEquals(out.getLength(), PartitionUpdate.serializer.serializedSize(update, version));
            }
        }
    }

    @Test
    public void aDamagedBodyThatStillParsesIsRejected()
    {
        int version = MessagingService.current_version;
        ByteBuffer blob = PaxosPartitionUpdateBlob.encode(update(7), version, rowKey, cfId, BALLOT);
        int updateLength = PartitionUpdate.toBytes(update(7), version).remaining();
        byte[] bytes = ByteBufferUtil.getArray(blob);
        // The update ends with the four value bytes of v and one END_OF_PARTITION byte.
        bytes[updateLength - 2] ^= (byte) 0xff;

        assertThatThrownBy(() -> PaxosPartitionUpdateBlob.decode(ByteBuffer.wrap(bytes), version, rowKey, cfId))
        .isInstanceOf(MarshalException.class);
    }

    @Test
    public void aLegacyBlobThatLooksLikeAFooterStillDecodesAsLegacy()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, b blob)");
        TableMetadata blobTable = currentTableMetadata();
        UUID blobTableId = blobTable.id.asUUID();
        int valueLength = 32;
        byte[] value = new byte[valueLength];
        Arrays.fill(value, (byte) 0x55);
        PartitionUpdate update = UpdateBuilder.create(blobTable, 1).newRow().add("b", ByteBuffer.wrap(value)).build();

        byte[] bytes = ByteBufferUtil.getArray(PartitionUpdate.toBytes(update, MessagingService.VERSION_40));
        int length = bytes.length;
        // The update ends with the cell value and one END_OF_PARTITION byte. The footer window
        // [length - 9, length - 1) lies inside the value.
        for (int i = length - 1 - valueLength; i < length - 1; i++)
            Assert.assertEquals((byte) 0x55, bytes[i]);

        // Patch the footer word. The stored CRC is the last four bytes: three value bytes and the
        // END_OF_PARTITION byte. It does not match the computed CRC except by a 2^-32 accident.
        ByteBuffer blob = ByteBuffer.wrap(bytes);
        blob.putInt(length - 8, (2 << 28) | (length >>> 4));
        Assert.assertEquals(2, (blob.getInt(length - 8) >>> 28) & 0xF);
        Assert.assertEquals(length >>> 4, blob.getInt(length - 8) & 0x0FFFFFFF);
        Assert.assertEquals(length, blob.remaining());

        PaxosPartitionUpdateBlob.Decoded decoded =
            PaxosPartitionUpdateBlob.decode(blob, MessagingService.VERSION_40, rowKey, blobTableId);
        Assert.assertNull(decoded.ballot);
        Assert.assertEquals(blobTableId, decoded.update.metadata().id.asUUID());
        Assert.assertFalse(PaxosPartitionUpdateBlob.updateIsEmpty(blob, MessagingService.VERSION_40, rowKey, blobTableId));

        // Both decodes hit the CRC mismatch; the rate limit leaves one WARN line.
        List<ILoggingEvent> warnings = eventsAtOrAbove(Level.WARN);
        Assert.assertEquals(1, warnings.size());
        Assert.assertEquals(Level.WARN, warnings.get(0).getLevel());
        Assert.assertTrue(warnings.get(0).getFormattedMessage().contains(blobTableId.toString()));
        Assert.assertTrue(warnings.get(0).getFormattedMessage().contains("CRC-32C mismatch"));
    }

    @Test
    public void aRawBlobWithNoFooterShapeDecodesWithoutAWarning()
    {
        ByteBuffer raw = PartitionUpdate.toBytes(update(7), MessagingService.VERSION_40);
        PaxosPartitionUpdateBlob.Decoded decoded = PaxosPartitionUpdateBlob.decode(raw, MessagingService.VERSION_40, rowKey, cfId);
        Assert.assertNull(decoded.ballot);
        Assert.assertTrue(eventsAtOrAbove(Level.WARN).isEmpty());
    }

    @Test
    public void aValidBlobDecodesWithoutAWarning()
    {
        ByteBuffer blob = PaxosPartitionUpdateBlob.encode(update(7), MessagingService.current_version, rowKey, cfId, BALLOT);
        Assert.assertEquals(BALLOT, PaxosPartitionUpdateBlob.decode(blob, VERSION_ABSENT, rowKey, cfId).ballot);
        Assert.assertTrue(eventsAtOrAbove(Level.WARN).isEmpty());
    }

    @Test
    public void aBlobForAnotherKeyIsRejected()
    {
        PartitionUpdate update = update(7);
        ByteBuffer otherKey = ByteBufferUtil.bytes(2);

        ByteBuffer blob = PaxosPartitionUpdateBlob.encode(update, MessagingService.current_version, rowKey, cfId, BALLOT);
        assertThatThrownBy(() -> PaxosPartitionUpdateBlob.decode(blob, VERSION_ABSENT, otherKey, cfId)).isInstanceOf(MarshalException.class);

        ByteBuffer legacy = PartitionUpdate.toBytes(update, MessagingService.current_version);
        assertThatThrownBy(() -> PaxosPartitionUpdateBlob.decode(legacy, MessagingService.current_version, otherKey, cfId)).isInstanceOf(MarshalException.class);
    }

    @Test
    public void aBlobForAnotherTableIsRejected()
    {
        PartitionUpdate update = update(7);
        UUID otherTable = UUID.randomUUID();

        ByteBuffer blob = PaxosPartitionUpdateBlob.encode(update, MessagingService.current_version, rowKey, cfId, BALLOT);
        assertThatThrownBy(() -> PaxosPartitionUpdateBlob.decode(blob, VERSION_ABSENT, rowKey, otherTable)).isInstanceOf(MarshalException.class);

        ByteBuffer legacy = PartitionUpdate.toBytes(update, MessagingService.current_version);
        assertThatThrownBy(() -> PaxosPartitionUpdateBlob.decode(legacy, MessagingService.current_version, rowKey, otherTable)).isInstanceOf(MarshalException.class);
    }

    @Test
    public void updateIsEmptyAgreesWithDecodeForCurrentAndLegacyBlobs()
    {
        for (int version : VERSIONS)
        {
            for (PartitionUpdate update : new PartitionUpdate[]{ emptyUpdate(), update(7) })
            {
                for (boolean permitEmptyEncoding : new boolean[]{ true, false })
                {
                    ByteBuffer blob = PaxosPartitionUpdateBlob.encode(update, version, rowKey, cfId, BALLOT, permitEmptyEncoding);
                    Assert.assertEquals(update.isEmpty(), PaxosPartitionUpdateBlob.updateIsEmpty(blob, VERSION_ABSENT, rowKey, cfId));
                }

                ByteBuffer legacy = PartitionUpdate.toBytes(update, version);
                Assert.assertEquals(update.isEmpty(), PaxosPartitionUpdateBlob.updateIsEmpty(legacy, version, rowKey, cfId));
                if (version == MessagingService.VERSION_40)
                    Assert.assertEquals(update.isEmpty(), PaxosPartitionUpdateBlob.updateIsEmpty(legacy, VERSION_ABSENT, rowKey, cfId));
            }
        }
    }

    @Test
    public void updateIsEmptyReadsALegacyBlobUnderANewerColumnVersionButNeverAnOlderOne()
    {
        ByteBuffer oldest = PartitionUpdate.toBytes(update(7), MessagingService.VERSION_40);
        Assert.assertFalse(PaxosPartitionUpdateBlob.updateIsEmpty(oldest, MessagingService.current_version, rowKey, cfId));

        ByteBuffer newest = PartitionUpdate.toBytes(emptyUpdate(), MessagingService.current_version);
        assertThatThrownBy(() -> PaxosPartitionUpdateBlob.updateIsEmpty(newest, MessagingService.VERSION_40, rowKey, cfId)).isInstanceOf(MarshalException.class);
    }

    @Test
    public void updateIsEmptyTreatsAnAbsentOrZeroLengthCellAsEmpty()
    {
        Assert.assertTrue(PaxosPartitionUpdateBlob.updateIsEmpty(null, VERSION_ABSENT, rowKey, cfId));
        Assert.assertTrue(PaxosPartitionUpdateBlob.updateIsEmpty(ByteBufferUtil.EMPTY_BYTE_BUFFER, VERSION_ABSENT, rowKey, cfId));
    }
}
