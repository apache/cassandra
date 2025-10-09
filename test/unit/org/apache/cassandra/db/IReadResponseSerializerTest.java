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

package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.tracked.TrackedDataResponse;
import org.apache.cassandra.service.reads.tracked.TrackedRead;
import org.apache.cassandra.service.reads.tracked.TrackedSummaryResponse;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Comprehensive tests for IReadResponse serializer covering all response kinds
 * (UNTRACKED, TRACKED_DATA, TRACKED_SUMMARY, NULL) and all supported versions.
 */
public class IReadResponseSerializerTest
{
    private TableMetadata metadata;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataTestHelper.setInstanceForTest();
        // Register the local node so that ClusterMetadata.current().myNodeId() returns a valid NodeId.
        // This is needed for TrackedRead.Id's static initializer.
        ClusterMetadataTestHelper.register(FBUtilities.getBroadcastAddressAndPort());
    }

    @Before
    public void setup()
    {
        metadata = TableMetadata.builder("ks", "t1")
                                .offline()
                                .addPartitionKeyColumn("p", Int32Type.instance)
                                .addRegularColumn("v", Int32Type.instance)
                                .partitioner(Murmur3Partitioner.instance)
                                .build();
    }

    // ==================== UNTRACKED (ReadResponse) Tests ====================

    @Test
    public void testUntrackedResponseAllVersions() throws IOException
    {
        ReadResponse response = createReadResponse();

        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
        {
            testRoundTrip(response, version.value, ReadKind.UNTRACKED);
        }
    }

    @Test
    public void testUntrackedResponseSerializedSizeMatchesActual() throws IOException
    {
        ReadResponse response = createReadResponse();

        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
        {
            DataOutputBuffer out = new DataOutputBuffer();
            IReadResponse.serializer.serialize(response, out, version.value);

            long expectedSize = IReadResponse.serializer.serializedSize(response, version.value);
            assertEquals("Serialized size should match actual bytes written for version " + version,
                         expectedSize, out.buffer().remaining());
        }
    }

    // ==================== TRACKED_DATA Tests ====================

    @Test
    public void testTrackedDataResponseVersion52Plus() throws IOException
    {
        TrackedDataResponse response = createTrackedDataResponse();

        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
        {
            if (version.value >= MessagingService.VERSION_52)
            {
                testRoundTrip(response, version.value, ReadKind.TRACKED_DATA);
            }
        }
    }

    @Test
    public void testTrackedDataResponseSerializedSizeMatchesActual() throws IOException
    {
        TrackedDataResponse response = createTrackedDataResponse();

        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
        {
            if (version.value >= MessagingService.VERSION_52)
            {
                DataOutputBuffer out = new DataOutputBuffer();
                IReadResponse.serializer.serialize(response, out, version.value);

                long expectedSize = IReadResponse.serializer.serializedSize(response, version.value);
                assertEquals("Serialized size should match actual bytes written for version " + version,
                             expectedSize, out.buffer().remaining());
            }
        }
    }

    @Test
    public void testTrackedDataResponsePreVersion52Rejected()
    {
        TrackedDataResponse response = createTrackedDataResponse();

        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
        {
            if (version.value < MessagingService.VERSION_52)
            {
                try
                {
                    DataOutputBuffer out = new DataOutputBuffer();
                    IReadResponse.serializer.serialize(response, out, version.value);
                    fail("Should have thrown for TRACKED_DATA on pre-VERSION_52: " + version);
                }
                catch (IllegalArgumentException | IOException e)
                {
                    // Expected - pre-VERSION_52 should not support tracked responses
                }
            }
        }
    }

    // ==================== TRACKED_SUMMARY Tests ====================

    @Test
    public void testTrackedSummaryResponseVersion52Plus() throws IOException
    {
        TrackedSummaryResponse response = createTrackedSummaryResponse();

        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
        {
            if (version.value >= MessagingService.VERSION_52)
            {
                testRoundTrip(response, version.value, ReadKind.TRACKED_SUMMARY);
            }
        }
    }

    @Test
    public void testTrackedSummaryResponseSerializedSizeMatchesActual() throws IOException
    {
        TrackedSummaryResponse response = createTrackedSummaryResponse();

        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
        {
            if (version.value >= MessagingService.VERSION_52)
            {
                DataOutputBuffer out = new DataOutputBuffer();
                IReadResponse.serializer.serialize(response, out, version.value);

                long expectedSize = IReadResponse.serializer.serializedSize(response, version.value);
                assertEquals("Serialized size should match actual bytes written for version " + version,
                             expectedSize, out.buffer().remaining());
            }
        }
    }

    @Test
    public void testTrackedSummaryResponsePreVersion52Rejected()
    {
        TrackedSummaryResponse response = createTrackedSummaryResponse();

        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
        {
            if (version.value < MessagingService.VERSION_52)
            {
                try
                {
                    DataOutputBuffer out = new DataOutputBuffer();
                    IReadResponse.serializer.serialize(response, out, version.value);
                    fail("Should have thrown for TRACKED_SUMMARY on pre-VERSION_52: " + version);
                }
                catch (IllegalArgumentException | IOException e)
                {
                    // Expected - pre-VERSION_52 should not support tracked responses
                }
            }
        }
    }

    // ==================== Kind Serializer Tests ====================

    @Test
    public void testKindSerializerRoundTrip() throws IOException
    {
        for (ReadKind kind : ReadKind.values())
        {
            for (MessagingService.Version version : MessagingService.Version.supportedVersions())
            {
                DataOutputBuffer out = new DataOutputBuffer();
                ReadKind.serializer.serialize(kind, out);

                long expectedSize = ReadKind.serializer.serializedSize(kind);
                assertEquals("Kind serializedSize should match actual bytes for " + kind + " version " + version,
                             expectedSize, out.buffer().remaining());

                DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
                ReadKind deserialized = ReadKind.serializer.deserialize(in);

                assertEquals("Kind should round-trip correctly for " + kind + " version " + version,
                             kind, deserialized);
            }
        }
    }

    @Test
    public void testKindIsTracked()
    {
        assertEquals("UNTRACKED.isTracked() should be false", false, ReadKind.UNTRACKED.isTracked());
        assertEquals("TRACKED_DATA.isTracked() should be true", true, ReadKind.TRACKED_DATA.isTracked());
        assertEquals("TRACKED_SUMMARY.isTracked() should be true", true, ReadKind.TRACKED_SUMMARY.isTracked());
    }

    // ==================== Helper Methods ====================

    private void testRoundTrip(IReadResponse response, int version, ReadKind expectedKind) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        IReadResponse.serializer.serialize(response, out, version);

        DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
        IReadResponse deserialized = IReadResponse.serializer.deserialize(in, version);

        assertNotNull("Deserialized response should not be null for version " + version, deserialized);
        assertEquals("Response kind should match for version " + version, expectedKind, deserialized.kind());
    }

    private ReadResponse createReadResponse()
    {
        ReadCommand command = createReadCommand();
        return command.createResponse(EmptyIterators.unfilteredPartition(metadata),
                                       new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true));
    }

    private TrackedDataResponse createTrackedDataResponse()
    {
        // Create a simple TrackedDataResponse with test data
        ByteBuffer testData = ByteBuffer.wrap(new byte[] { 1, 2, 3, 4, 5 });
        return new TrackedDataResponse(MessagingService.current_version, testData);
    }

    private TrackedSummaryResponse createTrackedSummaryResponse()
    {
        // Create a TrackedRead.Id with test values using the public constructor
        TrackedRead.Id readId = new TrackedRead.Id(1, 12345L);
        // Create an empty MutationSummary for testing
        MutationSummary summary = new MutationSummary.Builder(metadata.id).build();
        int dataNode = 1;
        int[] summaryNodes = new int[] { 2, 3 };
        return new TrackedSummaryResponse(readId, summary, dataNode, summaryNodes);
    }

    private ReadCommand createReadCommand()
    {
        return new StubReadCommand(1, metadata, false);
    }

    private static class StubRepairedDataInfo extends RepairedDataInfo
    {
        private final ByteBuffer repairedDigest;
        private final boolean conclusive;

        public StubRepairedDataInfo(ByteBuffer repairedDigest, boolean conclusive)
        {
            super(null);
            this.repairedDigest = repairedDigest;
            this.conclusive = conclusive;
        }

        @Override
        public ByteBuffer getDigest()
        {
            return repairedDigest;
        }

        @Override
        public boolean isConclusive()
        {
            return conclusive;
        }
    }

    private static class StubReadCommand extends SinglePartitionReadCommand
    {
        StubReadCommand(int key, TableMetadata metadata, boolean isDigest)
        {
            super(metadata.epoch,
                  isDigest,
                  0,
                  PotentialTxnConflicts.DISALLOW,
                  metadata,
                  FBUtilities.nowInSeconds(),
                  ColumnFilter.all(metadata),
                  RowFilter.none(),
                  DataLimits.NONE,
                  metadata.partitioner.decorateKey(ByteBufferUtil.bytes(key)),
                  null,
                  null,
                  false,
                  null);
        }

        @Override
        public boolean selectsFullPartition()
        {
            return true;
        }

        public UnfilteredPartitionIterator executeLocally(ReadExecutionController controller)
        {
            return EmptyIterators.unfilteredPartition(this.metadata());
        }
    }
}
