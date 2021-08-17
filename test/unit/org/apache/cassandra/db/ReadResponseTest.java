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
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReadResponseTest
{

    private final Random random = new Random();
    private TableMetadata metadata;

    @Before
    public void setup()
    {
        metadata = TableMetadata.builder("ks", "t1")
                                .addPartitionKeyColumn("p", Int32Type.instance)
                                .addRegularColumn("v", Int32Type.instance)
                                .partitioner(Murmur3Partitioner.instance)
                                .build();
    }

    @Test
    public void fromCommandWithConclusiveRepairedDigest()
    {
        ByteBuffer digest = digest();
        ReadCommand command = command(key(), metadata, digest, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata));
        assertTrue(response.isRepairedDigestConclusive());
        assertEquals(digest, response.repairedDataDigest());
        verifySerDe(response);
    }

    @Test
    public void fromCommandWithInconclusiveRepairedDigest()
    {
        ByteBuffer digest = digest();
        ReadCommand command = command(key(), metadata, digest, false);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata));
        assertFalse(response.isRepairedDigestConclusive());
        assertEquals(digest, response.repairedDataDigest());
        verifySerDe(response);
    }

    @Test
    public void fromCommandWithConclusiveEmptyRepairedDigest()
    {
        ReadCommand command = command(key(), metadata, ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata));
        assertTrue(response.isRepairedDigestConclusive());
        assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, response.repairedDataDigest());
        verifySerDe(response);
    }

    @Test
    public void fromCommandWithInconclusiveEmptyRepairedDigest()
    {
        ReadCommand command = command(key(), metadata, ByteBufferUtil.EMPTY_BYTE_BUFFER, false);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata));
        assertFalse(response.isRepairedDigestConclusive());
        assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, response.repairedDataDigest());
        verifySerDe(response);
    }

    /*
     * Digest responses should never include repaired data tracking as we only request
     * it in read repair or for range queries
     */
    @Test (expected = UnsupportedOperationException.class)
    public void digestResponseErrorsIfRepairedDataDigestRequested()
    {
        ReadCommand command = digestCommand(key(), metadata);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata));
        assertTrue(response.isDigestResponse());
        assertFalse(response.mayIncludeRepairedDigest());
        response.repairedDataDigest();
    }

    @Test (expected = UnsupportedOperationException.class)
    public void digestResponseErrorsIfIsConclusiveRequested()
    {
        ReadCommand command = digestCommand(key(), metadata);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata));
        assertTrue(response.isDigestResponse());
        assertFalse(response.mayIncludeRepairedDigest());
        response.isRepairedDigestConclusive();
    }

    @Test (expected = UnsupportedOperationException.class)
    public void digestResponseErrorsIfIteratorRequested()
    {
        ReadCommand command = digestCommand(key(), metadata);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata));
        assertTrue(response.isDigestResponse());
        assertFalse(response.mayIncludeRepairedDigest());
        response.makeIterator(command);
    }

    @Test
    public void makeDigestDoesntConsiderRepairedDataInfo()
    {
        // It shouldn't be possible to get false positive DigestMismatchExceptions based
        // on differing repaired data tracking info because it isn't requested on initial
        // requests, only following a digest mismatch. Having a test doesn't hurt though
        int key = key();
        ByteBuffer digest1 = digest();
        ReadCommand command1 = command(key, metadata, digest1, true);
        ReadResponse response1 = command1.createResponse(EmptyIterators.unfilteredPartition(metadata));

        ByteBuffer digest2 = digest();
        ReadCommand command2 = command(key, metadata, digest2, false);
        ReadResponse response2 = command1.createResponse(EmptyIterators.unfilteredPartition(metadata));

        assertEquals(response1.digest(command1), response2.digest(command2));
    }

    private void verifySerDe(ReadResponse response) {
        // check that roundtripping through ReadResponse.serializer behaves as expected.
        // ReadResponses from pre-4.0 nodes will never contain repaired data digest
        // or pending session info, but we run all messages through both pre/post 4.0
        // serde to check that the defaults are correctly applied
        roundTripSerialization(response, MessagingService.current_version);
        roundTripSerialization(response, MessagingService.VERSION_30);

    }

    private void roundTripSerialization(ReadResponse response, int version)
    {
        try
        {
            DataOutputBuffer out = new DataOutputBuffer();
            ReadResponse.serializer.serialize(response, out, version);

            DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
            ReadResponse deser = ReadResponse.serializer.deserialize(in, version);
            if (version < MessagingService.VERSION_40)
            {
                assertFalse(deser.mayIncludeRepairedDigest());
                // even though that means they should never be used, verify that the default values are present
                assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, deser.repairedDataDigest());
                assertTrue(deser.isRepairedDigestConclusive());
            }
            else
            {
                assertTrue(deser.mayIncludeRepairedDigest());
                assertEquals(response.repairedDataDigest(), deser.repairedDataDigest());
                assertEquals(response.isRepairedDigestConclusive(), deser.isRepairedDigestConclusive());
            }
        }
        catch (IOException e)
        {
            fail("Caught unexpected IOException during SerDe: " + e.getMessage());
        }
    }


    private int key()
    {
        return random.nextInt();
    }

    private ByteBuffer digest()
    {
        byte[] bytes = new byte[4];
        random.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }

    private ReadCommand digestCommand(int key, TableMetadata metadata)
    {
        return new StubReadCommand(key, metadata, true, ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
    }

    private ReadCommand command(int key, TableMetadata metadata, ByteBuffer repairedDigest, boolean conclusive)
    {
        return new StubReadCommand(key, metadata, false, repairedDigest, conclusive);
    }

    private static class StubReadCommand extends SinglePartitionReadCommand
    {

        private final ByteBuffer repairedDigest;
        private final boolean conclusive;

        StubReadCommand(int key, TableMetadata metadata,
                        boolean isDigest,
                        final ByteBuffer repairedDigest,
                        final boolean conclusive)
        {
            super(isDigest,
                  0,
                  false,
                  metadata,
                  FBUtilities.nowInSeconds(),
                  ColumnFilter.all(metadata),
                  RowFilter.NONE,
                  DataLimits.NONE,
                  metadata.partitioner.decorateKey(ByteBufferUtil.bytes(key)),
                  null,
                  null);
            this.repairedDigest = repairedDigest;
            this.conclusive = conclusive;
        }

        @Override
        public ByteBuffer getRepairedDataDigest()
        {
            return repairedDigest;
        }

        @Override
        public boolean isRepairedDataDigestConclusive()
        {
            return conclusive;
        }

        public UnfilteredPartitionIterator executeLocally(ReadExecutionController controller)
        {
            return EmptyIterators.unfilteredPartition(this.metadata());
        }
    }
}
