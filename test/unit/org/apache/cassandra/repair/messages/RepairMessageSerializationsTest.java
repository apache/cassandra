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

package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.SyncNodePair;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.SessionSummary;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.utils.MerkleTrees;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class RepairMessageSerializationsTest
{
    private static final int PROTOCOL_VERSION = MessagingService.current_version;
    private static final int GC_BEFORE = 1000000;

    private static IPartitioner originalPartitioner;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        originalPartitioner = StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @AfterClass
    public static void after()
    {
        DatabaseDescriptor.setPartitionerUnsafe(originalPartitioner);
    }

    @Test
    public void validationRequestMessage() throws IOException
    {
        RepairJobDesc jobDesc = buildRepairJobDesc();
        ValidationRequest msg = new ValidationRequest(jobDesc, GC_BEFORE);
        ValidationRequest deserialized = serializeRoundTrip(msg, ValidationRequest.serializer);
        Assert.assertEquals(jobDesc, deserialized.desc);
    }

    private RepairJobDesc buildRepairJobDesc()
    {
        List<Range<Token>> tokenRanges = buildTokenRanges();
        return new RepairJobDesc(nextTimeUUID(), nextTimeUUID(), "serializationsTestKeyspace", "repairMessages", tokenRanges);
    }

    private List<Range<Token>> buildTokenRanges()
    {
        List<Range<Token>> tokenRanges = new ArrayList<>(4);
        tokenRanges.add(new Range<>(new LongToken(1000), new LongToken(1001)));
        tokenRanges.add(new Range<>(new LongToken(2000), new LongToken(2001)));
        tokenRanges.add(new Range<>(new LongToken(3000), new LongToken(3001)));
        tokenRanges.add(new Range<>(new LongToken(4000), new LongToken(4001)));
        return tokenRanges;
    }

    private <T extends RepairMessage> T serializeRoundTrip(T msg, IVersionedSerializer<T> serializer) throws IOException
    {
        long size = serializer.serializedSize(msg, PROTOCOL_VERSION);

        ByteBuffer buf = ByteBuffer.allocate((int)size);
        DataOutputPlus out = new DataOutputBufferFixed(buf);
        serializer.serialize(msg, out, PROTOCOL_VERSION);
        Assert.assertEquals(size, buf.position());

        buf.flip();
        DataInputPlus in = new DataInputBuffer(buf, false);
        T deserialized = serializer.deserialize(in, PROTOCOL_VERSION);
        Assert.assertEquals(msg, deserialized);
        Assert.assertEquals(msg.hashCode(), deserialized.hashCode());
        return deserialized;
    }

    @Test
    public void validationCompleteMessage_NoMerkleTree() throws IOException
    {
        ValidationResponse deserialized = validationCompleteMessage(null);
        Assert.assertNull(deserialized.trees);
    }

    @Test
    public void validationCompleteMessage_WithMerkleTree() throws IOException
    {
        MerkleTrees trees = new MerkleTrees(Murmur3Partitioner.instance);
        trees.addMerkleTree(256, new Range<>(new LongToken(1000), new LongToken(1001)));
        ValidationResponse deserialized = validationCompleteMessage(trees);

        // a simple check to make sure we got some merkle trees back.
        Assert.assertEquals(trees.size(), deserialized.trees.size());
    }

    private ValidationResponse validationCompleteMessage(MerkleTrees trees) throws IOException
    {
        RepairJobDesc jobDesc = buildRepairJobDesc();
        ValidationResponse msg = trees == null ?
                                 new ValidationResponse(jobDesc) :
                                 new ValidationResponse(jobDesc, trees);
        ValidationResponse deserialized = serializeRoundTrip(msg, ValidationResponse.serializer);
        return deserialized;
    }

    @Test
    public void syncRequestMessage() throws IOException
    {
        InetAddressAndPort initiator = InetAddressAndPort.getByName("127.0.0.1");
        InetAddressAndPort src = InetAddressAndPort.getByName("127.0.0.2");
        InetAddressAndPort dst = InetAddressAndPort.getByName("127.0.0.3");

        SyncRequest msg = new SyncRequest(buildRepairJobDesc(), initiator, src, dst, buildTokenRanges(), PreviewKind.NONE, false);
        serializeRoundTrip(msg, SyncRequest.serializer);
    }

    @Test
    public void syncCompleteMessage() throws IOException
    {
        InetAddressAndPort src = InetAddressAndPort.getByName("127.0.0.2");
        InetAddressAndPort dst = InetAddressAndPort.getByName("127.0.0.3");
        List<SessionSummary> summaries = new ArrayList<>();
        summaries.add(new SessionSummary(src, dst,
                                         Lists.newArrayList(new StreamSummary(TableId.fromUUID(UUID.randomUUID()), 5, 100)),
                                         Lists.newArrayList(new StreamSummary(TableId.fromUUID(UUID.randomUUID()), 500, 10))
        ));
        SyncResponse msg = new SyncResponse(buildRepairJobDesc(), new SyncNodePair(src, dst), true, summaries);
        serializeRoundTrip(msg, SyncResponse.serializer);
    }

    @Test
    public void prepareMessage() throws IOException
    {
        PrepareMessage msg = new PrepareMessage(nextTimeUUID(), new ArrayList<TableId>() {{add(TableId.generate());}},
                                                buildTokenRanges(), true, 100000L, false,
                                                PreviewKind.NONE);
        serializeRoundTrip(msg, PrepareMessage.serializer);
    }

    @Test
    public void snapshotMessage() throws IOException
    {
        SnapshotMessage msg = new SnapshotMessage(buildRepairJobDesc());
        serializeRoundTrip(msg, SnapshotMessage.serializer);
    }

    @Test
    public void cleanupMessage() throws IOException
    {
        CleanupMessage msg = new CleanupMessage(nextTimeUUID());
        serializeRoundTrip(msg, CleanupMessage.serializer);
    }
}
