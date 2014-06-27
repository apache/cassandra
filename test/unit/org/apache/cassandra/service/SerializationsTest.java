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
package org.apache.cassandra.service;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputStreamAndChannel;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.NodePair;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.repair.messages.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;

public class SerializationsTest extends AbstractSerializationsTester
{
    static
    {
        System.setProperty("cassandra.partitioner", "RandomPartitioner");
    }

    private static final UUID RANDOM_UUID = UUID.fromString("b5c3d033-75aa-4c2f-a819-947aac7a0c54");
    private static final Range<Token> FULL_RANGE = new Range<>(StorageService.getPartitioner().getMinimumToken(), StorageService.getPartitioner().getMinimumToken());
    private static final RepairJobDesc DESC = new RepairJobDesc(getVersion() < MessagingService.VERSION_21 ? null : RANDOM_UUID, RANDOM_UUID, "Keyspace1", "Standard1", FULL_RANGE);

    private void testRepairMessageWrite(String fileName, RepairMessage... messages) throws IOException
    {
        try (DataOutputStreamAndChannel out = getOutput(fileName))
        {
            for (RepairMessage message : messages)
            {
                testSerializedSize(message, RepairMessage.serializer);
                RepairMessage.serializer.serialize(message, out, getVersion());
            }
            // also serialize MessageOut
            for (RepairMessage message : messages)
                message.createMessage().serialize(out,  getVersion());
        }
    }

    private void testValidationRequestWrite() throws IOException
    {
        ValidationRequest message = new ValidationRequest(DESC, 1234);
        testRepairMessageWrite("service.ValidationRequest.bin", message);
    }

    @Test
    public void testValidationRequestRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testValidationRequestWrite();

        try (DataInputStream in = getInput("service.ValidationRequest.bin"))
        {
            RepairMessage message = RepairMessage.serializer.deserialize(in, getVersion());
            assert message.messageType == RepairMessage.Type.VALIDATION_REQUEST;
            assert DESC.equals(message.desc);
            assert ((ValidationRequest) message).gcBefore == 1234;

            assert MessageIn.read(in, getVersion(), -1) != null;
        }
    }

    private void testValidationCompleteWrite() throws IOException
    {
        IPartitioner p = new RandomPartitioner();
        // empty validation
        MerkleTree mt = new MerkleTree(p, FULL_RANGE, MerkleTree.RECOMMENDED_DEPTH, (int) Math.pow(2, 15));
        Validator v0 = new Validator(DESC, FBUtilities.getBroadcastAddress(),  -1);
        ValidationComplete c0 = new ValidationComplete(DESC, mt);

        // validation with a tree
        mt = new MerkleTree(p, FULL_RANGE, MerkleTree.RECOMMENDED_DEPTH, Integer.MAX_VALUE);
        for (int i = 0; i < 10; i++)
            mt.split(p.getRandomToken());
        Validator v1 = new Validator(DESC, FBUtilities.getBroadcastAddress(), -1);
        ValidationComplete c1 = new ValidationComplete(DESC, mt);

        // validation failed
        ValidationComplete c3 = new ValidationComplete(DESC);

        testRepairMessageWrite("service.ValidationComplete.bin", c0, c1, c3);
    }

    @Test
    public void testValidationCompleteRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testValidationCompleteWrite();

        try (DataInputStream in = getInput("service.ValidationComplete.bin"))
        {
            // empty validation
            RepairMessage message = RepairMessage.serializer.deserialize(in, getVersion());
            assert message.messageType == RepairMessage.Type.VALIDATION_COMPLETE;
            assert DESC.equals(message.desc);

            assert ((ValidationComplete) message).success;
            assert ((ValidationComplete) message).tree != null;

            // validation with a tree
            message = RepairMessage.serializer.deserialize(in, getVersion());
            assert message.messageType == RepairMessage.Type.VALIDATION_COMPLETE;
            assert DESC.equals(message.desc);

            assert ((ValidationComplete) message).success;
            assert ((ValidationComplete) message).tree != null;

            // failed validation
            message = RepairMessage.serializer.deserialize(in, getVersion());
            assert message.messageType == RepairMessage.Type.VALIDATION_COMPLETE;
            assert DESC.equals(message.desc);

            assert !((ValidationComplete) message).success;
            assert ((ValidationComplete) message).tree == null;

            // MessageOuts
            for (int i = 0; i < 3; i++)
                assert MessageIn.read(in, getVersion(), -1) != null;
        }
    }

    private void testSyncRequestWrite() throws IOException
    {
        InetAddress local = InetAddress.getByAddress(new byte[]{127, 0, 0, 1});
        InetAddress src = InetAddress.getByAddress(new byte[]{127, 0, 0, 2});
        InetAddress dest = InetAddress.getByAddress(new byte[]{127, 0, 0, 3});
        SyncRequest message = new SyncRequest(DESC, local, src, dest, Collections.singleton(FULL_RANGE));

        testRepairMessageWrite("service.SyncRequest.bin", message);
    }

    @Test
    public void testSyncRequestRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testSyncRequestWrite();

        InetAddress local = InetAddress.getByAddress(new byte[]{127, 0, 0, 1});
        InetAddress src = InetAddress.getByAddress(new byte[]{127, 0, 0, 2});
        InetAddress dest = InetAddress.getByAddress(new byte[]{127, 0, 0, 3});

        try (DataInputStream in = getInput("service.SyncRequest.bin"))
        {
            RepairMessage message = RepairMessage.serializer.deserialize(in, getVersion());
            assert message.messageType == RepairMessage.Type.SYNC_REQUEST;
            assert DESC.equals(message.desc);
            assert local.equals(((SyncRequest) message).initiator);
            assert src.equals(((SyncRequest) message).src);
            assert dest.equals(((SyncRequest) message).dst);
            assert ((SyncRequest) message).ranges.size() == 1 && ((SyncRequest) message).ranges.contains(FULL_RANGE);

            assert MessageIn.read(in, getVersion(), -1) != null;
        }
    }

    private void testSyncCompleteWrite() throws IOException
    {
        InetAddress src = InetAddress.getByAddress(new byte[]{127, 0, 0, 2});
        InetAddress dest = InetAddress.getByAddress(new byte[]{127, 0, 0, 3});
        // sync success
        SyncComplete success = new SyncComplete(DESC, src, dest, true);
        // sync fail
        SyncComplete fail = new SyncComplete(DESC, src, dest, false);

        testRepairMessageWrite("service.SyncComplete.bin", success, fail);
    }

    @Test
    public void testSyncCompleteRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testSyncCompleteWrite();

        InetAddress src = InetAddress.getByAddress(new byte[]{127, 0, 0, 2});
        InetAddress dest = InetAddress.getByAddress(new byte[]{127, 0, 0, 3});
        NodePair nodes = new NodePair(src, dest);

        try (DataInputStream in = getInput("service.SyncComplete.bin"))
        {
            // success
            RepairMessage message = RepairMessage.serializer.deserialize(in, getVersion());
            assert message.messageType == RepairMessage.Type.SYNC_COMPLETE;
            assert DESC.equals(message.desc);

            assert nodes.equals(((SyncComplete) message).nodes);
            assert ((SyncComplete) message).success;

            // fail
            message = RepairMessage.serializer.deserialize(in, getVersion());
            assert message.messageType == RepairMessage.Type.SYNC_COMPLETE;
            assert DESC.equals(message.desc);

            assert nodes.equals(((SyncComplete) message).nodes);
            assert !((SyncComplete) message).success;

            // MessageOuts
            for (int i = 0; i < 2; i++)
                assert MessageIn.read(in, getVersion(), -1) != null;
        }
    }
}
