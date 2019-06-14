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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.Util;
import org.apache.cassandra.Util.PartitionerSwitcher;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.SyncNodePair;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.repair.messages.*;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.SessionSummary;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.UUIDGen;

public class SerializationsTest extends AbstractSerializationsTester
{
    private static PartitionerSwitcher partitionerSwitcher;
    private static UUID RANDOM_UUID;
    private static Range<Token> FULL_RANGE;
    private static RepairJobDesc DESC;

    private static final int PORT = 7010;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        partitionerSwitcher = Util.switchPartitioner(RandomPartitioner.instance);
        RANDOM_UUID = UUID.fromString("b5c3d033-75aa-4c2f-a819-947aac7a0c54");
        FULL_RANGE = new Range<>(Util.testPartitioner().getMinimumToken(), Util.testPartitioner().getMinimumToken());
        DESC = new RepairJobDesc(RANDOM_UUID, RANDOM_UUID, "Keyspace1", "Standard1", Arrays.asList(FULL_RANGE));
    }

    @AfterClass
    public static void tearDown()
    {
        partitionerSwitcher.close();
    }

    private <T extends RepairMessage> void testRepairMessageWrite(String fileName, IVersionedSerializer<T> serializer, T... messages) throws IOException
    {
        try (DataOutputStreamPlus out = getOutput(fileName))
        {
            for (T message : messages)
            {
                testSerializedSize(message, serializer);
                serializer.serialize(message, out, getVersion());
            }
        }
    }

    private void testValidationRequestWrite() throws IOException
    {
        ValidationRequest message = new ValidationRequest(DESC, 1234);
        testRepairMessageWrite("service.ValidationRequest.bin", ValidationRequest.serializer, message);
    }

    @Test
    public void testValidationRequestRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testValidationRequestWrite();

        try (DataInputStreamPlus in = getInput("service.ValidationRequest.bin"))
        {
            ValidationRequest message = ValidationRequest.serializer.deserialize(in, getVersion());
            assert DESC.equals(message.desc);
            assert message.nowInSec == 1234;
        }
    }

    private void testValidationCompleteWrite() throws IOException
    {
        IPartitioner p = RandomPartitioner.instance;

        MerkleTrees mt = new MerkleTrees(p);

        // empty validation
        mt.addMerkleTree((int) Math.pow(2, 15), FULL_RANGE);
        Validator v0 = new Validator(DESC, FBUtilities.getBroadcastAddressAndPort(), -1, PreviewKind.NONE);
        ValidationResponse c0 = new ValidationResponse(DESC, mt);

        // validation with a tree
        mt = new MerkleTrees(p);
        mt.addMerkleTree(Integer.MAX_VALUE, FULL_RANGE);
        for (int i = 0; i < 10; i++)
            mt.split(p.getRandomToken());
        Validator v1 = new Validator(DESC, FBUtilities.getBroadcastAddressAndPort(), -1, PreviewKind.NONE);
        ValidationResponse c1 = new ValidationResponse(DESC, mt);

        // validation failed
        ValidationResponse c3 = new ValidationResponse(DESC);

        testRepairMessageWrite("service.ValidationComplete.bin", ValidationResponse.serializer, c0, c1, c3);
    }

    @Test
    public void testValidationCompleteRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testValidationCompleteWrite();

        try (DataInputStreamPlus in = getInput("service.ValidationComplete.bin"))
        {
            // empty validation
            ValidationResponse message = ValidationResponse.serializer.deserialize(in, getVersion());
            assert DESC.equals(message.desc);

            assert message.success();
            assert message.trees != null;

            // validation with a tree
            message = ValidationResponse.serializer.deserialize(in, getVersion());
            assert DESC.equals(message.desc);

            assert message.success();
            assert message.trees != null;

            // failed validation
            message = ValidationResponse.serializer.deserialize(in, getVersion());
            assert DESC.equals(message.desc);

            assert !message.success();
            assert message.trees == null;
        }
    }

    private void testSyncRequestWrite() throws IOException
    {
        InetAddressAndPort local = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.1", PORT);
        InetAddressAndPort src = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.2", PORT);
        InetAddressAndPort dest = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.3", PORT);

        SyncRequest message = new SyncRequest(DESC, local, src, dest, Collections.singleton(FULL_RANGE), PreviewKind.NONE);
        testRepairMessageWrite("service.SyncRequest.bin", SyncRequest.serializer, message);
    }

    @Test
    public void testSyncRequestRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testSyncRequestWrite();

        InetAddressAndPort local = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.1", PORT);
        InetAddressAndPort src = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.2", PORT);
        InetAddressAndPort dest = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.3", PORT);

        try (DataInputStreamPlus in = getInput("service.SyncRequest.bin"))
        {
            SyncRequest message = SyncRequest.serializer.deserialize(in, getVersion());
            assert DESC.equals(message.desc);
            assert local.equals(message.initiator);
            assert src.equals(message.src);
            assert dest.equals(message.dst);
            assert message.ranges.size() == 1 && message.ranges.contains(FULL_RANGE);
        }
    }

    private void testSyncCompleteWrite() throws IOException
    {
        InetAddressAndPort src = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.2", PORT);
        InetAddressAndPort dest = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.3", PORT);
        // sync success
        List<SessionSummary> summaries = new ArrayList<>();
        summaries.add(new SessionSummary(src, dest,
                                         Lists.newArrayList(new StreamSummary(TableId.fromUUID(UUIDGen.getTimeUUID()), 5, 100)),
                                         Lists.newArrayList(new StreamSummary(TableId.fromUUID(UUIDGen.getTimeUUID()), 500, 10))
        ));
        SyncResponse success = new SyncResponse(DESC, src, dest, true, summaries);
        // sync fail
        SyncResponse fail = new SyncResponse(DESC, src, dest, false, Collections.emptyList());

        testRepairMessageWrite("service.SyncComplete.bin", SyncResponse.serializer, success, fail);
    }

    @Test
    public void testSyncCompleteRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testSyncCompleteWrite();

        InetAddressAndPort src = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.2", PORT);
        InetAddressAndPort dest = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.3", PORT);
        SyncNodePair nodes = new SyncNodePair(src, dest);

        try (DataInputStreamPlus in = getInput("service.SyncComplete.bin"))
        {
            // success
            SyncResponse message = SyncResponse.serializer.deserialize(in, getVersion());
            assert DESC.equals(message.desc);

            System.out.println(nodes);
            System.out.println(message.nodes);
            assert nodes.equals(message.nodes);
            assert message.success;

            // fail
            message = SyncResponse.serializer.deserialize(in, getVersion());
            assert DESC.equals(message.desc);

            assert nodes.equals(message.nodes);
            assert !message.success;
        }
    }
}
