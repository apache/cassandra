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

import org.junit.Test;

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;

public class SerializationsTest extends AbstractSerializationsTester
{
    static
    {
        System.setProperty("cassandra.partitioner", "RandomPartitioner");
    }

    public static Range<Token> FULL_RANGE = new Range<Token>(StorageService.getPartitioner().getMinimumToken(), StorageService.getPartitioner().getMinimumToken());

    private void testTreeRequestWrite() throws IOException
    {
        DataOutputStream out = getOutput("service.TreeRequest.bin");
        ActiveRepairService.TreeRequest.serializer.serialize(Statics.req, out, getVersion());
        Statics.req.createMessage().serialize(out, getVersion());
        out.close();

        // test serializedSize
        testSerializedSize(Statics.req, ActiveRepairService.TreeRequest.serializer);
    }

    @Test
    public void testTreeRequestRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testTreeRequestWrite();

        DataInputStream in = getInput("service.TreeRequest.bin");
        assert ActiveRepairService.TreeRequest.serializer.deserialize(in, getVersion()) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        in.close();
    }

    private void testTreeResponseWrite() throws IOException
    {
        // empty validation
        ActiveRepairService.Validator v0 = new ActiveRepairService.Validator(Statics.req);

        // validation with a tree
        IPartitioner p = new RandomPartitioner();
        MerkleTree mt = new MerkleTree(p, FULL_RANGE, MerkleTree.RECOMMENDED_DEPTH, Integer.MAX_VALUE);
        for (int i = 0; i < 10; i++)
            mt.split(p.getRandomToken());
        ActiveRepairService.Validator v1 = new ActiveRepairService.Validator(Statics.req, mt);

        DataOutputStream out = getOutput("service.TreeResponse.bin");
        ActiveRepairService.Validator.serializer.serialize(v0, out, getVersion());
        ActiveRepairService.Validator.serializer.serialize(v1, out, getVersion());
        v0.createMessage().serialize(out, getVersion());
        v1.createMessage().serialize(out, getVersion());
        out.close();

        // test serializedSize
        testSerializedSize(v0, ActiveRepairService.Validator.serializer);
        testSerializedSize(v1, ActiveRepairService.Validator.serializer);
    }

    @Test
    public void testTreeResponseRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testTreeResponseWrite();

        DataInputStream in = getInput("service.TreeResponse.bin");
        assert ActiveRepairService.Validator.serializer.deserialize(in, getVersion()) != null;
        assert ActiveRepairService.Validator.serializer.deserialize(in, getVersion()) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        in.close();
    }

    private static class Statics
    {
        private static final ActiveRepairService.CFPair pair = new ActiveRepairService.CFPair("Keyspace1", "Standard1");
        private static final ActiveRepairService.TreeRequest req = new ActiveRepairService.TreeRequest("sessionId", FBUtilities.getBroadcastAddress(), FULL_RANGE, pair);
    }
}
