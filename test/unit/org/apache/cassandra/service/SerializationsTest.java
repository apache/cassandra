package org.apache.cassandra.service;
/*
 * 
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
 * 
 */


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageSerializer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;

public class SerializationsTest extends AbstractSerializationsTester
{
    private static MessageSerializer messageSerializer = new MessageSerializer();

    public static Range FULL_RANGE = new Range(StorageService.getPartitioner().getMinimumToken(), StorageService.getPartitioner().getMinimumToken());

    private void testTreeRequestWrite() throws IOException
    {
        DataOutputStream out = getOutput("service.TreeRequest.bin");
        AntiEntropyService.TreeRequestVerbHandler.SERIALIZER.serialize(Statics.req, out, getVersion());
        messageSerializer.serialize(AntiEntropyService.TreeRequestVerbHandler.makeVerb(Statics.req, getVersion()), out, getVersion());
        out.close();
    }
    
    @Test
    public void testTreeRequestRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testTreeRequestWrite();
        
        DataInputStream in = getInput("service.TreeRequest.bin");
        assert AntiEntropyService.TreeRequestVerbHandler.SERIALIZER.deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        in.close();
    }
    
    private void testTreeResponseWrite() throws IOException
    {
        AntiEntropyService.Validator v0 = new AntiEntropyService.Validator(Statics.req);
        IPartitioner part = new RandomPartitioner();
        MerkleTree mt = new MerkleTree(part, FULL_RANGE, MerkleTree.RECOMMENDED_DEPTH, Integer.MAX_VALUE);
        List<Token> tokens = new ArrayList<Token>();
        for (int i = 0; i < 10; i++)
        {
            Token t = part.getRandomToken();
            tokens.add(t);
            mt.split(t);
        }
        AntiEntropyService.Validator v1 = new AntiEntropyService.Validator(Statics.req, mt);
        DataOutputStream out = getOutput("service.TreeResponse.bin");
        AntiEntropyService.TreeResponseVerbHandler.SERIALIZER.serialize(v0, out, getVersion());
        AntiEntropyService.TreeResponseVerbHandler.SERIALIZER.serialize(v1, out, getVersion());
        messageSerializer.serialize(AntiEntropyService.TreeResponseVerbHandler.makeVerb(FBUtilities.getBroadcastAddress(), v0), out, getVersion());
        messageSerializer.serialize(AntiEntropyService.TreeResponseVerbHandler.makeVerb(FBUtilities.getBroadcastAddress(), v1), out, getVersion());
        out.close();
    }
    
    @Test
    public void testTreeResponseRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testTreeResponseWrite();
        
        DataInputStream in = getInput("service.TreeResponse.bin");
        assert AntiEntropyService.TreeResponseVerbHandler.SERIALIZER.deserialize(in, getVersion()) != null;
        assert AntiEntropyService.TreeResponseVerbHandler.SERIALIZER.deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        in.close();
    }
    
    private static class Statics
    {
        private static final AntiEntropyService.CFPair pair = new AntiEntropyService.CFPair("Keyspace1", "Standard1");
        private static final AntiEntropyService.TreeRequest req = new AntiEntropyService.TreeRequest("sessionId", FBUtilities.getBroadcastAddress(), FULL_RANGE, pair);
    }
}
