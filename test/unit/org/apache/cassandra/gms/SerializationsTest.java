package org.apache.cassandra.gms;
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


import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SerializationsTest extends AbstractSerializationsTester
{
    private void testEndpointStateWrite() throws IOException 
    {
        DataOutputStream out = getOutput("gms.EndpointState.bin");
        HeartBeatState.serializer().serialize(Statics.HeartbeatSt, out, getVersion());
        EndpointState.serializer().serialize(Statics.EndpointSt, out, getVersion());
        VersionedValue.serializer.serialize(Statics.vv0, out, getVersion());
        VersionedValue.serializer.serialize(Statics.vv1, out, getVersion());
        out.close();
    }
    
    @Test
    public void testEndpointStateRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testEndpointStateWrite();
        
        DataInputStream in = getInput("gms.EndpointState.bin");
        assert HeartBeatState.serializer().deserialize(in, getVersion()) != null;
        assert EndpointState.serializer().deserialize(in, getVersion()) != null;
        assert VersionedValue.serializer.deserialize(in, getVersion()) != null;
        assert VersionedValue.serializer.deserialize(in, getVersion()) != null;
        in.close();
    }
     
    private void testGossipDigestWrite() throws IOException
    {
        Map<InetAddress, EndpointState> states = new HashMap<InetAddress, EndpointState>();
        states.put(InetAddress.getByName("127.0.0.1"), Statics.EndpointSt);
        states.put(InetAddress.getByName("127.0.0.2"), Statics.EndpointSt);
        GossipDigestAckMessage ack = new GossipDigestAckMessage(Statics.Digests, states);
        GossipDigestAck2Message ack2 = new GossipDigestAck2Message(states);
        GossipDigestSynMessage syn = new GossipDigestSynMessage("Not a real cluster name", Statics.Digests);
        
        DataOutputStream out = getOutput("gms.Gossip.bin");
        for (GossipDigest gd : Statics.Digests)
            GossipDigest.serializer().serialize(gd, out, getVersion());
        GossipDigestAckMessage.serializer().serialize(ack, out, getVersion());
        GossipDigestAck2Message.serializer().serialize(ack2, out, getVersion());
        GossipDigestSynMessage.serializer().serialize(syn, out, getVersion());
        out.close();
    }
    
    @Test
    public void testGossipDigestRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testGossipDigestWrite();
        
        int count = 0;
        DataInputStream in = getInput("gms.Gossip.bin");
        while (count < Statics.Digests.size())
            assert GossipDigestAck2Message.serializer().deserialize(in, getVersion()) != null;
        assert GossipDigestAckMessage.serializer().deserialize(in, getVersion()) != null;
        assert GossipDigestAck2Message.serializer().deserialize(in, getVersion()) != null;
        assert GossipDigestSynMessage.serializer().deserialize(in, getVersion()) != null;
        in.close();
    }
    
    private static class Statics
    {
        private static HeartBeatState HeartbeatSt = new HeartBeatState(101, 201);
        private static EndpointState EndpointSt = new EndpointState(HeartbeatSt);
        private static VersionedValue.VersionedValueFactory vvFact = new VersionedValue.VersionedValueFactory(StorageService.getPartitioner());
        private static VersionedValue vv0 = vvFact.load(23d);
        private static VersionedValue vv1 = vvFact.bootstrapping(StorageService.getPartitioner().getRandomToken());
        private static List<GossipDigest> Digests = new ArrayList<GossipDigest>();
        
        {
            HeartbeatSt.updateHeartBeat();
            EndpointSt.addApplicationState(ApplicationState.LOAD, vv0);
            EndpointSt.addApplicationState(ApplicationState.STATUS, vv1);
            for (int i = 0; i < 100; i++)
                Digests.add(new GossipDigest(FBUtilities.getBroadcastAddress(), 100 + i, 1000 + 2 * i));
        }
    }
}
