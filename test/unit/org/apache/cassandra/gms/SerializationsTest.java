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
package org.apache.cassandra.gms;

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SerializationsTest extends AbstractSerializationsTester
{
    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private void testEndpointStateWrite() throws IOException
    {
        DataOutputStreamPlus out = getOutput("gms.EndpointState.bin");
        HeartBeatState.serializer.serialize(Statics.HeartbeatSt, out, getVersion());
        EndpointState.serializer.serialize(Statics.EndpointSt, out, getVersion());
        VersionedValue.serializer.serialize(Statics.vv0, out, getVersion());
        VersionedValue.serializer.serialize(Statics.vv1, out, getVersion());
        out.close();

        // test serializedSize
        testSerializedSize(Statics.HeartbeatSt, HeartBeatState.serializer);
        testSerializedSize(Statics.EndpointSt, EndpointState.serializer);
        testSerializedSize(Statics.vv0, VersionedValue.serializer);
        testSerializedSize(Statics.vv1, VersionedValue.serializer);
    }

    @Test
    public void testEndpointStateRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testEndpointStateWrite();

        FileInputStreamPlus in = getInput("gms.EndpointState.bin");
        assert HeartBeatState.serializer.deserialize(in, getVersion()) != null;
        assert EndpointState.serializer.deserialize(in, getVersion()) != null;
        assert VersionedValue.serializer.deserialize(in, getVersion()) != null;
        assert VersionedValue.serializer.deserialize(in, getVersion()) != null;
        in.close();
    }

    private void testGossipDigestWrite() throws IOException
    {
        Map<InetAddressAndPort, EndpointState> states = new HashMap<>();
        states.put(InetAddressAndPort.getByName("127.0.0.1"), Statics.EndpointSt);
        states.put(InetAddressAndPort.getByName("127.0.0.2"), Statics.EndpointSt);
        GossipDigestAck ack = new GossipDigestAck(Statics.Digests, states);
        GossipDigestAck2 ack2 = new GossipDigestAck2(states);
        GossipDigestSyn syn = new GossipDigestSyn("Not a real cluster name",
                                                  StorageService.instance.getTokenMetadata().partitioner.getClass().getCanonicalName(),
                                                  Statics.Digests);

        DataOutputStreamPlus out = getOutput("gms.Gossip.bin");
        for (GossipDigest gd : Statics.Digests)
            GossipDigest.serializer.serialize(gd, out, getVersion());
        GossipDigestAck.serializer.serialize(ack, out, getVersion());
        GossipDigestAck2.serializer.serialize(ack2, out, getVersion());
        GossipDigestSyn.serializer.serialize(syn, out, getVersion());
        out.close();

        // test serializedSize
        for (GossipDigest gd : Statics.Digests)
            testSerializedSize(gd, GossipDigest.serializer);
        testSerializedSize(ack, GossipDigestAck.serializer);
        testSerializedSize(ack2, GossipDigestAck2.serializer);
        testSerializedSize(syn, GossipDigestSyn.serializer);
    }

    @Test
    public void testGossipDigestRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testGossipDigestWrite();

        int count = 0;
        FileInputStreamPlus in = getInput("gms.Gossip.bin");
        while (count < Statics.Digests.size())
            assert GossipDigestAck2.serializer.deserialize(in, getVersion()) != null;
        assert GossipDigestAck.serializer.deserialize(in, getVersion()) != null;
        assert GossipDigestAck2.serializer.deserialize(in, getVersion()) != null;
        assert GossipDigestSyn.serializer.deserialize(in, getVersion()) != null;
        in.close();
    }

    private static class Statics
    {
        private static HeartBeatState HeartbeatSt = new HeartBeatState(101, 201);
        private static EndpointState EndpointSt = new EndpointState(HeartbeatSt);
        private static IPartitioner partitioner = StorageService.instance.getTokenMetadata().partitioner;
        private static VersionedValue.VersionedValueFactory vvFact = new VersionedValue.VersionedValueFactory(partitioner);
        private static VersionedValue vv0 = vvFact.load(23d);
        private static VersionedValue vv1 = vvFact.bootstrapping(Collections.<Token>singleton(partitioner.getRandomToken()));
        private static List<GossipDigest> Digests = new ArrayList<GossipDigest>();

        {
            HeartbeatSt.updateHeartBeat();
            EndpointSt.addApplicationState(ApplicationState.LOAD, vv0);
            EndpointSt.addApplicationState(ApplicationState.STATUS_WITH_PORT, vv1);
            for (int i = 0; i < 100; i++)
                Digests.add(new GossipDigest(FBUtilities.getBroadcastAddressAndPort(), 100 + i, 1000 + 2 * i));
        }
    }
}
