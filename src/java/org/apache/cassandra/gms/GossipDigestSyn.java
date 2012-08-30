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
package org.apache.cassandra.gms;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessagingService;

/**
 * This is the first message that gets sent out as a start of the Gossip protocol in a
 * round.
 */
public class GossipDigestSyn
{
    public static final IVersionedSerializer<GossipDigestSyn> serializer = new GossipDigestSynSerializer();

    final String clusterId;
    final String partioner;
    final List<GossipDigest> gDigests;

    public GossipDigestSyn(String clusterId, String partioner, List<GossipDigest> gDigests)
    {
        this.clusterId = clusterId;
        this.partioner = partioner;
        this.gDigests = gDigests;
    }

    List<GossipDigest> getGossipDigests()
    {
        return gDigests;
    }
}

class GossipDigestSerializationHelper
{
    static void serialize(List<GossipDigest> gDigestList, DataOutput dos, int version) throws IOException
    {
        dos.writeInt(gDigestList.size());
        for (GossipDigest gDigest : gDigestList)
            GossipDigest.serializer.serialize(gDigest, dos, version);
    }

    static List<GossipDigest> deserialize(DataInput dis, int version) throws IOException
    {
        int size = dis.readInt();
        List<GossipDigest> gDigests = new ArrayList<GossipDigest>(size);
        for (int i = 0; i < size; ++i)
            gDigests.add(GossipDigest.serializer.deserialize(dis, version));
        return gDigests;
    }
    
    static int serializedSize(List<GossipDigest> digests, int version)
    {
        int size = TypeSizes.NATIVE.sizeof(digests.size());
        for (GossipDigest digest : digests)
            size += GossipDigest.serializer.serializedSize(digest, version);
        return size;
    }
}

class GossipDigestSynSerializer implements IVersionedSerializer<GossipDigestSyn>
{
    public void serialize(GossipDigestSyn gDigestSynMessage, DataOutput dos, int version) throws IOException
    {
        dos.writeUTF(gDigestSynMessage.clusterId);
        if (version >= MessagingService.VERSION_12)
            dos.writeUTF(gDigestSynMessage.partioner);
        GossipDigestSerializationHelper.serialize(gDigestSynMessage.gDigests, dos, version);
    }

    public GossipDigestSyn deserialize(DataInput dis, int version) throws IOException
    {
        String clusterId = dis.readUTF();
        String partioner = null;
        if (version >= MessagingService.VERSION_12)
            partioner = dis.readUTF();
        List<GossipDigest> gDigests = GossipDigestSerializationHelper.deserialize(dis, version);
        return new GossipDigestSyn(clusterId, partioner, gDigests);
    }

    public long serializedSize(GossipDigestSyn syn, int version)
    {
        long size = TypeSizes.NATIVE.sizeof(syn.clusterId);
        if (version >= MessagingService.VERSION_12)
            size += TypeSizes.NATIVE.sizeof(syn.partioner);
        size += GossipDigestSerializationHelper.serializedSize(syn.gDigests, version);
        return size;
    }
}

