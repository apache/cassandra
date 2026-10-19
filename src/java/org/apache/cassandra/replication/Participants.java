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
package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.agrona.collections.IntHashSet;

import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.ArraySerializers;

public class Participants
{
    public static final Participants EMPTY = new Participants(new int[0]);

    private final int[] hosts;

    Participants(Collection<Integer> participants)
    {
        int i = 0;
        int[] hosts = new int[participants.size()];
        for (int host : participants) hosts[i++] = host;
        Arrays.sort(hosts);
        this.hosts = hosts;
    }

    Participants(int[] hosts)
    {
        Arrays.sort(hosts);
        this.hosts = hosts;
    }

    int size()
    {
        return hosts.length;
    }

    int indexOf(int hostId)
    {
        int idx = Arrays.binarySearch(hosts, hostId);
        if (idx < 0)
            throw new IllegalArgumentException("Unknown host id " + hostId);
        return idx;
    }

    boolean contains(int hostId)
    {
        return Arrays.binarySearch(hosts, hostId) >= 0;
    }

    int get(int idx)
    {
        if (idx < 0 || idx >= hosts.length)
            throw new IllegalArgumentException("Out of bounds host idx " + idx);
        return hosts[idx];
    }

    @Override
    public String toString()
    {
        return Arrays.toString(hosts);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof Participants))
            return false;
        Participants that = (Participants) o;
        return Arrays.equals(this.hosts, that.hosts);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(hosts);
    }

    public Set<Integer> asSet()
    {
        IntHashSet set = new IntHashSet(hosts.length);
        for (int host : hosts)
            set.add(host);
        return set;
    }

    public Set<NodeId> asNodeIds()
    {
        Set<NodeId> set = new HashSet<>(hosts.length);
        for (int host : hosts)
            set.add(new NodeId(host));
        return set;
    }

    static Participants fromNodeIds(Collection<NodeId> nodeIds)
    {
        IntHashSet set = new IntHashSet(nodeIds.size());
        for (NodeId nodeId : nodeIds)
            set.add(nodeId.id());
        return new Participants(set);
    }

    void addTo(IntHashSet set)
    {
        for (int i = 0; i < size(); i++)
            set.add(hosts[i]);
    }

    static Participants merge(Participants left, Participants right)
    {
        IntHashSet merged = new IntHashSet(left.size() + right.size());
        left.addTo(merged);
        right.addTo(merged);
        return new Participants(merged);
    }

    static Participants fromReplicas(Iterable<Replica> replicas, ClusterMetadata cm)
    {
        IntHashSet set = new IntHashSet();
        for (Replica r : replicas)
            set.add(cm.directory.peerId(r.endpoint()).id());
        return new Participants(set);
    }

    boolean matches(EndpointsForToken expected, ClusterMetadata cm)
    {
        if (size() != expected.size())
            return false;

        for (Replica replica : expected)
            if (!contains(cm.directory.peerId(replica.endpoint()).id()))
                return false;

        return true;
    }

    public static final UnversionedSerializer<Participants> serializer = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(Participants participants, DataOutputPlus out) throws IOException
        {
            ArraySerializers.serializeVIntArray(participants.hosts, out);
        }

        @Override
        public Participants deserialize(DataInputPlus in) throws IOException
        {
            return new Participants(ArraySerializers.deserializeVIntArray(in));
        }

        @Override
        public long serializedSize(Participants participants)
        {
            return ArraySerializers.serializedVIntArraySize(participants.hosts);
        }
    };
}
