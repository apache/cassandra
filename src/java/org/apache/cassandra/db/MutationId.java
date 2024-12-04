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
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Clock;

public class MutationId implements Comparable<MutationId>, Serializable
{
    private static final MutationId NONE = new MutationId(Integer.MIN_VALUE, Long.MIN_VALUE);

    private static class LocalState
    {
        private final int node = ClusterMetadata.current().myNodeId().id();
        private final AtomicLong lastTimestamp = new AtomicLong();
        private LocalState() {}
    }

    private static class Holder
    {
        private static final LocalState instance = new LocalState();
    }

    public final int node;
    public final long timestamp;

    private MutationId(int node, long timestamp)
    {
        this.node = node;
        this.timestamp = timestamp;
    }

    public boolean isNone()
    {
        return node == Integer.MIN_VALUE && timestamp == Long.MIN_VALUE;
    }

    @Override
    public int compareTo(MutationId o)
    {
        int cmp = Long.compare(timestamp, o.timestamp);
        if (cmp != 0)
            return cmp;

        return Integer.compare(node, o.node);
    }


    public static MutationId create(int node, long timestamp)
    {
        if (node == Integer.MIN_VALUE && timestamp == Long.MIN_VALUE)
            return none();
        return new MutationId(node, timestamp);
    }

    // FIXME: used in place of figuring out if we should use a mutation id or not
    public static MutationId fixme()
    {
        return none();
    }

    public static MutationId none()
    {
        return NONE;
    }

    public static MutationId createNext()
    {
        LocalState state = Holder.instance;
        long timestamp = TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());
        while (true)
        {
            long lastMicros = state.lastTimestamp.get();
            if (timestamp <= lastMicros)
                timestamp = lastMicros + 1;

            if (state.lastTimestamp.compareAndSet(lastMicros, timestamp))
                return new MutationId(state.node, timestamp);
        }
    }

    public static MutationId createFor(TableMetadata metadata)
    {
        return metadata.hasLoggedReplication() ? createNext() : none();
    }

    public static MutationId minNotNone(MutationId l, MutationId r)
    {
        if (l.isNone() || r.isNone())
            return l.isNone() ? r : l;

        return l.compareTo(r) < 0 ? l : r;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        MutationId id = (MutationId) o;
        return node == id.node && timestamp == id.timestamp;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(node, timestamp);
    }

    @Override
    public String toString()
    {
        if (isNone())
            return "MutationId{NONE}";
        return "MutationId{" + node + ':' + timestamp + '}';
    }

    public static final IVersionedSerializer<MutationId> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(MutationId id, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(id.node);
            out.writeLong(id.timestamp);
        }

        @Override
        public MutationId deserialize(DataInputPlus in, int version) throws IOException
        {
            return create(in.readInt(), in.readLong());
        }

        @Override
        public long serializedSize(MutationId id, int version)
        {
            return TypeSizes.sizeof(id.node) + TypeSizes.sizeof(id.timestamp);
        }
    };
}
