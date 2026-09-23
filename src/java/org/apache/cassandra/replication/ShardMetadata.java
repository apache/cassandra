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
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.NullableSerializer;

public final class ShardMetadata
{
    final String keyspace;
    final long sinceEpoch;
    final Range<Token> range;
    final Participants participants;

    ShardMetadata(String keyspace, long sinceEpoch, Range<Token> range, Participants participants)
    {
        this.keyspace = keyspace;
        this.sinceEpoch = sinceEpoch;
        this.range = range;
        this.participants = participants;
    }

    @Override
    public String toString()
    {
        return '{' + keyspace + ", " + range + ", sinceEpoch=" + sinceEpoch + ", participants=" + participants + '}';

    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof ShardMetadata)) return false;
        ShardMetadata that = (ShardMetadata) o;
        return sinceEpoch == that.sinceEpoch
            && keyspace.equals(that.keyspace)
            && range.equals(that.range)
            && participants.equals(that.participants);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspace, sinceEpoch, range, participants);
    }

    public static final VersionedSerializer<ShardMetadata> serializer = new VersionedSerializer<>()
    {
        @Override
        public void serialize(ShardMetadata response, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(response.keyspace);
            out.writeLong(response.sinceEpoch);
            AbstractBounds.tokenSerializer.serialize(response.range, out, version.messagingVersion());
            Participants.serializer.serialize(response.participants, out);
        }

        @Override
        public ShardMetadata deserialize(DataInputPlus in, Version version) throws IOException
        {
            String keyspace = in.readUTF();
            long sinceEpoch = in.readLong();
            Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, IPartitioner.global(), version.messagingVersion());
            Participants participants = Participants.serializer.deserialize(in);
            return new ShardMetadata(keyspace, sinceEpoch, range, participants);
        }

        @Override
        public long serializedSize(ShardMetadata response, Version version)
        {
            long size = 0;
            size += TypeSizes.sizeof(response.keyspace);
            size += TypeSizes.sizeof(response.sinceEpoch);
            size += AbstractBounds.tokenSerializer.serializedSize(response.range, version.messagingVersion());
            size += Participants.serializer.serializedSize(response.participants);
            return size;
        }
    };

    public static final org.apache.cassandra.io.VersionedSerializer<ShardMetadata, Version> nullableSerializer =
        NullableSerializer.wrap(serializer);
}
