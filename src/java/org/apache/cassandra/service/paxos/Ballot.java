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

package org.apache.cassandra.service.paxos;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.service.paxos.Ballot.Flag.NONE;
import static org.apache.cassandra.utils.ByteArrayUtil.getLong;
import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION, inner = INTERFACES)
public class Ballot extends TimeUUID
{
    public static final long serialVersionUID = 1L;

    public enum Flag
    {
        NONE, LOCAL, GLOBAL;
        static final Flag[] FLAGS = values();
    }

    private static final Ballot epoch = atUnixMicrosWithLsb(0, 0, NONE);

    public static Ballot none()
    {
        return epoch;
    }

    private Ballot(long rawTimestamp, long lsb)
    {
        super(rawTimestamp, lsb);
    }

    public boolean equals(Object that)
    {
        if (that == null) return false;
        if (that == this) return true;
        if (that.getClass() != Ballot.class) return false;
        return super.equals((TimeUUID) that);
    }

    public static Ballot atUnixMicrosWithLsb(long unixMicros, long uniqueLsb, Flag flag)
    {
        return new Ballot(unixMicrosToRawTimestamp(unixMicros) + flag.ordinal(), uniqueLsb);
    }

    public static Ballot fromUuid(UUID uuid)
    {
        return fromBytes(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    public static Ballot fromBytes(long msb, long lsb)
    {
        return new Ballot(msbToRawTimestamp(msb), lsb);
    }

    public static Ballot fromString(String uuidString)
    {
        return fromUuid(UUID.fromString(uuidString));
    }

    public static Ballot deserialize(byte[] bytes)
    {
        if (bytes.length == 0)
            return null;
        return fromBytes(getLong(bytes, 0), getLong(bytes, 8));
    }

    public static Ballot deserialize(ByteBuffer buffer)
    {
        if (!buffer.hasRemaining())
            return null;
        return fromBytes(buffer.getLong(buffer.position()), buffer.getLong(buffer.position() + 8));
    }

    public static Ballot deserialize(DataInputPlus in) throws IOException
    {
        long msb = in.readLong();
        long lsb = in.readLong();
        return fromBytes(msb, lsb);
    }

    public Flag flag()
    {
        int i = (int)(uuidTimestamp() % 10);
        if (i < Flag.FLAGS.length)
            return Flag.FLAGS[i];
        return NONE;
    }


    public static class Serializer extends AbstractSerializer<Ballot> implements IVersionedSerializer<Ballot>
    {
        public static final Serializer instance = new Serializer();

        public <V> Ballot deserialize(V value, ValueAccessor<V> accessor)
        {
            return accessor.isEmpty(value) ? null : accessor.toBallot(value);
        }

        public Class<Ballot> getType()
        {
            return Ballot.class;
        }

        @Override
        public void serialize(Ballot t, DataOutputPlus out, int version) throws IOException
        {
            t.serialize(out);
        }

        @Override
        public Ballot deserialize(DataInputPlus in, int version) throws IOException
        {
            return Ballot.deserialize(in);
        }

        @Override
        public long serializedSize(Ballot t, int version)
        {
            return sizeInBytes();
        }
    }

}
