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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.SignedBytes;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.vint.VIntCoding;

public class Epoch implements Comparable<Epoch>, Serializable
{
    public static final EpochSerializer serializer = new EpochSerializer();
    public static final IVersionedSerializer<Epoch> messageSerializer = new IVersionedSerializer<Epoch>()
    {
        @Override
        public void serialize(Epoch t, DataOutputPlus out, int version) throws IOException
        {
            serializer.serialize(t, out);
        }

        @Override
        public Epoch deserialize(DataInputPlus in, int version) throws IOException
        {
            return serializer.deserialize(in);
        }

        @Override
        public long serializedSize(Epoch t, int version)
        {
            return serializer.serializedSize(t);
        }
    };

    public static final Epoch FIRST = new Epoch(1, 1);
    public static final Epoch EMPTY = new Epoch(0, 0);
    public static final Epoch UPGRADE_STARTUP = new Epoch(Long.MIN_VALUE, Long.MIN_VALUE);
    public static final Epoch UPGRADE_GOSSIP = new Epoch(Long.MIN_VALUE, Long.MIN_VALUE + 1);
    private static final Set<Epoch> beforeFirst = Sets.newHashSet(EMPTY, UPGRADE_GOSSIP, UPGRADE_STARTUP);

    private final long period;
    private final long epoch;

    private Epoch(long period, long epoch)
    {
        this.period = period;
        this.epoch = epoch;
    }

    public static Epoch create(long period, long epoch)
    {
        if (period == EMPTY.period && epoch == EMPTY.epoch)
            return EMPTY;
        if (period == UPGRADE_GOSSIP.period)
        {
            if (epoch == UPGRADE_GOSSIP.epoch)
                return UPGRADE_GOSSIP;
            if (epoch == UPGRADE_STARTUP.epoch)
                return UPGRADE_STARTUP;
        }
        if (period == FIRST.period && epoch == FIRST.epoch)
            return FIRST;
        return new Epoch(period, epoch);
    }

    public static Epoch max(Epoch l, Epoch r)
    {
        return l.compareTo(r) > 0 ? l : r;
    }

    public static Epoch min(Epoch l, Epoch r)
    {
        return l.compareTo(r) < 0 ? l : r;
    }

    public boolean isDirectlyBefore(Epoch epoch)
    {
        if (epoch.equals(Epoch.FIRST))
            return beforeFirst.contains(this);
        return this.epoch + 1 == epoch.epoch;
    }

    public boolean isDirectlyAfter(Epoch epoch)
    {
        return epoch.isDirectlyBefore(this);
    }

    public Epoch nextEpoch(boolean newPeriod)
    {
        if (beforeFirst.contains(this))
            return FIRST;
        if (newPeriod)
            return new Epoch(getPeriod() + 1, getEpoch() + 1);
        else
            return new Epoch(getPeriod(), getEpoch() + 1);
    }

    @Override
    public int compareTo(Epoch other)
    {
        int cmp = Long.compare(period, other.period);
        if (cmp != 0)
            return cmp;
        return Long.compare(epoch, other.epoch);
    }

    public boolean isBefore(Epoch other)
    {
        return compareTo(other) < 0;
    }

    public boolean isEqualOrBefore(Epoch other)
    {
        return compareTo(other) <= 0;
    }

    public boolean isAfter(Epoch other)
    {
        return compareTo(other) > 0;
    }

    public boolean isEqualOrAfter(Epoch other)
    {
        return compareTo(other) >= 0;
    }

    public boolean is(Epoch other)
    {
        return equals(other);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof Epoch)) return false;
        Epoch epoch1 = (Epoch) o;
        return period == epoch1.period && epoch == epoch1.epoch;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(period, epoch);
    }

    @Override
    public String toString()
    {
        return "Epoch{" +
                "period=" + period +
                ", epoch=" + epoch +
                '}';
    }

    public long getPeriod()
    {
        return period;
    }

    public long getEpoch()
    {
        return epoch;
    }

    public ByteBuffer toVersionedBytes()
    {
        int periodSize = Ints.checkedCast(VIntCoding.computeUnsignedVIntSize(period));
        ByteBuffer versionedBytes = ByteBuffer.allocate(1 + periodSize + VIntCoding.computeUnsignedVIntSize(epoch));
        versionedBytes.put(SignedBytes.checkedCast(Version.V0.asInt()));
        VIntCoding.writeUnsignedVInt(period, versionedBytes);
        VIntCoding.writeUnsignedVInt(epoch, versionedBytes);
        return (ByteBuffer)versionedBytes.flip();
    }

    public static Epoch fromVersionedBytes(ByteBuffer buffer)
    {
        // Checks version is valid
        Version.fromInt(buffer.get());
        long period = VIntCoding.getUnsignedVInt(buffer, 1);
        long epoch = VIntCoding.getUnsignedVInt(buffer, 1 + VIntCoding.computeUnsignedVIntSize(period));
        return create(period, epoch);
    }

    public static class EpochSerializer
    {
        // convenience methods for messageSerializer et al
        public void serialize(Epoch t, DataOutputPlus out) throws IOException
        {
            serialize(t, out, Version.V0);
        }

        public Epoch deserialize(DataInputPlus in) throws IOException
        {
            return deserialize(in, Version.V0);
        }

        public long serializedSize(Epoch t)
        {
            return serializedSize(t, Version.V0);
        }

        public void serialize(Epoch t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUnsignedVInt(t.period);
            out.writeUnsignedVInt(t.epoch);
        }

        public Epoch deserialize(DataInputPlus in, Version version) throws IOException
        {
            return Epoch.create(in.readUnsignedVInt(),
                    in.readUnsignedVInt());
        }

        public long serializedSize(Epoch t, Version version)
        {
            return VIntCoding.computeUnsignedVIntSize(t.epoch) +
                    VIntCoding.computeUnsignedVIntSize(t.period);
        }
    }
}