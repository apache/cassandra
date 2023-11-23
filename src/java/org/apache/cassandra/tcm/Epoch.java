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
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
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

    public static final Epoch FIRST = new Epoch(1);
    public static final Epoch EMPTY = new Epoch(0);
    public static final Epoch UPGRADE_STARTUP = new Epoch(Long.MIN_VALUE);
    public static final Epoch UPGRADE_GOSSIP = new Epoch(Long.MIN_VALUE + 1);
    private static final Set<Epoch> beforeFirst = Sets.newHashSet(EMPTY, UPGRADE_GOSSIP, UPGRADE_STARTUP);

    private final long epoch;

    private Epoch(long epoch)
    {
        this.epoch = epoch;
    }

    public static Epoch create(long epoch)
    {
        if (epoch == EMPTY.epoch)
            return EMPTY;
        if (epoch == UPGRADE_GOSSIP.epoch)
            return UPGRADE_GOSSIP;
        if (epoch == UPGRADE_STARTUP.epoch)
            return UPGRADE_STARTUP;
        if (epoch == FIRST.epoch)
            return FIRST;
        return new Epoch(epoch);
    }

    public static Epoch max(Epoch l, Epoch r)
    {
        return l.compareTo(r) > 0 ? l : r;
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

    public Epoch nextEpoch()
    {
        if (beforeFirst.contains(this))
            return FIRST;

        return new Epoch(epoch + 1);
    }

    @Override
    public int compareTo(Epoch other)
    {
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
        return epoch == epoch1.epoch;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(epoch);
    }

    @Override
    public String toString()
    {
        return "Epoch{" +
               "epoch=" + epoch +
               '}';
    }

    public long getEpoch()
    {
        return epoch;
    }

    public static class EpochSerializer implements MetadataSerializer<Epoch>
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
            out.writeUnsignedVInt(t.epoch);
        }

        public Epoch deserialize(DataInputPlus in, Version version) throws IOException
        {
            return Epoch.create(in.readUnsignedVInt());
        }

        public long serializedSize(Epoch t, Version version)
        {
            return VIntCoding.computeUnsignedVIntSize(t.epoch);
        }
    }
}
