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

package org.apache.cassandra.tcm.log;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;

public class Entry implements Comparable<Entry>
{
    public static final Serializer serializer = new Serializer();

    public final Id id;
    public final Epoch epoch;
    public final Transformation transform;

    public Entry(Id id, Epoch epoch, Transformation transform)
    {
        this.id = id;
        this.epoch = epoch;
        this.transform = transform;
    }

    public Entry maybeUnwrapExecuted()
    {
        if (transform instanceof Transformation.Executed)
            return new Entry(id, epoch, ((Transformation.Executed) transform).original());

        return this;
    }

    @Override
    public int compareTo(Entry other)
    {
        return this.epoch.compareTo(other.epoch);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof Entry)) return false;
        Entry that = (Entry) o;
        return Objects.equals(id, that.id) && Objects.equals(epoch, that.epoch) && Objects.equals(transform, that.transform);
    }

    public int hashCode()
    {
        return Objects.hash(id, epoch, transform);
    }

    public String toString()
    {
        return "Entry{" +
               "id=" + id +
               ", epoch=" + epoch +
               ", transform=" + transform +
               '}';
    }

    static final class Serializer implements MetadataSerializer<Entry>
    {
        public void serialize(Entry t, DataOutputPlus out, Version version) throws IOException
        {
            Id.serializer.serialize(t.id, out, version);
            Epoch.serializer.serialize(t.epoch, out, version);
            Transformation.serializer.serialize(t.transform, out, version);
        }

        public Entry deserialize(DataInputPlus in, Version version) throws IOException
        {
            Id entryId = Id.serializer.deserialize(in, version);
            Epoch epoch = Epoch.serializer.deserialize(in, version);
            Transformation transform = Transformation.serializer.deserialize(in, version);
            return new Entry(entryId, epoch, transform);
        }

        public long serializedSize(Entry t, Version version)
        {
            return Id.serializer.serializedSize(t.id, version) +
                   Epoch.serializer.serializedSize(t.epoch, version) +
                   Transformation.serializer.serializedSize(t.transform, version);
        }
    }
    public static class Id
    {
        public static final EntryIdSerializer serializer = new EntryIdSerializer();
        public static final Id NONE = new Id(-1L);

        public final long entryId;

        public Id(long entryId)
        {
            this.entryId = entryId;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Id groupId = (Id) o;
            return entryId == groupId.entryId;
        }

        public int hashCode()
        {
            return Objects.hash(entryId);
        }

        public String toString()
        {
            return "EntryId{" +
                   "entryId=" + entryId +
                   '}';
        }

        public static class EntryIdSerializer implements MetadataSerializer<Id>
        {
            public void serialize(Id id, DataOutputPlus out, Version version) throws IOException
            {
                out.writeLong(id.entryId);
            }

            public Id deserialize(DataInputPlus in, Version version) throws IOException
            {
                return new Id(in.readLong());
            }

            public long serializedSize(Id t, Version version)
            {
                return TypeSizes.LONG_SIZE;
            }
        }
    }

    public static class DefaultEntryIdGen implements Supplier<Id>
    {
        private final AtomicLong counter = new AtomicLong(Clock.Global.currentTimeMillis() & 0x00000000ffffffffL);
        private final long addrComponent;

        public DefaultEntryIdGen()
        {
            this (FBUtilities.getBroadcastAddressAndPort());
        }

        public DefaultEntryIdGen(InetAddressAndPort addr)
        {
            // TODO properly handle ipv6
            byte[] bytes = addr.addressBytes;
            long addrComponent = 0;
            for (int i = 0; i < bytes.length; i++)
                addrComponent |= (long) bytes[i] << (i * 8);
            this.addrComponent = addrComponent << Integer.SIZE;
        }

        public Id get()
        {
            return new Id(addrComponent | counter.getAndIncrement());
        }
    }
}
