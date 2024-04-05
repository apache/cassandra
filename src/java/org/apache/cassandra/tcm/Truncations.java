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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class Truncations implements MetadataValue<Truncations>
{
    public static final Truncations EMPTY = new Truncations(Epoch.EMPTY, ImmutableMap.of());

    public static final Serializer serializer = new Serializer();

    private final Epoch lastModified;
    private final ImmutableMap<UUID, Long> tablesTruncations;

    public Truncations(Epoch lastModified, ImmutableMap<UUID, Long> tablesTruncations)
    {
        this.lastModified = lastModified;
        this.tablesTruncations = tablesTruncations;
    }

    @Override
    public Truncations withLastModified(Epoch epoch)
    {
        return new Truncations(epoch, tablesTruncations);
    }

    @Override
    public Epoch lastModified()
    {
        return lastModified;
    }

    public ImmutableMap<UUID, Long> diff(Truncations next)
    {
        Map<UUID, Long> diff = new HashMap<>();
        for (Map.Entry<UUID, Long> entry : next.tablesTruncations.entrySet())
        {
            UUID maybeNewKey = entry.getKey();
            long maybeNewValue = entry.getValue();
            Long maybeAlreadyExistingTruncation = tablesTruncations.get(maybeNewKey);
            if (maybeAlreadyExistingTruncation == null || maybeNewValue > maybeAlreadyExistingTruncation)
                diff.put(maybeNewKey, maybeNewValue);
        }
        return ImmutableMap.copyOf(diff);
    }

    public Truncations withTruncation(TableId tableId, long trucationTimestamp)
    {
        Map<UUID, Long> map = new HashMap<>(tablesTruncations);
        Long maybeExistingTimestamp = map.get(tableId.asUUID());

        // it does not make sense to insert truncation with timestamp smaller than what is already there
        if (maybeExistingTimestamp == null || maybeExistingTimestamp < trucationTimestamp)
        {
            map.put(tableId.asUUID(), trucationTimestamp);
            return new Truncations(lastModified, ImmutableMap.copyOf(map));
        }

        return this;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Truncations that = (Truncations) o;
        return Objects.equals(lastModified, that.lastModified) && Objects.equals(tablesTruncations, that.tablesTruncations);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lastModified, tablesTruncations);
    }

    @Override
    public String toString()
    {
        return "Truncations{" +
               "lastModified=" + lastModified +
               ", truncations=" + tablesTruncations +
               '}';
    }

    public static final class Serializer implements MetadataSerializer<Truncations>
    {

        @Override
        public void serialize(Truncations t, DataOutputPlus out, Version version) throws IOException
        {
            Epoch.serializer.serialize(t.lastModified, out, version);
            out.writeInt(t.tablesTruncations.size());
            for (Map.Entry<UUID, Long> entry : t.tablesTruncations.entrySet())
            {
                TableId.fromUUID(entry.getKey()).serialize(out);
                out.writeLong(entry.getValue());
            }
        }

        @Override
        public Truncations deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            int size = in.readInt();
            if (size == 0) return new Truncations(lastModified, ImmutableMap.of());
            ImmutableMap.Builder<UUID, Long> result = ImmutableMap.builder();
            for (int i = 0; i < size; i++)
            {
                UUID tableId = TableId.deserialize(in).asUUID();
                Long truncationRecord = in.readLong();
                result.put(tableId, truncationRecord);
            }
            return new Truncations(lastModified, result.build());
        }

        @Override
        public long serializedSize(Truncations t, Version version)
        {
            long size = Epoch.serializer.serializedSize(t.lastModified, version);
            size += TypeSizes.sizeof(t.tablesTruncations.size());
            for (Map.Entry<UUID, Long> entry : t.tablesTruncations.entrySet())
            {
                size += 16; // TableId.serializedSize(), not sure why it is not static
                size += TypeSizes.sizeof(entry.getValue());
            }
            return size;
        }
    }
}
