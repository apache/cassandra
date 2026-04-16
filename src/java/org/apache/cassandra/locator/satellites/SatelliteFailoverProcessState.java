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
package org.apache.cassandra.locator.satellites;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.serializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.serializedMapSize;

public class SatelliteFailoverProcessState implements MetadataValue<SatelliteFailoverProcessState>
{
    public static final SatelliteFailoverProcessState EMPTY =
        new SatelliteFailoverProcessState(Epoch.EMPTY, ImmutableMap.of());

    @Nonnull
    public final Epoch lastModified;

    @Nonnull
    public final ImmutableMap<String, KeyspaceFailoverState> keyspaceStates;

    public SatelliteFailoverProcessState(@Nonnull Epoch lastModified,
                                         @Nonnull Map<String, KeyspaceFailoverState> keyspaceStates)
    {
        checkNotNull(lastModified);
        checkNotNull(keyspaceStates);
        this.lastModified = lastModified;
        this.keyspaceStates = ImmutableMap.copyOf(keyspaceStates);
    }

    @Override
    public SatelliteFailoverProcessState withLastModified(Epoch epoch)
    {
        return new SatelliteFailoverProcessState(epoch, keyspaceStates);
    }

    @Override
    public Epoch lastModified()
    {
        return lastModified;
    }

    public SatelliteFailoverProcessState withFailoverInitiated(@Nonnull String keyspace,
                                                               @Nonnull String fromDC,
                                                               @Nonnull Epoch processStarted,
                                                               @Nonnull NormalizedRanges<Token> fullRange)
    {
        checkNotNull(keyspace);
        checkNotNull(fromDC);
        checkNotNull(processStarted);
        checkNotNull(fullRange);

        KeyspaceFailoverState ksState = KeyspaceFailoverState.create(fromDC, processStarted, fullRange);
        return withUpdatedKeyspaceState(keyspace, ksState);
    }

    public SatelliteFailoverProcessState withRangesTransitioning(@Nonnull String keyspace,
                                                                  @Nonnull NormalizedRanges<Token> ranges)
    {
        checkNotNull(keyspace);
        checkNotNull(ranges);

        KeyspaceFailoverState ksState = keyspaceStates.get(keyspace);
        if (ksState == null)
            return this;

        KeyspaceFailoverState updated = ksState.withRangesTransitioning(ranges);
        return withUpdatedKeyspaceState(keyspace, updated);
    }

    public SatelliteFailoverProcessState withRangesNormal(@Nonnull String keyspace,
                                                           @Nonnull NormalizedRanges<Token> ranges)
    {
        checkNotNull(keyspace);
        checkNotNull(ranges);

        KeyspaceFailoverState ksState = keyspaceStates.get(keyspace);
        if (ksState == null)
            return this;

        KeyspaceFailoverState updated = ksState.withRangesNormal(ranges);
        if (updated.isComplete())
            return withoutKeyspace(keyspace);

        return withUpdatedKeyspaceState(keyspace, updated);
    }

    @Nullable
    public KeyspaceFailoverState getKeyspaceState(String keyspace)
    {
        return keyspaceStates.get(keyspace);
    }

    public boolean hasActiveTransfer(String keyspace)
    {
        return keyspaceStates.containsKey(keyspace);
    }

    private SatelliteFailoverProcessState withUpdatedKeyspaceState(String keyspace, KeyspaceFailoverState state)
    {
        ImmutableMap.Builder<String, KeyspaceFailoverState> builder = ImmutableMap.builder();
        for (Map.Entry<String, KeyspaceFailoverState> entry : keyspaceStates.entrySet())
        {
            if (!entry.getKey().equals(keyspace))
                builder.put(entry.getKey(), entry.getValue());
        }
        builder.put(keyspace, state);
        return new SatelliteFailoverProcessState(lastModified, builder.build());
    }

    public SatelliteFailoverProcessState withoutKeyspace(String keyspace)
    {
        ImmutableMap.Builder<String, KeyspaceFailoverState> builder = ImmutableMap.builder();
        for (Map.Entry<String, KeyspaceFailoverState> entry : keyspaceStates.entrySet())
        {
            if (!entry.getKey().equals(keyspace))
                builder.put(entry.getKey(), entry.getValue());
        }
        return new SatelliteFailoverProcessState(lastModified, builder.build());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SatelliteFailoverProcessState that = (SatelliteFailoverProcessState) o;
        return lastModified.equals(that.lastModified)
               && keyspaceStates.equals(that.keyspaceStates);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lastModified, keyspaceStates);
    }

    @Override
    public String toString()
    {
        return "SatelliteFailoverProcessState{" +
               "keyspaces=" + keyspaceStates.keySet() +
               ", lastModified=" + lastModified +
               '}';
    }

    private static final MetadataSerializer<String> stringSerializer = new MetadataSerializer<>()
    {
        @Override
        public void serialize(String t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(t);
        }

        @Override
        public String deserialize(DataInputPlus in, Version version) throws IOException
        {
            return in.readUTF();
        }

        @Override
        public long serializedSize(String t, Version version)
        {
            return TypeSizes.sizeof(t);
        }
    };

    public static final MetadataSerializer<SatelliteFailoverProcessState> serializer = new MetadataSerializer<>()
    {
        @Override
        public void serialize(SatelliteFailoverProcessState t, DataOutputPlus out, Version version) throws IOException
        {
            Epoch.serializer.serialize(t.lastModified, out, version);
            serializeMap(t.keyspaceStates, out, version, stringSerializer, KeyspaceFailoverState.serializer);
        }

        @Override
        public SatelliteFailoverProcessState deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            Map<String, KeyspaceFailoverState> states = deserializeMap(in, version, stringSerializer, KeyspaceFailoverState.serializer, Maps::newHashMapWithExpectedSize);
            return new SatelliteFailoverProcessState(lastModified, states);
        }

        @Override
        public long serializedSize(SatelliteFailoverProcessState t, Version version)
        {
            return Epoch.serializer.serializedSize(t.lastModified, version)
                   + serializedMapSize(t.keyspaceStates, version, stringSerializer, KeyspaceFailoverState.serializer);
        }
    };
}
