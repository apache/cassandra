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

package org.apache.cassandra.tcm.sequences;

import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.db.TypeSizes.sizeof;

public class LockedRanges
{
    public static final Serializer serializer = new Serializer();
    public static final LockedRanges EMPTY = new LockedRanges(ImmutableMap.<Key, AffectedRanges>builder().build());
    public static final Key NOT_LOCKED = new Key(Epoch.EMPTY);
    public final ImmutableMap<Key, AffectedRanges> locked;

    public LockedRanges(ImmutableMap<Key, AffectedRanges> locked)
    {
        this.locked = locked;
    }

    public LockedRanges lock(Key key, AffectedRanges ranges)
    {
        assert !key.equals(NOT_LOCKED) : "Can't lock ranges with noop key";

        if (ranges == AffectedRanges.EMPTY)
            return this;

        // TODO might we need the ability for the holder of a key to lock multiple sets over time?
        return new LockedRanges(ImmutableMap.<Key, AffectedRanges>builderWithExpectedSize(locked.size())
                .putAll(locked)
                .put(key, ranges)
                .build());
    }

    public LockedRanges unlock(Key key)
    {
        if (key.equals(NOT_LOCKED))
            return this;
        ImmutableMap.Builder<Key, AffectedRanges> builder = ImmutableMap.builderWithExpectedSize(locked.size());
        locked.forEach((k, r) -> {
            if (!k.equals(key)) builder.put(k, r);
        });
        return new LockedRanges(builder.build());
    }

    public Key intersects(AffectedRanges ranges)
    {
        for (Map.Entry<Key, AffectedRanges> e : locked.entrySet())
        {
            if (ranges.intersects(e.getValue()))
                return e.getKey();
        }
        return NOT_LOCKED;
    }

    @Override
    public String toString()
    {
        return "LockedRanges{" +
                "locked=" + locked +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof LockedRanges)) return false;
        LockedRanges that = (LockedRanges) o;
        return locked.equals(that.locked);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(locked);
    }

    public static Key keyFor(Epoch epoch)
    {
        return new Key(epoch);
    }

    public static interface AffectedRangesBuilder
    {
        AffectedRangesBuilder add(ReplicationParams params, Range<Token> range);
        AffectedRanges build();
    }

    public interface AffectedRanges
    {
        static final AffectedRanges EMPTY = new AffectedRanges()
        {
            public boolean intersects(AffectedRanges other)
            {
                return false;
            }

            public void foreach(BiConsumer<ReplicationParams, Set<Range<Token>>> fn) {}

            @Override
            public String toString()
            {
                return "EMPTY";
            }

            public Map<ReplicationParams, Set<Range<Token>>> asMap()
            {
                return Collections.emptyMap();
            }
        };

        static AffectedRanges singleton(ReplicationParams replicationParams, Range<Token> tokenRange)
        {
            return builder().add(replicationParams, tokenRange).build();
        }

        static AffectedRangesBuilder builder()
        {
            return new AffectedRangesImpl();
        }

        boolean intersects(AffectedRanges other);
        void foreach(BiConsumer<ReplicationParams, Set<Range<Token>>> fn);
        Map<ReplicationParams, Set<Range<Token>>> asMap();

        public static final class Serializer
        {
            public static final Serializer instance = new Serializer();

            public void serialize(AffectedRanges t, DataOutputPlus out, Version version) throws IOException
            {
                Map<ReplicationParams, Set<Range<Token>>> map = t.asMap();
                out.writeInt(map.size());
                for (Map.Entry<ReplicationParams, Set<Range<Token>>> rangeEntry : map.entrySet())
                {
                    ReplicationParams params = rangeEntry.getKey();
                    Set<Range<Token>> ranges = rangeEntry.getValue();
                    ReplicationParams.serializer.serialize(params, out, version);
                    out.writeInt(ranges.size());
                    for (Range<Token> range : ranges)
                    {
                        Token.metadataSerializer.serialize(range.left, out, version);
                        Token.metadataSerializer.serialize(range.right, out, version);
                    }
                }
            }

            public AffectedRanges deserialize(DataInputPlus in, IPartitioner partitioner, Version version) throws IOException
            {
                int size = in.readInt();
                Map<ReplicationParams, Set<Range<Token>>> map = Maps.newHashMapWithExpectedSize(size);
                for (int x = 0; x < size; x++)
                {
                    ReplicationParams params = ReplicationParams.serializer.deserialize(in, version);
                    int rangeSize = in.readInt();
                    Set<Range<Token>> range = Sets.newHashSetWithExpectedSize(rangeSize);
                    for (int y = 0; y < rangeSize; y++)
                    {
                        range.add(new Range<>(Token.metadataSerializer.deserialize(in, partitioner, version),
                                Token.metadataSerializer.deserialize(in, partitioner, version)));
                    }
                    map.put(params, range);
                }
                return new AffectedRangesImpl(map);
            }

            public long serializedSize(AffectedRanges t, Version version)
            {
                Map<ReplicationParams, Set<Range<Token>>> map = t.asMap();
                long size = sizeof(map.size());
                for (Map.Entry<ReplicationParams, Set<Range<Token>>> rangeEntry : map.entrySet())
                {
                    ReplicationParams params = rangeEntry.getKey();
                    Set<Range<Token>> ranges = rangeEntry.getValue();
                    size += ReplicationParams.serializer.serializedSize(params, version);
                    size += sizeof(ranges.size());
                    for (Range<Token> range : ranges)
                    {
                        size += Token.metadataSerializer.serializedSize(range.left, version);
                        size += Token.metadataSerializer.serializedSize(range.right, version);
                    }
                }
                return size;
            }
        }
    }

    private static final class AffectedRangesImpl implements AffectedRangesBuilder, AffectedRanges
    {
        private final Map<ReplicationParams, Set<Range<Token>>> map;

        public AffectedRangesImpl()
        {
            this(new HashMap<>());
        }

        public AffectedRangesImpl(Map<ReplicationParams, Set<Range<Token>>> map)
        {
            this.map = map;
        }

        @Override
        public AffectedRangesBuilder add(ReplicationParams params, Range<Token> range)
        {
            Set<Range<Token>> ranges = map.get(params);
            if (ranges == null)
            {
                ranges = new HashSet<>();
                map.put(params, ranges);
            }

            ranges.add(range);
            return this;
        }

        @Override
        public Map<ReplicationParams, Set<Range<Token>>> asMap()
        {
            return map;
        }

        @Override
        public AffectedRanges build()
        {
            return this;
        }

        @Override
        public void foreach(BiConsumer<ReplicationParams, Set<Range<Token>>> fn)
        {
            map.forEach((k, v) -> fn.accept(k, Collections.unmodifiableSet(v)));
        }

        @Override
        public boolean intersects(AffectedRanges other)
        {
            if (other == EMPTY)
                return false;

            for (Map.Entry<ReplicationParams, Set<Range<Token>>> e : ((AffectedRangesImpl) other).map.entrySet())
            {
                for (Range<Token> otherRange : e.getValue())
                {
                    for (Range<Token> thisRange : map.get(e.getKey()))
                    {
                        if (thisRange.intersects(otherRange))
                            return true;
                    }
                }
            }

            return false;
        }

        @Override
        public String toString()
        {
            return "AffectedRangesImpl{" +
                    "map=" + map +
                    '}';
        }
    }

    public static class Key
    {
        public static final Serializer serializer = new Serializer();
        private final Epoch epoch;

        private Key(Epoch epoch)
        {
            this.epoch = epoch;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key1 = (Key) o;
            return epoch.equals(key1.epoch);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(epoch);
        }

        @Override
        public String toString()
        {
            return "Key{" +
                    "key=" + epoch +
                    '}';
        }

        public static final class Serializer
        {
            public void serialize(Key t, DataOutputPlus out, Version version) throws IOException
            {
                Epoch.serializer.serialize(t.epoch, out, version);
            }

            public Key deserialize(DataInputPlus in, Version version) throws IOException
            {
                return new Key(Epoch.serializer.deserialize(in, version));
            }

            public long serializedSize(Key t, Version version)
            {
                return Epoch.serializer.serializedSize(t.epoch, version);
            }
        }
    }

    public static class Serializer
    {
        public void serialize(LockedRanges t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeInt(t.locked.size());
            for (Map.Entry<Key, AffectedRanges> entry : t.locked.entrySet())
            {
                Key key = entry.getKey();
                Epoch.serializer.serialize(key.epoch, out, version);
                AffectedRanges.Serializer.instance.serialize(entry.getValue(), out, version);
            }
        }

        // TODO have this use CM.current.tokenMap.partitioner so it can implement IMetadataSerializer?
        public LockedRanges deserialize(DataInputPlus in, IPartitioner partitioner, Version version) throws IOException
        {
            int size = in.readInt();
            if (size == 0) return new LockedRanges(ImmutableMap.of());
            ImmutableMap.Builder<Key, AffectedRanges> result = ImmutableMap.builder();
            for (int i = 0; i < size; i++)
            {
                Key key = new Key(Epoch.serializer.deserialize(in, version));
                AffectedRanges ranges = AffectedRanges.Serializer.instance.deserialize(in, partitioner, version);
                result.put(key, ranges);
            }
            return new LockedRanges(result.build());
        }

        public long serializedSize(LockedRanges t, Version version)
        {
            long size = sizeof(t.locked.size());
            for (Map.Entry<Key, AffectedRanges> entry : t.locked.entrySet())
            {
                Key key = entry.getKey();
                size += Epoch.serializer.serializedSize(key.epoch, version);
                size += AffectedRanges.Serializer.instance.serializedSize(entry.getValue(), version);
            }
            return size;
        }
    }
}