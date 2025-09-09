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

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.utils.CollectionSerializers.deserializeCollectionToConsumer;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeMapToConsumer;
import static org.apache.cassandra.utils.CollectionSerializers.serializeCollection;
import static org.apache.cassandra.utils.CollectionSerializers.serializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.serializedCollectionSize;
import static org.apache.cassandra.utils.CollectionSerializers.serializedMapSize;

public class LogStreamManifest
{
    public final ImmutableMap<String, ImmutableSet<Range<Token>>> keyspaceRanges;

    public LogStreamManifest(ImmutableMap<String, ImmutableSet<Range<Token>>> keyspaceRanges)
    {
        this.keyspaceRanges = keyspaceRanges;
    }

    public static LogStreamManifest create(Map<String, Set<Range<Token>>> keyspaceRanges)
    {
        ImmutableMap.Builder<String, ImmutableSet<Range<Token>>> builder = ImmutableMap.builder();
        keyspaceRanges.forEach((keyspace, ranges) -> builder.put(keyspace, ImmutableSet.copyOf(ranges)));
        return new LogStreamManifest(builder.build());
    }

    public static class Serializer
    {
        private static final IVersionedSerializer<String> strSerializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(String str, DataOutputPlus out, int version) throws IOException
            {
                out.writeUTF(str);
            }

            @Override
            public String deserialize(DataInputPlus in, int version) throws IOException
            {
                return in.readUTF();
            }

            @Override
            public long serializedSize(String str, int version)
            {
                return TypeSizes.sizeof(str);
            }
        };

        private static final IVersionedSerializer<Range<Token>> rangeSerializer = new IVersionedSerializer<Range<Token>>()
        {
            @Override
            public void serialize(Range<Token> range, DataOutputPlus out, int version) throws IOException
            {
                Token.serializer.serialize(range.left, out, version);
                Token.serializer.serialize(range.right, out, version);
            }

            @Override
            public Range<Token> deserialize(DataInputPlus in, int version) throws IOException
            {
                return new Range<>(Token.serializer.deserialize(in, version), Token.serializer.deserialize(in, version));
            }

            @Override
            public long serializedSize(Range<Token> range, int version)
            {
                return Token.serializer.serializedSize(range.left, version)
                       + Token.serializer.serializedSize(range.right, version);
            }
        };

        private static final IVersionedSerializer<ImmutableSet<Range<Token>>> rangeSetSerializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(ImmutableSet<Range<Token>> t, DataOutputPlus out, int version) throws IOException
            {
                serializeCollection(t, out, version, rangeSerializer);
            }

            @Override
            public ImmutableSet<Range<Token>> deserialize(DataInputPlus in, int version) throws IOException
            {
                ImmutableSet.Builder<Range<Token>> builder = ImmutableSet.builder();
                deserializeCollectionToConsumer(in, version, rangeSerializer, builder::add);
                return builder.build();
            }

            @Override
            public long serializedSize(ImmutableSet<Range<Token>> t, int version)
            {
                return serializedCollectionSize(t, version, rangeSerializer);
            }
        };


        public void serialize(LogStreamManifest header, DataOutputPlus out, int version) throws IOException
        {
            serializeMap(header.keyspaceRanges, out, version, strSerializer, rangeSetSerializer);
        }

        public LogStreamManifest deserialize(DataInputPlus in, int version) throws IOException
        {
            ImmutableMap.Builder<String, ImmutableSet<Range<Token>>> builder = ImmutableMap.builder();
            deserializeMapToConsumer(in, version, strSerializer, rangeSetSerializer, builder::put);
            return new LogStreamManifest(builder.build());
        }

        public long serializedSize(LogStreamManifest header, int version)
        {
            return serializedMapSize(header.keyspaceRanges, version, strSerializer, rangeSetSerializer);
        }
    }

    public static final Serializer serializer = new Serializer();

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        LogStreamManifest that = (LogStreamManifest) o;
        return Objects.equals(keyspaceRanges, that.keyspaceRanges);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(keyspaceRanges);
    }

    @Override
    public String toString()
    {
        return String.format("MutationLogStreamHeader{keyspaceRanges=%s}", keyspaceRanges);
    }
}
