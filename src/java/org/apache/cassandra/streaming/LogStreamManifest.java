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

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.EmbeddedAsymmetricVersionedSerializer;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.replication.Version;
import org.apache.cassandra.replication.VersionedSerializer;
import org.apache.cassandra.utils.StringSerializer;

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

    public static class Serializer implements VersionedSerializer<LogStreamManifest>
    {
        private static final VersionedSerializer<Range<Token>> rangeSerializer = new VersionedSerializer<>()
        {
            @Override
            public void serialize(Range<Token> range, DataOutputPlus out, Version version) throws IOException
            {
                Token.serializer.serialize(range.left, out, version.messagingVersion());
                Token.serializer.serialize(range.right, out, version.messagingVersion());
            }

            @Override
            public Range<Token> deserialize(DataInputPlus in, Version version) throws IOException
            {
                return new Range<>(
                    Token.serializer.deserialize(in, version.messagingVersion()),
                    Token.serializer.deserialize(in, version.messagingVersion())
                );
            }

            @Override
            public long serializedSize(Range<Token> range, Version version)
            {
                return Token.serializer.serializedSize(range.left, version.messagingVersion())
                     + Token.serializer.serializedSize(range.right, version.messagingVersion());
            }
        };

        private static final VersionedSerializer<ImmutableSet<Range<Token>>> rangeSetSerializer = new VersionedSerializer<>()
        {
            @Override
            public void serialize(ImmutableSet<Range<Token>> t, DataOutputPlus out, Version version) throws IOException
            {
                serializeCollection(t, out, version, rangeSerializer);
            }

            @Override
            public ImmutableSet<Range<Token>> deserialize(DataInputPlus in, Version version) throws IOException
            {
                ImmutableSet.Builder<Range<Token>> builder = ImmutableSet.builder();
                deserializeCollectionToConsumer(in, version, rangeSerializer, builder::add);
                return builder.build();
            }

            @Override
            public long serializedSize(ImmutableSet<Range<Token>> t, Version version)
            {
                return serializedCollectionSize(t, version, rangeSerializer);
            }
        };

        @Override
        public void serialize(LogStreamManifest header, DataOutputPlus out, Version version) throws IOException
        {
            serializeMap(header.keyspaceRanges, out, version, StringSerializer.instance, rangeSetSerializer);
        }

        @Override
        public LogStreamManifest deserialize(DataInputPlus in, Version version) throws IOException
        {
            ImmutableMap.Builder<String, ImmutableSet<Range<Token>>> builder = ImmutableMap.builder();
            deserializeMapToConsumer(in, version, StringSerializer.instance, rangeSetSerializer, builder::put);
            return new LogStreamManifest(builder.build());
        }

        @Override
        public long serializedSize(LogStreamManifest header, Version version)
        {
            return serializedMapSize(header.keyspaceRanges, version, StringSerializer.instance, rangeSetSerializer);
        }
    }

    public static final Serializer serializer = new Serializer();
    public static final IVersionedAsymmetricSerializer<LogStreamManifest, LogStreamManifest> embedded =
        EmbeddedAsymmetricVersionedSerializer.mtEmbedded(serializer);

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
