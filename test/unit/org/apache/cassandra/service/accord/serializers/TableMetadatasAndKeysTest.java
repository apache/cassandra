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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.junit.Test;

import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.AccordGenerators;

import static accord.utils.Property.qt;
import static org.apache.cassandra.service.accord.serializers.TableMetadatasTest.buildSchema;
import static org.apache.cassandra.service.accord.serializers.TableMetadatasTest.toMetadatas;

public class TableMetadatasAndKeysTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static final Gen<IPartitioner> partitionerGen = AccordGenerators.partitioner();

    @Test
    public void test()
    {
        Gen<Routable.Domain> domainGen = Gens.enums().all(Routable.Domain.class);
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(Gens.random(), TableMetadatasTest.tables().filter(m -> !m.isEmpty())).check((rs, tables) -> {
            TableMetadatas metadatas = toMetadatas(tables);
            Schema.instance = buildSchema(tables);

            Seekables<?, ?> keysOrRanges;
            switch (domainGen.next(rs))
            {
                case Key:
                    keysOrRanges = createKeys(tables).next(rs);
                    break;
                case Range:
                    keysOrRanges = createRanges(tables).next(rs);
                    break;
                default:    throw new UnsupportedOperationException();
            }

            TableMetadatasAndKeys tablesAndKeys = new TableMetadatasAndKeys(metadatas, keysOrRanges);

            Serializers.testSerde(output, TableMetadatasAndKeys.serializer, tablesAndKeys);
            var serializer = serializer(tablesAndKeys);
            var partitionSerializer = partitionKeySerializer(tablesAndKeys);
            for (Seekable s : keysOrRanges)
            {
                Serializers.testSerde(output, serializer, s);
                if (s instanceof PartitionKey)
                    Serializers.testSerde(output, partitionSerializer, (PartitionKey) s);
            }
        });
    }

    private static Gen<Keys> createKeys(LinkedHashMap<TableId, TableMetadata> tables)
    {
        return rs -> {
            IPartitioner partitioner = partitionerGen.next(rs);
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            Gen<PartitionKey> keyGen = AccordGenerators.keys(partitioner, new ArrayList<>(tables.keySet()));
            return Keys.of(Gens.lists(keyGen).unique().ofSizeBetween(1, 100).next(rs));
        };
    }

    private static Gen<Ranges> createRanges(LinkedHashMap<TableId, TableMetadata> tables)
    {
        return rs -> {
            var partitioner = partitionerGen.next(rs);
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            return AccordGenerators.rangesSplitOrArbitrary(partitioner, Gens.ints().between(1, 100), Gens.constant(new ArrayList<>(tables.keySet()))).next(rs);
        };
    }

    private static UnversionedSerializer<Seekable> serializer(TableMetadatasAndKeys tableAndKeys)
    {
        return new UnversionedSerializer<>()
        {
            @Override
            public void serialize(Seekable t, DataOutputPlus out) throws IOException
            {
                tableAndKeys.serializeSeekable(t, out);
            }

            @Override
            public Seekable deserialize(DataInputPlus in) throws IOException
            {
                return tableAndKeys.deserializeSeekable(in);
            }

            @Override
            public long serializedSize(Seekable t)
            {
                return tableAndKeys.serializedSeekableSize(t);
            }
        };
    }

    public static UnversionedSerializer<PartitionKey> partitionKeySerializer(TableMetadatasAndKeys tableAndKeys)
    {
        return new UnversionedSerializer<>()
        {
            @Override
            public void serialize(PartitionKey t, DataOutputPlus out) throws IOException
            {
                tableAndKeys.serializeKey(t, out);
            }

            @Override
            public PartitionKey deserialize(DataInputPlus in) throws IOException
            {
                return tableAndKeys.deserializeKey(in);
            }

            @Override
            public long serializedSize(PartitionKey t)
            {
                return tableAndKeys.serializedKeySize(t);
            }
        };
    }
}