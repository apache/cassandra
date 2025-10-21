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
import java.util.stream.Collectors;

import org.junit.Test;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Routables;
import accord.primitives.RoutingKeys;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.AccordGenerators;

import static accord.utils.Property.qt;

public class IVersionedWithKeysSerializerTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void test()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(Gens.random(), routables()).check((rs, superset) -> {
            var serializer = serializer(superset);
            Serializers.testSerde(output, serializer, superset);
            if (superset.isEmpty()) return;
            // find subsets
            Gen<Routables<?>> gen = subset(superset);
            for (int i = 0; i < 100; i++)
                Serializers.testSerde(output, serializer, gen.next(rs));
        });
    }

    private static Gen<Routables<?>> routables()
    {
        Gen<IPartitioner> partitionerGen = AccordGenerators.partitioner();
        Gen<Routable.Kind> routableKindGen = Gens.enums().all(Routable.Kind.class);
        return rs -> {
            IPartitioner partitioner = partitionerGen.next(rs);
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            switch (routableKindGen.next(rs))
            {
                case SeekableKey:   return seekablekeysSuperset(rs, partitioner);
                case UnseekableKey: return unseekablekeysSuperset(rs, partitioner);
                case Range:         return rangesSuperset(rs, partitioner);
                default: throw new UnsupportedOperationException();
            }
        };
    }

    static Keys seekablekeysSuperset(RandomSource rs, IPartitioner partitioner)
    {
        return Keys.of(Gens.lists(AccordGenerators.keys(partitioner)).unique().ofSizeBetween(0, 100).next(rs));
    }

    private static RoutingKeys unseekablekeysSuperset(RandomSource rs, IPartitioner partitioner)
    {
        return RoutingKeys.of(Gens.arrays(RoutingKey.class, (Gen<RoutingKey>) (Gen<?>) AccordGenerators.routingKeysGen(partitioner)).unique().ofSizeBetween(0, 100).next(rs));
    }

    static Ranges rangesSuperset(RandomSource rs, IPartitioner partitioner)
    {
        return AccordGenerators.rangesSplitOrArbitrary(partitioner, Gens.ints().between(0, 100)).next(rs);
    }

    private static Gen<Routables<?>> subset(Routables<?> superset)
    {
        switch (superset.domainKind())
        {
            case SeekableKey:   return seekablekeysSubset((Keys) superset);
            case UnseekableKey: return unseekablekeysSubset((RoutingKeys) superset);
            case Range:         return rangesSubset((Ranges) superset);
            default: throw new UnsupportedOperationException();
        }
    }

    private static Gen<Routables<?>> seekablekeysSubset(Keys superset)
    {
        return Gens.select(superset.stream().collect(Collectors.toList())).map(l -> Keys.of(l.toArray(Key[]::new)));
    }

    private static Gen<Routables<?>> unseekablekeysSubset(RoutingKeys superset)
    {
        return Gens.select(superset.stream().collect(Collectors.toList())).map(l -> RoutingKeys.of(l.toArray(RoutingKey[]::new)));
    }

    private static Gen<Routables<?>> rangesSubset(Ranges superset)
    {
        return Gens.select(superset.stream().collect(Collectors.toList())).map(l -> Ranges.of(l.toArray(Range[]::new)));
    }

    private static UnversionedSerializer<Routables<?>> serializer(Routables<?> superset)
    {
        class S extends IVersionedWithKeysSerializer.AbstractWithKeysSerializer implements UnversionedSerializer<Routables<?>>
        {
            @Override
            public void serialize(Routables<?> t, DataOutputPlus out) throws IOException
            {
                serializeSubsetInternal(t, superset, out);
            }

            @Override
            public Routables<?> deserialize(DataInputPlus in) throws IOException
            {
                return deserializeSubsetInternal(superset, in);
            }

            @Override
            public long serializedSize(Routables<?> t)
            {
                return serializedSubsetSizeInternal(t, superset);
            }

            @Override
            public void skip(DataInputPlus in) throws IOException
            {
                skipSubsetInternal(superset.size(), in);
            }
        }
        return new S();
    }
}