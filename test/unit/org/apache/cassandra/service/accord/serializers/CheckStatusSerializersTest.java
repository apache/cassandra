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

import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import accord.api.RoutingKey;
import accord.primitives.SaveStatus;
import accord.primitives.KnownMap;
import accord.primitives.Ballot;
import accord.primitives.FullKeyRoute;
import accord.primitives.Routable;
import accord.primitives.Unseekables;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.CassandraGenerators;

import static accord.utils.Property.qt;
import static org.apache.cassandra.utils.AccordGenerators.fromQT;

public class CheckStatusSerializersTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void serde()
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        qt().forAll(foundKnownMap()).check(map -> Serializers.testSerde(buffer, CheckStatusSerializers.knownMap, map));
    }

    private static Gen<KnownMap> foundKnownMap()
    {
        return rs -> {
            SaveStatus saveStatus = Gens.pick(SaveStatus.values()).next(rs);
            Ballot promised = AccordGens.ballot().next(rs);
            Routable.Domain domain = Gens.pick(Routable.Domain.values()).next(rs);
            Unseekables<?> keysOrRanges;
            switch (domain)
            {
                case Key:
                    // TODO (desired): don't hard code murmur
                    Gen<TokenKey> keyGen = AccordGenerators.routingKeyGen(fromQT(CassandraGenerators.TABLE_ID_GEN), Gens.constant(AccordGenerators.RoutingKeyKind.TOKEN), fromQT(CassandraGenerators.murmurToken()), Murmur3Partitioner.instance);
                    TokenKey homeKey = keyGen.next(rs);
                    List<TokenKey> forOrdering = Gens.lists(keyGen).unique().ofSizeBetween(1, 10).next(rs);
                    forOrdering.sort(Comparator.naturalOrder());
                    // TODO (desired): don't hard code keys type
                    keysOrRanges = new FullKeyRoute(homeKey, forOrdering.toArray(RoutingKey[]::new));
                    break;
                case Range:
                    keysOrRanges = AccordGenerators.ranges(Murmur3Partitioner.instance).next(rs);
                    break;
                default:
                    throw new AssertionError("Unknown domain");
            }
            return KnownMap.create(keysOrRanges, saveStatus);
        };
    }
}