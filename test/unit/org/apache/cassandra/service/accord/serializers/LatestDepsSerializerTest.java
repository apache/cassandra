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
import java.util.Arrays;

import org.junit.Test;

import accord.api.RoutingKey;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Known;
import accord.primitives.LatestDeps;
import accord.primitives.Txn;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.RandomTestRunner;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.CassandraGenerators;

import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routable.Domain.Range;
import static org.apache.cassandra.utils.AccordGenerators.fromQT;

public class LatestDepsSerializerTest
{
    {
        DatabaseDescriptor.toolInitialization();
    }

    @Test
    public void emptySerializerTest() throws Throwable
    {
        DataOutputBuffer buf = new DataOutputBuffer();
        Serializers.testSerde(buf, LatestDepsSerializers.latestDeps, LatestDeps.EMPTY);
    }

    @Test
    public void testN()
    {
        for (int i = 0 ; i < 10000 ; ++i)
        {
            Gen<IPartitioner> partitioners = AccordGenerators.partitioner();
            RandomTestRunner.test().check(rs -> {
                try
                {
                    testOne(partitioners.next(rs), rs);
                }
                catch (IOException e)
                {
                    throw new AssertionError(e);
                }
            });
        }
    }

    private void testOne(IPartitioner partitioner, RandomSource rs) throws IOException
    {
        DatabaseDescriptor.setPartitionerUnsafe(partitioner);
        TableId tableId = fromQT(CassandraGenerators.TABLE_ID_GEN).next(rs);
        Gen<Token> tokens = fromQT(CassandraGenerators.token(partitioner));
        Gen<TokenKey> routingKeys = AccordGenerators.routingKeyGen(ignore -> tableId, tokens, partitioner);
        Gen<Deps> deps = AccordGens.deps(AccordGens.keyDeps(routingKeys, AccordGens.txnIds(Gens.pick(Txn.Kind.values()), ignore -> Key)),
                                         AccordGens.rangeDeps(AccordGenerators.range(partitioner, ignore -> tableId), AccordGens.txnIds(Gens.pick(Txn.Kind.values()), ignore -> Range)));
        Gen<Known.KnownDeps> knownDeps = Gens.pick(Known.KnownDeps.values());
        Gen<Ballot> ballots = AccordGens.ballot();
        int size = 1 + rs.nextInt(7);
        RoutingKey[] starts = new RoutingKey[size + 1];
        LatestDeps.LatestEntry[] entries = new LatestDeps.LatestEntry[size];
        for (int i = 0 ; i <= size ; ++i)
            starts[i] = routingKeys.next(rs);
        Arrays.sort(starts);
        for (int i = 0 ; i < size ; ++i)
        {
            if (rs.nextBoolean()) continue;
            entries[i] = new LatestDeps.LatestEntry(knownDeps.next(rs),
                                                    rs.nextBoolean() ? rs.nextBoolean() ? Ballot.ZERO : Ballot.MAX : ballots.next(rs),
                                                    rs.nextBoolean() ? null : deps.next(rs),
                                                    rs.nextBoolean() ? null : deps.next(rs));
        }
        LatestDeps latestDeps = LatestDeps.SerializerSupport.create(true, starts, entries);
        DataOutputBuffer buf = new DataOutputBuffer();
        Serializers.testSerde(buf, LatestDepsSerializers.latestDeps, latestDeps);
    }


}