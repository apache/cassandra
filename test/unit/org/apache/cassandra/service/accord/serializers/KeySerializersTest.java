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
import accord.local.StoreParticipants;
import accord.primitives.AbstractRanges;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.FullKeyRoute;
import accord.primitives.FullRangeRoute;
import accord.primitives.KeyRoute;
import accord.primitives.PartialKeyRoute;
import accord.primitives.PartialRangeRoute;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.RangeRoute;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Unseekable;
import accord.utils.Gen;
import accord.utils.RandomSource;
import accord.utils.RandomTestRunner;
import accord.utils.UnhandledEnum;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.CassandraGenerators;

import static accord.utils.Property.qt;
import static org.apache.cassandra.utils.AccordGenerators.fromQT;
import static org.apache.cassandra.utils.AccordGenerators.maybeUpdatePartitioner;
import static org.apache.cassandra.utils.AccordGenerators.partitioner;

public class KeySerializersTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        // If the first example is "[]" then need a partitioner for static init
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void ranges()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(rangesGen()).check(expected -> {
            maybeUpdatePartitioner(expected);
            Serializers.testSerde(output, KeySerializers.ranges, expected);
        });
    }

    @Test
    public void storeParticipants()
    {
        DataOutputBuffer output = new DataOutputBuffer();
        for (int i = 0 ; i < 10000 ; ++i)
        {
            RandomTestRunner.test().check(rs -> testTwo(rs, output));
        }
    }

    private void testTwo(RandomSource rs, DataOutputBuffer output)
    {
        IPartitioner partitioner = partitioner().next(rs);
        DatabaseDescriptor.setPartitionerUnsafe(partitioner);
        testOne(rs, output, keyRoute(partitioner, rs));
        testOne(rs, output, rangeRoute(partitioner, rs));
    }

    private void testOne(RandomSource rs, DataOutputBuffer output, Participants<?> superset)
    {
        Route<?> route = null;
        if (rs.nextBoolean()) superset = ((Route<?>)superset).participantsOnly();
        else route = (Route<?>)subset(rs, superset, false);
        Participants<?> hasTouched = subset(rs, superset, true);
        Participants<?> touches = subset(rs, hasTouched, true);
        Participants<?> owns = subset(rs, touches, true);
        Participants<?> executes = rs.nextBoolean() ? subset(rs, owns, true) : null;
        Participants<?> waitsOn = executes != null ? subset(rs, executes, true) : null;
        StoreParticipants participants = StoreParticipants.create(route, owns, executes, waitsOn, touches, hasTouched);
        try
        {
            Serializers.testSerde(output, CommandSerializers.participants, participants);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static KeyRoute keyRoute(IPartitioner partitioner, RandomSource rs)
    {
        TableId tableId = fromQT(CassandraGenerators.TABLE_ID_GEN).next(rs);
        Gen<Token> tokenGen = fromQT(CassandraGenerators.token(partitioner));
        Gen<TokenKey> keyGen = AccordGenerators.routingKeyGen(ignore -> tableId, tokenGen, partitioner);
        RoutingKey[] ks = new RoutingKey[rs.nextInt(1, 10)];
        for (int i = 0 ; i < ks.length ; ++i)
            ks[i] = keyGen.next(rs);
        Arrays.sort(ks);
        int count = 1;
        for (int i = 1 ; i < ks.length ; ++i)
        {
            if (!ks[count - 1].equals(ks[i]))
                ks[count++] = ks[i];
        }
        if (count != ks.length)
            ks = Arrays.copyOf(ks, count);

        float f = rs.nextFloat();
        if (f < 0.66f)
        {
            int homeKey = rs.nextInt(ks.length);
            return f < 0.33f ? new FullKeyRoute(ks[homeKey], ks) : new PartialKeyRoute(ks[homeKey], ks);
        }
        return new PartialKeyRoute(keyGen.next(rs), ks);
    }

    private static RangeRoute rangeRoute(IPartitioner partitioner, RandomSource rs)
    {
        TableId tableId = fromQT(CassandraGenerators.TABLE_ID_GEN).next(rs);
        Gen<Token> tokenGen = fromQT(CassandraGenerators.token(partitioner));
        Gen<TokenKey> keyGen = AccordGenerators.routingKeyGen(ignore -> tableId, tokenGen, partitioner);
        TokenKey[] ks = new TokenKey[rs.nextInt(1, 10) * 2];
        for (int i = 0 ; i < ks.length ; ++i)
            ks[i] = keyGen.next(rs);
        Arrays.sort(ks);
        int count = 1;
        for (int i = 1 ; i < ks.length ; ++i)
        {
            if (!ks[count - 1].equals(ks[i]))
                ks[count++] = ks[i];
        }
        Range[] ranges = new Range[count / 2];
        for (int i = 0 ; i < ranges.length ; ++i)
            ranges[i] = TokenRange.create(ks[i*2], ks[i*2+1]);

        float f = rs.nextFloat();
        if (ranges.length > 0 && f < 0.66f)
        {
            RoutingKey homeKey = rs.nextBoolean() ? ks[rs.nextInt(ranges.length * 2)] : ranges[rs.nextInt(ranges.length)].someIntersectingRoutingKey(null);
            return f < 0.33f ? new FullRangeRoute(homeKey, ranges) : new PartialRangeRoute(homeKey, ranges);
        }
        return new PartialRangeRoute(keyGen.next(rs), ranges);
    }

    private static Participants<?> subset(RandomSource rs, Participants<?> superset, boolean changeType)
    {
        if (rs.nextBoolean())
            return changeType && superset instanceof Route<?> && rs.nextBoolean() ? ((Route)superset).participantsOnly() : superset;

        int count = superset.isEmpty() ? 0 : rs.nextInt(superset.size());
        Participants<?> subset = selectSubset(rs, count, superset);
        if (superset instanceof Route<?> && (!changeType || rs.nextBoolean()))
            return superset.intersecting(subset);
        return subset;
    }

    private static <R extends Unseekable> Participants<R> selectSubset(RandomSource rs, int count, Participants<R> superset)
    {
        switch (superset.domain())
        {
            default: throw UnhandledEnum.unknown(superset.domain());
            case Key:
            {
                AbstractUnseekableKeys in = (AbstractUnseekableKeys) superset;
                RoutingKey[] out = new RoutingKey[count];
                int j = 0;
                for (int i = 0 ; i < out.length ; ++i)
                {
                    j += count == (in.size() - j) ? 0 : rs.nextInt(0, in.size() - j);
                    out[i] = in.get(j);
                }
                return (Participants<R>) RoutingKeys.of(out);
            }

            case Range:
            {
                AbstractRanges in = (AbstractRanges) superset;
                Range[] out = new Range[count];
                int j = 0;
                for (int i = 0 ; i < out.length ; ++i)
                {
                    j += count == (in.size() - j) ? 0 : rs.nextInt(0, in.size() - j);
                    out[i] = in.get(j);
                }
                return (Participants<R>) Ranges.of(out);
            }

        }

    }

    private static Gen<Ranges> rangesGen()
    {
        return partitioner().flatMap(p -> AccordGenerators.rangesSplitOrArbitrary(p));
    }
}