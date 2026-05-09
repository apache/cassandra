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

package org.apache.cassandra.service.accord;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import accord.api.RoutingKey;
import accord.local.AbstractDurableBeforeTest;
import accord.local.DurableBefore;
import accord.primitives.Ranges;
import accord.utils.Gen;
import accord.utils.RandomSource;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.serializers.CommandStoreSerializers;

import static org.apache.cassandra.service.accord.serializers.CommandStoreSerializersTest.durableBeforeLinear;

public class DurableBeforeIntegrationTest extends AbstractDurableBeforeTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Override
    protected RoutingKey key(int prefix, int hash)
    {
        return new TokenKey(TableId.fromLong(1 + prefix), new Murmur3Partitioner.LongToken(hash));
    }

    @Test
    @Override
    public void test()
    {
        super.test();
    }

    @Override
    protected void assertEquals(Object a, Object b)
    {
        Assert.assertEquals(a, b);
    }

    @Override
    protected void assertTrue(boolean isTrue)
    {
        Assert.assertTrue(isTrue);
    }

    @Override
    protected void check(RandomSource rs, DurableBefore tree, DurableBeforeLinear linear, Gen<Ranges> genRanges)
    {
        super.check(rs, tree, linear, genRanges);
        if (rs.decide(0.1f))
            testSer(tree, linear);
    }

    static void testSer(DurableBefore tree, DurableBeforeLinear linear)
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            Serializers.testSerde(buffer, CommandStoreSerializers.durableBefore, tree);
            Serializers.testSerde(buffer, durableBeforeLinear, linear, CommandStoreSerializers.durableBefore, DurableBeforeLinear::isEqualTo);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }
}
