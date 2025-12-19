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

import accord.api.RoutingKey;
import accord.local.MaxConflicts;
import accord.local.MaxConflictsTest;
import accord.utils.RandomSource;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.serializers.CommandStoreSerializers;

public class MaxConflictsIntegrationTest extends MaxConflictsTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Override
    public void test()
    {
        super.test();
    }
    
    @Override
    protected RoutingKey key(int prefix, int hash)
    {
        return new TokenKey(TableId.fromLong(1 + prefix), new Murmur3Partitioner.LongToken(hash));
    }

    @Override
    protected void check(RandomSource rs, MaxConflicts prev, MaxConflicts next)
    {
        super.check(rs, prev, next);
        if (rs.decide(0.1f))
            testSer(next);
    }

    static void testSer(MaxConflicts tree)
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            Serializers.testSerde(buffer, CommandStoreSerializers.maxConflicts, tree);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
