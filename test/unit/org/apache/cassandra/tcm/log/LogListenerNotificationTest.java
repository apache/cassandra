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

package org.apache.cassandra.tcm.log;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataKey;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.extensions.IntValue;
import org.apache.cassandra.tcm.listeners.LogListener;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.tcm.transformations.cms.PreInitialize;

import static org.apache.cassandra.tcm.MetadataKeys.make;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogListenerNotificationTest
{
    @BeforeClass
    public static void setup()
    {
        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void testNotifyListener()
    {
        long seed = System.nanoTime();
        Random random = new Random(seed);

        List<Entry> input = new ArrayList<>();
        Epoch epoch = Epoch.FIRST;
        for (int i = 0; i < 1000; i++)
        {
            epoch = epoch.nextEpoch();
            input.add(new Entry(new Entry.Id(i),
                                epoch,
                                new TestTransform(random.nextInt(),
                                                  affectedMetadata(random))));
        }

        ClusterMetadata cm = cm();
        LocalLog log = LocalLog.sync(cm, LogStorage.None, false);
        log.append(new Entry(Entry.Id.NONE, Epoch.FIRST, PreInitialize.forTesting()));
        LogListener listener = new LogListener()
        {
            int counter = 0;
            @Override
            public void notify(Entry entry, Transformation.Result result)
            {
                Entry expected = input.get(counter);
                assertEquals(expected, entry);
                TestTransform transform = (TestTransform) expected.transform;
                Set<MetadataKey> updatedMetadata = result.success().affectedMetadata;
                assertEquals(transform.keys.size(), updatedMetadata.size());
                for (MetadataKey expectedKey : transform.keys)
                    assertTrue(updatedMetadata.contains(expectedKey));

                counter++;
            }
        };

        log.addListener(listener);
        log.append(input);
    }

    static ClusterMetadata cm()
    {
        return new ClusterMetadata(Murmur3Partitioner.instance);
    }

    static Set<MetadataKey> affectedMetadata(Random random)
    {
        int required = random.nextInt(9) + 1;
        Set<MetadataKey> keys = new HashSet<>();
        while (keys.size() < required)
            keys.add(make("test.random." + random.nextInt(100)));
        return keys;
    }

    static class TestTransform extends CustomTransformation.PokeInt
    {
        private final ImmutableSet<MetadataKey> keys;
        public TestTransform(int v, Set<MetadataKey> keys)
        {
            super(v);
            this.keys = ImmutableSet.copyOf(keys);
        }

        @Override
        public Result execute(ClusterMetadata prev)
        {
            return new Success(prev.transformer()
                                   .with(CustomTransformation.PokeInt.METADATA_KEY, IntValue.create(v))
                                   .build().metadata, keys);
        }
    }
}
