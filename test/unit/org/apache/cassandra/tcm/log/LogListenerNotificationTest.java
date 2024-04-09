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
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.tcm.transformations.cms.PreInitialize;

import static org.apache.cassandra.tcm.MetadataKeys.CORE_METADATA;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.randomPlacements;
import static org.apache.cassandra.tcm.sequences.SequencesUtils.affectedRanges;
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
        DataPlacements placements = randomPlacements(random);

        List<Entry> input = new ArrayList<>();
        Epoch epoch = Epoch.FIRST;
        for (int i = 0; i < 1000; i++)
        {
            epoch = epoch.nextEpoch();
            input.add(new Entry(new Entry.Id(i),
                                epoch,
                                new TestTransform(random.nextInt(),
                                                  affectedRanges(placements, random),
                                                  affectedMetadata(random))));
        }

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
                assertEquals(transform.ranges, result.success().affectedRanges);

                counter++;
            }
        };
        LocalLog log = LocalLog.logSpec()
                               .sync()
                               .withInitialState(cm())
                               .withLogListener(listener)
                               .createLog();
        log.readyUnchecked();
        log.append(new Entry(Entry.Id.NONE, Epoch.FIRST, PreInitialize.forTesting()));
        log.append(input);
    }

    static ClusterMetadata cm()
    {
        return new ClusterMetadata(Murmur3Partitioner.instance);
    }

    static Set<MetadataKey> affectedMetadata(Random random)
    {
        List<MetadataKey> src = new ArrayList<>(CORE_METADATA);
        int required = random.nextInt(src.size());
        Set<MetadataKey> keys = new HashSet<>();
        while (keys.size() < required)
            keys.add(src.get(random.nextInt(src.size())));
        return keys;
    }

    static class TestTransform extends CustomTransformation.PokeInt
    {
        private final LockedRanges.AffectedRanges ranges;
        private final ImmutableSet<MetadataKey> keys;
        public TestTransform(int v, LockedRanges.AffectedRanges ranges, Set<MetadataKey> keys)
        {
            super(v);
            this.ranges = ranges;
            this.keys = ImmutableSet.copyOf(keys);
        }

        @Override
        public Result execute(ClusterMetadata prev)
        {
            return new Success(prev.transformer()
                                   .with(CustomTransformation.PokeInt.METADATA_KEY, IntValue.create(v))
                                   .build().metadata, ranges, keys);
        }
    }
}
