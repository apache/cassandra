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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import accord.local.Node;
import accord.primitives.Range;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Invariants;
import org.agrona.collections.Long2LongHashMap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.AccordGenerators;

import static accord.utils.Property.qt;

public class WatermarkCollectorTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void snapshotSerializer()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(snapshotGen()).check(expected -> {
            maybeUpdatePartitioner(expected);
            Serializers.testSerde(output, WatermarkCollector.serializer, expected);
        });
    }

    private static void maybeUpdatePartitioner(WatermarkCollector.Snapshot snapshot)
    {
        if (!snapshot.closed.isEmpty()) DatabaseDescriptor.setPartitionerUnsafe(((TokenRange)snapshot.closed.get(0).getKey()).start().token().getPartitioner());
        else if (!snapshot.retired.isEmpty()) DatabaseDescriptor.setPartitionerUnsafe(((TokenRange)snapshot.retired.get(0).getKey()).start().token().getPartitioner());
    }

    private Gen<WatermarkCollector.Snapshot> snapshotGen()
    {
        Gen<IPartitioner> partitionerGen = AccordGenerators.partitioner();
        Gen.LongGen epochGen = AccordGens.epochs();
        Gen<Long2LongHashMap> syncedGen = syncedGen();
        return rs -> {
            IPartitioner partitioner = partitionerGen.next(rs);
            Gen<Range> rangeGen = AccordGenerators.range(partitioner);
            Gen<Map<Range, Long>> mapGen = mapGen(Gens.ints().between(0, 10), rangeGen, epochGen);
            return new WatermarkCollector.Snapshot(new ArrayList<>(mapGen.next(rs).entrySet()), new ArrayList<>(mapGen.next(rs).entrySet()), syncedGen.next(rs));
        };
    }

    private static Gen<Long2LongHashMap> syncedGen()
    {
        Gen.IntGen sizeGen = Gens.ints().between(0, 10);
        Gen<Node.Id> idGen = AccordGens.nodes();
        Gen.LongGen epochGen = AccordGens.epochs();
        return rs -> {
            Long2LongHashMap map = new Long2LongHashMap(-1);
            Gen<Node.Id> uniqueIdGen = idGen.filter(id -> !map.containsKey(id.id));
            for (int i = 0, size = sizeGen.nextInt(rs); i < size; i++)
                map.put(uniqueIdGen.next(rs).id, epochGen.next(rs));
            return map;
        };
    }

    private static <K, V> Gen<Map<K, V>> mapGen(Gen.IntGen sizeGen, Gen<K> keyGen, Gen<V> valueGen)
    {
        //TODO (ux): should move this to Gens
        return rs -> {
            int size = sizeGen.nextInt(rs);
            Invariants.require(size >= 0, "Only 0 and possitive allowed; given %d", size);
            if (size == 0)
                return Map.of();
            Map<K, V> map = new HashMap<>();
            Gen<K> uniqueKeyGen = keyGen.filter(k -> !map.containsKey(k));
            for (int i = 0; i < size; i++)
                map.put(uniqueKeyGen.next(rs), valueGen.next(rs));
            return map;
        };
    }
}
