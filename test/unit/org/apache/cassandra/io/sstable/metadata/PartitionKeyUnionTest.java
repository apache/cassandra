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
package org.apache.cassandra.io.sstable.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;

import org.junit.Test;

import static accord.utils.Property.qt;
import static org.apache.cassandra.io.sstable.metadata.MetadataCollector.CARDINALITY_PRECISION;
import static org.apache.cassandra.io.sstable.metadata.MetadataCollector.CARDINALITY_SPARSE_PRECISION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PartitionKeyUnionTest
{
    private static final double MAX_RELATIVE_ERROR = 0.1;

    @Test
    public void testNoSketches()
    {
        PartitionKeyUnion union = new PartitionKeyUnion();
        assertTrue(union.isEmpty());
        assertEquals(0, union.cardinality());
    }

    @Test
    public void testRandomSketches()
    {
        qt().withExamples(100).check(random -> {
            int poolSize = random.nextInt(1, 20_000);
            long[] pool = new long[poolSize];
            // Keys are generated uniformly by using nextLong so we do not need to
            // perform a hash prior to passing it into sketch
            for (int i = 0; i < poolSize; i++)
                pool[i] = random.nextLong();

            int sstables = random.nextInt(1, 8);

            PartitionKeyUnion union = new PartitionKeyUnion();
            Set<Long> exactDistinctKeys = new HashSet<>();
            for (int i = 0; i < sstables; i++)
            {
                // Each SSTable holds a random portion from the keys
                // that we've generated
                float fraction = random.nextFloat();
                List<Long> sstable = new ArrayList<>();
                for (long partition : pool)
                    if (random.decide(fraction))
                        sstable.add(partition);

                union.add(sketch(sstable));
                exactDistinctKeys.addAll(sstable);
            }

            assertEquals(exactDistinctKeys.size(), union.cardinality(), Math.max(10, MAX_RELATIVE_ERROR * exactDistinctKeys.size()));
        });
    }

    private static ICardinality sketch(List<Long> hashes) throws IOException
    {
        HyperLogLogPlus sketch = new HyperLogLogPlus(CARDINALITY_PRECISION, CARDINALITY_SPARSE_PRECISION);
        for (long hash : hashes)
            sketch.offerHashed(hash);
        return HyperLogLogPlus.Builder.build(sketch.getBytes());
    }
}
