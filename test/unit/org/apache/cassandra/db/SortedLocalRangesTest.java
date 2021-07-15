/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import java.util.List;
import java.util.Optional;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.SplitterTest;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.apache.cassandra.dht.SplitterTest.getSplitter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class SortedLocalRangesTest
{
    final private static Random random = new Random(1047504572034957L);

    @Mock
    ColumnFamilyStore cfs;

    @Mock
    StorageService storageService;

    @Mock
    TokenMetadata tmd;

    IPartitioner partitioner;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);

        partitioner = DatabaseDescriptor.getPartitioner();

        when(cfs.getPartitioner()).thenReturn(partitioner);
        when(cfs.getKeyspaceName()).thenReturn("keyspace");
        when(cfs.getTableName()).thenReturn("table");

        when(storageService.getTokenMetadata()).thenReturn(tmd);
    }

    SortedLocalRanges makeRanges(long ringVersion, List<Splitter.WeightedRange> ranges)
    {
        when(tmd.getRingVersion()).thenReturn(ringVersion);
        return new SortedLocalRanges(storageService, cfs, ringVersion, ranges);
    }

    @Test
    public void testNoRanges()
    {
        long ringVersion = 1;
        List<Splitter.WeightedRange> ranges = ImmutableList.of();
        SortedLocalRanges sortedRanges = makeRanges(ringVersion, ranges);

        assertNotNull(sortedRanges);
        assertNotNull(sortedRanges.toString());

        assertEquals(sortedRanges, sortedRanges);
        assertEquals(sortedRanges.hashCode(), sortedRanges.hashCode());

        assertFalse(sortedRanges.isOutOfDate());
        assertTrue(sortedRanges.getRanges().isEmpty());
        assertEquals(ringVersion, sortedRanges.getRingVersion());

        // if there are no ranges, the splitter will return an array with the maximum token regardless of how many splits
        assertEquals(1, sortedRanges.split(0).size());
        assertEquals(1, sortedRanges.split(1).size());
        assertEquals(1, sortedRanges.split(2).size());
    }

    @Test
    public void testSplit()
    {
        long ringVersion = 1;

        for (int i = 1; i <= 100; i++)
        {
            int numTokens = 172 + random.nextInt(128);
            int rf = random.nextInt(4) + 2;
            int parts = random.nextInt(5) + 1;
            List<Splitter.WeightedRange> ranges = SplitterTest.generateLocalRanges(numTokens,
                                                                                   rf,
                                                                                   getSplitter(partitioner),
                                                                                   random,
                                                                                   partitioner instanceof RandomPartitioner);
            SortedLocalRanges sortedRanges = makeRanges(ringVersion, ranges);

            List<PartitionPosition> boundaries = sortedRanges.split(parts);
            assertNotNull(boundaries);
            assertEquals(parts, boundaries.size());
        }
    }

    @Test
    public void testSplitNoSplitter()
    {
        long ringVersion = 1;
        int numTokens = 172 + random.nextInt(128);
        int rf = random.nextInt(4) + 2;
        int parts = random.nextInt(5) + 1;
        List<Splitter.WeightedRange> ranges = SplitterTest.generateLocalRanges(numTokens,
                                                                               rf,
                                                                               getSplitter(partitioner),
                                                                               random,
                                                                               partitioner instanceof RandomPartitioner);

        // mock a partitioner without the splitter and verify split ranges are the same as the local ranges
        IPartitioner partitioner = Mockito.mock(IPartitioner.class);
        when(cfs.getPartitioner()).thenReturn(partitioner);
        when(partitioner.splitter()).thenReturn(Optional.empty());

        SortedLocalRanges sortedRanges = makeRanges(ringVersion, ranges);

        List<PartitionPosition> boundaries = sortedRanges.split(parts);
        assertNotNull(boundaries);
        assertEquals(ranges.size(), boundaries.size()); // it ignores the parts and just returns the ranges
    }

    @Test
    public void testEquals()
    {
        long ringVersion = 1;
        int numTokens = 172 + random.nextInt(128);
        int rf = random.nextInt(4) + 2;
        List<Splitter.WeightedRange> ranges = SplitterTest.generateLocalRanges(numTokens,
                                                                               rf,
                                                                               getSplitter(partitioner),
                                                                               random,
                                                                               partitioner instanceof RandomPartitioner);

        SortedLocalRanges sortedRanges1 = makeRanges(ringVersion, ranges);
        SortedLocalRanges sortedRanges2 = makeRanges(ringVersion, ranges);

        assertEquals(sortedRanges1, sortedRanges2);
        assertEquals(sortedRanges1.hashCode(), sortedRanges2.hashCode());
        assertEquals(sortedRanges1.toString(), sortedRanges2.toString());

        sortedRanges1.invalidate();
        assertEquals(sortedRanges1, sortedRanges2);

        sortedRanges2.invalidate();
        assertEquals(sortedRanges1, sortedRanges2);

        ringVersion++;

        // different ring version
        SortedLocalRanges sortedRanges3 = makeRanges(ringVersion, ranges);
        assertNotEquals(sortedRanges1, sortedRanges3);
        assertNotEquals(sortedRanges1.hashCode(), sortedRanges3.hashCode());
        assertNotEquals(sortedRanges1.toString(), sortedRanges3.toString());

        // different ranges
        ranges = SplitterTest.generateLocalRanges(numTokens,
                                                  rf,
                                                  getSplitter(partitioner),
                                                  random,
                                                  partitioner instanceof RandomPartitioner);
        SortedLocalRanges sortedRanges4 = makeRanges(ringVersion, ranges);
        assertNotEquals(sortedRanges1, sortedRanges4);
        assertNotEquals(sortedRanges1.hashCode(), sortedRanges4.hashCode());
        assertNotEquals(sortedRanges1.toString(), sortedRanges4.toString());

    }
}