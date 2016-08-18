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
package org.apache.cassandra.db.compaction;

import static org.junit.Assert.*;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.*;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.KeyspaceParams;

public class CompactionIteratorTest
{

    private static final int NOW = 1000;
    private static final int GC_BEFORE = 100;
    private static final String KSNAME = "CompactionIteratorTest";
    private static final String CFNAME = "Integer1";

    static final DecoratedKey kk;
    static final CFMetaData metadata;
    private static final int RANGE = 1000;
    private static final int COUNT = 100;

    Map<List<Unfiltered>, DeletionTime> deletionTimes = new HashMap<>();

    static {
        DatabaseDescriptor.daemonInitialization();

        kk = Util.dk("key");

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KSNAME,
                                    KeyspaceParams.simple(1),
                                    metadata = SchemaLoader.standardCFMD(KSNAME,
                                                                         CFNAME,
                                                                         1,
                                                                         UTF8Type.instance,
                                                                         Int32Type.instance,
                                                                         Int32Type.instance));
    }

    // See org.apache.cassandra.db.rows.UnfilteredRowsGenerator.parse for the syntax used in these tests.

    @Test
    public void testGcCompactionSupersedeLeft()
    {
        testCompaction(new String[] {
            "5<=[140] 10[150] [140]<20 22<[130] [130]<25 30[150]"
        }, new String[] {
            "7<[160] 15[180] [160]<30 40[120]"
        },
        3);
    }

    @Test
    public void testGcCompactionSupersedeMiddle()
    {
        testCompaction(new String[] {
            "5<=[140] 10[150] [140]<40 60[150]"
        }, new String[] {
            "7<=[160] 15[180] [160]<=30 40[120]"
        },
        3);
    }

    @Test
    public void testGcCompactionSupersedeRight()
    {
        testCompaction(new String[] {
            "9<=[140] 10[150] [140]<40 60[150]"
        }, new String[] {
            "7<[160] 15[180] [160]<30 40[120]"
        },
        3);
    }

    @Test
    public void testGcCompactionSwitchInSuperseded()
    {
        testCompaction(new String[] {
            "5<=[140] 10[150] [140]<20 20<=[170] [170]<=50 60[150]"
        }, new String[] {
            "7<[160] 15[180] [160]<30 40[120]"
        },
        5);
    }

    @Test
    public void testGcCompactionBoundaries()
    {
        testCompaction(new String[] {
            "5<=[120] [120]<9 9<=[140] 10[150] [140]<40 40<=[120] 60[150] [120]<90"
        }, new String[] {
            "7<[160] 15[180] [160]<30 40[120] 45<[140] [140]<80 88<=[130] [130]<100"
        },
        7);
    }

    @Test
    public void testGcCompactionMatches()
    {
        testCompaction(new String[] {
            "5<=[120] [120]<=9 9<[140] 10[150] [140]<40 40<=[120] 60[150] [120]<90 120<=[100] [100]<130"
        }, new String[] {
            "9<[160] 15[180] [160]<40 40[120] 45<[140] [140]<90 90<=[110] [110]<100 120<=[100] [100]<130"
        },
        5);
    }

    @Test
    public void testGcCompactionRowDeletion()
    {
        testCompaction(new String[] {
            "10[150] 20[160] 25[160] 30[170] 40[120] 50[120]"
        }, new String[] {
            "10<=[155] 20[200D180] 30[200D160] [155]<=30 40[150D130] 50[150D100]"
        },
        "25[160] 30[170] 50[120]");
    }

    @Test
    public void testGcCompactionPartitionDeletion()
    {
        testCompaction(new String[] {
            "10[150] 20[160] 25[160] 30[170] 40[120] 50[120]"
        }, new String[] {
            // Dxx| stands for partition deletion at time xx
            "D165|10<=[155] 20[200D180] 30[200D160] [155]<=30 40[150D130] 50[150D100]"
        },
        "30[170]");
    }

    void testCompaction(String[] inputs, String[] tombstones, String expected)
    {
        testNonGcCompaction(inputs, tombstones);

        UnfilteredRowsGenerator generator = new UnfilteredRowsGenerator(metadata.comparator, false);
        List<List<Unfiltered>> inputLists = parse(inputs, generator);
        List<List<Unfiltered>> tombstoneLists = parse(tombstones, generator);
        List<Unfiltered> result = compact(inputLists, tombstoneLists);
        System.out.println("GC compaction resulted in " + size(result) + " Unfiltereds");
        generator.verifyValid(result);
        verifyEquivalent(inputLists, result, tombstoneLists, generator);
        List<Unfiltered> expectedResult = generator.parse(expected, NOW - 1);
        if (!expectedResult.equals(result))
            fail("Expected " + expected + ", got " + generator.str(result));
    }

    void testCompaction(String[] inputs, String[] tombstones, int expectedCount)
    {
        testNonGcCompaction(inputs, tombstones);

        UnfilteredRowsGenerator generator = new UnfilteredRowsGenerator(metadata.comparator, false);
        List<List<Unfiltered>> inputLists = parse(inputs, generator);
        List<List<Unfiltered>> tombstoneLists = parse(tombstones, generator);
        List<Unfiltered> result = compact(inputLists, tombstoneLists);
        System.out.println("GC compaction resulted in " + size(result) + " Unfiltereds");
        generator.verifyValid(result);
        verifyEquivalent(inputLists, result, tombstoneLists, generator);
        if (size(result) > expectedCount)
            fail("Expected compaction with " + expectedCount + " elements, got " + size(result) + ": " + generator.str(result));
    }

    int testNonGcCompaction(String[] inputs, String[] tombstones)
    {
        UnfilteredRowsGenerator generator = new UnfilteredRowsGenerator(metadata.comparator, false);
        List<List<Unfiltered>> inputLists = parse(inputs, generator);
        List<List<Unfiltered>> tombstoneLists = parse(tombstones, generator);
        List<Unfiltered> result = compact(inputLists, Collections.emptyList());
        System.out.println("Non-GC compaction resulted in " + size(result) + " Unfiltereds");
        generator.verifyValid(result);
        verifyEquivalent(inputLists, result, tombstoneLists, generator);
        return size(result);
    }

    private static int size(List<Unfiltered> data)
    {
        return data.stream().mapToInt(x -> x instanceof RangeTombstoneBoundaryMarker ? 2 : 1).sum();
    }

    private void verifyEquivalent(List<List<Unfiltered>> sources, List<Unfiltered> result, List<List<Unfiltered>> tombstoneSources, UnfilteredRowsGenerator generator)
    {
        // sources + tombstoneSources must be the same as result + tombstoneSources
        List<Unfiltered> expected = compact(Iterables.concat(sources, tombstoneSources), Collections.emptyList());
        List<Unfiltered> actual = compact(Iterables.concat(ImmutableList.of(result), tombstoneSources), Collections.emptyList());
        if (!expected.equals(actual))
        {
            System.out.println("Equivalence test failure between sources:");
            for (List<Unfiltered> partition : sources)
                generator.dumpList(partition);
            System.out.println("and compacted " + generator.str(result));
            System.out.println("with tombstone sources:");
            for (List<Unfiltered> partition : tombstoneSources)
                generator.dumpList(partition);
            System.out.println("expected " + generator.str(expected));
            System.out.println("got " + generator.str(actual));
            fail("Failed equivalence test.");
        }
    }

    private List<List<Unfiltered>> parse(String[] inputs, UnfilteredRowsGenerator generator)
    {
        return ImmutableList.copyOf(Lists.transform(Arrays.asList(inputs), x -> parse(x, generator)));
    }

    private List<Unfiltered> parse(String input, UnfilteredRowsGenerator generator)
    {
        Matcher m = Pattern.compile("D(\\d+)\\|").matcher(input);
        if (m.lookingAt())
        {
            int del = Integer.parseInt(m.group(1));
            input = input.substring(m.end());
            List<Unfiltered> list = generator.parse(input, NOW - 1);
            deletionTimes.put(list, new DeletionTime(del, del));
            return list;
        }
        else
            return generator.parse(input, NOW - 1);
    }

    private List<Unfiltered> compact(Iterable<List<Unfiltered>> sources, Iterable<List<Unfiltered>> tombstoneSources)
    {
        List<Iterable<UnfilteredRowIterator>> content = ImmutableList.copyOf(Iterables.transform(sources, list -> ImmutableList.of(listToIterator(list, kk))));
        Map<DecoratedKey, Iterable<UnfilteredRowIterator>> transformedSources = new TreeMap<>();
        transformedSources.put(kk, Iterables.transform(tombstoneSources, list -> listToIterator(list, kk)));
        try (CompactionController controller = new Controller(Keyspace.openAndGetStore(metadata), transformedSources, GC_BEFORE);
             CompactionIterator iter = new CompactionIterator(OperationType.COMPACTION,
                                                              Lists.transform(content, x -> new Scanner(x)),
                                                              controller, NOW, null))
        {
            List<Unfiltered> result = new ArrayList<>();
            assertTrue(iter.hasNext());
            try (UnfilteredRowIterator partition = iter.next())
            {
                Iterators.addAll(result, partition);
            }
            assertFalse(iter.hasNext());
            return result;
        }
    }

    private UnfilteredRowIterator listToIterator(List<Unfiltered> list, DecoratedKey key)
    {
        return UnfilteredRowsGenerator.source(list, metadata, key, deletionTimes.getOrDefault(list, DeletionTime.LIVE));
    }

    NavigableMap<DecoratedKey, List<Unfiltered>> generateContent(Random rand, UnfilteredRowsGenerator generator,
                                                                 List<DecoratedKey> keys, int pcount, int rcount)
    {
        NavigableMap<DecoratedKey, List<Unfiltered>> map = new TreeMap<>();
        for (int i = 0; i < pcount; ++i)
        {
            DecoratedKey key = keys.get(rand.nextInt(keys.size()));
            map.put(key, generator.generateSource(rand, rcount, RANGE, NOW - 5, x -> NOW - 1));
        }
        return map;
    }

    @Test
    public void testRandom()
    {
        UnfilteredRowsGenerator generator = new UnfilteredRowsGenerator(metadata.comparator, false);
        for (int seed = 1; seed < 100; ++seed)
        {
            Random rand = new Random(seed);
            List<List<Unfiltered>> sources = new ArrayList<>();
            for (int i = 0; i < 10; ++i)
                sources.add(generator.generateSource(rand, COUNT, RANGE, NOW - 5, x -> NOW - 15));
            int srcSz = sources.stream().mapToInt(CompactionIteratorTest::size).sum();
            List<List<Unfiltered>> tombSources = new ArrayList<>();
            for (int i = 0; i < 10; ++i)
                sources.add(generator.generateSource(rand, COUNT, RANGE, NOW - 5, x -> NOW - 15));
            List<Unfiltered> result = compact(sources, tombSources);
            verifyEquivalent(sources, result, tombSources, generator);
            assertTrue(size(result) < srcSz);
        }
    }

    class Controller extends CompactionController
    {
        private final Map<DecoratedKey, Iterable<UnfilteredRowIterator>> tombstoneSources;

        public Controller(ColumnFamilyStore cfs, Map<DecoratedKey, Iterable<UnfilteredRowIterator>> tombstoneSources, int gcBefore)
        {
            super(cfs, Collections.emptySet(), gcBefore);
            this.tombstoneSources = tombstoneSources;
        }

        @Override
        public Iterable<UnfilteredRowIterator> shadowSources(DecoratedKey key, boolean tombstoneOnly)
        {
            assert tombstoneOnly;
            return tombstoneSources.get(key);
        }
    }

    class Scanner extends AbstractUnfilteredPartitionIterator implements ISSTableScanner
    {
        Iterator<UnfilteredRowIterator> iter;

        Scanner(Iterable<UnfilteredRowIterator> content)
        {
            iter = content.iterator();
        }

        @Override
        public boolean isForThrift()
        {
            return false;
        }

        @Override
        public CFMetaData metadata()
        {
            return metadata;
        }

        @Override
        public boolean hasNext()
        {
            return iter.hasNext();
        }

        @Override
        public UnfilteredRowIterator next()
        {
            return iter.next();
        }

        @Override
        public long getLengthInBytes()
        {
            return 0;
        }

        @Override
        public long getCurrentPosition()
        {
            return 0;
        }

        @Override
        public long getBytesScanned()
        {
            return 0;
        }

        @Override
        public long getCompressedLengthInBytes()
        {
            return 0;
        }

        @Override
        public String getBackingFiles()
        {
            return null;
        }
    }
}
