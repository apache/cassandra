///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.cassandra.utils.btree;
//
//
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Random;
//import java.util.concurrent.ThreadLocalRandom;
//
//import com.google.common.base.Stopwatch;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import org.apache.cassandra.db.BufferDecoratedKey;
//import org.apache.cassandra.db.DecoratedKey;
//import org.apache.cassandra.db.PartitionPosition;
//import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
//import org.apache.cassandra.dht.Token;
//import org.apache.cassandra.utils.Interval;
//import org.apache.cassandra.utils.IntervalTree;
//
//import static java.lang.String.format;
//import static java.util.concurrent.TimeUnit.NANOSECONDS;
//
//public class IntervalTreePerfTest
//{
//    private static final Logger logger = LoggerFactory.getLogger(IntervalTreePerfTest.class);
//
//    private static final byte[] bigArray = new byte[1024 * 1024 * 64];
//
//    static
//    {
//        Random r = new Random();
//        r.nextBytes(bigArray);
//    }
//
//
//    private static List<Interval<PartitionPosition, Integer>> randomIntervals(int range, int increment, int count)
//    {
//        List<PartitionPosition> a = random(range, increment, count);
//        List<PartitionPosition> b = random(range, increment, count);
//        List<Interval<PartitionPosition, Integer>> r = new ArrayList<>();
//        for (int i = 0 ; i < count ; i++)
//        {
//            r.add(a.get(i).compareTo(b.get(i)) < 0 ? Interval.create(a.get(i), b.get(i), i)
//                                      : Interval.create(b.get(i), a.get(i), i));
//        }
//        return r;
//    }
//
//    private static List<PartitionPosition> random(int range, int increment, int count)
//    {
//        List<PartitionPosition> random = new ArrayList<>();
//        for (int i = 0 ; i < count ; i++)
//        {
//            int base = i * increment;
//            Token token = new LongToken(ThreadLocalRandom.current().nextInt(base, base + range));
//            ByteBuffer key = ByteBuffer.allocate(16);
//            key.putLong(token.getLongValue()).putLong(token.getLongValue()).flip();
//            DecoratedKey dk = new BufferDecoratedKey(token, key);
//            random.add(dk);
//        }
//        return random;
//    }
//
//    @Test
//    public void test()
//    {
//        long checksum = 0;
//        int iterations = 1000;
//        int totalTableCount = 100000;
//        int additionalIntervalsCount = 1000;
//        long rangePersstable = Long.MAX_VALUE / 5;
//        int range = 1000000;
//
//        List<Interval<PartitionPosition, Integer>> randomIntervals = new ArrayList<>();
//        List<Interval<PartitionPosition, Integer>> additionalIntervals = new ArrayList<>();
//        List<Interval<PartitionPosition, Integer>> insertIntervals = randomIntervals;
//        for (int level = 0; level <= 7; level++)
//        {
//            for (int i = 0; i < (Long.MAX_VALUE / rangePersstable) * 2; i++)
//            {
//                long start = Long.MIN_VALUE + (rangePersstable * i);
//                if (randomIntervals.size() == totalTableCount)
//                    insertIntervals = additionalIntervals;
//                if (additionalIntervals.size() == additionalIntervalsCount)
//                    break;
//                Token startToken = new LongToken(start);
//                ByteBuffer startKey = ByteBuffer.allocate(16);
//                startKey.putLong(start).putLong(start).flip();
//                DecoratedKey startDK = new BufferDecoratedKey(startToken, startKey);
//                Token endToken = new LongToken(start + rangePersstable);
//                ByteBuffer endKey = ByteBuffer.allocate(16);
//                endKey.putLong(start + rangePersstable).putLong(start + rangePersstable).flip();
//                DecoratedKey endDK = new BufferDecoratedKey(endToken, endKey);
//                insertIntervals.add(new Interval<>(startDK, endDK, null));
//            }
//            rangePersstable /= 10;
//        }
//        Collections.shuffle(randomIntervals);
//        Collections.shuffle(additionalIntervals);
//
//        long min = Long.MAX_VALUE;
//        long max = Long.MIN_VALUE;
//        long total = 0;
//        logger.info("Testing interval tree 3 adding " + additionalIntervals.size() + " intervals");
//        IntervalTree<PartitionPosition, Integer, Interval<PartitionPosition, Integer>> tree3 = new IntervalTree<>(randomIntervals);
//        for (Interval<PartitionPosition, Integer> i : additionalIntervals)
//        {
//            for (byte b : bigArray)
//                checksum += b;
//            Stopwatch sw = Stopwatch.createStarted();
//            tree3.update(null, new Interval[] {i});
//            long elapsed = sw.elapsed(NANOSECONDS);
//            total += elapsed;
//            min = Math.min(min, elapsed);
//            max = Math.max (max, elapsed);
//        }
//        logger.info(format("tree 3 update average %.3f, min %.3f, max %.3f", NANOSECONDS.toMicros(total / additionalIntervals.size()) / 1000.0,  NANOSECONDS.toMicros(min) / 1000.0, NANOSECONDS.toMicros(max) / 1000.0));
//
//        logger.info("Testing leveled compaction building " + randomIntervals.size() + " intervals");
//        long searchMin = Long.MAX_VALUE;
//        long searchMax = Long.MIN_VALUE;
//        long searchTotal = 0;
//        Token searchToken = new LongToken(0);
//        ByteBuffer searchKey = ByteBuffer.allocate(16).putLong(0).putLong(0).flip();
//        DecoratedKey searchDK = new BufferDecoratedKey(searchToken, searchKey);
//
//        min = Long.MAX_VALUE;
//        max = Long.MIN_VALUE;
//        total = 0;
//        for (int i = 0 ; i < iterations; i++)
//        {
//            Stopwatch sw = Stopwatch.createStarted();
//            Object[] tree = buildBtree();
//            long elapsed = sw.elapsed(NANOSECONDS);
//            min = Math.min(min, elapsed);
//            max = Math.max(max, elapsed);
//            total += elapsed;
//            for (byte b : bigArray)
//                checksum += b;
//            sw = Stopwatch.createStarted();
//            List<Integer> result = IntervalBTree.<Object, Object, Object, List<Integer>>accumulate(tree, , searchDK, (i1,i2,k,l) -> {l.add((Integer)((Interval)k).data)}, null, null, new ArrayList<>());
//            long searchElapsed = sw.elapsed(NANOSECONDS);
//            searchMin = Math.min(searchMin, searchElapsed);
//            searchMax = Math.max(searchMax, searchElapsed);
//            searchTotal += searchElapsed;
////            sw = Stopwatch.createStarted();
////            tree.finish();
////            System.out.println("Finishing took " + sw.elapsed(TimeUnit.MILLISECONDS) + "ms");
//        }
//        logger.info(format("Legacy tree Average %.3f, min %.3f, max %.3f", NANOSECONDS.toMicros(total / iterations) / 1000.0,  NANOSECONDS.toMicros(min) / 1000.0, NANOSECONDS.toMicros(max) / 1000.0));
//        logger.info("Legacy tree search Average {}, min {}, max {}", NANOSECONDS.toMicros(searchTotal / iterations),  NANOSECONDS.toMicros(searchMin), NANOSECONDS.toMicros(searchMax));
//
//        logger.info("Testing random ranges");
//        randomIntervals = new ArrayList<>(new HashSet<>(randomIntervals(range, 0, 100000)));
//
//        min = Long.MAX_VALUE;
//        max = Long.MIN_VALUE;
//        total = 0;
//        logger.info("Testing interval tree 3 adding " + additionalIntervals.size() + " intervals");
//        tree3 = new IntervalTree<>(randomIntervals);
//        for (Interval<PartitionPosition, Integer> i : additionalIntervals)
//        {
//            for (byte b : bigArray)
//                checksum += b;
//            Stopwatch sw = Stopwatch.createStarted();
//            tree3.update(null, new Interval[] {i});
//            long elapsed = sw.elapsed(NANOSECONDS);
//            total += elapsed;
//            min = Math.min(min, elapsed);
//            max = Math.max (max, elapsed);
//        }
//        logger.info(format("tree 3 update average %.3f, min %.3f, max %.3f", NANOSECONDS.toMicros(total / additionalIntervals.size()) / 1000.0,  NANOSECONDS.toMicros(min) / 1000.0, NANOSECONDS.toMicros(max) / 1000.0));
//
//        min = Long.MAX_VALUE;
//        max = Long.MIN_VALUE;
//        searchMin = Long.MAX_VALUE;
//        searchMax = Long.MIN_VALUE;
//        total = 0;
//        searchTotal = 0;
//        for (int i = 0 ; i < iterations; i++)
//        {
//            Stopwatch sw = Stopwatch.createStarted();
//            Object[] tree = buildBtree(randomIntervals)
//            IntervalTree<PartitionPosition, Integer, Interval<PartitionPosition, Integer>> tree = new LegacyIntervalTree<>(randomIntervals);
//            long elapsed = sw.elapsed(NANOSECONDS);
//            min = Math.min(min, elapsed);
//            max = Math.max(max, elapsed);
//            total += elapsed;
//            for (byte b : bigArray)
//                checksum += b;
//            sw = Stopwatch.createStarted();
//            List<Integer> result = tree.search(searchDK);
//            long searchElapsed = sw.elapsed(NANOSECONDS);
//            searchMin = Math.min(searchMin, searchElapsed);
//            searchMax = Math.max(searchMax, searchElapsed);
//            searchTotal += searchElapsed;
////            sw = Stopwatch.createStarted();
////            tree.finish();
////            System.out.println("Finishing took " + sw.elapsed(TimeUnit.MILLISECONDS) + "ms");
//        }
//        logger.info("Legacy tree Average {}, min {}, max {}", NANOSECONDS.toMicros(total / iterations),  NANOSECONDS.toMicros(min), NANOSECONDS.toMicros(max));
//        logger.info("Checksum {}", checksum);
//    }
//
//    private static Object[] buildBtree(List<Interval<PartitionPosition, Integer>> intervals)
//    {
//        List<Interval<PartitionPosition, Integer>> sorted = new ArrayList<>(intervals);
//        sorted.sort(Interval.minOrdering());
//        try (IntervalBTree.FastInteralTreeBuilder<Interval> builder = (IntervalBTree.FastInteralTreeBuilder) IntervalBTree.fastBuilder(Interval.maxOrdering()))
//        {
//            for (Interval interval : intervals)
//                builder.add(interval);
//            return builder.build();
//        }
//    }
//}