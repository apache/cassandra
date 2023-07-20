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

package org.apache.cassandra.service.paxos;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.google.common.collect.Lists;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.config.CassandraRelevantProperties.PARTITIONER;
import static org.apache.cassandra.dht.Range.deoverlap;
import static org.apache.cassandra.service.paxos.Ballot.Flag.NONE;
import static org.apache.cassandra.service.paxos.Ballot.none;
import static org.apache.cassandra.service.paxos.BallotGenerator.Global.atUnixMicros;
import static org.apache.cassandra.service.paxos.BallotGenerator.Global.nextBallot;
import static org.apache.cassandra.service.paxos.Commit.latest;
import static org.apache.cassandra.service.paxos.PaxosRepairHistory.trim;

public class PaxosRepairHistoryTest
{
    static final Logger logger = LoggerFactory.getLogger(PaxosRepairHistoryTest.class);
    private static final AtomicInteger tableNum = new AtomicInteger();
    static
    {
        PARTITIONER.setString(Murmur3Partitioner.class.getName());
        DatabaseDescriptor.daemonInitialization();
        assert DatabaseDescriptor.getPartitioner() instanceof Murmur3Partitioner;
    }

    private static final Token MIN_TOKEN = Murmur3Partitioner.instance.getMinimumToken();

    private static Token t(long t)
    {
        return new LongToken(t);
    }

    private static Ballot b(int b)
    {
        return Ballot.atUnixMicrosWithLsb(b, 0, NONE);
    }

    private static Range<Token> r(Token l, Token r)
    {
        return new Range<>(l, r);
    }

    private static Range<Token> r(long l, long r)
    {
        return r(t(l), t(r));
    }

    private static Pair<Token, Ballot> pt(long t, int b)
    {
        return Pair.create(t(t), b(b));
    }

    private static Pair<Token, Ballot> pt(long t, Ballot b)
    {
        return Pair.create(t(t), b);
    }

    private static Pair<Token, Ballot> pt(Token t, int b)
    {
        return Pair.create(t, b(b));
    }

    private static PaxosRepairHistory h(Pair<Token, Ballot>... points)
    {
        int length = points.length + (points[points.length - 1].left == null ? 0 : 1);
        Token[] tokens = new Token[length - 1];
        Ballot[] ballots = new Ballot[length];
        for (int i = 0 ; i < length - 1 ; ++i)
        {
            tokens[i] = points[i].left;
            ballots[i] = points[i].right;
        }
        ballots[length - 1] = length == points.length ? points[length - 1].right : none();
        return new PaxosRepairHistory(tokens, ballots);
    }

    static
    {
        assert t(100).equals(t(100));
        assert b(111).equals(b(111));
    }

    private static class Builder
    {
        PaxosRepairHistory history = PaxosRepairHistory.EMPTY;

        Builder add(Ballot ballot, Range<Token>... ranges)
        {
            history = PaxosRepairHistory.add(history, Lists.newArrayList(ranges), ballot);
            return this;
        }

        Builder clear()
        {
            history = PaxosRepairHistory.EMPTY;
            return this;
        }
    }

    static Builder builder()
    {
        return new Builder();
    }

    private static void checkSystemTableIO(PaxosRepairHistory history)
    {
        Assert.assertEquals(history, PaxosRepairHistory.fromTupleBufferList(history.toTupleBufferList()));
        String tableName = "test" + tableNum.getAndIncrement();
        SystemKeyspace.savePaxosRepairHistory("test", tableName, history, false);
        Assert.assertEquals(history, SystemKeyspace.loadPaxosRepairHistory("test", tableName));
    }

    @BeforeClass
    public static void init() throws Exception
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void testAdd()
    {
        Builder builder = builder();
        Assert.assertEquals(h(pt(10, none()), pt(20, 5), pt(30, none()), pt(40, 5)),
                            builder.add(b(5), r(10, 20), r(30, 40)).history);

        Assert.assertEquals(none(), builder.history.ballotForToken(t(0)));
        Assert.assertEquals(none(), builder.history.ballotForToken(t(10)));
        Assert.assertEquals(b(5), builder.history.ballotForToken(t(11)));
        Assert.assertEquals(b(5), builder.history.ballotForToken(t(20)));
        Assert.assertEquals(none(), builder.history.ballotForToken(t(21)));

        builder.clear();
        Assert.assertEquals(h(pt(10, none()), pt(20, 5), pt(30, none()), pt(40, 6)),
                            builder.add(b(5), r(10, 20)).add(b(6), r(30, 40)).history);
        builder.clear();
        Assert.assertEquals(h(pt(10, none()), pt(20, 5), pt(30, 6), pt(40, 5)),
                            builder.add(b(5), r(10, 40)).add(b(6), r(20, 30)).history);

        builder.clear();
        Assert.assertEquals(h(pt(10, none()), pt(20, 6), pt(30, 5)),
                            builder.add(b(6), r(10, 20)).add(b(5), r(15, 30)).history);

        builder.clear();
        Assert.assertEquals(h(pt(10, none()), pt(20, 5), pt(30, 6)),
                            builder.add(b(5), r(10, 25)).add(b(6), r(20, 30)).history);
    }

    @Test
    public void testTrim()
    {
        Assert.assertEquals(h(pt(10, none()), pt(20, 5), pt(30, none()), pt(40, 5), pt(50, none()), pt(60, 5)),
                            trim(h(pt(0, none()), pt(70, 5)), Lists.newArrayList(r(10, 20), r(30, 40), r(50, 60))));

        Assert.assertEquals(h(pt(10, none()), pt(20, 5)),
                            trim(h(pt(0, none()), pt(20, 5)), Lists.newArrayList(r(10, 30))));

        Assert.assertEquals(h(pt(10, none()), pt(20, 5)),
                            trim(h(pt(10, none()), pt(30, 5)), Lists.newArrayList(r(0, 20))));
    }

    @Test
    public void testFullRange()
    {
        // test full range is collapsed
        Builder builder = builder();
        Assert.assertEquals(h(pt(null, 5)),
                            builder.add(b(5), r(MIN_TOKEN, MIN_TOKEN)).history);

        Assert.assertEquals(b(5), builder.history.ballotForToken(MIN_TOKEN));
        Assert.assertEquals(b(5), builder.history.ballotForToken(t(0)));
    }

    @Test
    public void testWrapAroundRange()
    {
        Builder builder = builder();
        Assert.assertEquals(h(pt(-100, 5), pt(100, none()), pt(null, 5)),
                            builder.add(b(5), r(100, -100)).history);

        Assert.assertEquals(b(5), builder.history.ballotForToken(MIN_TOKEN));
        Assert.assertEquals(b(5), builder.history.ballotForToken(t(-101)));
        Assert.assertEquals(b(5), builder.history.ballotForToken(t(-100)));
        Assert.assertEquals(none(), builder.history.ballotForToken(t(-99)));
        Assert.assertEquals(none(), builder.history.ballotForToken(t(0)));
        Assert.assertEquals(none(), builder.history.ballotForToken(t(100)));
        Assert.assertEquals(b(5), builder.history.ballotForToken(t(101)));
    }

    private static Token[] tks(long ... tks)
    {
        return LongStream.of(tks).mapToObj(LongToken::new).toArray(Token[]::new);
    }

    private static Ballot[] uuids(String ... uuids)
    {
        return Stream.of(uuids).map(Ballot::fromString).toArray(Ballot[]::new);
    }

    @Test
    public void testRegression()
    {
        Assert.assertEquals(none(), trim(
                new PaxosRepairHistory(
                        tks(-9223372036854775807L, -3952873730080618203L, -1317624576693539401L, 1317624576693539401L, 6588122883467697005L),
                        uuids("1382954c-1dd2-11b2-8fb2-f45d70d6d6d8", "138260a4-1dd2-11b2-abb2-c13c36b179e1", "1382951a-1dd2-11b2-1dd8-b7e242b38dbe", "138294fc-1dd2-11b2-83c4-43fb3a552386", "13829510-1dd2-11b2-f353-381f2ed963fa", "1382954c-1dd2-11b2-8fb2-f45d70d6d6d8")),
                Collections.singleton(new Range<>(new LongToken(-1317624576693539401L), new LongToken(1317624576693539401L))))
            .ballotForToken(new LongToken(-4208619967696141037L)));
    }

    @Test
    public void testInequality()
    {
        Collection<Range<Token>> ranges = Collections.singleton(new Range<>(Murmur3Partitioner.MINIMUM, Murmur3Partitioner.MINIMUM));
        PaxosRepairHistory a = PaxosRepairHistory.add(PaxosRepairHistory.EMPTY, ranges, none());
        PaxosRepairHistory b = PaxosRepairHistory.add(PaxosRepairHistory.EMPTY, ranges, nextBallot(NONE));
        Assert.assertNotEquals(a, b);
    }

    @Test
    public void testRandomTrims()
    {
        ExecutorService executor = Executors.newFixedThreadPool(FBUtilities.getAvailableProcessors());
        List<Future<?>> results = new ArrayList<>();
        int count = 1000;
        for (int numberOfAdditions : new int[] { 1, 10, 100 })
        {
            for (float maxCoveragePerRange : new float[] { 0.01f, 0.1f, 0.5f })
            {
                for (float chanceOfMinToken : new float[] { 0.01f, 0.1f })
                {
                    results.addAll(testRandomTrims(executor, count, numberOfAdditions, 3, maxCoveragePerRange, chanceOfMinToken));
                }
            }
        }
        FBUtilities.waitOnFutures(results);
        executor.shutdown();
    }

    private List<Future<?>> testRandomTrims(ExecutorService executor, int tests, int numberOfAdditions, int maxNumberOfRangesPerAddition, float maxCoveragePerRange, float chanceOfMinToken)
    {
        return ThreadLocalRandom.current()
                .longs(tests)
                .mapToObj(seed -> executor.submit(() -> testRandomTrims(seed, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinToken)))
                .collect(Collectors.toList());
    }

    private void testRandomTrims(long seed, int numberOfAdditions, int maxNumberOfRangesPerAddition, float maxCoveragePerRange, float chanceOfMinToken)
    {
        Random random = new Random(seed);
        logger.info("Seed {} ({}, {}, {}, {})", seed, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinToken);
        PaxosRepairHistory history = RandomPaxosRepairHistory.build(random, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinToken);
        // generate a random list of ranges that cover the whole ring
        long[] tokens = random.longs(16).distinct().toArray();
        if (random.nextBoolean())
            tokens[0] = Long.MIN_VALUE;
        Arrays.sort(tokens);
        List<List<Range<Token>>> ranges = IntStream.range(0, tokens.length <= 3 ? 1 : 1 + random.nextInt((tokens.length - 1) / 2))
                .mapToObj(ignore -> new ArrayList<Range<Token>>())
                .collect(Collectors.toList());

        for (int i = 1 ; i < tokens.length ; ++i)
            ranges.get(random.nextInt(ranges.size())).add(new Range<>(new LongToken(tokens[i - 1]), new LongToken(tokens[i])));
        ranges.get(random.nextInt(ranges.size())).add(new Range<>(new LongToken(tokens[tokens.length - 1]), new LongToken(tokens[0])));

        List<PaxosRepairHistory> splits = new ArrayList<>();
        for (List<Range<Token>> rs : ranges)
        {
            PaxosRepairHistory trimmed = PaxosRepairHistory.trim(history, rs);
            splits.add(trimmed);
            if (rs.isEmpty())
                continue;

            Range<Token> prev = rs.get(rs.size() - 1);
            for (Range<Token> range : rs)
            {
                if (prev.right.equals(range.left))
                {
                    Assert.assertEquals(history.ballotForToken(((LongToken)range.left).decreaseSlightly()), trimmed.ballotForToken(((LongToken)range.left).decreaseSlightly()));
                    Assert.assertEquals(history.ballotForToken(range.left), trimmed.ballotForToken(range.left));
                }
                else
                {
                    if (!range.left.isMinimum())
                        Assert.assertEquals(none(), trimmed.ballotForToken(range.left));
                    if (!prev.right.isMinimum())
                        Assert.assertEquals(none(), trimmed.ballotForToken(prev.right.nextValidToken()));
                }
                Assert.assertEquals(history.ballotForToken(range.left.nextValidToken()), trimmed.ballotForToken(range.left.nextValidToken()));
                if (!range.left.nextValidToken().equals(range.right))
                    Assert.assertEquals(history.ballotForToken(((LongToken)range.right).decreaseSlightly()), trimmed.ballotForToken(((LongToken)range.right).decreaseSlightly()));

                if (range.right.isMinimum())
                    Assert.assertEquals(history.ballotForToken(new LongToken(Long.MAX_VALUE)), trimmed.ballotForToken(new LongToken(Long.MAX_VALUE)));
                else
                    Assert.assertEquals(history.ballotForToken(range.right), trimmed.ballotForToken(range.right));
                prev = range;
            }
        }

        PaxosRepairHistory merged = PaxosRepairHistory.EMPTY;
        for (PaxosRepairHistory split : splits)
            merged = PaxosRepairHistory.merge(merged, split);

        Assert.assertEquals(history, merged);
        checkSystemTableIO(history);
    }

    @Test
    public void testRandomAdds()
    {
        ExecutorService executor = Executors.newFixedThreadPool(FBUtilities.getAvailableProcessors());
        List<Future<?>> results = new ArrayList<>();
        int count = 1000;
        for (int numberOfAdditions : new int[] { 1, 10, 100 })
        {
            for (float maxCoveragePerRange : new float[] { 0.01f, 0.1f, 0.5f })
            {
                for (float chanceOfMinToken : new float[] { 0.01f, 0.1f })
                {
                    results.addAll(testRandomAdds(executor, count, 3, numberOfAdditions, 3, maxCoveragePerRange, chanceOfMinToken));
                }
            }
        }
        FBUtilities.waitOnFutures(results);
        executor.shutdown();
    }

    private List<Future<?>> testRandomAdds(ExecutorService executor, int tests, int numberOfMerges, int numberOfAdditions, int maxNumberOfRangesPerAddition, float maxCoveragePerRange, float chanceOfMinToken)
    {
        return ThreadLocalRandom.current()
                .longs(tests)
                .mapToObj(seed -> executor.submit(() -> testRandomAdds(seed, numberOfMerges, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinToken)))
                .collect(Collectors.toList());
    }

    private void testRandomAdds(long seed, int numberOfMerges, int numberOfAdditions, int maxNumberOfRangesPerAddition, float maxCoveragePerRange, float chanceOfMinToken)
    {
        Random random = new Random(seed);
        String id = String.format("%d, %d, %d, %d, %f, %f", seed, numberOfMerges, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinToken);
        logger.info(id);
        List<RandomWithCanonical> merge = new ArrayList<>();
        while (numberOfMerges-- > 0)
        {
            RandomWithCanonical build = new RandomWithCanonical();
            build.addRandom(random, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinToken);
            merge.add(build);
        }

        RandomWithCanonical check = new RandomWithCanonical();
        for (RandomWithCanonical add : merge)
            check = check.merge(add);
        check.serdeser();

        for (Token token : check.canonical.keySet())
        {
            LongToken tk = (LongToken) token;
            Assert.assertEquals(id, check.ballotForToken(tk.decreaseSlightly()), check.test.ballotForToken(tk.decreaseSlightly()));
            Assert.assertEquals(id, check.ballotForToken(tk), check.test.ballotForToken(token));
            Assert.assertEquals(id, check.ballotForToken(tk.nextValidToken()), check.test.ballotForToken(token.nextValidToken()));
        }

        // check some random
        {
            int count = 1000;
            while (count-- > 0)
            {
                LongToken token = new LongToken(random.nextLong());
                Assert.assertEquals(id, check.ballotForToken(token), check.test.ballotForToken(token));
            }
        }

    }

    static class RandomPaxosRepairHistory
    {
        PaxosRepairHistory test = PaxosRepairHistory.EMPTY;

        void add(Collection<Range<Token>> ranges, Ballot ballot)
        {
            test = PaxosRepairHistory.add(test, ranges, ballot);
        }

        void merge(RandomPaxosRepairHistory other)
        {
            test = PaxosRepairHistory.merge(test, other.test);
        }

        void addOneRandom(Random random, int maxRangeCount, float maxCoverage, float minChance)
        {
            int count = maxRangeCount == 1 ? 1 : 1 + random.nextInt(maxRangeCount - 1);
            Ballot ballot = atUnixMicros(random.nextInt(Integer.MAX_VALUE), NONE);
            List<Range<Token>> ranges = new ArrayList<>();
            while (count-- > 0)
            {
                long length = (long) (2 * random.nextDouble() * maxCoverage * Long.MAX_VALUE);
                if (length == 0) length = 1;
                Range<Token> range;
                if (random.nextFloat() <= minChance)
                {
                    if (random.nextBoolean()) range = new Range<>(Murmur3Partitioner.MINIMUM, new LongToken(Long.MIN_VALUE + length));
                    else range = new Range<>(new LongToken(Long.MAX_VALUE - length), Murmur3Partitioner.MINIMUM);
                }
                else
                {
                    long start = random.nextLong();
                    range = new Range<>(new LongToken(start), new LongToken(start + length));
                }
                ranges.add(range);
            }
            ranges.sort(Range::compareTo);
            add(deoverlap(ranges), ballot);
        }

        void addRandom(Random random, int count, int maxNumberOfRangesPerAddition, float maxCoveragePerAddition, float minTokenChance)
        {
            while (count-- > 0)
                addOneRandom(random, maxNumberOfRangesPerAddition, maxCoveragePerAddition, minTokenChance);
        }

        static PaxosRepairHistory build(Random random, int count, int maxNumberOfRangesPerAddition, float maxCoveragePerRange, float chanceOfMinToken)
        {
            RandomPaxosRepairHistory result = new RandomPaxosRepairHistory();
            result.addRandom(random, count, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinToken);
            return result.test;
        }
    }

    static class RandomWithCanonical extends RandomPaxosRepairHistory
    {
        NavigableMap<Token, Ballot> canonical = new TreeMap<>();
        {
            canonical.put(Murmur3Partitioner.MINIMUM, none());
        }

        Ballot ballotForToken(LongToken token)
        {
            return canonical
                    .floorEntry(token.token == Long.MIN_VALUE ? token : token.decreaseSlightly())
                    .getValue();
        }

        RandomWithCanonical merge(RandomWithCanonical other)
        {
            RandomWithCanonical result = new RandomWithCanonical();
            result.test = PaxosRepairHistory.merge(test, other.test);
            result.canonical = new TreeMap<>();
            result.canonical.putAll(canonical);
            for (Map.Entry<Token, Ballot> entry : other.canonical.entrySet())
            {
                Token left = entry.getKey();
                Token right = other.canonical.higherKey(left);
                if (right == null) right = Murmur3Partitioner.MINIMUM;
                result.addCanonical(new Range<>(left, right), entry.getValue());
            }
            return result;
        }

        void serdeser()
        {
            PaxosRepairHistory tmp = PaxosRepairHistory.fromTupleBufferList(test.toTupleBufferList());
            Assert.assertEquals(test, tmp);
            test = tmp;
        }

        void add(Collection<Range<Token>> addRanges, Ballot ballot)
        {
            super.add(addRanges, ballot);
            for (Range<Token> range : addRanges)
                addCanonical(range, ballot);
        }

        void addCanonical(Range<Token> range, Ballot ballot)
        {
            canonical.put(range.left, canonical.floorEntry(range.left).getValue());
            if (!range.right.isMinimum())
                canonical.put(range.right, canonical.floorEntry(range.right).getValue());

            for (Range<Token> r : range.unwrap())
            {
                (r.right.isMinimum()
                        ? canonical.subMap(r.left, true, new LongToken(Long.MAX_VALUE), true)
                        : canonical.subMap(r.left, true, r.right, false)
                ).entrySet().forEach(e -> e.setValue(latest(e.getValue(), ballot)));
            }
        }
    }
}
