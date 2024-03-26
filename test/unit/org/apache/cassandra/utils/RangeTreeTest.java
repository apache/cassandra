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

package org.apache.cassandra.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.impl.IntKey;
import accord.impl.IntKey.Routing;
import accord.primitives.Range;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.SearchableRangeList;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.LongArrayList;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

@RunWith(Parameterized.class)
public class RangeTreeTest
{
    private static final Logger logger = LoggerFactory.getLogger(RangeTreeTest.class);
    private static final Comparator<Routing> COMPARATOR = Comparator.naturalOrder();
    private static final RangeTree.Accessor<Routing, Range> END_INCLUSIVE = new RangeTree.Accessor<>()
    {
        @Override
        public Routing start(Range range)
        {
            return (Routing) range.start();
        }

        @Override
        public Routing end(Range range)
        {
            return (Routing) range.end();
        }

        @Override
        public boolean contains(Range range, Routing routing)
        {
            return range.contains(routing);
        }

        @Override
        public boolean contains(Routing start, Routing end, Routing routing)
        {
            if (routing.compareTo(start) <= 0)
                return false;
            if (routing.compareTo(end) > 0)
                return false;
            return true;
        }

        @Override
        public boolean intersects(Range range, Routing start, Routing end)
        {
            return range.compareIntersecting(IntKey.range(start, end)) == 0;
        }

        @Override
        public boolean intersects(Range left, Range right)
        {
            return left.compareIntersecting(right) == 0;
        }
    };
    private static final RangeTree.Accessor<Routing, Range> ALL_INCLUSIVE = new RangeTree.Accessor<>()
    {
        @Override
        public Routing start(Range range)
        {
            return (Routing) range.start();
        }

        @Override
        public Routing end(Range range)
        {
            return (Routing) range.end();
        }

        @Override
        public boolean contains(Range range, Routing routing)
        {
            return range.contains(routing) || range.start().equals(routing);
        }

        @Override
        public boolean contains(Routing start, Routing end, Routing routing)
        {
            if (routing.compareTo(start) < 0)
                return false;
            if (routing.compareTo(end) > 0)
                return false;
            return true;
        }

        @Override
        public boolean intersects(Range range, Routing start, Routing end)
        {
            return range.compareIntersecting(IntKey.range(start, end)) == 0 || range.end().equals(start) || range.start().equals(end);
        }

        @Override
        public boolean intersects(Range left, Range right)
        {
            return left.compareIntersecting(right) == 0 || left.end().equals(right.start()) || left.start().equals(right.end());
        }
    };

    private static final Gen.IntGen SMALL_INT_GEN = rs -> rs.nextInt(0, 10);
    private static final int MIN_TOKEN = 0, MAX_TOKEN = 1 << 16;
    private static final int TOKEN_RANGE_SIZE = MAX_TOKEN - MIN_TOKEN + 1;
    private static final Gen<Gen.IntGen> TOKEN_DISTRIBUTION = Gens.mixedDistribution(MIN_TOKEN, MAX_TOKEN + 1);
    private static final Gen<Gen.IntGen> RANGE_SIZE_DISTRIBUTION = Gens.mixedDistribution(10, (int) (TOKEN_RANGE_SIZE * .01));

    // Used to test different worse case patterns and see how the tree performs.
    private enum Pattern
    {
        RANDOM, // tends to have high selectivity: matches 50-100% of the tree in testing
        NO_OVERLP, // tests to have low selectivity; matches 1-2 elements in testing
        SMALL_RANGES // lower selectivity than RANDOM but still matches ~30% of the tree in testing
    }

    // Having different models makes sure that the tree is flexiable enough and can be used with the semantics the user
    // needs (with regard to inclusivity).  It also adds more confidence that the search logic is correct as different
    // algorithems help validate this.
    private enum ModelType {List, IntervalTree, SearchableRangeList}
    private final Pattern pattern;
    private final ModelType modelType;

    public RangeTreeTest(Pattern pattern, ModelType modelType)
    {
        this.pattern = pattern;
        this.modelType = modelType;
    }

    @Parameterized.Parameters(name = "{0}, {1}")
    public static Collection<Object[]> data() {
        return Stream.of(Pattern.values())
                     .flatMap(p ->
                              Stream.of(ModelType.values())
                                    .map(m -> new Object[]{ p, m }))
                     .collect(Collectors.toList());
    }

    @Test
    public void test()
    {
        int samples = 3_000;
        int examples = 10;
        LongArrayList byToken = new LongArrayList(samples * examples, -1);
        LongArrayList modelByToken = new LongArrayList(samples * examples, -1);
        LongArrayList byTokenLength = new LongArrayList(samples * examples, -1);
        LongArrayList byRange = new LongArrayList(samples * examples, -1);
        LongArrayList modelByRange = new LongArrayList(samples * examples, -1);
        LongArrayList byRangeLength = new LongArrayList(samples * examples, -1);
        qt().withExamples(examples).check(rs -> {
            var map = create(modelType);
            var model = createModel(modelType);

            Gen<Range> rangeGen = rangeGen(rs, pattern, samples);
            for (int i = 0; i < samples; i++)
            {
                var range = rangeGen.next(rs);
                var value = SMALL_INT_GEN.nextInt(rs);
                map.put(range, value);
                model.put(range, value);
            }
            model.done();
            Assertions.assertThat(map.actual()).hasSize(samples);
            if (rangeGen instanceof NoOverlap)
                ((NoOverlap) rangeGen).reset();
            Gen.IntGen tokenGe = TOKEN_DISTRIBUTION.next(rs);
            for (int i = 0; i < samples; i++)
            {
                {
                    // key lookup
                    var lookup = IntKey.routing(tokenGe.nextInt(rs));
                    var actual = timed(byToken, () -> map.intersectsToken(lookup));
                    var expected = timed(modelByToken, () -> model.intersectsToken(lookup));
                    byTokenLength.addLong(expected.size());
                    Assertions.assertThat(sort(actual))
                              .describedAs("Write=%d; token=%s", i, lookup)
                              .isEqualTo(sort(expected));
                }
                {
                    // range lookup
                    var lookup = rangeGen.next(rs);
                    var actual = timed(byRange, () -> map.intersects(lookup));
                    var expected = timed(modelByRange, () -> model.intersects(lookup));
                    byRangeLength.addLong(expected.size());
                    Assertions.assertThat(sort(actual))
                              .describedAs("Write=%d; range=%s", i, lookup)
                              .isEqualTo(sort(expected));
                }
            }
        });
        StringBuilder sb = new StringBuilder();
        sb.append("=======");
        sb.append("\nPattern: " + pattern);
        sb.append("\nModel: " + modelType);
        sb.append("\nBy Token:");
        sb.append("\n\tSizes: " + stats(byTokenLength, false));
        sb.append("\n\t" + modelType + ": " + stats(modelByToken, true));
        sb.append("\n\tTree: " + stats(byToken, true));
        sb.append("\nBy Range:");
        sb.append("\n\tSizes: " + stats(byRangeLength, false));
        sb.append("\n\t" + modelType + ": " + stats(modelByRange, true));
        sb.append("\n\tTree: " + stats(byRange, true));
        logger.info(sb.toString());
    }

    private static class NoOverlap implements Gen<Range>
    {
        private final int delta;
        private int idx = 0;

        public NoOverlap(int samples)
        {
            this.delta = TOKEN_RANGE_SIZE / samples;
        }

        @Override
        public Range next(RandomSource random)
        {
            int a = delta * idx++;
            int b = a + delta;
            return IntKey.range(a, b);
        }

        private void reset()
        {
            idx = 0;
        }
    }

    private static Gen<Range> rangeGen(RandomSource randomSource, Pattern pattern, int samples)
    {
        Gen.IntGen tokenGen = TOKEN_DISTRIBUTION.next(randomSource);
        switch (pattern)
        {
            case RANDOM:
                return rs -> {
                    int a = tokenGen.nextInt(rs);
                    int b = tokenGen.nextInt(rs);
                    while (a == b)
                        b = tokenGen.nextInt(rs);
                    if (a > b)
                    {
                        int tmp = a;
                        a = b;
                        b = tmp;
                    }
                    return IntKey.range(a, b);
                };
            case SMALL_RANGES:
                Gen.IntGen rangeSizeGen = RANGE_SIZE_DISTRIBUTION.next(randomSource);
                return rs -> {
                    int a = tokenGen.nextInt(rs);
                    int rangeSize = rangeSizeGen.nextInt(rs);
                    int b = a + rangeSize;
                    if (b > MAX_TOKEN)
                    {
                        b = a;
                        a = b - rangeSize;
                    }
                    return IntKey.range(a, b);
                };
            case NO_OVERLP:
                return new NoOverlap(samples);
            default:
                throw new AssertionError();
        }
    }

    private static String stats(LongArrayList list, boolean isTime)
    {
        LongUnaryOperator fn = isTime ? TimeUnit.NANOSECONDS::toMicros : l -> l;
        String postfix = isTime ? "micro" : "";
        long[] array = list.toLongArray();
        Arrays.sort(array);
        StringBuilder sb = new StringBuilder();
        sb.append("Min: ").append(fn.applyAsLong(array[0])).append(postfix);
        sb.append(", Median: ").append(fn.applyAsLong(array[array.length / 2])).append(postfix);
        sb.append(", Max: ").append(fn.applyAsLong(array[array.length - 1])).append(postfix);
        return sb.toString();
    }

    private static <T> T timed(LongArrayList target, Supplier<T> fn)
    {
        long nowNs = System.nanoTime();
        try
        {
            return fn.get();
        }
        finally
        {
            target.add(System.nanoTime() - nowNs);
        }
    }

    private static List<Map.Entry<Range, Integer>> sort(List<Map.Entry<Range, Integer>> array)
    {
        array.sort((a, b) -> {
            int rc = a.getKey().compare(b.getKey());
            if (rc == 0)
                rc = a.getValue().compareTo(b.getValue());
            return rc;
        });
        return array;
    }

    private interface Model
    {
        Object actual();

        void put(Range range, int value);

        List<Map.Entry<Range, Integer>> intersectsToken(Routing key);

        List<Map.Entry<Range, Integer>> intersects(Range range);

        void done();
    }

    private static RangeTreeModel create(ModelType modelType)
    {
        switch (modelType)
        {
            case List:
            case SearchableRangeList:
                return new RangeTreeModel(new RTree<>(COMPARATOR, END_INCLUSIVE));
            case IntervalTree: return new RangeTreeModel(new RTree<>(COMPARATOR, ALL_INCLUSIVE));
            default:
                throw new AssertionError("Unknown type: " + modelType);
        }
    }

    private static Model createModel(ModelType modelType)
    {
        switch (modelType)
        {
            case List: return new ListModel();
            case SearchableRangeList: return new SearchableRangeListModel();
            case IntervalTree: return new IntervalTreeModel();
            default:
                throw new AssertionError("Unknown type: " + modelType);
        }
    }

    private static class RangeTreeModel implements Model
    {
        private final RangeTree<Routing, Range, Integer> tree;

        private RangeTreeModel(RangeTree<Routing, Range, Integer> tree)
        {
            this.tree = tree;
        }

        @Override
        public RangeTree<Routing, Range, Integer> actual()
        {
            return tree;
        }

        @Override
        public void put(Range range, int value)
        {
            tree.add(range, value);
        }

        @Override
        public List<Map.Entry<Range, Integer>> intersectsToken(Routing key)
        {
            return tree.searchToken(key);
        }

        @Override
        public List<Map.Entry<Range, Integer>> intersects(Range range)
        {
            return tree.search(range);
        }

        @Override
        public void done()
        {

        }
    }

    private static class ListModel implements Model
    {
        List<Map.Entry<Range, Integer>> actual = new ArrayList<>();

        @Override
        public List<Map.Entry<Range, Integer>> actual()
        {
            return actual;
        }

        @Override
        public void put(Range range, int value)
        {
            actual.add(Map.entry(range, value));
        }

        @Override
        public List<Map.Entry<Range, Integer>> intersectsToken(Routing key)
        {
            return actual.stream()
                         .filter(p -> p.getKey().contains(key))
                         .collect(Collectors.toList());
        }

        @Override
        public List<Map.Entry<Range, Integer>> intersects(Range range)
        {
            return actual.stream()
                         .filter(p -> p.getKey().compareIntersecting(range) == 0)
                         .collect(Collectors.toList());
        }

        @Override
        public void done()
        {

        }
    }

    private static class IntervalTreeModel implements Model
    {
        IntervalTree.Builder<Routing, Integer, Interval<Routing, Integer>> builder = IntervalTree.builder();
        IntervalTree<Routing, Integer, Interval<Routing, Integer>> actual = null;

        @Override
        public IntervalTree<Routing, Integer, Interval<Routing, Integer>> actual()
        {
            return actual;
        }

        @Override
        public void put(Range range, int value)
        {
            builder.add(new Interval<>((Routing) range.start(), (Routing) range.end(), value));
        }

        @Override
        public List<Map.Entry<Range, Integer>> intersectsToken(Routing key)
        {
            return map(actual.matches(key));
        }

        @Override
        public List<Map.Entry<Range, Integer>> intersects(Range range)
        {
            return map(actual.matches(new Interval<>((Routing) range.start(), (Routing) range.end(), null)));
        }

        private static List<Map.Entry<Range, Integer>> map(List<Interval<Routing, Integer>> matches)
        {
            return matches.stream().map(i -> Map.entry(IntKey.range(i.min, i.max), i.data)).collect(Collectors.toList());
        }

        @Override
        public void done()
        {
            assert builder != null;
            actual = builder.build();
            builder = null;
        }
    }

    private static class SearchableRangeListModel implements Model
    {
        private final Map<Range, IntArrayList> map = new HashMap<>();
        private Range[] ranges;
        private SearchableRangeList list = null;

        @Override
        public Object actual()
        {
            return list;
        }

        @Override
        public void put(Range range, int value)
        {
            map.computeIfAbsent(range, ignore -> new IntArrayList()).addInt(value);
        }

        @Override
        public List<Map.Entry<Range, Integer>> intersectsToken(Routing key)
        {
            List<Map.Entry<Range, Integer>> matches = new ArrayList<>();
            // find ranges, then add the values
            list.forEach(key, (a, b, c, d, idx) -> {
                Range match = ranges[idx];
                map.get(match).forEachInt(v -> matches.add(Map.entry(match, v)));
            }, (a, b, c, d, start, end) -> {
                for (int i = start; i < end; i++)
                {
                    Range match = ranges[i];
                    map.get(match).forEachInt(v -> matches.add(Map.entry(match, v)));
                }
            }, 0, 0, 0, 0, 0);
            return matches;
        }

        @Override
        public List<Map.Entry<Range, Integer>> intersects(Range range)
        {
            List<Map.Entry<Range, Integer>> matches = new ArrayList<>();
            // find ranges, then add the values
            list.forEach(range, (a, b, c, d, idx) -> {
                Range match = ranges[idx];
                map.get(match).forEachInt(v -> matches.add(Map.entry(match, v)));
            }, (a, b, c, d, start, end) -> {
                for (int i = start; i < end; i++)
                {
                    Range match = ranges[i];
                    map.get(match).forEachInt(v -> matches.add(Map.entry(match, v)));
                }
            }, 0, 0, 0, 0, 0);
            return matches;
        }

        @Override
        public void done()
        {
            List<Range> ranges = new ArrayList<>(map.keySet());
            ranges.sort(Range::compare);
            list = SearchableRangeList.build(this.ranges = ranges.toArray(Range[]::new));
        }
    }
}