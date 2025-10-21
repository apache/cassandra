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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.junit.Test;

import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.primitives.Range;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Property.Command;
import accord.utils.Property.UnitCommand;
import accord.utils.RandomSource;
import org.apache.cassandra.service.accord.RangeTreeRangeAccessor;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;

public class StatefulRangeTreeTest
{
    private static final Gen.IntGen SMALL_INT_GEN = rs -> rs.nextInt(0, 10);
    private static final Gen.IntGen NUM_CHILDREN_GEN = rs -> rs.nextInt(2, 12);
    private static final Gen<Gen.IntGen> SIZE_TARGET_DISTRIBUTION = Gens.mixedDistribution(1 << 3, 1 << 9);
    private static final int MIN_TOKEN = 0, MAX_TOKEN = 1 << 16;
    private static final int TOKEN_RANGE_SIZE = MAX_TOKEN - MIN_TOKEN + 1;
    private static final Gen<Gen.IntGen> TOKEN_DISTRIBUTION = Gens.mixedDistribution(MIN_TOKEN, MAX_TOKEN + 1);
    private static final Gen<Gen.IntGen> RANGE_SIZE_DISTRIBUTION = Gens.mixedDistribution(10, (int) (TOKEN_RANGE_SIZE * .01));
    static final Comparator<Map.Entry<Range, Integer>> COMPARATOR = (a, b) -> {
        int rc = a.getKey().compare(b.getKey());
        if (rc == 0)
            rc = a.getValue().compareTo(b.getValue());
        return rc;
    };

    /**
     * Stateful test for RTree.
     * <p>
     * This test is very similar to {@link RangeTreeTest#test} but is fully mutable, so can not
     * use the immutable search trees (else rebuidling becomes a large cost).  Both tests should exist as they use different
     * models, which helps build confidence that the RTree does the correct thing; that test also covers start and end
     * inclusive, which this test does not.
     */
    @Test
    public void test()
    {
        stateful().check(commands(() -> State::new, state -> new Sut(state.sizeTarget, state.numChildren))
                         .add((rs, state) -> new Create(state.newRange(rs), SMALL_INT_GEN.nextInt(rs)))
                         .add((rs, state) -> new Read(state.newRange(rs)))
                         .add((rs, state) -> new KeyRead(IntKey.routing(state.tokenGen.nextInt(rs))))
                         .add((rs, state) -> new RangeRead(state.rangeGen.next(rs)))
                         .add(Iterate.instance)
                         .add(Clear.instance)
                         .addAllIf(state -> !state.uniqRanges.isEmpty(),
                                   b -> b.add((rs, state) -> new Read(rs.pickOrderedSet(state.uniqRanges)))
                                         .add((rs, state) -> {
                                             Range range = rs.pickOrderedSet(state.uniqRanges);
                                             int token = rs.nextInt(((IntKey.Routing) range.start()).key, ((IntKey.Routing) range.end()).key) + 1;
                                             return new KeyRead(IntKey.routing(token));
                                         })
                                         .add((rs, state) -> new RangeRead(rs.pickOrderedSet(state.uniqRanges)))
                                         .add((rs, state) -> new Update(rs.pickOrderedSet(state.uniqRanges), SMALL_INT_GEN.nextInt(rs)))
                                         .add((rs, state) -> new Delete(rs.pickOrderedSet(state.uniqRanges)))
                         )
                         .build());
    }

    private static Gen<Range> rangeGen(RandomSource rand)
    {
        Gen.IntGen tokenGen = TOKEN_DISTRIBUTION.next(rand);
        switch (rand.nextInt(0, 3))
        {
            case 0: // pure random
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
            case 1: // small range
                Gen.IntGen rangeSizeGen = RANGE_SIZE_DISTRIBUTION.next(rand);
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
            case 2: // single element
                return rs -> {
                    int a = tokenGen.nextInt(rs);
                    int b = a + 1;
                    return IntKey.range(a, b);
                };
            default:
                throw new AssertionError();
        }
    }

    static class Create implements UnitCommand<State, Sut>
    {
        private final Range range;
        private final int value;

        Create(Range range, int value)
        {
            this.range = range;
            this.value = value;
        }

        @Override
        public void applyUnit(State state)
        {
            state.add(range, value);
        }

        @Override
        public void runUnit(Sut sut)
        {
            sut.tree.add(range, value);
        }

        @Override
        public void checkPostconditions(State state, Void expected,
                                        Sut sut, Void actual)
        {
            Assertions.assertThat(sut.tree.size()).isEqualTo(state.list.size());
        }

        @Override
        public String detailed(State state)
        {
            return "Create(" + range + ", " + value + ")";
        }
    }

    static abstract class AbstractRead<T> implements Command<State, Sut, List<T>>
    {
        private final Comparator<T> comparator;

        protected AbstractRead(Comparator<T> comparator)
        {
            this.comparator = comparator;
        }

        @Override
        public void checkPostconditions(State state, List<T> expected,
                                        Sut sut, List<T> actual)
        {
            expected.sort(comparator);
            actual.sort(comparator);
            Assertions.assertThat(actual).isEqualTo(expected);
        }
    }

    static class Read extends AbstractRead<Integer>
    {
        private final Range range;

        Read(Range range)
        {
            super(Comparator.naturalOrder());
            this.range = range;
        }

        @Override
        public List<Integer> apply(State state)
        {
            return state.get(range);
        }

        @Override
        public List<Integer> run(Sut sut)
        {
            return sut.tree.get(range);
        }

        @Override
        public String detailed(State state)
        {
            return "Read(" + range + ")";
        }
    }

    static class RangeRead extends AbstractRead<Map.Entry<Range, Integer>>
    {
        private final Range range;

        RangeRead(Range range)
        {
            super(COMPARATOR);
            this.range = range;
        }

        @Override
        public List<Map.Entry<Range, Integer>> apply(State state)
        {
            return state.list.stream().filter(e -> e.getKey().compareIntersecting(range) == 0).collect(Collectors.toList());
        }

        @Override
        public List<Map.Entry<Range, Integer>> run(Sut sut)
        {
            return sut.tree.search(range);
        }

        @Override
        public String detailed(State state)
        {
            return "Range Read(" + range + ")";
        }
    }

    static class KeyRead extends AbstractRead<Map.Entry<Range, Integer>>
    {
        final RoutingKey key;

        KeyRead(RoutingKey key)
        {
            super(COMPARATOR);
            this.key = key;
        }

        @Override
        public List<Map.Entry<Range, Integer>> apply(State state)
        {
            return state.list.stream().filter(e -> e.getKey().contains(key)).collect(Collectors.toList());
        }

        @Override
        public List<Map.Entry<Range, Integer>> run(Sut sut)
        {
            return sut.tree.searchToken(key);
        }

        @Override
        public String detailed(State state)
        {
            return "Token Read(" + key + ")";
        }
    }

    static class Update implements UnitCommand<State, Sut>
    {
        private final Range range;
        private final int value;

        Update(Range range, int value)
        {
            this.range = range;
            this.value = value;
        }

        @Override
        public void applyUnit(State state)
        {
            state.update(range, value);
        }

        @Override
        public void runUnit(Sut sut)
        {
            sut.tree.get(range, e -> e.setValue(value));
        }

        @Override
        public String detailed(State state)
        {
            return "Update(" + range + ", " + value + ")";
        }
    }

    static class Delete implements UnitCommand<State, Sut>
    {
        private final Range range;

        Delete(Range range)
        {
            this.range = range;
        }

        @Override
        public void applyUnit(State state)
        {
            state.remove(range);
        }

        @Override
        public void runUnit(Sut sut)
        {
            sut.tree.remove(range);
        }

        @Override
        public void checkPostconditions(State state, Void expected,
                                        Sut sut, Void actual)
        {
            Assertions.assertThat(sut.tree.size()).isEqualTo(state.list.size());
        }

        @Override
        public String detailed(State state)
        {
            return "Delete(" + range + ")";
        }
    }

    static class Clear implements UnitCommand<State, Sut>
    {
        static final Clear instance = new Clear();

        @Override
        public void applyUnit(State state)
        {
            state.uniqRanges.clear();
            state.list.clear();
        }

        @Override
        public void runUnit(Sut sut)
        {
            sut.tree.clear();
        }

        @Override
        public String detailed(State state)
        {
            return "Clear(size=" + state.list.size() + ")";
        }
    }

    static class Iterate extends AbstractRead<Map.Entry<Range, Integer>>
    {
        static final Iterate instance = new Iterate();

        public Iterate()
        {
            super(COMPARATOR);
        }

        @Override
        public List<Map.Entry<Range, Integer>> apply(State state)
        {
            return state.list;
        }

        @Override
        public List<Map.Entry<Range, Integer>> run(Sut sut)
        {
            return sut.tree.stream().collect(Collectors.toList());
        }

        @Override
        public String detailed(State state)
        {
            return "Iterate(size=" + state.list.size() + ")";
        }
    }

    private static class State
    {
        private final List<Map.Entry<Range, Integer>> list = new ArrayList<>();
        private final TreeSet<Range> uniqRanges = new TreeSet<>(Range::compare);
        private final int sizeTarget, numChildren;
        private final Gen.IntGen tokenGen;
        private final Gen<Range> rangeGen;

        private State(RandomSource rs)
        {
            this.numChildren = NUM_CHILDREN_GEN.nextInt(rs);
            this.sizeTarget = SIZE_TARGET_DISTRIBUTION.next(rs).filter(s -> s > numChildren).nextInt(rs);
            this.tokenGen = TOKEN_DISTRIBUTION.next(rs);
            this.rangeGen = rangeGen(rs);
        }

        public Range newRange(RandomSource rs)
        {
            Range range;
            while ((uniqRanges.contains(range = rangeGen.next(rs))))
            {
            }
            return range;
        }

        public void add(Range range, int value)
        {
            list.add(new MutableEntry<>(range, value));
            uniqRanges.add(range);
        }

        public List<Integer> get(Range range)
        {
            if (!uniqRanges.contains(range))
                return Collections.emptyList();
            return list.stream().filter(e -> e.getKey().equals(range)).map(e -> e.getValue()).collect(Collectors.toList());
        }

        public void update(Range range, int value)
        {
            if (!uniqRanges.contains(range))
                return;
            list.forEach(e -> {
                if (e.getKey().equals(range))
                    e.setValue(value);
            });
        }

        public void remove(Range range)
        {
            if (!uniqRanges.contains(range))
                return;
            uniqRanges.remove(range);
            list.removeIf(e -> e.getKey().equals(range));
        }

        @Override
        public String toString()
        {
            return "State{" +
                   "sizeTarget=" + sizeTarget +
                   ", numChildren=" + numChildren +
                   '}';
        }
    }

    public static class Sut
    {
        private final RangeTree<RoutingKey, Range, Integer> tree;

        private Sut(int sizeTarget, int numChildren)
        {
            tree = new RTree(Comparator.naturalOrder(), RangeTreeRangeAccessor.instance, sizeTarget, numChildren);
        }
    }
}
