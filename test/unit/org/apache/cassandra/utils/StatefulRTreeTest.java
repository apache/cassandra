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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.primitives.Range;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Property.Command;
import accord.utils.Property.Commands;
import accord.utils.Property.UnitCommand;
import org.apache.cassandra.service.accord.RTreeRangeAccessor;

import static accord.utils.Property.stateful;

public class StatefulRTreeTest
{
    private static final Gen.IntGen SMALL_INT_GEN = rs -> rs.nextInt(0, 10);
    private static final int MIN_TOKEN = 0, MAX_TOKEN = 1 << 16;
    private static final int TOKEN_RANGE_SIZE = MAX_TOKEN - MIN_TOKEN + 1;
    private static final Gen<Gen.IntGen> TOKEN_DISTRIBUTION = Gens.mixedDistribution(MIN_TOKEN, MAX_TOKEN + 1);
    private static final Gen<Gen.IntGen> RANGE_SIZE_DISTRIBUTION = Gens.mixedDistribution(10, (int) (TOKEN_RANGE_SIZE * .01));

    @Test
    public void test()
    {
        stateful().check(new Commands<State, Sut>()
        {
            @Override
            public Gen<State> genInitialState()
            {
                return rs -> {
                    Gen.IntGen tokenGen = TOKEN_DISTRIBUTION.next(rs);
                    Gen.IntGen rangeSizeGen = RANGE_SIZE_DISTRIBUTION.next(rs);
                    Gen<Range> rangeGen = r -> {
                        int a = tokenGen.nextInt(r);
                        int rangeSize = rangeSizeGen.nextInt(r);
                        int b = a + rangeSize;
                        if (b > MAX_TOKEN)
                        {
                            b = a;
                            a = b - rangeSize;
                        }
                        return IntKey.range(a, b);
                    };
                    return new State(rs.pickInt(1 << 3, 1 << 5, 1 << 7, 1 << 9),
                                     rs.nextInt(2, 12),
                                     rangeGen);
                };
            }

            @Override
            public Sut createSut(State state)
            {
                return new Sut(state.sizeTarget, state.numChildren);
            }

            @Override
            public Gen<Command<State, Sut, ?>> commands(State state)
            {
                List<Gen<Command<State, Sut, ?>>> possible = new ArrayList<>();
                // create
                possible.add(rs -> {
                    var existing = state.uniqRanges;
                    Range range;
                    while ((existing.contains(range = state.rangeGen.next(rs)))) {}
                    int value = SMALL_INT_GEN.nextInt(rs);
                    return new Create(range, value);
                });
                // read
                // update
                // delete
                return Gens.oneOf(possible);
            }
        });
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
        public String detailed(State state)
        {
            return "Create(" + range + ", " + value + ")";
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
    }

    private static class State
    {
        private final List<Map.Entry<Range, Integer>> list = new ArrayList<>();
        private final Set<Range> uniqRanges = new HashSet<>();
        private final int sizeTarget, numChildren;
        private final Gen<Range> rangeGen;

        private State(int sizeTarget, int numChildren, Gen<Range> rangeGen)
        {
            this.sizeTarget = sizeTarget;
            this.numChildren = numChildren;
            this.rangeGen = rangeGen;
        }

        public void add(Range range, int value)
        {
            list.add(Map.entry(range, value));
            uniqRanges.add(range);
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
        private final RTree<RoutingKey, Range, Integer> tree;

        private Sut(int sizeTarget, int numChildren)
        {
            tree = new RTree(Comparator.naturalOrder(), RTreeRangeAccessor.instance, sizeTarget, numChildren);
        }
    }
}
