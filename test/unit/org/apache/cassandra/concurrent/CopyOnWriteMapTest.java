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

package org.apache.cassandra.concurrent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gen.IntGen;
import accord.utils.Gens;
import accord.utils.Property.Command;
import accord.utils.Property.Commands;
import accord.utils.RandomSource;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.stateful;

public class CopyOnWriteMapTest
{
    private static final Gen<IntGen> KEY_DIST = Gens.mixedDistribution(0, 100);
    private static final Gen<IntGen> VAL_DIST = Gens.mixedDistribution(0, 100);
    private static final Gen<IntGen> WEIGHT_DIST = Gens.mixedDistribution(0, 40);

    @Test
    public void statefulTest()
    {
        stateful().check(new Commands<State, Sut>()
        {
            @Override
            public Gen<State> genInitialState()
            {
                return rs -> new State(rs);
            }

            @Override
            public Sut createSut(State state)
            {
                Sut sut = new Sut();
                sut.state.putAll(state.state);
                return sut;
            }

            @Override
            public Gen<Command<State, Sut, ?>> commands(State state)
            {
                Map<Gen<Command<State, Sut, ?>>, Integer> possible = new HashMap<>();
                for (Command<State, Sut, ?> c : Arrays.asList(size, isEmpty, keySet, values, entrySet, equalsSelf))
                    possible.put(rs -> c, state.weightSimple);

                possible.put(rs -> containsKey(state.keyGen.nextInt(rs)), state.weightRead);
                possible.put(rs -> containsValue(state.valueGen.nextInt(rs)), state.weightRead);
                possible.put(rs -> get(state.keyGen.nextInt(rs)), state.weightRead);
                possible.put(rs -> getOrDefault(state.keyGen.nextInt(rs), state.valueGen.nextInt(rs)), state.weightRead);


                possible.put(rs -> put(state.keyGen.nextInt(rs), state.valueGen.nextInt(rs)), state.weightUpdate);
                possible.put(rs -> remove(state.keyGen.nextInt(rs)), state.weightUpdate);
                possible.put(rs -> putAll(randomMap(rs, state)), state.weightUpdate);
                possible.put(rs -> replaceAllIncrement, state.weightUpdate);
                possible.put(rs -> putIfAbsent(state.keyGen.nextInt(rs), state.valueGen.nextInt(rs)), state.weightUpdate);
                possible.put(rs -> remove(state.keyGen.nextInt(rs), state.valueGen.nextInt(rs)), state.weightUpdate);
                possible.put(rs -> replace(state.keyGen.nextInt(rs), state.valueGen.nextInt(rs), state.valueGen.nextInt(rs)), state.weightUpdate);
                possible.put(rs -> replace(state.keyGen.nextInt(rs), state.valueGen.nextInt(rs)), state.weightUpdate);
                possible.put(rs -> computeIfAbsent(state.keyGen.nextInt(rs), state.valueGen.nextInt(rs)), state.weightUpdate);
                possible.put(rs -> computeIfPresent(state.keyGen.nextInt(rs), state.valueGen.nextInt(rs)), state.weightUpdate);
                possible.put(rs -> compute(state.keyGen.nextInt(rs), state.valueGen.nextInt(rs)), state.weightUpdate);
                possible.put(rs -> merge(state.keyGen.nextInt(rs), state.valueGen.nextInt(rs)), state.weightUpdate);

                possible.put(rs -> clear, state.weightClear);
                return Gens.oneOf(possible);
            }
        });
    }

    private static Map<Integer, Integer> randomMap(RandomSource rs, State state)
    {
        int size = rs.nextInt(0, 5);
        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < size; i++)
        {
            int k;
            do
            {
                k = state.keyGen.nextInt(rs);
            }
            while (map.containsKey(k));
            map.put(k, state.valueGen.nextInt(rs));
        }
        return map;
    }

    private static class Base<V> implements Command<State, Sut, V>
    {
        private final Function<? super Map<Integer, Integer>, V> fn;
        private final Function<State, String> toString;

        protected Base(Function<? super Map<Integer, Integer>, V> fn, String toString)
        {
            this(fn, ignore -> toString);
        }

        protected Base(Function<? super Map<Integer, Integer>, V> fn,
                       Function<State, String> toString)
        {
            this.fn = fn;
            this.toString = toString;
        }

        @Override
        public V apply(State state) throws Throwable
        {
            return fn.apply(state.state);
        }

        @Override
        public V run(Sut sut) throws Throwable
        {
            return fn.apply(sut.state);
        }

        @Override
        public void checkPostconditions(State state, V expected,
                                        Sut sut, V actual)
        {
            Assertions.assertThat(actual).isEqualTo(expected);
        }

        @Override
        public String detailed(State state)
        {
            return toString.apply(state);
        }
    }

    private static final Base<Integer> size = new Base<>(Map::size, "size");
    private static final Base<Boolean> isEmpty = new Base<>(Map::isEmpty, "isEmpty");
    private static final Base<Void> clear = new Base<>(m -> {m.clear(); return null;}, "clear");
    private static final Base<Set<Integer>> keySet = new Base<>(Map::keySet, "keySet");
    private static final Base<Collection<Integer>> values = new Base<>(Map::values, "values") {
        @Override
        public void checkPostconditions(State state, Collection<Integer> expected, Sut sut, Collection<Integer> actual)
        {
            Assertions.assertThat(new ArrayList<>(actual)).containsExactlyInAnyOrderElementsOf(expected);
        }
    };
    private static final Base<Set<Map.Entry<Integer, Integer>>> entrySet = new Base<>(Map::entrySet, "entrySet");
    private static final Base<Boolean> equalsSelf = new Base<>(m -> m.equals(m), "equals(this)");
    private static final Base<Void> replaceAllIncrement = new Base<>(m -> {m.replaceAll((k, v) -> v++); return null;}, "replaceAll(increment)");

    private static Command<State, Sut, Boolean> containsKey(int key)
    {
        return new Base<>(map -> map.containsKey(key), ignore -> "containsKey(" + key + ")");
    }

    private static Command<State, Sut, Boolean> containsValue(int value)
    {
        return new Base<>(map -> map.containsValue(value), ignore -> "containsValue(" + value + ")");
    }

    private static Command<State, Sut, Integer> put(int key, int value)
    {
        return new Base<>(map -> map.put(key, value), ignore -> "put(" + key + ", " + value + ")");
    }

    private static Command<State, Sut, Void> putAll(Map<Integer, Integer> other)
    {
        return new Base<>(map -> {map.putAll(other); return null;}, ignore -> "putAll(" + other + ")");
    }

    private static Command<State, Sut, Integer> putIfAbsent(int key, int value)
    {
        return new Base<>(map -> map.putIfAbsent(key, value), ignore -> "putIfAbsent(" + key + ", " + value + ")");
    }

    private static Command<State, Sut, Integer> get(int key)
    {
        return new Base<>(map -> map.get(key), ignore -> "get(" + key + ")");
    }

    private static Command<State, Sut, Integer> getOrDefault(int key, int value)
    {
        return new Base<>(map -> map.getOrDefault(key, value), ignore -> "getOrDefault(" + key + ", " + value + ")");
    }

    private static Command<State, Sut, Integer> remove(int key)
    {
        return new Base<>(map -> map.remove(key), ignore -> "remove(" + key + ")");
    }

    private static Command<State, Sut, Boolean> remove(int key, int value)
    {
        return new Base<>(map -> map.remove(key, value), ignore -> "remove(" + key + ", " + value + ")");
    }

    private static Command<State, Sut, Boolean> replace(int key, int value, int newValue)
    {
        return new Base<>(map -> map.replace(key, value, newValue), ignore -> "replace(" + key + ", " + value + ", " + newValue + ")");
    }

    private static Command<State, Sut, Integer> replace(int key, int value)
    {
        return new Base<>(map -> map.replace(key, value), ignore -> "replace(" + key + ", " + value + ")");
    }

    private static Command<State, Sut, Integer> computeIfAbsent(int key, int value)
    {
        return new Base<>(map -> map.computeIfAbsent(key, ignore -> value), ignore -> "computeIfAbsent(" + key + ", " + value + ")");
    }

    private static Command<State, Sut, Integer> computeIfPresent(int key, int value)
    {
        return new Base<>(map -> map.computeIfPresent(key, (i1, old) -> old + value), ignore -> "computeIfPresent(" + key + ", " + value + ")");
    }

    private static Command<State, Sut, Integer> compute(int key, int value)
    {
        return new Base<>(map -> map.compute(key, (i1, old) -> (old == null ? 0 : old) - value), ignore -> "compute(" + key + ", " + value + ")");
    }

    private static Command<State, Sut, Integer> merge(int key, int value)
    {
        return new Base<>(map -> map.merge(key, value, (a, b) -> a + b), ignore -> "merge(" + key + ", " + value + ")");
    }
    
    private static class State
    {
        private final Map<Integer, Integer> state = new HashMap<>();
        private final IntGen keyGen, valueGen;
        private final int weightSimple;
        private final int weightRead;
        private final int weightUpdate;
        private final int weightClear;

        private State(RandomSource rs)
        {
            this.keyGen = KEY_DIST.next(rs);
            this.valueGen = VAL_DIST.next(rs);
            IntGen weightGen = WEIGHT_DIST.next(rs);
            weightSimple = weightGen.nextInt(rs);
            weightRead = weightGen.nextInt(rs);
            weightUpdate = weightGen.nextInt(rs);
            weightClear = weightGen.nextInt(rs);
            state.putAll(randomMap(rs, this));
        }
    }
    
    private static class Sut
    {
        private final CopyOnWriteMap<Integer, Integer> state = new CopyOnWriteMap<>();
    }
}