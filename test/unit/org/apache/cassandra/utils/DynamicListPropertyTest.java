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

import org.junit.Test;

import accord.utils.Property;
import accord.utils.RandomSource;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stateful property-based tests for {@link DynamicList}.
 * <p>
 * Uses an {@link java.util.ArrayList} as the oracle model to verify that
 * append, remove, and indexed get operations maintain consistency with the
 * skip list's internal structure.
 */
public class DynamicListPropertyTest
{
    private static class State
    {
        final DynamicList<Integer> sut;
        final ArrayList<Integer> model = new ArrayList<>();
        final ArrayList<DynamicList.Node<Integer>> nodes = new ArrayList<>();
        int counter = 0;

        State(RandomSource rs)
        {
            int maxExpectedSize = rs.nextInt(4, 128);
            sut = new DynamicList<>(maxExpectedSize);
        }

        @Override
        public String toString()
        {
            return "State{size=" + model.size() + ", model=" + model + '}';
        }
    }

    private static Property.Command<State, Void, ?> append(RandomSource rs, State state)
    {
        int value = state.counter++;
        return new Property.SimpleCommand<>("Append(" + value + ')', s -> {
            DynamicList.Node<Integer> node = s.sut.append(value);
            assertThat(node).as("append should return a non-null node").isNotNull();
            s.model.add(value);
            s.nodes.add(node);
            assertThat(s.sut.size()).as("size after append").isEqualTo(s.model.size());
        });
    }

    private static Property.Command<State, Void, ?> remove(RandomSource rs, State state)
    {
        int index = rs.nextInt(state.model.size());
        int value = state.model.get(index);
        return new Property.SimpleCommand<>("Remove(index=" + index + ", value=" + value + ')', s -> {
            DynamicList.Node<Integer> node = s.nodes.get(index);
            s.sut.remove(node);
            s.model.remove(index);
            s.nodes.remove(index);
            assertThat(s.sut.size()).as("size after remove").isEqualTo(s.model.size());
        });
    }

    private static Property.Command<State, Void, ?> get(RandomSource rs, State state)
    {
        int index = rs.nextInt(state.model.size());
        return new Property.SimpleCommand<>("Get(" + index + ')', s -> {
            Integer actual = s.sut.get(index);
            Integer expected = s.model.get(index);
            assertThat(actual).as("get(%d)", index).isEqualTo(expected);
        });
    }

    private static Property.Command<State, Void, ?> getOutOfBounds(RandomSource rs, State state)
    {
        return new Property.SimpleCommand<>("GetOutOfBounds(size=" + state.model.size() + ')', s -> {
            Integer result = s.sut.get(s.sut.size());
            assertThat(result).as("get(size) should return null").isNull();
        });
    }

    private static Property.Command<State, Void, ?> verifyContents(RandomSource rs, State state)
    {
        return new Property.SimpleCommand<>("VerifyContents(size=" + state.model.size() + ')', s -> {
            assertThat(s.sut.size()).as("size").isEqualTo(s.model.size());
            for (int i = 0; i < s.model.size(); i++)
                assertThat(s.sut.get(i)).as("get(%d)", i).isEqualTo(s.model.get(i));
            assertThat(s.sut.get(s.model.size())).as("get(size) should be null").isNull();
        });
    }

    private static Property.Command<State, Void, ?> appendWithMaxSize(RandomSource rs, State state)
    {
        int value = state.counter++;
        int maxSize = state.model.size() + rs.nextInt(0, 3);
        return new Property.SimpleCommand<>("AppendWithMaxSize(value=" + value + ", maxSize=" + maxSize + ')', s -> {
            DynamicList.Node<Integer> node = s.sut.append(value, maxSize);
            if (s.model.size() >= maxSize)
            {
                assertThat(node).as("append should return null when at max size").isNull();
            }
            else
            {
                assertThat(node).as("append should return non-null when below max size").isNotNull();
                s.model.add(value);
                s.nodes.add(node);
            }
            assertThat(s.sut.size()).as("size after appendWithMaxSize").isEqualTo(s.model.size());
        });
    }

    private static Property.Command<State, Void, ?> verifyStructure(RandomSource rs, State state)
    {
        return new Property.SimpleCommand<>("VerifyStructure(size=" + state.model.size() + ')', s -> {
            assertThat(s.sut.isWellFormed()).as("isWellFormed").isTrue();
        });
    }

    @Test
    public void appendRemoveGetMaintainsModelConsistency()
    {
        stateful().withExamples(500).withSteps(1000).check(commands(() -> State::new)
                                                          .add(3, DynamicListPropertyTest::append)
                                                          .addIf(s -> !s.model.isEmpty(), DynamicListPropertyTest::remove)
                                                          .addIf(s -> !s.model.isEmpty(), DynamicListPropertyTest::get)
                                                          .add(DynamicListPropertyTest::getOutOfBounds)
                                                          .add(DynamicListPropertyTest::verifyContents)
                                                          .add(DynamicListPropertyTest::verifyStructure)
                                                          .add(DynamicListPropertyTest::appendWithMaxSize)
                                                          .build());
    }
}
