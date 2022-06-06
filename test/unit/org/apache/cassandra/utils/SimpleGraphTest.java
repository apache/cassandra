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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.assertj.core.api.Assertions;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleGraphTest
{
    @Test
    public void empty()
    {
        Assertions.assertThatThrownBy(() -> new SimpleGraph.Builder<String>().build())
                  .isInstanceOf(AssertionError.class)
                  .hasMessage("Edges empty");
    }

    /**
     * If vertices have edges that form a circle this should not cause {@link SimpleGraph#findPaths(Object, Object)} to
     * hang.
     */
    @Test
    public void recursive()
    {
        SimpleGraph<String> graph = of("A", "B",
                                       "B", "C",
                                       "C", "A");
        // no paths to identity
        assertThat(graph.findPaths("A", "A")).isEmpty();
        assertThat(graph.findPaths("B", "B")).isEmpty();
        assertThat(graph.findPaths("C", "C")).isEmpty();

        assertThat(graph.findPaths("C", "B")).isEqualTo(Collections.singletonList(Arrays.asList("C", "A", "B")));

        // all options return and don't have duplicate keys
        for (String i : graph.vertices())
        {
            for (String j : graph.vertices())
            {
                List<List<String>> paths = graph.findPaths(i, j);
                for (List<String> path : paths)
                {
                    Map<String, Integer> distinct = countDistinct(path);
                    for (Map.Entry<String, Integer> e : distinct.entrySet())
                        assertThat(e.getValue()).describedAs("Duplicate vertex %s found; %s", e.getKey(), path).isEqualTo(1);
                }
            }
        }
    }

    @Test
    public void simple()
    {
        SimpleGraph<String> graph = of("A", "B",
                                       "B", "C",
                                       "C", "D");

        assertThat(graph.findPaths("A", "B")).isEqualTo(Collections.singletonList(Arrays.asList("A", "B")));
        assertThat(graph.findPaths("A", "C")).isEqualTo(Collections.singletonList(Arrays.asList("A", "B", "C")));
        assertThat(graph.findPaths("B", "D")).isEqualTo(Collections.singletonList(Arrays.asList("B", "C", "D")));

        assertThat(graph.hasEdge("A", "B")).isTrue();
        assertThat(graph.hasEdge("C", "D")).isTrue();
        assertThat(graph.hasEdge("B", "A")).isFalse();
        assertThat(graph.hasEdge("C", "B")).isFalse();
    }

    private static <T> Map<T, Integer> countDistinct(List<T> list)
    {
        Map<T, Integer> map = new HashMap<>();
        for (T t : list)
            map.compute(t, (ignore, accum) -> accum == null ? 1 : accum + 1);
        return map;
    }

    static <T> SimpleGraph<T> of(T... values)
    {
        assert values.length % 2 == 0: "graph requires even number of values, but given " + values.length;
        SimpleGraph.Builder<T> builder = new SimpleGraph.Builder<>();
        for (int i = 0; i < values.length; i = i + 2)
            builder.addEdge(values[i], values[i + 1]);
        return builder.build();
    }
}