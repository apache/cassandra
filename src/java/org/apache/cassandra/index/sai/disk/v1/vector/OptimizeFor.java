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

package org.apache.cassandra.index.sai.disk.v1.vector;

import java.util.function.Function;

import static java.lang.Math.pow;

/**
 * Allows the vector index searches to be optimised for latency or recall. This is used by the
 * {@link org.apache.cassandra.index.sai.disk.v1.segment.VectorIndexSegmentSearcher} to determine how many results to ask the graph
 * to search for. If we are optimising for {@link #RECALL} we ask for more than the requested limit which
 * (since it will search deeper in the graph) will tend to surface slightly better results.
 */
public enum OptimizeFor
{
    LATENCY(limit -> 0.979 + 4.021 * pow(limit, -0.761)), // f(1) =  5.0, f(100) = 1.1, f(1000) = 1.0
    RECALL(limit -> 0.509 + 9.491 * pow(limit, -0.402));  // f(1) = 10.0, f(100) = 2.0, f(1000) = 1.1

    private final Function<Integer, Double> limitMultiplier;

    OptimizeFor(Function<Integer, Double> limitMultiplier)
    {
        this.limitMultiplier = limitMultiplier;
    }

    public int topKFor(int limit)
    {
        return (int)(limitMultiplier.apply(limit) * limit);
    }

    public static OptimizeFor fromString(String value)
    {
        return valueOf(value.toUpperCase());
    }
}
