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
package org.apache.cassandra.journal;

import java.io.IOException;
import java.util.Collection;

/**
 * Decides which static segments of a {@link Journal} are compacted, and how. Used by {@link Compactor}
 * to run periodic or on-demand compaction passes.
 */
public interface SegmentCompactor<K, V>
{
    /** A no-op compactor */
    SegmentCompactor<?, ?> NOOP = (SegmentCompactor<Object, Object>) (segments) -> segments;

    static <K, V> SegmentCompactor<K, V> noop()
    {
        //noinspection unchecked
        return (SegmentCompactor<K, V>) NOOP;
    }

    /**
     * Picks which of the given candidate segments should be compacted this pass. Defaults to selecting
     * all of them.
     *
     * @param candidates all static segments currently eligible for compaction
     * @return the subset of {@code candidates} to pass to {@link #compact(Collection)}
     */
    default Collection<StaticSegment<K, V>> select(Collection<StaticSegment<K, V>> candidates)
    {
        return candidates;
    }

    /**
     * Compacts the given {@code segments}, previously chosen by {@link #select(Collection)}.
     *
     * @param segments the segments to compact
     * @return the new segments that replace them; empty when the segments are dropped
     */
    Collection<StaticSegment<K, V>> compact(Collection<StaticSegment<K, V>> segments) throws IOException;

    /**
     * Invoked at the end of every {@link Compactor} run, whether or not any segments were selected or
     * compacted this pass, and whether or not the pass failed.
     */
    default void onCompacted()
    {
    }
}
