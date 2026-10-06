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

package org.apache.cassandra.db.tries;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.google.common.collect.Streams;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

interface DataPoint
{
    static LivePoint combineLive(LivePoint a, LivePoint b)
    {
        if (a == null)
            return b;
        if (b == null)
            return a;
        return LivePoint.combine(a, b);
    }

    static DeletionMarker combineDeletion(DeletionMarker a, DeletionMarker b)
    {
        if (a == null)
            return b;
        if (b == null)
            return a;
        return DeletionMarker.combine(a, b);
    }

    static LivePoint deleteLive(DeletionMarker deletion, LivePoint live)
    {
        if (deletion == null || live == null)
            return live;
        return deletion.applyTo(live);
    }

    static LivePoint deleteLive(LivePoint live, DeletionMarker deletion)
    {
        return deleteLive(deletion, live);
    }

    DeletionMarker marker();
    LivePoint live();
    ByteComparable position();

    DataPoint withMarker(DeletionMarker newMarker);
    DataPoint remap(ByteComparable newKey);

    static String toString(ByteComparable position)
    {
        if (position == null)
            return "null";
        return position.byteComparableAsString(VERSION);
    }

    static List<DataPoint> verify(List<DataPoint> dataPoints)
    {
        int active = -1;
        ByteComparable prev = null;
        for (DataPoint dp : dataPoints)
        {
            DeletionMarker marker = dp.marker();
            if (marker == null)
                continue;
            assertTrue("Order violation " + toString(prev) + " vs " + toString(marker.position),
                       prev == null || ByteComparable.compare(prev, marker.position, VERSION) < 0);
            assertEquals("Range close violation", active, marker.leftSide);
            assertTrue(marker.at != marker.leftSide || marker.at != marker.rightSide);
            prev = marker.position;
            active = marker.rightSide;
        }
        assertEquals(-1, active);
        return dataPoints;
    }

    static DataPoint resolve(LivePoint a, DeletionMarker m)
    {
        if (a == null)
            return m;
        if (m == null)
            return a;
        return new CombinedDataPoint(a, m);
    }

    static DataPoint combine(DataPoint a, DataPoint b)
    {
        LivePoint live = combine(a.live(), b.live(), LivePoint::combine);
        DeletionMarker marker = combine(a.marker(), b.marker(), DeletionMarker::combine);
        if (marker != null && live != null)
            live = marker.applyTo(live);
        return resolve(live, marker);
    }

    static <T> T combine(T a, T b, BiFunction<T, T, T> combiner)
    {
        if (a == null)
            return b;
        if (b == null)
            return a;
        return combiner.apply(a, b);
    }

    DataPoint toContent();

    /// Extract the values of the provided trie into a list.
    static List<DataPoint> toList(DeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        return Streams.stream(trie.mergedTrie(DataPoint::resolve).entryIterator())
                      .map(en -> en.getValue().remap(en.getKey()))
                      .collect(Collectors.toList());
    }

    /// Extract the values of the provided trie into a list.
    static List<LivePoint> contentOnlyList(DeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        return Streams.stream(trie.contentOnlyTrie().entryIterator())
                      .map(en -> en.getValue().remap(en.getKey()))
                      .collect(Collectors.toList());
    }

    /// Extract the values of the provided trie into a list.
    static List<DeletionMarker> deletionOnlyList(DeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        return Streams.stream(trie.deletionOnlyTrie().entryIterator())
                      .map(en -> en.getValue().remap(en.getKey()))
                      .collect(Collectors.toList());
    }

    static InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> fromList(List<DataPoint> list)
    {
        return fromList(list, false);
    }

    static InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> fromList(List<DataPoint> list, boolean forceCopy)
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        try
        {
            // If we put a deletion first, the deletion branch will start at the root which works but isn't interesting
            // enough as a test. So put the live data first.
            for (DataPoint i : list)
            {
                LivePoint live = i.live();
                if (live != null)
                {
                    trie.apply(
                        DeletionAwareTrie.<LivePoint,DeletionMarker>singleton(live.position, VERSION, live),
                            DataPoint::combineLive,
                            DataPoint::combineDeletion,
                            DataPoint::deleteLive,
                            DataPoint::deleteLive,
                            false,
                            v -> forceCopy
                    );
                }
            }

            // If we simply put all deletions with putAlternativeRecursive, we won't get correct branches as they
            // won't always close the intervals they open. Deletions need to be put as ranges instead.
            int active = -1;
            int activeStartedAt = -1;
            for (int i = 0; i < list.size(); ++i)
            {
                DeletionMarker marker = list.get(i).marker();
                if (marker == null || marker.leftSide == marker.rightSide)
                    continue;
                assert marker.leftSide == active;
                if (active != -1)
                {
                    if (marker == null || marker.leftSide == marker.rightSide)
                        continue;

                    DeletionMarker startMarker = list.get(activeStartedAt).marker();
                    assert startMarker != null;
                    int prefixLength = ByteComparable.diffPoint(startMarker.position, marker.position, VERSION) - 1;
                    trie.apply(
                            DeletionAwareTrie.deletion(ByteComparable.cut(startMarker.position, prefixLength),
                                    ByteComparable.skipFirst(startMarker.position, prefixLength),
                                    ByteComparable.skipFirst(marker.position, prefixLength),
                                    VERSION, marker.leftSideAsCovering),
                            DataPoint::combineLive,
                            DataPoint::combineDeletion,
                            DataPoint::deleteLive,
                            DataPoint::deleteLive,
                            false,
                            v -> forceCopy
                    );
                }

                active = marker.rightSide;
                activeStartedAt = i;
            }
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return trie;
    }

    static DeletionAwareTrie<LivePoint, DeletionMarker> dumpDeletionAwareTrie(DeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        System.out.println("DeletionAware");
        System.out.println(trie.dump());
        System.out.println("Merged");
        System.out.println(trie.mergedTrie(DataPoint::resolve).dump());
        return trie;
    }
}
