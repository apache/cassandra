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

import org.junit.BeforeClass;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;

/// Multithreaded test for [InMemoryDeletionAwareTrie] that creates both data and deletion entries.
///
/// This test extends [ThreadedTestBase] to verify that [InMemoryDeletionAwareTrie] works correctly
/// under concurrent access with both live data points and deletion markers being added.
public class InMemoryDeletionAwareTrieThreadedTest extends ThreadedTestBase<LivePoint, InMemoryDeletionAwareTrie<LivePoint, DeletionMarker>>
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    @Override
    LivePoint value(ByteComparable b)
    {
        // Create a live point with a timestamp based on the key's hash
        // This ensures all threads will create the same value for the same key
        String keyStr = b.byteComparableAsString(VERSION);
        int timestamp = Math.abs(keyStr.hashCode()) % 1000 + 1; // Ensure positive timestamp
        return new LivePoint(b, timestamp);
    }

    @Override
    InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> makeTrie(OpOrder readOrder)
    {
        return InMemoryDeletionAwareTrie.longLived(VERSION, readOrder);
    }

    @Override
    void add(InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie, ByteComparable b, LivePoint v, int iteration) throws TrieSpaceExhaustedException
    {
        // Alternate between adding live data and deletion markers
        if (iteration % 3 == 0)
        {
            // Add live data point using apply with singleton trie
            DeletionAwareTrie<LivePoint, DeletionMarker> singletonTrie = DeletionAwareTrie.singleton(b, VERSION, v);
            trie.apply(singletonTrie,
                      DataPoint::combineLive, // Combine live data using DataPoint utility
                      DataPoint::combineDeletion, // Combine deletion markers using DataPoint utility
                      DataPoint::deleteLive, // Apply deletions to existing data using DataPoint utility
                      DataPoint::deleteLive, // Apply deletions to incoming data using DataPoint utility
                      true, // deletionsAtFixedPoints = true (singleton deletions satisfy invariant)
                      x -> false); // needsForcedCopy = never force copy for this test
        }
        else if (iteration % 3 == 1)
        {
            // Add deletion marker using DeletionAwareTrie.deletion
            // Create a deletion marker that deletes data with timestamp less than current
            int deletionTime = v.timestamp + 10; // Delete older data
            DeletionMarker marker = new DeletionMarker(b, deletionTime, deletionTime);

            DeletionAwareTrie<LivePoint, DeletionMarker> deletionTrie =
            DeletionAwareTrie.deletedRange(b, b, true, b, true, TrieUtil.VERSION, marker);

            trie.apply(deletionTrie,
                      (existing, incoming) -> existing, // Keep existing live data (no incoming live data in deletion trie)
                      DataPoint::combineDeletion, // Combine deletion markers using DataPoint utility
                      DataPoint::deleteLive, // Apply deletions to existing data using DataPoint utility
                      DataPoint::deleteLive, // Apply deletions to incoming data using DataPoint utility
                      true, // deletionsAtFixedPoints = true (singleton deletions satisfy invariant)
                      x -> false); // needsForcedCopy = never force copy for this test
        }
        else
        {
            // Add a merge of singleton and deletion
            DeletionAwareTrie<LivePoint, DeletionMarker> singletonTrie = DeletionAwareTrie.singleton(b, VERSION, v);

            // Create a deletion marker that deletes data with timestamp less than current
            int deletionTime = v.timestamp + 5; // Delete slightly older data
            DeletionMarker marker = new DeletionMarker(b, deletionTime, deletionTime);
            DeletionAwareTrie<LivePoint, DeletionMarker> deletionTrie =
            DeletionAwareTrie.deletedRange(b, b, true, b, true, TrieUtil.VERSION, marker);

            // Merge singleton and deletion into a combined trie
            DeletionAwareTrie<LivePoint, DeletionMarker> combinedTrie =
                singletonTrie.mergeWith(deletionTrie,
                                       DataPoint::combineLive,
                                       DataPoint::combineDeletion,
                                       DataPoint::deleteLive,
                                       true); // deletionsAtFixedPoints = true

            trie.apply(combinedTrie,
                      DataPoint::combineLive, // Combine live data using DataPoint utility
                      DataPoint::combineDeletion, // Combine deletion markers using DataPoint utility
                      DataPoint::deleteLive, // Apply deletions to existing data using DataPoint utility
                      DataPoint::deleteLive, // Apply deletions to incoming data using DataPoint utility
                      true, // deletionsAtFixedPoints = true (singleton deletions satisfy invariant)
                      x -> false); // needsForcedCopy = never force copy for this test
        }
    }
}
