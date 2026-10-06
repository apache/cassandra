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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Predicates;
import com.google.common.collect.Streams;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.directComparable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for DeletionAwareTrie#tailTrie and #tailTries methods, specifically focusing on
 * the includeCoveringDeletions flag functionality.
 */
public class DeletionAwareTailTrieTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    static ByteComparable EMPTY = ByteComparable.preencoded(VERSION, new byte[0]);
    static String value1 = "value1";
    static String value2 = "value2";
    static String value3 = "value3";
    static String value4 = "value4";
    static String value5 = "value5";
    static String value6 = "value6";
    static String value7 = "value7";
    static ByteComparable key1 = directComparable("partition/key1");
    static ByteComparable key2 = directComparable("partition/key2");
    static ByteComparable key3 = directComparable("partition/key3");
    static ByteComparable key4 = directComparable("partition/key4");
    static ByteComparable key5 = directComparable("partition/key5");
    static ByteComparable key6 = directComparable("partition/key6");
    static ByteComparable key7 = directComparable("partition/key7");
    static ByteComparable partition = directComparable("partition");

    static InMemoryDeletionAwareTrie<String, TestRangeState> trie = makeTrie();

    @Test
    public void testTailAtPartitionWithCoveringDeletionIncluded()
    {
        testTailTrie("partition", true,
                     EMPTY, TestRangeState.open(100),
                     directComparable("/key1"), value1,
                     directComparable("/key2"), TestRangeState.boundary(100, 200),
                     directComparable("/key2"), TestRangeState.boundary(200, 100),
                     directComparable("/key3"), value3,
                     directComparable("/key4"), TestRangeState.boundary(100, 150),
                     directComparable("/key4"), value4,
                     directComparable("/key5"), TestRangeState.boundary(150, 200),
                     directComparable("/key5"), value5,
                     directComparable("/key5"), TestRangeState.boundary(200, 150),
                     directComparable("/key6"), value6,
                     directComparable("/key7"), value7,
                     directComparable("/key7"), TestRangeState.boundary(150, 100),
                     EMPTY, TestRangeState.close(100));
    }

    @Test
    public void testTailBelowPartitionWithCoveringDeletionIncluded()
    {
        testTailTrie("partition/key", true,
                     EMPTY, TestRangeState.open(100),
                     directComparable("1"), value1,
                     directComparable("2"), TestRangeState.boundary(100, 200),
                     directComparable("2"), TestRangeState.boundary(200, 100),
                     directComparable("3"), value3,
                     directComparable("4"), TestRangeState.boundary(100, 150),
                     directComparable("4"), value4,
                     directComparable("5"), TestRangeState.boundary(150, 200),
                     directComparable("5"), value5,
                     directComparable("5"), TestRangeState.boundary(200, 150),
                     directComparable("6"), value6,
                     directComparable("7"), value7,
                     directComparable("7"), TestRangeState.boundary(150, 100),
                     EMPTY, TestRangeState.close(100));
    }

    @Test
    public void testTailAtDataKeyWithCoveringDeletionIncluded()
    {
        testTailTrie("partition/key3", true,
                     EMPTY, TestRangeState.open(100),
                     EMPTY, value3,
                     EMPTY, TestRangeState.close(100));
    }

    @Test
    public void testTailAtDeletedKeyWithCoveringDeletionIncluded()
    {
        testTailTrie("partition/key2", true,
                     EMPTY, TestRangeState.open(200),
                     EMPTY, TestRangeState.close(200));
    }

    @Test
    public void testTailAtDataKeyWithCoveringRangeDeletionIncluded()
    {
        testTailTrie("partition/key6", true,
                     EMPTY, TestRangeState.open(150),
                     EMPTY, value6,
                     EMPTY, TestRangeState.close(150));
    }

    @Test
    public void testTailAtDeletedKeyWithCoveringRangeDeletionIncluded()
    {
        testTailTrie("partition/key5", true,
                     EMPTY, TestRangeState.open(200),
                     EMPTY, value5,
                     EMPTY, TestRangeState.close(200));
    }

    @Test
    public void testTriesCoveringIncluded()
    {
        testTailTries(true,
                      tail("partition/key1",
                           EMPTY, TestRangeState.open(100),
                           EMPTY, value1,
                           EMPTY, TestRangeState.close(100)),
                      tail("partition/key3",
                           EMPTY, TestRangeState.open(100),
                           EMPTY, value3,
                           EMPTY, TestRangeState.close(100)),
                      tail("partition/key4",
                           EMPTY, TestRangeState.open(150),
                           EMPTY, value4,
                           EMPTY, TestRangeState.close(150)),
                      tail("partition/key5",
                           EMPTY, TestRangeState.open(200),
                           EMPTY, value5,
                           EMPTY, TestRangeState.close(200)),
                      tail("partition/key6",
                           EMPTY, TestRangeState.open(150),
                           EMPTY, value6,
                           EMPTY, TestRangeState.close(150)),
                      tail("partition/key7",
                           EMPTY, TestRangeState.open(150),
                           EMPTY, value7,
                           EMPTY, TestRangeState.close(150)));
    }

    @Test
    public void testTailAtPartitionWithCoveringDeletionExcluded()
    {
        testTailTrie("partition", false,
                     EMPTY, TestRangeState.open(100),
                     directComparable("/key1"), value1,
                     directComparable("/key2"), TestRangeState.boundary(100, 200),
                     directComparable("/key2"), TestRangeState.boundary(200, 100),
                     directComparable("/key3"), value3,
                     directComparable("/key4"), TestRangeState.boundary(100, 150),
                     directComparable("/key4"), value4,
                     directComparable("/key5"), TestRangeState.boundary(150, 200),
                     directComparable("/key5"), value5,
                     directComparable("/key5"), TestRangeState.boundary(200, 150),
                     directComparable("/key6"), value6,
                     directComparable("/key7"), value7,
                     directComparable("/key7"), TestRangeState.boundary(150, 100),
                     EMPTY, TestRangeState.close(100));
    }

    @Test
    public void testTailBelowPartitionWithCoveringDeletionExcluded()
    {
        testTailTrie("partition/key", false,
                     directComparable("1"), value1,
                     directComparable("2"), TestRangeState.open(200),
                     directComparable("2"), TestRangeState.close(200),
                     directComparable("3"), value3,
                     directComparable("4"), TestRangeState.open(150),
                     directComparable("4"), value4,
                     directComparable("5"), TestRangeState.boundary(150, 200),
                     directComparable("5"), value5,
                     directComparable("5"), TestRangeState.boundary(200, 150),
                     directComparable("6"), value6,
                     directComparable("7"), value7,
                     directComparable("7"), TestRangeState.close(150));
    }

    @Test
    public void testTailAtDataKeyWithCoveringDeletionExcluded()
    {
        testTailTrie("partition/key3", false,
                     EMPTY, value3);
    }

    @Test
    public void testTailAtDataKeyWithCoveringRangeDeletionExcluded()
    {
        testTailTrie("partition/key6", false,
                     EMPTY, value6);
    }

    @Test
    public void testTailAtDeletedKeyWithCoveringRangeDeletionExcluded()
    {
        testTailTrie("partition/key5", false,
                     EMPTY, TestRangeState.open(200),
                     EMPTY, value5,
                     EMPTY, TestRangeState.close(200));
    }

    @Test
    public void testTriesCoveringExcluded()
    {
        testTailTries(false,
                      tail("partition/key1",
                           EMPTY, value1),
                      tail("partition/key3",
                           EMPTY, value3),
                      tail("partition/key4",
                      // Note: Because in forward the range deletion starts in this branch, the ignoreCoveringDeletions
                      // option does not treat it as covering and must close it at the end of the tail.
                                 arr(EMPTY, TestRangeState.open(150),
                                     EMPTY, value4,
                                     EMPTY, TestRangeState.close(150)),
                                 arr(EMPTY, value4)),
                      tail("partition/key5",
                           EMPTY, TestRangeState.open(200),
                           EMPTY, value5,
                           EMPTY, TestRangeState.close(200)),
                      tail("partition/key6",
                           EMPTY, value6),
                      tail("partition/key7",
                           arr(EMPTY, value7),
                      // Note: Because in reverse the range deletion starts in this branch, the ignoreCoveringDeletions
                      // option does not treat it as covering and must close it at the end of the tail.
                           arr(EMPTY, TestRangeState.open(150),
                               EMPTY, value7,
                               EMPTY, TestRangeState.close(150))));
    }

    static Object[] arr(Object... data)
    {
        return data;
    }

    static class TailExpectations
    {
        final ByteComparable key;
        final Object[] forwardExpectations;
        final Object[] reverseExpectations;

        TailExpectations(ByteComparable key, Object[] forwardExpectations, Object[] reverseExpectations)
        {
            this.key = key;
            this.forwardExpectations = forwardExpectations;
            this.reverseExpectations = reverseExpectations;
        }
    }

    static TailExpectations tail(String key, Object... expectations)
    {
        return new TailExpectations(directComparable(key), expectations, expectations);
    }

    static TailExpectations tail(String key, Object[] fwdExpectations, Object[] revExpectations)
    {
        return new TailExpectations(directComparable(key), fwdExpectations, revExpectations);
    }

    void testTailTrie(String key, boolean includeCoveringDeletions, Object... expectedData)
    {
        testTailTrie(Direction.FORWARD, key, includeCoveringDeletions, expectedData);
        testTailTrie(Direction.REVERSE, key, includeCoveringDeletions, expectedData);
    }
    void testTailTrie(Direction tailDirection, String key, boolean includeCoveringDeletions, Object... expectedData)
    {
        // Get tail trie at "partition" with includeCoveringDeletions=true
        DeletionAwareTrie<String, TestRangeState> tail = trie.tailTrie(directComparable(key), includeCoveringDeletions);
        assertNotNull("Tail trie should not be null", tail);

        // Verify the tail has the deletion branch at its root
        if (Stream.of(expectedData).anyMatch(TestRangeState.class::isInstance))
        {
            DeletionAwareCursor<String, TestRangeState> cursor = tail.cursor(tailDirection);
            RangeCursor<TestRangeState> deletionBranchCursor = cursor.deletionBranchCursor(tailDirection);
            assertNotNull("Deletion branch should be present at root when including covering deletions", deletionBranchCursor);
        }

        System.out.println(trie.dump());
        System.out.println(tail.dump());

        // Verify the content of the tail includes all data.
        var list = collectEntriesAsList(tail, tailDirection);
        var expected = tailDirection.isForward() ? Arrays.asList(expectedData)
                       : makeReversedExpectations(expectedData);

        assertEquals(expected, list);
    }

    private List<Object> makeReversedExpectations(Object[] data)
    {
        // reverse the pairs
        List<Object> reversed = new ArrayList<>();
        for (int i = data.length - 2; i >= 0; i-=2)
        {
            reversed.add(data[i]);
            reversed.add(data[i + 1]);
        }
        return reversed;
    }

    private void testTailTries(boolean includeCoveringDeletions, TailExpectations... tails)
    {
        testTailTries(Direction.FORWARD, includeCoveringDeletions, tails);
        testTailTries(Direction.REVERSE, includeCoveringDeletions, tails);
    }

    private void testTailTries(Direction direction, boolean includeCoveringDeletions, TailExpectations... tails)
    {
        int idx = direction.select(0, tails.length - 1);
        for (var tailEntry : trie.tailTries(direction, Predicates.alwaysTrue(), includeCoveringDeletions))
        {
            var tail = tails[idx];
            System.out.println("Trie at " + tailEntry.getKey().byteComparableAsString(VERSION));
            System.out.println(tailEntry.getValue().dump());
            // Check the key
            assertEquals(0, ByteComparable.compare(tail.key, tailEntry.getKey(), VERSION));

            // Verify the content of the tail includes all data.
            var list = collectEntriesAsList(tailEntry.getValue(), direction);
            var expected = direction.isForward() ? Arrays.asList(tail.forwardExpectations)
                                                 : makeReversedExpectations(tail.reverseExpectations);

            assertEquals(expected, list);
            idx += direction.increase;
        }
    }

    private static InMemoryDeletionAwareTrie<String, TestRangeState> makeTrie()
    {
        try
        {
            // Create an in-memory trie with live data and deletions
            InMemoryDeletionAwareTrie<String, TestRangeState> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);

            // Add live data at partition/key1, partition/key2, partition/key3

            trie.putRecursive(key1, value1, (e, u) -> u);
            trie.putRecursive(key3, value3, (e, u) -> u);
            trie.putRecursive(key4, value4, (e, u) -> u);
            trie.putRecursive(key5, value5, (e, u) -> u);
            trie.putRecursive(key6, value6, (e, u) -> u);
            trie.putRecursive(key7, value7, (e, u) -> u);

            // Deletion at key2
            trie.apply(DeletionAwareTrie.deletedRange(key2,
                                                      EMPTY,
                                                      true,
                                                      EMPTY,
                                                      true,
                                                      VERSION,
                                                      TestRangeState.covering(200)),
                       (e, u) -> {
                           throw new AssertionError("Should not merge data");
                       },
                       TestRangeState::upsert,
                       (d, v) -> d, // keep covered data
                       (d, v) -> {
                           throw new AssertionError();
                       },
                       false,
                       x -> false);
            // Deletion at key5
            trie.apply(DeletionAwareTrie.deletedRange(key5,
                                                      EMPTY,
                                                      true,
                                                      EMPTY,
                                                      true,
                                                      VERSION,
                                                      TestRangeState.covering(200)),
                       (e, u) -> {
                           throw new AssertionError("Should not merge data");
                       },
                       TestRangeState::upsert,
                       (d, v) -> d, // keep covered data
                       (d, v) -> {
                           throw new AssertionError();
                       },
                       false,
                       x -> false);

            // Deletion range key4-key6 inclusive
            trie.apply(DeletionAwareTrie.deletedRange(partition,
                                                      directComparable("/key4"),
                                                      true,
                                                      directComparable("/key7"),
                                                      true,
                                                      VERSION,
                                                      TestRangeState.covering(150)),
                       (e, u) -> {
                           throw new AssertionError("Should not merge data");
                       },
                       TestRangeState::upsert,
                       (d, v) -> d, // keep covered data
                       (d, v) -> {
                           throw new AssertionError();
                       },
                       false,
                       x -> false);

            // Create a deletion range at partition level covering key1 to key3
            TestRangeState deletion = TestRangeState.covering(100);
            RangeTrie<TestRangeState> rangeTrie = RangeTrie.branch(EMPTY, VERSION, deletion);

            // Apply the deletion branch at partition level
            DeletionAwareTrie<TestRangeState, TestRangeState> deletionBranch =
                DeletionAwareTrie.deletionBranch(partition, VERSION, rangeTrie);

            trie.apply(deletionBranch,
                       (e, u) -> {
                           throw new AssertionError("Should not merge data");
                       },
                       TestRangeState::upsert,
                       (d, v) -> d, // keep covered data
                       (d, v) -> {
                           throw new AssertionError();
                       },
                       false,
                       x -> false);

            return trie;
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    private static List<Object> collectEntriesAsList(DeletionAwareTrie<String, TestRangeState> tail, Direction direction)
    {
        return Streams.stream(tail.mergedTrie((x, y) -> x != null
                                                        ? y != null && y.isBoundary()
                                                          ? Pair.create(y, x) // deletion first
                                                          : x
                                                        : y)
                                  .entrySet(direction))
                      .flatMap(en -> en.getValue() instanceof Pair
                                     ? Stream.of(en.getKey(), ((Pair<?, ?>) en.getValue()).left,
                                                 en.getKey(), ((Pair<?, ?>) en.getValue()).right)
                                     : Stream.of(en.getKey(), en.getValue()))
                      .collect(Collectors.toList());
    }

    /**
     * Test tailTries iteration with includeCoveringDeletions=true.
     */
    @Test
    public void testTailTriesWithCoveringDeletionsIncluded() throws Exception
    {
        InMemoryDeletionAwareTrie<String, TestRangeState> trie = makeTrie();
        
        // Iterate with includeCoveringDeletions=true
        List<ByteComparable> keys = new ArrayList<>();
        for (var entry : trie.tailTries(Direction.FORWARD, v -> v instanceof String, true))
        {
            keys.add(entry.getKey());
            assertNotNull("Tail trie should not be null", entry.getValue());
        }
        
        assertTrue("Should have found some tail tries", keys.size() > 0);
    }

    /**
     * Test tailTries iteration with includeCoveringDeletions=false.
     */
    @Test
    public void testTailTriesWithCoveringDeletionsExcluded() throws Exception
    {
        InMemoryDeletionAwareTrie<String, TestRangeState> trie = makeTrie();
        
        // Iterate with includeCoveringDeletions=false
        List<ByteComparable> keys = new ArrayList<>();
        for (var entry : trie.tailTries(Direction.FORWARD, v -> v instanceof String, false))
        {
            keys.add(entry.getKey());
            assertNotNull("Tail trie should not be null", entry.getValue());
        }
        
        assertTrue("Should have found some tail tries", keys.size() > 0);
    }
}
