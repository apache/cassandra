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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;

import com.google.common.base.Predicates;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.directComparable;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;
import static org.junit.Assert.assertEquals;

public class PrefixTailDeletionAwareTrieTest
extends PrefixTailTestBase<InMemoryDeletionAwareTrie<Object, TestRangeState>,
                           DeletionAwareTrie<Object, TestRangeState>>
{
    @Override
    InMemoryDeletionAwareTrie<Object, TestRangeState>[] makeArray(int length)
    {
        return new InMemoryDeletionAwareTrie[length];
    }

    @Override
    InMemoryDeletionAwareTrie<Object, TestRangeState> makeInMemoryTrie()
    {
        return InMemoryDeletionAwareTrie.shortLived(VERSION);
    }

    @Override
    void applyPrefixed(InMemoryDeletionAwareTrie<Object, TestRangeState> destination, ByteComparable prefix, InMemoryDeletionAwareTrie<Object, TestRangeState> tail, InMemoryBaseTrie.UpsertTransformer<Object, Object> upsertTransformer) throws TrieSpaceExhaustedException
    {
        destination.apply(tail.prefixedBy(prefix),
                          upsertTransformer,
                          (x, y) -> (TestRangeState) upsertTransformer.apply(x, y),
                          (x, y) -> { throw new AssertionError(); },
                          (x, y) -> { throw new AssertionError(); },
                          true,
                          Predicates.alwaysFalse());
    }

    @Override
    void apply(InMemoryDeletionAwareTrie<Object, TestRangeState> destination, DeletionAwareTrie<Object, TestRangeState> tail, UpsertTransformerWithKeys upsertTransformer) throws TrieSpaceExhaustedException
    {
        class Updater implements InMemoryTrie.UpsertTransformer<Object, Object>
        {
            InMemoryDeletionAwareTrie<Object, TestRangeState>.Mutator<Object, TestRangeState> mutator =
                destination.mutator(this,
                                    this::applyRangeState,
                                    (x, y) -> { throw new AssertionError(); },
                                    (x, y) -> { throw new AssertionError(); },
                                    true,
                                    Predicates.alwaysFalse());

            @Override
            public Object apply(Object existing, @Nonnull Object update)
            {
                return upsertTransformer.apply(existing, update, mutator);
            }

            public TestRangeState applyRangeState(TestRangeState existing, @Nonnull TestRangeState update)
            {
                return (TestRangeState) upsertTransformer.apply(existing, update, mutator);
            }
        }
        new Updater().mutator.apply(tail);
    }

    @Override
    DeletionAwareTrie<Object, TestRangeState> merge(InMemoryDeletionAwareTrie<Object, TestRangeState>[] tries, Trie.CollectionMergeResolver<Object> resolver)
    {
        return DeletionAwareTrie.merge(Arrays.asList(tries),
                                       resolver,
                                       TestRangeState::combineCollection,
                                       (d, v) -> { throw new AssertionError(); },
                                       true);
    }

    @Override
    DeletionAwareTrie<Object, TestRangeState> cast(InMemoryDeletionAwareTrie<Object, TestRangeState> inMemoryTrie)
    {
        return inMemoryTrie;
    }

    @Override
    void addToInMemoryTrie(Preencoded[] src,
                                  NavigableMap<Preencoded, ByteBuffer> content,
                                  InMemoryDeletionAwareTrie<Object, TestRangeState> trie)

    {
        for (Preencoded b : src)
            addToInMemoryTrie(content, trie, b);
    }

    @Override
    void addNthToInMemoryTrie(Preencoded[] src,
                                     NavigableMap<Preencoded, ByteBuffer> content,
                                     InMemoryDeletionAwareTrie<Object, TestRangeState> trie,
                                     int divisor,
                                     int remainder)

    {
        int i = 0;
        for (Preencoded b : src)
        {
            if (i++ % divisor != remainder)
                continue;

            addToInMemoryTrie(content, trie, b);
        }
    }

    private static void addToInMemoryTrie(Map<Preencoded, ByteBuffer> content, InMemoryDeletionAwareTrie<Object, TestRangeState> trie, Preencoded b)
    {
        // Note: Because we don't ensure order when calling resolve, just use a hash of the key as payload
        // (so that all sources have the same value).
        int payload = InMemoryTrieTestBase.asString(b).hashCode() & 0x7fffffff; // must be positive for TestRangeState
        ByteBuffer v = ByteBufferUtil.bytes(payload);
        content.put(b, v);
        if (InMemoryTrieTestBase.VERBOSE)
            System.out.println("Adding " + InMemoryTrieTestBase.asString(b) + ": " + ByteBufferUtil.bytesToHex(v));

        try
        {
            DeletionAwareTrie<Object, TestRangeState> toInsert;
            toInsert = (payload & 1) == 1 ? DeletionAwareTrie.deletedRange(ByteComparable.EMPTY, b, true, b, true, TrieUtil.VERSION, new TestRangeState(b, payload, payload)) : DeletionAwareTrie.singleton(b, VERSION, v);

            trie.apply(toInsert,
                       THROWING_UPSERT,
                       (x, y) -> y,
                       (x, y) -> {
                           throw new AssertionError();
                       },
                       (x, y) -> {
                           throw new AssertionError();
                       },
                       true,
                       Predicates.alwaysFalse());
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }

        if (InMemoryTrieTestBase.VERBOSE)
            System.out.println(trie.dump(InMemoryTrieTestBase::string));
    }

    @Override
    Trie<ByteBuffer> processContent(DeletionAwareTrie<Object, TestRangeState> trie)
    {
        return trie.mergedTrie((v, rs) ->
                               {
                                   if (v != null)
                                       return (v instanceof ByteBuffer) ? (ByteBuffer) v : null;

                                   assert rs != null;
                                   // We only want one side of the branch deletion marker pairs.
                                   if (rs.rightSide == -1)
                                       return null;

                                   return ByteBufferUtil.bytes(rs.rightSide);
                               });
    }

    @Test
    public void testPrefixedBySeparatelyDeletionsAtRoot() throws TrieSpaceExhaustedException
    {
        for (var prefix : Arrays.asList("a", "testd", "", "aaaa"))
            testPrefixedBySeparatelyCases("", prefix);
    }


    @Test
    public void testPrefixedBySeparatelyDeletionsBelowRoot() throws TrieSpaceExhaustedException
    {
        for (var delPrefix : Arrays.asList("a", "testd", "b", "aaaa"))
            for (var prefix : Arrays.asList("a", "testd", "b", "aaaa", ""))
                testPrefixedBySeparatelyCases(delPrefix, prefix);
    }

    private void testPrefixedBySeparatelyCases(String delPrefix, String prefix) throws TrieSpaceExhaustedException
    {
        System.out.println("delPrefix " + delPrefix + " prefix " + prefix);
        testPrefixedBySeparately("a", "b", "c", delPrefix, prefix);
        testPrefixedBySeparately("b", "a", "c", delPrefix, prefix);

        testPrefixedBySeparately("a", "ab", "ac", delPrefix, prefix);
        testPrefixedBySeparately("ab", "a", "a", delPrefix, prefix);

        testPrefixedBySeparately("testd", "testdelbegin", "testdelend", delPrefix, prefix);
        testPrefixedBySeparately("testdata", "testd", "testd", delPrefix, prefix);
    }

    void testPrefixedBySeparately(String dataKey, String delStart, String delEnd, String delBranchRoot, String testPrefix) throws TrieSpaceExhaustedException
    {
        TestRangeState deletion = TestRangeState.covering(5);
        Integer data = Integer.valueOf(55);
        InMemoryDeletionAwareTrie<Object, TestRangeState> source = makeTestDATrie(dataKey,
                                                                                  delBranchRoot,
                                                                                  delStart,
                                                                                  delEnd,
                                                                                  deletion,
                                                                                  data);

        var prefixed = source.prefixedBySeparately(directComparable(testPrefix), delBranchRoot.isEmpty());
        // quick sanity check first
        assertEquals(data, prefixed.get(directComparable(testPrefix + dataKey)));
        assertEquals(deletion, prefixed.applicableDeletion(directComparable(testPrefix + delBranchRoot + delStart)).succedingState(Direction.FORWARD));
        assertEquals(deletion, prefixed.applicableDeletion(directComparable(testPrefix + delBranchRoot + delEnd)).succedingState(Direction.FORWARD));


        InMemoryDeletionAwareTrie<Object, TestRangeState> expected = makeTestDATrie(testPrefix + dataKey,
                                                                                    "",
                                                                                    testPrefix + delBranchRoot + delStart,
                                                                                    testPrefix + delBranchRoot + delEnd,
                                                                                    deletion,
                                                                                    data);
//        System.out.println("Source\n" + source.dump());
//        System.out.println("Expected\n" + expected.dump());
//        System.out.println("Prefixed FORWARD\n" + prefixed.dump());
//        System.out.println("Prefixed REVERSE\n" + prefixed.process(Direction.REVERSE, new TrieDumper.DeletionAware<>(Object::toString, Object::toString)));

        BiConsumer<DeletionAwareTrie<Object, TestRangeState>, DeletionAwareTrie<Object, TestRangeState>>
            verifier = delBranchRoot.isEmpty() ? TrieUtil::assertTriesEqual : TrieUtil::assertTrieContentEqual;

        verifier.accept(expected, prefixed);

        for (var prefix : Arrays.asList(testPrefix, "", "a", "b", "test", dataKey, delBranchRoot, testPrefix + delBranchRoot))
        {
            Preencoded prefixKey = directComparable(prefix);
            for (boolean includeCovering : Arrays.asList(false, true))
                verifier.accept(expected.tailTrie(prefixKey, includeCovering), prefixed.tailTrie(prefixKey, includeCovering));
            // Make sure prefixedBySeparately does not return the original source's deletion branch
            // after taking tailCursor at some position below the root (tailTrie above does not check for deletion
            // branches when it sees a covering one; use descendAlong to explicitly skip below the deletion branch at
            // the root).
            verifier.accept(descendAlongTail(expected, prefixKey), descendAlongTail(prefixed, prefixKey));
        }

    }

    private <T, D extends RangeState<D>> DeletionAwareTrie<T, D> descendAlongTail(DeletionAwareTrie<T, D> trie, ByteComparable key)
    {
        DeletionAwareCursor<T, D> c = trie.cursor(Direction.FORWARD);
        if (c.descendAlong(key.asComparableBytes(c.byteComparableVersion())))
            return c::tailCursor;
        else
            return null;

    }

    private static @NonNull InMemoryDeletionAwareTrie<Object, TestRangeState> makeTestDATrie(String dataKey, String delBranchRoot, String delStart, String delEnd, TestRangeState deletion, Integer data) throws TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<Object, TestRangeState> source = InMemoryDeletionAwareTrie.shortLived(VERSION);
        var mutator = source.mutator((a, b) -> b,
                                     TestRangeState::upsert,
                                     (v, d) -> v,
                                     (d, v) -> v,
                                     true,
                                     Predicates.alwaysFalse());

        mutator.apply(DeletionAwareTrie.deletedRange(directComparable(delBranchRoot),
                                                     directComparable(delStart),
                                                     true,
                                                     directComparable(delEnd),
                                                     true,
                                                     VERSION,
                                                     deletion));
        source.putRecursive(directComparable(dataKey), data, (a, b) -> b);
        return source;
    }
}
