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

import com.google.common.base.Predicates;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;

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
    void apply(InMemoryDeletionAwareTrie<Object, TestRangeState> destination, DeletionAwareTrie<Object, TestRangeState> tail, InMemoryBaseTrie.UpsertTransformerWithKeyProducer<Object, Object> upsertTransformer) throws TrieSpaceExhaustedException
    {
        destination.apply(tail,
                          upsertTransformer,
                          (x, y, keyProducer) -> (TestRangeState) upsertTransformer.apply(x, y, (InMemoryBaseTrie.KeyProducer) keyProducer),
                          (x, y, keyProducer) -> { throw new AssertionError(); },
                          (x, y) -> { throw new AssertionError(); },
                          true,
                          Predicates.alwaysFalse());
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
        int payload = InMemoryTrieTestBase.asString(b).hashCode();
        ByteBuffer v = ByteBufferUtil.bytes(payload);
        content.put(b, v);
        if (InMemoryTrieTestBase.VERBOSE)
            System.out.println("Adding " + InMemoryTrieTestBase.asString(b) + ": " + ByteBufferUtil.bytesToHex(v));

        try
        {
            DeletionAwareTrie<Object, TestRangeState> toInsert = ((payload & 1) == 1)
                           ? DeletionAwareTrie.deletion(ByteComparable.EMPTY, b, b, VERSION, new TestRangeState(b, payload, payload, payload, false))
                           : DeletionAwareTrie.singleton(b, VERSION, v);

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
            System.out.println(trie.dump(x -> InMemoryTrieTestBase.string(x)));
    }

    @Override
    Trie<ByteBuffer> processContent(DeletionAwareTrie<Object, TestRangeState> trie)
    {
        return trie.mergedTrie((v, rs) ->
                               {
                                   if (v != null)
                                       return (v instanceof ByteBuffer) ? (ByteBuffer) v : null;

                                   assert rs != null;
                                   return ByteBufferUtil.bytes(rs.at);
                               });
    }
}
