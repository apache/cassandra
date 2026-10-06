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

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;

/// Consistency test for [InMemoryDeletionAwareTrie] that validates concurrent operations
/// with both live data and deletion markers under different atomicity guarantees.
/// 
/// This test extends [ConsistencyTestBase] to verify that [InMemoryDeletionAwareTrie] maintains
/// correctness and consistency under concurrent access patterns typical of Cassandra's
/// memtable operations with deletions.
public class InMemoryDeletionAwareTrieConsistencyTest
extends ConsistencyTestBase<InMemoryDeletionAwareTrieConsistencyTest.Content,
                           DeletionAwareTrie<InMemoryDeletionAwareTrieConsistencyTest.Content, ConsistencyTestBase.TestRangeState>,
                           InMemoryDeletionAwareTrie<InMemoryDeletionAwareTrieConsistencyTest.Content, ConsistencyTestBase.TestRangeState>>
{

    @SuppressWarnings("rawtypes") // type does not matter, we are always throwing an exception
    static final InMemoryBaseTrie.UpsertTransformer UPSERT_THROW = (x, y) -> { throw new AssertionError(); };
    @SuppressWarnings("rawtypes") // type does not matter, we are always throwing an exception
    static final BiFunction BIFUNCTION_THROW = (x, y) -> { throw new AssertionError(); };

    @Override
    InMemoryDeletionAwareTrie<Content, TestRangeState> makeTrie(OpOrder readOrder)
    {
        return InMemoryDeletionAwareTrie.longLived(VERSION, readOrder);
    }

    @Override
    Content value(ByteComparable b, ByteComparable cprefix, ByteComparable c, int add, int seqId)
    {
        String pk = b.byteComparableAsString(VERSION);
        String ck = (cprefix != null ? cprefix.byteComparableAsString(VERSION) : "") + c.byteComparableAsString(VERSION);
        return new Value(pk, ck, add, seqId);
    }

    @Override
    Content metadata(ByteComparable b)
    {
        return new Metadata(b.byteComparableAsString(VERSION));
    }

    @Override
    String pk(Content c)
    {
        return c.pk;
    }

    @Override
    String ck(Content c)
    {
        return ((Value) c).ck;
    }

    @Override
    int seq(Content c)
    {
        return ((Value) c).seq;
    }

    @Override
    int value(Content c)
    {
        return ((Value) c).value;
    }

    @Override
    int updateCount(Content c)
    {
        return ((Metadata) c).updateCount;
    }

    @Override
    DeletionAwareTrie<Content, TestRangeState> makeSingleton(ByteComparable b, Content content)
    {
        return DeletionAwareTrie.singleton(b, VERSION, content);
    }

    @Override
    DeletionAwareTrie<Content, TestRangeState> withRootMetadata(DeletionAwareTrie<Content, TestRangeState> wrapped, Content metadata)
    {
        return TrieUtil.withRootMetadata(wrapped, metadata);
    }

    @Override
    DeletionAwareTrie<Content, TestRangeState> merge(Collection<DeletionAwareTrie<Content, TestRangeState>> tries,
                                                      Trie.CollectionMergeResolver<Content> mergeResolver)
    {
        return DeletionAwareTrie.merge(tries,
                                      mergeResolver,
                                      Trie.throwingResolver(),
                                      BIFUNCTION_THROW,
                                      true); // deletionsAtFixedPoints = true for consistency
    }

    @Override
    void apply(InMemoryDeletionAwareTrie<Content, TestRangeState> trie,
               DeletionAwareTrie<Content, TestRangeState> mutation,
               InMemoryBaseTrie.UpsertTransformer<Content, Content> mergeResolver,
               Predicate<InMemoryBaseTrie.NodeFeatures<Content>> forcedCopyChecker) throws TrieSpaceExhaustedException
    {
        trie.apply(mutation,
                  mergeResolver, // Use the provided merge resolver for content
                   (del, incoming) -> { throw new AssertionError(); },
                   (del, incoming) -> { throw new AssertionError(); },
                   (del, incoming) -> { throw new AssertionError(); },
                  true, // deletionsAtFixedPoints = true for consistency
                  forcedCopyChecker); // Use the provided forced copy checker
    }

    @Override
    void delete(InMemoryDeletionAwareTrie<Content, TestRangeState> trie,
                ByteComparable deletionPrefix,
                TestRangeState partitionMarker,
                RangeTrie<TestRangeState> deletionBranch,
                InMemoryBaseTrie.UpsertTransformer<Content, TestRangeState> mergeResolver,
                Predicate<InMemoryBaseTrie.NodeFeatures<TestRangeState>> forcedCopyChecker) throws TrieSpaceExhaustedException
    {
        DeletionAwareTrie<TestRangeState, TestRangeState> deletion = DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY, VERSION, deletionBranch);
        deletion = TrieUtil.withRootMetadata(deletion, partitionMarker);
        deletion = deletion.prefixedBy(deletionPrefix);

        trie.apply(deletion,
                  mergeResolver,
                  (existing, incoming) -> TestRangeState.combine(existing, incoming),
                  mergeResolver,
                  BIFUNCTION_THROW,
                  true,
                  forcedCopyChecker);
    }

    @Override
    boolean isPartition(Content c)
    {
        return c != null && c.isPartition();
    }

    @Override
    Content mergeMetadata(Content c1, Content c2)
    {
        if (c1 == null) return c2;
        if (c2 == null) return c1;
        return ((Metadata) c1).mergeWith((Metadata) c2);
    }

    @Override
    Content deleteMetadata(Content existing, int entriesToRemove)
    {
        if (existing == null) return null;
        return ((Metadata) existing).delete(entriesToRemove);
    }

    @Override
    void printStats(InMemoryDeletionAwareTrie<Content, TestRangeState> trie,
                    Predicate<InMemoryBaseTrie.NodeFeatures<Content>> forcedCopyChecker)
    {
        System.out.format("DeletionAware Reuse %s %s on-heap %,d (+%,d) off-heap %,d\n",
                          trie.cellAllocator.getClass().getSimpleName(),
                          trie.bufferType,
                          trie.usedSizeOnHeap(),
                          trie.unusedReservedOnHeapMemory(),
                          trie.usedSizeOffHeap());
    }

    // Content hierarchy for deletion-aware consistency testing
    abstract static class Content
    {
        final String pk;

        Content(String pk)
        {
            this.pk = pk;
        }

        abstract boolean isPartition();
    }

    static class Value extends Content
    {
        final String ck;
        final int value;
        final int seq;

        Value(String pk, String ck, int value, int seq)
        {
            super(pk);
            this.ck = ck;
            this.value = value;
            this.seq = seq;
        }

        @Override
        public String toString()
        {
            return "Value{" +
                   "pk='" + pk + '\'' +
                   ", ck='" + ck + '\'' +
                   ", value=" + value +
                   ", seq=" + seq +
                   '}';
        }

        @Override
        boolean isPartition()
        {
            return false;
        }
    }

    static class Metadata extends Content
    {
        int updateCount;

        Metadata(String pk)
        {
            super(pk);
            updateCount = 1;
        }

        @Override
        boolean isPartition()
        {
            return true;
        }

        Metadata mergeWith(Metadata other)
        {
            Metadata m = new Metadata(pk);
            m.updateCount = updateCount + other.updateCount;
            return m;
        }

        Metadata delete(int entriesToRemove)
        {
            assert updateCount >= entriesToRemove;
            if (updateCount == entriesToRemove)
                return null;
            Metadata m = new Metadata(pk);
            m.updateCount = updateCount - entriesToRemove;
            return m;
        }

        @Override
        public String toString()
        {
            return "Metadata{" +
                   "pk='" + pk + '\'' +
                   ", updateCount=" + updateCount +
                   '}';
        }
    }
}
