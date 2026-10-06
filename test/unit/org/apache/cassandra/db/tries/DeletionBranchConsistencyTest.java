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
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;

/// Consistency test for [InMemoryDeletionAwareTrie] that validates concurrent operations
/// with both live data and deletion markers under different atomicity guarantees.
/// This test extends [ConsistencyTestBase] to verify that [InMemoryDeletionAwareTrie] maintains
/// correctness and consistency under concurrent access patterns typical of Cassandra's
/// memtable operations with deletions.
public class DeletionBranchConsistencyTest
extends ConsistencyTestBase<ConsistencyTestBase.TestStateMetadata,
                           DeletionAwareTrie<ConsistencyTestBase.TestStateMetadata, ConsistencyTestBase.TestRangeState>,
                           InMemoryDeletionAwareTrie<ConsistencyTestBase.TestStateMetadata, ConsistencyTestBase.TestRangeState>>
{

    @SuppressWarnings("rawtypes") // type does not matter, we are always throwing an exception
    static final InMemoryBaseTrie.UpsertTransformer UPSERT_THROW = (x, y) -> { throw new AssertionError(); };
    @SuppressWarnings("rawtypes") // type does not matter, we are always throwing an exception
    static final BiFunction BIFUNCTION_THROW = (x, y) -> { throw new AssertionError(); };

    @Override
    InMemoryDeletionAwareTrie<TestStateMetadata, TestRangeState> makeTrie(OpOrder readOrder)
    {
        return InMemoryDeletionAwareTrie.longLived(VERSION, readOrder);
    }

    @Override
    TestStateMetadata value(ByteComparable b, ByteComparable cprefix, ByteComparable c, int add, int seqId)
    {
        String pk = b.byteComparableAsString(VERSION);
        String ck = (cprefix != null ? cprefix.byteComparableAsString(VERSION) : "") + c.byteComparableAsString(VERSION);
        return new TestStateMetadata<>(new Value(pk, ck, add, seqId));
    }

    @Override
    TestStateMetadata metadata(ByteComparable b)
    {
        return new TestStateMetadata<>(new Metadata(b.byteComparableAsString(VERSION)));
    }

    @Override
    String pk(TestStateMetadata c)
    {
        return ((Content)c.metadata).pk;
    }

    @Override
    String ck(TestStateMetadata c)
    {
        return ((Value) c.metadata).ck;
    }

    @Override
    int seq(TestStateMetadata c)
    {
        return ((Value) c.metadata).seq;
    }

    @Override
    int value(TestStateMetadata c)
    {
        return ((Value) c.metadata).value;
    }

    @Override
    int updateCount(TestStateMetadata c)
    {
        return ((Metadata) c.metadata).updateCount;
    }

    @Override
    DeletionAwareTrie<TestStateMetadata, TestRangeState> makeSingleton(ByteComparable b, TestStateMetadata content)
    {
        return DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY, VERSION, RangeTrie.singleton(b, VERSION, content));
    }

    @Override
    DeletionAwareTrie<TestStateMetadata, TestRangeState> withRootMetadata(DeletionAwareTrie<TestStateMetadata, TestRangeState> wrapped, TestStateMetadata metadata)
    {
        return TrieUtil.withRootMetadata(wrapped, metadata);
    }

    @Override
    DeletionAwareTrie<TestStateMetadata, TestRangeState> merge(Collection<DeletionAwareTrie<TestStateMetadata, TestRangeState>> tries,
                                                      Trie.CollectionMergeResolver<TestStateMetadata> mergeResolver)
    {
        return DeletionAwareTrie.merge(tries,
                                      mergeResolver,
                                      Trie.throwingResolver(),
                                      BIFUNCTION_THROW,
                                      true); // deletionsAtFixedPoints = true for consistency
    }

    @Override
    void apply(InMemoryDeletionAwareTrie<TestStateMetadata, TestRangeState> trie,
               DeletionAwareTrie<TestStateMetadata, TestRangeState> mutation,
               InMemoryBaseTrie.UpsertTransformer<TestStateMetadata, TestStateMetadata> mergeResolver,
               Predicate<InMemoryBaseTrie.NodeFeatures<TestStateMetadata>> forcedCopyChecker) throws TrieSpaceExhaustedException
    {
        trie.apply(mutation,
                   mergeResolver, // Use the provided merge resolver for content
                   (x, y) -> mergeResolver.apply((TestStateMetadata) x, (TestStateMetadata) y),
                   UPSERT_THROW,
                   BIFUNCTION_THROW,
                  false, // deletionsAtFixedPoints = true for consistency
                  forcedCopyChecker); // Use the provided forced copy checker
    }

    @Override
    void delete(InMemoryDeletionAwareTrie<TestStateMetadata, TestRangeState> trie,
                ByteComparable deletionPrefix,
                TestRangeState partitionMarker,
                RangeTrie<TestRangeState> deletionBranch,
                InMemoryBaseTrie.UpsertTransformer<TestStateMetadata, TestRangeState> mergeResolver,
                Predicate<InMemoryBaseTrie.NodeFeatures<TestRangeState>> forcedCopyChecker) throws TrieSpaceExhaustedException
    {
        DeletionAwareTrie<TestRangeState, TestRangeState> deletion = DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY, VERSION, deletionBranch);
        deletion = TrieUtil.withRootMetadata(deletion, partitionMarker);
        deletion = deletion.prefixedBy(deletionPrefix);

        trie.apply(deletion,
                  mergeResolver,
                  (existing, incoming) -> (existing instanceof TestStateMetadata)
                                          ? mergeResolver.apply((TestStateMetadata) existing, incoming)
                                          : TestRangeState.combine(existing, incoming),
                  mergeResolver,
                  BIFUNCTION_THROW,
                  false,
                  forcedCopyChecker);
    }

    @Override
    boolean isPartition(TestStateMetadata c)
    {
        return c != null && ((Content) c.metadata).isPartition();
    }

    @Override
    TestStateMetadata mergeMetadata(TestStateMetadata c1, TestStateMetadata c2)
    {
        if (c1 == null) return c2;
        if (c2 == null) return c1;
        return toTestStateMetadata(((Metadata) c1.metadata).mergeWith((Metadata) c2.metadata));
    }

    @Override
    TestStateMetadata deleteMetadata(TestStateMetadata existing, int entriesToRemove)
    {
        if (existing == null) return null;
        return toTestStateMetadata(((Metadata) existing.metadata).delete(entriesToRemove));
    }

    @Override
    Iterable<Map.Entry<ByteComparable.Preencoded, TestStateMetadata>> getEntrySet(BaseTrie<TestStateMetadata, ?, ?> trie)
    {
        return ((DeletionAwareTrie<TestStateMetadata, TestRangeState>) trie)
               .mergedTrie(DeletionBranchConsistencyTest::mergeStateAndMetadata)
               .entrySet();
    }

    static TestStateMetadata mergeStateAndMetadata(TestStateMetadata m, TestRangeState s)
    {
        if (!(s instanceof TestStateMetadata))
            return m;
        TestStateMetadata m2 = (TestStateMetadata) s;
        if (m == null)
            return m2;
        return toTestStateMetadata(((Metadata) m.metadata).mergeWith((Metadata) m2.metadata));
    }

    static TestStateMetadata toTestStateMetadata(Content c)
    {
        return c != null ? new TestStateMetadata(c) : null;
    }

    @Override
    void printStats(InMemoryDeletionAwareTrie<TestStateMetadata, TestRangeState> trie,
                    Predicate<InMemoryBaseTrie.NodeFeatures<TestStateMetadata>> forcedCopyChecker)
    {
        System.out.format("DeletionAware Reuse %s %s on-heap %,d (+%,d) off-heap %,d\n",
                          trie.cellAllocator.getClass().getSimpleName(),
                          trie.bufferType,
                          trie.usedSizeOnHeap(),
                          trie.unusedReservedOnHeapMemory(),
                          trie.usedSizeOffHeap());
    }

    // TestStateMetadata hierarchy for deletion-aware consistency testing
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
