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
import java.util.function.Predicate;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;

public class InMemoryTrieConsistencyTest extends ConsistencyTestBase<InMemoryTrieConsistencyTest.Content, Trie<InMemoryTrieConsistencyTest.Content>, InMemoryTrie<InMemoryTrieConsistencyTest.Content>>
{
    @Override
    InMemoryTrie<Content> makeTrie(OpOrder readOrder)
    {
        return InMemoryTrie.longLived(VERSION, readOrder);
    }

    @Override
    Value value(ByteComparable b, ByteComparable cprefix, ByteComparable c, int add, int seqId)
    {
        return new Value(b.byteComparableAsString(VERSION),
                         (cprefix != null ? cprefix.byteComparableAsString(VERSION) : "") + c.byteComparableAsString(VERSION), add, seqId);
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
    Trie<Content> makeSingleton(ByteComparable b, Content content)
    {
        return Trie.singleton(b, VERSION, content);
    }

    @Override
    Trie<Content> withRootMetadata(Trie<Content> wrapped, Content metadata)
    {
        return TrieUtil.withRootMetadata(wrapped, metadata);
    }

    @Override
    Trie<Content> merge(Collection<Trie<Content>> tries, Trie.CollectionMergeResolver<Content> mergeResolver)
    {
        return Trie.merge(tries, mergeResolver);
    }

    @Override
    void apply(InMemoryTrie<Content> trie, Trie<Content> mutation, InMemoryBaseTrie.UpsertTransformer<Content, Content> mergeResolver, Predicate<InMemoryBaseTrie.NodeFeatures<Content>> forcedCopyChecker) throws TrieSpaceExhaustedException
    {
        trie.apply(mutation, mergeResolver, forcedCopyChecker);
    }

    @Override
    void delete(InMemoryTrie<Content> trie,
                ByteComparable deletionPrefix,
                TestRangeState partitionMarker,
                RangeTrie<TestRangeState> deletion,
                InMemoryBaseTrie.UpsertTransformer<Content, TestRangeState> mergeResolver,
                Predicate<InMemoryBaseTrie.NodeFeatures<TestRangeState>> forcedCopyChecker) throws TrieSpaceExhaustedException
    {
        deletion = TrieUtil.withRootMetadata(deletion, partitionMarker);
        deletion = deletion.prefixedBy(deletionPrefix);
        trie.apply(deletion, mergeResolver, forcedCopyChecker);
    }

    @Override
    boolean isPartition(Content c)
    {
        return c != null && c.isPartition();
    }

    @Override
    Content mergeMetadata(Content c1, Content c2)
    {
        return ((Metadata) c1).mergeWith((Metadata) c2);
    }

    @Override
    Content deleteMetadata(Content c1, int entriesToRemove)
    {
        return ((Metadata) c1).delete(entriesToRemove);
    }

    @Override
    void printStats(InMemoryTrie<Content> trie, Predicate<InMemoryBaseTrie.NodeFeatures<Content>> forcedCopyChecker)
    {
        System.out.format("Reuse %s %s atomicity %s on-heap %,d (+%,d) off-heap %,d\n",
                          trie.cellAllocator.getClass().getSimpleName(),
                          trie.bufferType,
                          forcedCopyChecker == this.<Content>noAtomicity() ? "none" :
                          forcedCopyChecker == this.<Content>forceAtomic() ? "atomic" : "consistent partition",
                          trie.usedSizeOnHeap(),
                          trie.unusedReservedOnHeapMemory(),
                          trie.usedSizeOffHeap());
    }

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
