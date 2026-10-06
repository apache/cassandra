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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.IntArrayList;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.asString;
import static org.apache.cassandra.db.tries.TrieUtil.assertMapEquals;
import static org.apache.cassandra.db.tries.TrieUtil.generateKeys;

public class CellReuseTest
{
    private static final boolean VERBOSE = false;
    static Predicate<InMemoryTrie.NodeFeatures<Object>> FORCE_COPY_PARTITION = features -> {
        var c = features.content();
        if (c != null && c instanceof Boolean)
            return (Boolean) c;
        else
            return false;
    };

    static Predicate<InMemoryTrie.NodeFeatures<Object>> NO_ATOMICITY = features -> false;

    private static final int COUNT = 10000;
    Random rand = new Random(2);

    @Test
    public void testCellReuseBytesPartitionCopying() throws Exception
    {
        testCellReuseBytes(FORCE_COPY_PARTITION);
    }

    @Test
    public void testCellReuseBytesNoCopying() throws Exception
    {
        testCellReuseBytes(NO_ATOMICITY);
    }

    public void testCellReuseBytes(Predicate<InMemoryTrie.NodeFeatures<Object>> forceCopyPredicate) throws Exception
    {
        ByteComparable[] src = generateKeys(rand, COUNT);
        InMemoryTrie<Object> trieLong = makeInMemoryTrie(src, opOrder -> InMemoryTrie.longLived(VERSION, BufferType.ON_HEAP, opOrder, new TestContentSerializer()),
                                                             forceCopyPredicate);

        verifyFreeCellsMatchUnreachable(trieLong);
    }

    @Test
    public void testCellReusePojoPartitionCopying() throws Exception
    {
        testCellReusePojo(FORCE_COPY_PARTITION);
    }

    @Test
    public void testCellReusePojoNoCopying() throws Exception
    {
        testCellReusePojo(NO_ATOMICITY);
    }

    public void testCellReusePojo(Predicate<InMemoryTrie.NodeFeatures<Object>> forceCopyPredicate) throws Exception
    {
        ByteComparable[] src = generateKeys(rand, COUNT);
        InMemoryTrie<Object> trieLong = makeInMemoryTrie(src, opOrder -> InMemoryTrie.longLived(VERSION, BufferType.ON_HEAP, opOrder),
                                                         forceCopyPredicate);

        verifyFreeCellsMatchUnreachable(trieLong);
    }

    public static void verifyFreeCellsMatchUnreachable(InMemoryBaseTrie<?> trieLong)
    {
        // dump some information first
        System.out.println(String.format(" LongLived %s sizes %10s %10s",
                                         trieLong.bufferManager.bufferType(),
                                         FBUtilities.prettyPrintMemory(trieLong.usedSizeOnHeap()),
                                         FBUtilities.prettyPrintMemory(trieLong.usedSizeOffHeap())));

        Pair<BitSet, BitSet> longReachable = reachableCells(trieLong);
        BitSet reachable = longReachable.left;
        int lrcells = reachable.cardinality();
        int lrobjs = longReachable.right.cardinality();
        System.out.println(String.format(" LongLived reachable cells %,d objs %,d cell space %,d obj space %,d",
                                         lrcells,
                                         lrobjs,
                                         lrcells * 32,
                                         lrobjs * 4
        ));

        BufferManagerMultibuf mgr = ((BufferManagerMultibuf) trieLong.bufferManager);
        verifyReachability(trieLong, reachable, (mgr.cellAllocator).indexesInPipeline(), mgr.getAllocatedPos(), 5, "cells");

        ContentManager<?> contentManager = trieLong.contentManager;
        if (contentManager instanceof ContentManagerPojo)
        {
            ContentManagerPojo<?> pojo = (ContentManagerPojo<?>) contentManager;
            verifyReachability(trieLong, longReachable.right, pojo.objectAllocator.indexesInPipeline(), pojo.getAllocatedPos(), 0, "objects");
            IntArrayList filtered = pojo.collectReleasedUnclearedContentIndexes();
            Assert.assertTrue(filtered.size() + " indexes released but not cleared " + filtered, filtered.isEmpty());
        }
    }

    private static void verifyReachability(InMemoryBaseTrie<?> trieLong, BitSet reachable, IntArrayList availableList, int allocatedMax, int shift, String name)
    {
        BitSet available = new BitSet(reachable.size());
        for (int v : availableList)
            available.set(v >> shift);

        // Check no reachable cell is marked for reuse
        BitSet intersection = new BitSet(available.size());
        intersection.or(available);
        intersection.and(reachable);
        assertCellSetEmpty(intersection, trieLong, " reachable " + name + " marked as available");

        // Check all unreachable cells are marked for reuse
        BitSet unreachable = new BitSet(reachable.size());
        unreachable.or(reachable);
        unreachable.flip(0, allocatedMax >> shift);
        unreachable.andNot(available);
        assertCellSetEmpty(unreachable, trieLong, " unreachable " + name + " not marked as available");
    }

    static class TestException extends RuntimeException
    {
    }

    @Test
    public void testAbortedMutation() throws Exception
    {
        ByteComparable[] src = generateKeys(rand, COUNT);
        OpOrder order = new OpOrder();
        InMemoryTrie<Object> trie = InMemoryTrie.longLived(VERSION, order);
        InMemoryTrie<Object> check = InMemoryTrie.shortLived(VERSION);
        int step = Math.min(100, COUNT / 100);
        int throwStep = (COUNT + 10) / 5;   // do 4 throwing inserts
        int nextThrow = throwStep;

        for (int i = 0; i < src.length; i += step)
            try (OpOrder.Group g = order.start())
            {
                int last = Math.min(i + step, src.length);
                addToInMemoryTrie(Arrays.copyOfRange(src, i, last), trie, FORCE_COPY_PARTITION);
                addToInMemoryTrie(Arrays.copyOfRange(src, i, last), check, NO_ATOMICITY);
                if (i >= nextThrow)
                {
                    nextThrow += throwStep;
                    try
                    {
                        addThrowingEntry(src[rand.nextBoolean() ? last : i],    // try both inserting new value and
                                                                                // overwriting existing
                                         trie, FORCE_COPY_PARTITION);
                        ++i;
                        Assert.fail("Expected failed mutation");
                    }
                    catch (TestException e)
                    {
                        // expected
                    }
                }
            }

        assertMapEquals(trie.filteredEntrySet(ByteBuffer.class).iterator(),
                        check.filteredEntrySet(ByteBuffer.class).iterator());
    }

    public static void assertCellSetEmpty(BitSet set, InMemoryBaseTrie<?> trie, String message)
    {
        if (set.isEmpty())
            return;

        for (int i = set.nextSetBit(0); i >= 0; i = set.nextSetBit(i + 1))
        {
            System.out.println(String.format("Cell at %08x: %08x %08x %08x %08x %08x %08x %08x %08x",
                                             (i << 5),
                                             trie.getIntVolatile((i << 5) + 0),
                                             trie.getIntVolatile((i << 5) + 4),
                                             trie.getIntVolatile((i << 5) + 8),
                                             trie.getIntVolatile((i << 5) + 12),
                                             trie.getIntVolatile((i << 5) + 16),
                                             trie.getIntVolatile((i << 5) + 20),
                                             trie.getIntVolatile((i << 5) + 24),
                                             trie.getIntVolatile((i << 5) + 28)
            ));

        }
        Assert.fail(set.cardinality() + message);
    }

    public static Pair<BitSet, BitSet> reachableCells(InMemoryBaseTrie<?> trie)
    {
        if (VERBOSE)
            System.out.println(trie.dump(Object::toString));
        BitSet set = new BitSet();
        BitSet objs = new BitSet();
        markChild(trie, trie.root, set, objs);
        return Pair.create(set, objs);
    }

    private static void mark(InMemoryBaseTrie<?> trie, int node, BitSet set, BitSet objs)
    {
        set.set(node >> 5);
        if (VERBOSE)
            System.out.println(trie.dumpNode(node));
        switch (trie.offset(node))
        {
            case InMemoryTrie.SPLIT_OFFSET:
                for (int i = 0; i < InMemoryTrie.SPLIT_START_LEVEL_LIMIT; ++i)
                {
                    int mid = trie.getSplitCellPointer(node, i, InMemoryTrie.SPLIT_START_LEVEL_LIMIT);
                    if (mid != InMemoryTrie.NONE)
                    {
                        if (VERBOSE)
                            System.out.println(trie.dumpNode(mid));
                        set.set(mid >> 5);
                        for (int j = 0; j < InMemoryTrie.SPLIT_OTHER_LEVEL_LIMIT; ++j)
                        {
                            int tail = trie.getSplitCellPointer(mid, j, InMemoryTrie.SPLIT_OTHER_LEVEL_LIMIT);
                            if (tail != InMemoryTrie.NONE)
                            {
                                if (VERBOSE)
                                    System.out.println(trie.dumpNode(tail));
                                set.set(tail >> 5);
                                for (int k = 0; k < InMemoryTrie.SPLIT_OTHER_LEVEL_LIMIT; ++k)
                                    markChild(trie, trie.getSplitCellPointer(tail, k, InMemoryTrie.SPLIT_OTHER_LEVEL_LIMIT), set, objs);
                            }
                        }
                    }
                }
                break;
            case InMemoryTrie.SPARSE_OFFSET:
                for (int i = 0; i < InMemoryTrie.SPARSE_CHILD_COUNT; ++i)
                    markChild(trie, trie.getIntVolatile(node + InMemoryTrie.SPARSE_CHILDREN_OFFSET + i * 4), set, objs);
                break;
            case InMemoryTrie.PREFIX_OFFSET:
                markPrefixContent(trie, node + InMemoryTrie.PREFIX_CONTENT_OFFSET, set, objs);
                markPrefixContent(trie, node + InMemoryTrie.PREFIX_ALTERNATE_OFFSET, set, objs);
                markChild(trie, trie.followPrefixTransition(node), set, objs);
                break;
            default:
                assert trie.offset(node) <= InMemoryTrie.CHAIN_MAX_OFFSET && trie.offset(node) >= InMemoryTrie.CHAIN_MIN_OFFSET;
                markChild(trie, trie.getIntVolatile((node & -32) + InMemoryTrie.LAST_POINTER_OFFSET), set, objs);
                break;
        }
    }

    private static void markPrefixContent(InMemoryBaseTrie<?> trie, int pointerAddress, BitSet set, BitSet objs)
    {
        int content = trie.getIntVolatile(pointerAddress);
        markChild(trie, content, set, objs);
    }

    private static void markChild(InMemoryBaseTrie<?> trie, int child, BitSet set, BitSet objs)
    {
        if (!InMemoryTrie.isNullOrLeaf(child))
            mark(trie, child, set, objs);

        if (InMemoryTrie.isLeaf(child))
        {
            int cell = trie.contentManager.cellOrObjectSlotUsed(child);
            if (cell < 0)
                objs.set(~cell);
            else
            {
                set.set(cell >> 5);
                if (VERBOSE)
                    System.out.println(trie.dumpNode(child));
            }
        }
    }

    static InMemoryTrie<Object> makeInMemoryTrie(ByteComparable[] src,
                                                 Function<OpOrder, InMemoryTrie<Object>> creator,
                                                 Predicate<InMemoryTrie.NodeFeatures<Object>> forceCopyPredicate)
    throws TrieSpaceExhaustedException
    {
        OpOrder order = new OpOrder();
        InMemoryTrie<Object> trie = creator.apply(order);
        int step = Math.max(Math.min(100, COUNT / 100), 1);
        for (int i = 0; i < src.length; i += step)
            try (OpOrder.Group g = order.start())
            {
                addToInMemoryTrie(Arrays.copyOfRange(src, i, i + step), trie, forceCopyPredicate);
            }

        return trie;
    }

    static void addToInMemoryTrie(ByteComparable[] src,
                                  InMemoryTrie<Object> trie,
                                  Predicate<InMemoryTrie.NodeFeatures<Object>> forceCopyPredicate) throws TrieSpaceExhaustedException
    {
        for (ByteComparable b : src)
        {
            // Note: Because we don't ensure order when calling resolve, just use a hash of the key as payload
            // (so that all sources have the same value).
            int payload = asString(b).hashCode();
            ByteBuffer v = ByteBufferUtil.bytes(payload);
            Trie<Object> update = Trie.singleton(b, VERSION, v);
            update = TrieUtil.withRootMetadata(update, Boolean.TRUE);
            update = update.prefixedBy(source("prefix"));
            applyUpdating(trie, update, forceCopyPredicate);
        }
    }

    static ByteComparable source(String key)
    {
        return ByteComparable.preencoded(VERSION, key.getBytes(StandardCharsets.UTF_8));
    }

    static void addThrowingEntry(ByteComparable b,
                                 InMemoryTrie<Object> trie,
                                 Predicate<InMemoryTrie.NodeFeatures<Object>> forceCopyPredicate) throws TrieSpaceExhaustedException
    {
        int payload = asString(b).hashCode();
        ByteBuffer v = ByteBufferUtil.bytes(payload);
        Trie<Object> update = Trie.singleton(b, VERSION, v);

        // Create an update with two metadata entries, so that the lower is already a copied node.
        // Abort processing on the lower metadata, where the new branch is not attached yet (so as not to affect the
        // contents).
        update = TrieUtil.withRootMetadata(update, Boolean.FALSE);
        update = update.prefixedBy(source("fix"));
        update = TrieUtil.withRootMetadata(update, Boolean.TRUE);
        update = update.prefixedBy(source("pre"));

        trie.apply(update,
                   (existing, upd) ->
                   {
                       if (upd instanceof Boolean)
                       {
                           if (!((Boolean) upd))
                               throw new TestException();
                           return true;
                       }
                       else
                           return upd;
                   },
                   forceCopyPredicate);
    }

    public static <T> void applyUpdating(InMemoryTrie<T> trie,
                                         Trie<T> mutation,
                                         final Predicate<InMemoryTrie.NodeFeatures<T>> needsForcedCopy)
    throws TrieSpaceExhaustedException
    {
        trie.apply(mutation, (x, y) -> y, needsForcedCopy);
    }

    class TestContentSerializer implements ContentSerializer<Object>
    {

        @Override
        public int idIfSpecial(Object content, boolean shouldPresentAfterBranch)
        {
            return content == Boolean.TRUE ? 0 : -1;
        }

        @Override
        public int serialize(Object content, boolean shouldPresentAfterBranch, UnsafeBuffer buffer, int offset) throws TrieSpaceExhaustedException
        {
            ByteBuffer buf = (ByteBuffer) content;
            buffer.putInt(offset, buf.remaining());
            buffer.putBytes(offset + 4, buf, buf.position(), buf.remaining());
            return 0;
        }

        @Override
        public Object special(int id)
        {
            return Boolean.TRUE;
        }

        @Override
        public Object deserialize(UnsafeBuffer buffer, int inBufferPos, int offsetBits)
        {
            int length = buffer.getInt(inBufferPos);
            ByteBuffer buf = ByteBuffer.allocate(length);
            buffer.getBytes(inBufferPos, buf, length);
            return buf;
        }

        @Override
        public void releaseSpecial(int id)
        {
            // no release
        }

        @Override
        public boolean releaseNeeded(int offset)
        {
            return false;
        }

        @Override
        public void release(UnsafeBuffer buffer, int inBufferPos, int offsetBits)
        {
            throw new AssertionError("Should not be called");
        }

        @Override
        public boolean shouldPreserveSpecialWithoutChildren(int id)
        {
            return false;
        }

        @Override
        public boolean shouldPreserveWithoutChildren(int offset)
        {
            return true;
        }

        @Override
        public boolean shouldPreserveWithoutChildren(UnsafeBuffer buffer, int inBufferPos, int offsetBits)
        {
            throw new AssertionError("Should not be called");
        }

        @Override
        public boolean shouldPresentSpecialAfterBranch(int id)
        {
            return false;
        }

        @Override
        public boolean shouldPresentAfterBranch(int offsetBits)
        {
            return false;
        }

        @VisibleForTesting
        @Override
        public void releaseReferencesUnsafe()
        {
            // no release
        }

        @Override
        public String dumpSpecial(int id)
        {
            return "PARTITION";
        }

        @Override
        public String dumpContent(UnsafeBuffer buffer, int inBufferPos, int offsetBits)
        {
            return ByteBufferUtil.bytesToHex((ByteBuffer) deserialize(buffer, inBufferPos, offsetBits));
        }

        @Override
        public int updateInPlace(UnsafeBuffer buffer, int inBufferPos, int offsetBits, Object newContent) throws TrieSpaceExhaustedException
        {
            return serialize(newContent, false, buffer, inBufferPos);
        }

        @Override
        public void completeMutation()
        {
            // no release
        }

        @Override
        public void abortMutation()
        {
            // no release
        }

        @Override
        public long usedSizeOffHeap()
        {
            return 0;
        }

        @Override
        public long usedSizeOnHeap()
        {
            return 0;
        }

        @VisibleForTesting
        @Override
        public long unusedReservedOnHeapMemory()
        {
            return 0;
        }
    }
}
