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
package org.apache.cassandra.index.sasi.disk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sasi.plan.Expression.Op;
import org.apache.cassandra.index.sasi.sa.IndexedTerm;
import org.apache.cassandra.index.sasi.sa.IntegralSA;
import org.apache.cassandra.index.sasi.sa.SA;
import org.apache.cassandra.index.sasi.sa.TermIterator;
import org.apache.cassandra.index.sasi.sa.SuffixSA;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.ShortArrayList;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OnDiskIndexBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(OnDiskIndexBuilder.class);

    public enum Mode
    {
        PREFIX(EnumSet.of(Op.EQ, Op.MATCH, Op.PREFIX, Op.NOT_EQ, Op.RANGE)),
        CONTAINS(EnumSet.of(Op.EQ, Op.MATCH, Op.CONTAINS, Op.PREFIX, Op.SUFFIX, Op.NOT_EQ)),
        SPARSE(EnumSet.of(Op.EQ, Op.NOT_EQ, Op.RANGE));

        Set<Op> supportedOps;

        Mode(Set<Op> ops)
        {
            supportedOps = ops;
        }

        public static Mode mode(String mode)
        {
            return Mode.valueOf(mode.toUpperCase());
        }

        public boolean supports(Op op)
        {
            return supportedOps.contains(op);
        }
    }

    public enum TermSize
    {
        INT(4), LONG(8), UUID(16), VARIABLE(-1);

        public final int size;

        TermSize(int size)
        {
            this.size = size;
        }

        public boolean isConstant()
        {
            return this != VARIABLE;
        }

        public static TermSize of(int size)
        {
            switch (size)
            {
                case -1:
                    return VARIABLE;

                case 4:
                    return INT;

                case 8:
                    return LONG;

                case 16:
                    return UUID;

                default:
                    throw new IllegalStateException("unknown state: " + size);
            }
        }

        public static TermSize sizeOf(AbstractType<?> comparator)
        {
            if (comparator instanceof Int32Type || comparator instanceof FloatType)
                return INT;

            if (comparator instanceof LongType || comparator instanceof DoubleType
                    || comparator instanceof TimestampType || comparator instanceof DateType)
                return LONG;

            if (comparator instanceof TimeUUIDType || comparator instanceof UUIDType)
                return UUID;

            return VARIABLE;
        }
    }

    public static final int BLOCK_SIZE = 4096;
    public static final int MAX_TERM_SIZE = 1024;
    public static final int SUPER_BLOCK_SIZE = 64;
    public static final int IS_PARTIAL_BIT = 15;

    private static final SequentialWriterOption WRITER_OPTION = SequentialWriterOption.newBuilder()
                                                                                      .bufferSize(BLOCK_SIZE)
                                                                                      .build();

    private final List<MutableLevel<InMemoryPointerTerm>> levels = new ArrayList<>();
    private MutableLevel<InMemoryDataTerm> dataLevel;

    private final TermSize termSize;

    private final AbstractType<?> keyComparator, termComparator;

    private final Map<ByteBuffer, TokenTreeBuilder> terms;
    private final Mode mode;
    private final boolean marksPartials;

    private ByteBuffer minKey, maxKey;
    private long estimatedBytes;

    public OnDiskIndexBuilder(AbstractType<?> keyComparator, AbstractType<?> comparator, Mode mode)
    {
        this(keyComparator, comparator, mode, true);
    }

    public OnDiskIndexBuilder(AbstractType<?> keyComparator, AbstractType<?> comparator, Mode mode, boolean marksPartials)
    {
        this.keyComparator = keyComparator;
        this.termComparator = comparator;
        this.terms = new HashMap<>();
        this.termSize = TermSize.sizeOf(comparator);
        this.mode = mode;
        this.marksPartials = marksPartials;
    }

    public OnDiskIndexBuilder add(ByteBuffer term, DecoratedKey key, long keyPosition)
    {
        if (term.remaining() >= MAX_TERM_SIZE)
        {
            logger.error("Rejecting value (value size {}, maximum size {}).",
                         FBUtilities.prettyPrintMemory(term.remaining()),
                         FBUtilities.prettyPrintMemory(Short.MAX_VALUE));
            return this;
        }

        TokenTreeBuilder tokens = terms.get(term);
        if (tokens == null)
        {
            terms.put(term, (tokens = new DynamicTokenTreeBuilder()));

            // on-heap size estimates from jol
            // 64 bytes for TTB + 48 bytes for TreeMap in TTB + size bytes for the term (map key)
            estimatedBytes += 64 + 48 + term.remaining();
        }

        tokens.add((Long) key.getToken().getTokenValue(), keyPosition);

        // calculate key range (based on actual key values) for current index
        minKey = (minKey == null || keyComparator.compare(minKey, key.getKey()) > 0) ? key.getKey() : minKey;
        maxKey = (maxKey == null || keyComparator.compare(maxKey, key.getKey()) < 0) ? key.getKey() : maxKey;

        // 60 ((boolean(1)*4) + (long(8)*4) + 24) bytes for the LongOpenHashSet created when the keyPosition was added
        // + 40 bytes for the TreeMap.Entry + 8 bytes for the token (key).
        // in the case of hash collision for the token we may overestimate but this is extremely rare
        estimatedBytes += 60 + 40 + 8;

        return this;
    }

    public long estimatedMemoryUse()
    {
        return estimatedBytes;
    }

    private void addTerm(InMemoryDataTerm term, SequentialWriter out) throws IOException
    {
        InMemoryPointerTerm ptr = dataLevel.add(term);
        if (ptr == null)
            return;

        int levelIdx = 0;
        for (;;)
        {
            MutableLevel<InMemoryPointerTerm> level = getIndexLevel(levelIdx++, out);
            if ((ptr = level.add(ptr)) == null)
                break;
        }
    }

    public boolean isEmpty()
    {
        return terms.isEmpty();
    }

    public void finish(Pair<ByteBuffer, ByteBuffer> range, File file, TermIterator terms)
    {
        finish(Descriptor.CURRENT, range, file, terms);
    }

    /**
     * Finishes up index building process by creating/populating index file.
     *
     * @param indexFile The file to write index contents to.
     *
     * @return true if index was written successfully, false otherwise (e.g. if index was empty).
     *
     * @throws FSWriteError on I/O error.
     */
    public boolean finish(File indexFile) throws FSWriteError
    {
        return finish(Descriptor.CURRENT, indexFile);
    }

    @VisibleForTesting
    protected boolean finish(Descriptor descriptor, File file) throws FSWriteError
    {
        // no terms means there is nothing to build
        if (terms.isEmpty())
        {
            file.createFileIfNotExists();
            return false;
        }

        // split terms into suffixes only if it's text, otherwise (even if CONTAINS is set) use terms in original form
        SA sa = ((termComparator instanceof UTF8Type || termComparator instanceof AsciiType) && mode == Mode.CONTAINS)
                    ? new SuffixSA(termComparator, mode) : new IntegralSA(termComparator, mode);

        for (Map.Entry<ByteBuffer, TokenTreeBuilder> term : terms.entrySet())
            sa.add(term.getKey(), term.getValue());

        finish(descriptor, Pair.create(minKey, maxKey), file, sa.finish());
        return true;
    }

    protected void finish(Descriptor descriptor, Pair<ByteBuffer, ByteBuffer> range, File file, TermIterator terms)
    {
        SequentialWriter out = null;

        try
        {
            out = new SequentialWriter(file, WRITER_OPTION);

            out.writeUTF(descriptor.version.toString());

            out.writeShort(termSize.size);

            // min, max term (useful to find initial scan range from search expressions)
            ByteBufferUtil.writeWithShortLength(terms.minTerm(), out);
            ByteBufferUtil.writeWithShortLength(terms.maxTerm(), out);

            // min, max keys covered by index (useful when searching across multiple indexes)
            ByteBufferUtil.writeWithShortLength(range.left, out);
            ByteBufferUtil.writeWithShortLength(range.right, out);

            out.writeUTF(mode.toString());
            out.writeBoolean(marksPartials);

            out.skipBytes((int) (BLOCK_SIZE - out.position()));

            dataLevel = mode == Mode.SPARSE ? new DataBuilderLevel(out, new MutableDataBlock(termComparator, mode))
                                            : new MutableLevel<>(out, new MutableDataBlock(termComparator, mode));
            while (terms.hasNext())
            {
                Pair<IndexedTerm, TokenTreeBuilder> term = terms.next();
                addTerm(new InMemoryDataTerm(term.left, term.right), out);
            }

            dataLevel.finalFlush();
            for (MutableLevel l : levels)
                l.flush(); // flush all of the buffers

            // and finally write levels index
            final long levelIndexPosition = out.position();

            out.writeInt(levels.size());
            for (int i = levels.size() - 1; i >= 0; i--)
                levels.get(i).flushMetadata();

            dataLevel.flushMetadata();

            out.writeLong(levelIndexPosition);

            // sync contents of the output and disk,
            // since it's not done implicitly on close
            out.sync();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file);
        }
        finally
        {
            FileUtils.closeQuietly(out);
        }
    }

    private MutableLevel<InMemoryPointerTerm> getIndexLevel(int idx, SequentialWriter out)
    {
        if (levels.size() == 0)
            levels.add(new MutableLevel<>(out, new MutableBlock<>()));

        if (levels.size() - 1 < idx)
        {
            int toAdd = idx - (levels.size() - 1);
            for (int i = 0; i < toAdd; i++)
                levels.add(new MutableLevel<>(out, new MutableBlock<>()));
        }

        return levels.get(idx);
    }

    protected static void alignToBlock(SequentialWriter out) throws IOException
    {
        long endOfBlock = out.position();
        if ((endOfBlock & (BLOCK_SIZE - 1)) != 0) // align on the block boundary if needed
            out.skipBytes((int) (FBUtilities.align(endOfBlock, BLOCK_SIZE) - endOfBlock));
    }

    private class InMemoryTerm
    {
        protected final IndexedTerm term;

        public InMemoryTerm(IndexedTerm term)
        {
            this.term = term;
        }

        public int serializedSize()
        {
            return (termSize.isConstant() ? 0 : 2) + term.getBytes().remaining();
        }

        public void serialize(DataOutputPlus out) throws IOException
        {
            if (termSize.isConstant())
            {
                out.write(term.getBytes());
            }
            else
            {
                out.writeShort(term.getBytes().remaining() | ((marksPartials && term.isPartial() ? 1 : 0) << IS_PARTIAL_BIT));
                out.write(term.getBytes());
            }

        }
    }

    private class InMemoryPointerTerm extends InMemoryTerm
    {
        protected final int blockCnt;

        public InMemoryPointerTerm(IndexedTerm term, int blockCnt)
        {
            super(term);
            this.blockCnt = blockCnt;
        }

        public int serializedSize()
        {
            return super.serializedSize() + 4;
        }

        public void serialize(DataOutputPlus out) throws IOException
        {
            super.serialize(out);
            out.writeInt(blockCnt);
        }
    }

    private class InMemoryDataTerm extends InMemoryTerm
    {
        private final TokenTreeBuilder keys;

        public InMemoryDataTerm(IndexedTerm term, TokenTreeBuilder keys)
        {
            super(term);
            this.keys = keys;
        }
    }

    private class MutableLevel<T extends InMemoryTerm>
    {
        private final LongArrayList blockOffsets = new LongArrayList();

        protected final SequentialWriter out;

        private final MutableBlock<T> inProcessBlock;
        private InMemoryPointerTerm lastTerm;

        public MutableLevel(SequentialWriter out, MutableBlock<T> block)
        {
            this.out = out;
            this.inProcessBlock = block;
        }

        /**
         * @return If we flushed a block, return the last term of that block; else, null.
         */
        public InMemoryPointerTerm add(T term) throws IOException
        {
            InMemoryPointerTerm toPromote = null;

            if (!inProcessBlock.hasSpaceFor(term))
            {
                flush();
                toPromote = lastTerm;
            }

            inProcessBlock.add(term);

            lastTerm = new InMemoryPointerTerm(term.term, blockOffsets.size());
            return toPromote;
        }

        public void flush() throws IOException
        {
            blockOffsets.add(out.position());
            inProcessBlock.flushAndClear(out);
        }

        public void finalFlush() throws IOException
        {
            flush();
        }

        public void flushMetadata() throws IOException
        {
            flushMetadata(blockOffsets);
        }

        protected void flushMetadata(LongArrayList longArrayList) throws IOException
        {
            out.writeInt(longArrayList.size());
            for (int i = 0; i < longArrayList.size(); i++)
                out.writeLong(longArrayList.get(i));
        }
    }

    /** builds standard data blocks and super blocks, as well */
    private class DataBuilderLevel extends MutableLevel<InMemoryDataTerm>
    {
        private final LongArrayList superBlockOffsets = new LongArrayList();

        /** count of regular data blocks written since current super block was init'd */
        private int dataBlocksCnt;
        private TokenTreeBuilder superBlockTree;

        public DataBuilderLevel(SequentialWriter out, MutableBlock<InMemoryDataTerm> block)
        {
            super(out, block);
            superBlockTree = new DynamicTokenTreeBuilder();
        }

        public InMemoryPointerTerm add(InMemoryDataTerm term) throws IOException
        {
            InMemoryPointerTerm ptr = super.add(term);
            if (ptr != null)
            {
                dataBlocksCnt++;
                flushSuperBlock(false);
            }
            superBlockTree.add(term.keys);
            return ptr;
        }

        public void flushSuperBlock(boolean force) throws IOException
        {
            if (dataBlocksCnt == SUPER_BLOCK_SIZE || (force && !superBlockTree.isEmpty()))
            {
                superBlockOffsets.add(out.position());
                superBlockTree.finish().write(out);
                alignToBlock(out);

                dataBlocksCnt = 0;
                superBlockTree = new DynamicTokenTreeBuilder();
            }
        }

        public void finalFlush() throws IOException
        {
            super.flush();
            flushSuperBlock(true);
        }

        public void flushMetadata() throws IOException
        {
            super.flushMetadata();
            flushMetadata(superBlockOffsets);
        }
    }

    private static class MutableBlock<T extends InMemoryTerm>
    {
        protected final DataOutputBufferFixed buffer;
        protected final ShortArrayList offsets;

        public MutableBlock()
        {
            buffer = new DataOutputBufferFixed(BLOCK_SIZE);
            offsets = new ShortArrayList();
        }

        public final void add(T term) throws IOException
        {
            offsets.add((short) buffer.position());
            addInternal(term);
        }

        protected void addInternal(T term) throws IOException
        {
            term.serialize(buffer);
        }

        public boolean hasSpaceFor(T element)
        {
            return sizeAfter(element) < BLOCK_SIZE;
        }

        protected int sizeAfter(T element)
        {
            return getWatermark() + 4 + element.serializedSize();
        }

        protected int getWatermark()
        {
            return 4 + offsets.size() * 2 + (int) buffer.position();
        }

        public void flushAndClear(SequentialWriter out) throws IOException
        {
            out.writeInt(offsets.size());
            for (int i = 0; i < offsets.size(); i++)
                out.writeShort(offsets.get(i));

            out.write(buffer.buffer());

            alignToBlock(out);

            offsets.clear();
            buffer.clear();
        }
    }

    private static class MutableDataBlock extends MutableBlock<InMemoryDataTerm>
    {
        private static final int MAX_KEYS_SPARSE = 5;

        private final AbstractType<?> comparator;
        private final Mode mode;

        private int offset = 0;

        private final List<TokenTreeBuilder> containers = new ArrayList<>();
        private TokenTreeBuilder combinedIndex;

        public MutableDataBlock(AbstractType<?> comparator, Mode mode)
        {
            this.comparator = comparator;
            this.mode = mode;
            this.combinedIndex = initCombinedIndex();
        }

        protected void addInternal(InMemoryDataTerm term) throws IOException
        {
            TokenTreeBuilder keys = term.keys;

            if (mode == Mode.SPARSE)
            {
                if (keys.getTokenCount() > MAX_KEYS_SPARSE)
                    throw new IOException(String.format("Term - '%s' belongs to more than %d keys in %s mode, which is not allowed.",
                                                        comparator.getString(term.term.getBytes()), MAX_KEYS_SPARSE, mode.name()));

                writeTerm(term, keys);
            }
            else
            {
                writeTerm(term, offset);

                offset += keys.serializedSize();
                containers.add(keys);
            }

            if (mode == Mode.SPARSE)
                combinedIndex.add(keys);
        }

        protected int sizeAfter(InMemoryDataTerm element)
        {
            return super.sizeAfter(element) + ptrLength(element);
        }

        public void flushAndClear(SequentialWriter out) throws IOException
        {
            super.flushAndClear(out);

            out.writeInt(mode == Mode.SPARSE ? offset : -1);

            if (containers.size() > 0)
            {
                for (TokenTreeBuilder tokens : containers)
                    tokens.write(out);
            }

            if (mode == Mode.SPARSE && combinedIndex != null)
                combinedIndex.finish().write(out);

            alignToBlock(out);

            containers.clear();
            combinedIndex = initCombinedIndex();

            offset = 0;
        }

        private int ptrLength(InMemoryDataTerm term)
        {
            return (term.keys.getTokenCount() > 5)
                    ? 5 // 1 byte type + 4 byte offset to the tree
                    : 1 + (8 * (int) term.keys.getTokenCount()); // 1 byte size + n 8 byte tokens
        }

        private void writeTerm(InMemoryTerm term, TokenTreeBuilder keys) throws IOException
        {
            term.serialize(buffer);
            buffer.writeByte((byte) keys.getTokenCount());
            for (Pair<Long, LongSet> key : keys)
                buffer.writeLong(key.left);
        }

        private void writeTerm(InMemoryTerm term, int offset) throws IOException
        {
            term.serialize(buffer);
            buffer.writeByte(0x0);
            buffer.writeInt(offset);
        }

        private TokenTreeBuilder initCombinedIndex()
        {
            return mode == Mode.SPARSE ? new DynamicTokenTreeBuilder() : null;
        }
    }
}
