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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.Term;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.plan.Expression.Op;
import org.apache.cassandra.index.sasi.utils.MappedBuffer;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.AbstractGuavaIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.index.sasi.disk.OnDiskBlock.SearchResult;

public class OnDiskIndex implements Iterable<OnDiskIndex.DataTerm>, Closeable
{
    public enum IteratorOrder
    {
        DESC(1), ASC(-1);

        public final int step;

        IteratorOrder(int step)
        {
            this.step = step;
        }

        public int startAt(OnDiskBlock<DataTerm> block, Expression e)
        {
            switch (this)
            {
                case DESC:
                    return e.lower == null
                            ? 0
                            : startAt(block.search(e.validator, e.lower.value), e.lower.inclusive);

                case ASC:
                    return e.upper == null
                            ? block.termCount() - 1
                            : startAt(block.search(e.validator, e.upper.value), e.upper.inclusive);

                default:
                    throw new IllegalArgumentException("Unknown order: " + this);
            }
        }

        public int startAt(SearchResult<DataTerm> found, boolean inclusive)
        {
            switch (this)
            {
                case DESC:
                    if (found.cmp < 0)
                        return found.index + 1;

                    return inclusive || found.cmp != 0 ? found.index : found.index + 1;

                case ASC:
                    if (found.cmp < 0) // search term was bigger then whole data set
                        return found.index;
                    return inclusive && (found.cmp == 0 || found.cmp < 0) ? found.index : found.index - 1;

                default:
                    throw new IllegalArgumentException("Unknown order: " + this);
            }
        }
    }

    public final Descriptor descriptor;
    protected final OnDiskIndexBuilder.Mode mode;
    protected final OnDiskIndexBuilder.TermSize termSize;

    protected final AbstractType<?> comparator;
    protected final MappedBuffer indexFile;
    protected final long indexSize;
    protected final boolean hasMarkedPartials;

    protected final Function<Long, DecoratedKey> keyFetcher;

    protected final String indexPath;

    protected final PointerLevel[] levels;
    protected final DataLevel dataLevel;

    protected final ByteBuffer minTerm, maxTerm, minKey, maxKey;

    public OnDiskIndex(File index, AbstractType<?> cmp, Function<Long, DecoratedKey> keyReader)
    {
        keyFetcher = keyReader;

        comparator = cmp;
        indexPath = index.absolutePath();


        try (FileInputStreamPlus backingFile = new FileInputStreamPlus(index))
        {
            descriptor = new Descriptor(backingFile.readUTF());

            termSize = OnDiskIndexBuilder.TermSize.of(backingFile.readShort());

            minTerm = ByteBufferUtil.readWithShortLength(backingFile);
            maxTerm = ByteBufferUtil.readWithShortLength(backingFile);

            minKey = ByteBufferUtil.readWithShortLength(backingFile);
            maxKey = ByteBufferUtil.readWithShortLength(backingFile);

            mode = OnDiskIndexBuilder.Mode.mode(backingFile.readUTF());
            hasMarkedPartials = backingFile.readBoolean();

            FileChannel channel = index.newReadChannel();
            indexSize = channel.size();
            indexFile = new MappedBuffer(new ChannelProxy(index, channel));
        }
        catch (IOException e)
        {
            throw new FSReadError(e, index);
        }

        // start of the levels
        indexFile.position(indexFile.getLong(indexSize - 8));

        int numLevels = indexFile.getInt();
        levels = new PointerLevel[numLevels];
        for (int i = 0; i < levels.length; i++)
        {
            int blockCount = indexFile.getInt();
            levels[i] = new PointerLevel(indexFile.position(), blockCount);
            indexFile.position(indexFile.position() + blockCount * 8);
        }

        int blockCount = indexFile.getInt();
        dataLevel = new DataLevel(indexFile.position(), blockCount);
    }

    public boolean hasMarkedPartials()
    {
        return hasMarkedPartials;
    }

    public OnDiskIndexBuilder.Mode mode()
    {
        return mode;
    }

    public ByteBuffer minTerm()
    {
        return minTerm;
    }

    public ByteBuffer maxTerm()
    {
        return maxTerm;
    }

    public ByteBuffer minKey()
    {
        return minKey;
    }

    public ByteBuffer maxKey()
    {
        return maxKey;
    }

    public DataTerm min()
    {
        return dataLevel.getBlock(0).getTerm(0);
    }

    public DataTerm max()
    {
        DataBlock block = dataLevel.getBlock(dataLevel.blockCount - 1);
        return block.getTerm(block.termCount() - 1);
    }

    /**
     * Search for rows which match all of the terms inside the given expression in the index file.
     *
     * @param exp The expression to use for the query.
     *
     * @return Iterator which contains rows for all of the terms from the given range.
     */
    public RangeIterator<Long, Token> search(Expression exp)
    {
        assert mode.supports(exp.getOp());

        if (exp.getOp() == Expression.Op.PREFIX && mode == OnDiskIndexBuilder.Mode.CONTAINS && !hasMarkedPartials)
            throw new UnsupportedOperationException("prefix queries in CONTAINS mode are not supported by this index");

        // optimization in case single term is requested from index
        // we don't really need to build additional union iterator
        if (exp.getOp() == Op.EQ)
        {
            DataTerm term = getTerm(exp.lower.value);
            return term == null ? null : term.getTokens();
        }

        // convert single NOT_EQ to range with exclusion
        final Expression expression = (exp.getOp() != Op.NOT_EQ)
                                        ? exp
                                        : new Expression(exp).setOp(Op.RANGE)
                                                .setLower(new Expression.Bound(minTerm, true))
                                                .setUpper(new Expression.Bound(maxTerm, true))
                                                .addExclusion(exp.lower.value);

        List<ByteBuffer> exclusions = new ArrayList<>(expression.exclusions.size());

        Iterables.addAll(exclusions, expression.exclusions.stream().filter(exclusion -> {
            // accept only exclusions which are in the bounds of lower/upper
            return !(expression.lower != null && comparator.compare(exclusion, expression.lower.value) < 0)
                && !(expression.upper != null && comparator.compare(exclusion, expression.upper.value) > 0);
        }).collect(Collectors.toList()));

        Collections.sort(exclusions, comparator);

        if (exclusions.size() == 0)
            return searchRange(expression);

        List<Expression> ranges = new ArrayList<>(exclusions.size());

        // calculate range splits based on the sorted exclusions
        Iterator<ByteBuffer> exclusionsIterator = exclusions.iterator();

        Expression.Bound min = expression.lower, max = null;
        while (exclusionsIterator.hasNext())
        {
            max = new Expression.Bound(exclusionsIterator.next(), false);
            ranges.add(new Expression(expression).setOp(Op.RANGE).setLower(min).setUpper(max));
            min = max;
        }

        assert max != null;
        ranges.add(new Expression(expression).setOp(Op.RANGE).setLower(max).setUpper(expression.upper));

        RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();
        for (Expression e : ranges)
        {
            RangeIterator<Long, Token> range = searchRange(e);
            if (range != null)
                builder.add(range);
        }

        return builder.build();
    }

    private RangeIterator<Long, Token> searchRange(Expression range)
    {
        Expression.Bound lower = range.lower;
        Expression.Bound upper = range.upper;

        int lowerBlock = lower == null ? 0 : getDataBlock(lower.value);
        int upperBlock = upper == null
                ? dataLevel.blockCount - 1
                // optimization so we don't have to fetch upperBlock when query has lower == upper
                : (lower != null && comparator.compare(lower.value, upper.value) == 0) ? lowerBlock : getDataBlock(upper.value);

        return (mode != OnDiskIndexBuilder.Mode.SPARSE || lowerBlock == upperBlock || upperBlock - lowerBlock <= 1)
                ? searchPoint(lowerBlock, range)
                : searchRange(lowerBlock, lower, upperBlock, upper);
    }

    private RangeIterator<Long, Token> searchRange(int lowerBlock, Expression.Bound lower, int upperBlock, Expression.Bound upper)
    {
        // if lower is at the beginning of the block that means we can just do a single iterator per block
        SearchResult<DataTerm> lowerPosition = (lower == null) ? null : searchIndex(lower.value, lowerBlock);
        SearchResult<DataTerm> upperPosition = (upper == null) ? null : searchIndex(upper.value, upperBlock);

        RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();

        // optimistically assume that first and last blocks are full block reads, saves at least 3 'else' conditions
        int firstFullBlockIdx = lowerBlock, lastFullBlockIdx = upperBlock;

        // 'lower' doesn't cover the whole block so we need to do a partial iteration
        // Two reasons why that can happen:
        //   - 'lower' is not the first element of the block
        //   - 'lower' is first element but it's not inclusive in the query
        if (lowerPosition != null && (lowerPosition.index > 0 || !lower.inclusive))
        {
            DataBlock block = dataLevel.getBlock(lowerBlock);
            int start = (lower.inclusive || lowerPosition.cmp != 0) ? lowerPosition.index : lowerPosition.index + 1;

            builder.add(block.getRange(start, block.termCount()));
            firstFullBlockIdx = lowerBlock + 1;
        }

        if (upperPosition != null)
        {
            DataBlock block = dataLevel.getBlock(upperBlock);
            int lastIndex = block.termCount() - 1;

            // The save as with 'lower' but here we need to check if the upper is the last element of the block,
            // which means that we only have to get individual results if:
            //  - if it *is not* the last element, or
            //  - it *is* but shouldn't be included (dictated by upperInclusive)
            if (upperPosition.index != lastIndex || !upper.inclusive)
            {
                int end = (upperPosition.cmp < 0 || (upperPosition.cmp == 0 && upper.inclusive))
                                ? upperPosition.index + 1 : upperPosition.index;

                builder.add(block.getRange(0, end));
                lastFullBlockIdx = upperBlock - 1;
            }
        }

        int totalSuperBlocks = (lastFullBlockIdx - firstFullBlockIdx) / OnDiskIndexBuilder.SUPER_BLOCK_SIZE;

        // if there are no super-blocks, we can simply read all of the block iterators in sequence
        if (totalSuperBlocks == 0)
        {
            for (int i = firstFullBlockIdx; i <= lastFullBlockIdx; i++)
                builder.add(dataLevel.getBlock(i).getBlockIndex().iterator(keyFetcher));

            return builder.build();
        }

        // first get all of the blocks which are aligned before the first super-block in the sequence,
        // e.g. if the block range was (1, 9) and super-block-size = 4, we need to read 1, 2, 3, 4 - 7 is covered by
        // super-block, 8, 9 is a remainder.

        int superBlockAlignedStart = firstFullBlockIdx == 0 ? 0 : (int) FBUtilities.align(firstFullBlockIdx, OnDiskIndexBuilder.SUPER_BLOCK_SIZE);
        for (int blockIdx = firstFullBlockIdx; blockIdx < Math.min(superBlockAlignedStart, lastFullBlockIdx); blockIdx++)
            builder.add(getBlockIterator(blockIdx));

        // now read all of the super-blocks matched by the request, from the previous comment
        // it's a block with index 1 (which covers everything from 4 to 7)

        int superBlockIdx = superBlockAlignedStart / OnDiskIndexBuilder.SUPER_BLOCK_SIZE;
        for (int offset = 0; offset < totalSuperBlocks - 1; offset++)
            builder.add(dataLevel.getSuperBlock(superBlockIdx++).iterator());

        // now it's time for a remainder read, again from the previous example it's 8, 9 because
        // we have over-shot previous block but didn't request enough to cover next super-block.

        int lastCoveredBlock = superBlockIdx * OnDiskIndexBuilder.SUPER_BLOCK_SIZE;
        for (int offset = 0; offset <= (lastFullBlockIdx - lastCoveredBlock); offset++)
            builder.add(getBlockIterator(lastCoveredBlock + offset));

        return builder.build();
    }

    private RangeIterator<Long, Token> searchPoint(int lowerBlock, Expression expression)
    {
        Iterator<DataTerm> terms = new TermIterator(lowerBlock, expression, IteratorOrder.DESC);
        RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();

        while (terms.hasNext())
        {
            try
            {
                builder.add(terms.next().getTokens());
            }
            finally
            {
                expression.checkpoint();
            }
        }

        return builder.build();
    }

    private RangeIterator<Long, Token> getBlockIterator(int blockIdx)
    {
        DataBlock block = dataLevel.getBlock(blockIdx);
        return (block.hasCombinedIndex)
                ? block.getBlockIndex().iterator(keyFetcher)
                : block.getRange(0, block.termCount());
    }

    public Iterator<DataTerm> iteratorAt(ByteBuffer query, IteratorOrder order, boolean inclusive)
    {
        Expression e = new Expression("", comparator);
        Expression.Bound bound = new Expression.Bound(query, inclusive);

        switch (order)
        {
            case DESC:
                e.setLower(bound);
                break;

            case ASC:
                e.setUpper(bound);
                break;

            default:
                throw new IllegalArgumentException("Unknown order: " + order);
        }

        return new TermIterator(levels.length == 0 ? 0 : getBlockIdx(findPointer(query), query), e, order);
    }

    private int getDataBlock(ByteBuffer query)
    {
        return levels.length == 0 ? 0 : getBlockIdx(findPointer(query), query);
    }

    public Iterator<DataTerm> iterator()
    {
        return new TermIterator(0, new Expression("", comparator), IteratorOrder.DESC);
    }

    public void close() throws IOException
    {
        FileUtils.closeQuietly(indexFile);
    }

    private PointerTerm findPointer(ByteBuffer query)
    {
        PointerTerm ptr = null;
        for (PointerLevel level : levels)
        {
            if ((ptr = level.getPointer(ptr, query)) == null)
                return null;
        }

        return ptr;
    }

    private DataTerm getTerm(ByteBuffer query)
    {
        SearchResult<DataTerm> term = searchIndex(query, getDataBlock(query));
        return term.cmp == 0 ? term.result : null;
    }

    private SearchResult<DataTerm> searchIndex(ByteBuffer query, int blockIdx)
    {
        return dataLevel.getBlock(blockIdx).search(comparator, query);
    }

    private int getBlockIdx(PointerTerm ptr, ByteBuffer query)
    {
        int blockIdx = 0;
        if (ptr != null)
        {
            int cmp = ptr.compareTo(comparator, query);
            blockIdx = (cmp == 0 || cmp > 0) ? ptr.getBlock() : ptr.getBlock() + 1;
        }

        return blockIdx;
    }

    protected class PointerLevel extends Level<PointerBlock>
    {
        public PointerLevel(long offset, int count)
        {
            super(offset, count);
        }

        public PointerTerm getPointer(PointerTerm parent, ByteBuffer query)
        {
            return getBlock(getBlockIdx(parent, query)).search(comparator, query).result;
        }

        protected PointerBlock cast(MappedBuffer block)
        {
            return new PointerBlock(block);
        }
    }

    protected class DataLevel extends Level<DataBlock>
    {
        protected final int superBlockCnt;
        protected final long superBlocksOffset;

        public DataLevel(long offset, int count)
        {
            super(offset, count);
            long baseOffset = blockOffsets + blockCount * 8;
            superBlockCnt = indexFile.getInt(baseOffset);
            superBlocksOffset = baseOffset + 4;
        }

        protected DataBlock cast(MappedBuffer block)
        {
            return new DataBlock(block);
        }

        public OnDiskSuperBlock getSuperBlock(int idx)
        {
            assert idx < superBlockCnt : String.format("requested index %d is greater than super block count %d", idx, superBlockCnt);
            long blockOffset = indexFile.getLong(superBlocksOffset + idx * 8);
            return new OnDiskSuperBlock(indexFile.duplicate().position(blockOffset));
        }
    }

    protected class OnDiskSuperBlock
    {
        private final TokenTree tokenTree;

        public OnDiskSuperBlock(MappedBuffer buffer)
        {
            tokenTree = new TokenTree(descriptor, buffer);
        }

        public RangeIterator<Long, Token> iterator()
        {
            return tokenTree.iterator(keyFetcher);
        }
    }

    protected abstract class Level<T extends OnDiskBlock>
    {
        protected final long blockOffsets;
        protected final int blockCount;

        public Level(long offsets, int count)
        {
            this.blockOffsets = offsets;
            this.blockCount = count;
        }

        public T getBlock(int idx) throws FSReadError
        {
            assert idx >= 0 && idx < blockCount;

            // calculate block offset and move there
            // (long is intentional, we'll just need mmap implementation which supports long positions)
            long blockOffset = indexFile.getLong(blockOffsets + idx * 8);
            return cast(indexFile.duplicate().position(blockOffset));
        }

        protected abstract T cast(MappedBuffer block);
    }

    protected class DataBlock extends OnDiskBlock<DataTerm>
    {
        public DataBlock(MappedBuffer data)
        {
            super(descriptor, data, BlockType.DATA);
        }

        protected DataTerm cast(MappedBuffer data)
        {
            return new DataTerm(data, termSize, getBlockIndex());
        }

        public RangeIterator<Long, Token> getRange(int start, int end)
        {
            RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();
            NavigableMap<Long, Token> sparse = new TreeMap<>();

            for (int i = start; i < end; i++)
            {
                DataTerm term = getTerm(i);

                if (term.isSparse())
                {
                    NavigableMap<Long, Token> tokens = term.getSparseTokens();
                    for (Map.Entry<Long, Token> t : tokens.entrySet())
                    {
                        Token token = sparse.get(t.getKey());
                        if (token == null)
                            sparse.put(t.getKey(), t.getValue());
                        else
                            token.merge(t.getValue());
                    }
                }
                else
                {
                    builder.add(term.getTokens());
                }
            }

            PrefetchedTokensIterator prefetched = sparse.isEmpty() ? null : new PrefetchedTokensIterator(sparse);

            if (builder.rangeCount() == 0)
                return prefetched;

            builder.add(prefetched);
            return builder.build();
        }
    }

    protected class PointerBlock extends OnDiskBlock<PointerTerm>
    {
        public PointerBlock(MappedBuffer block)
        {
            super(descriptor, block, BlockType.POINTER);
        }

        protected PointerTerm cast(MappedBuffer data)
        {
            return new PointerTerm(data, termSize, hasMarkedPartials);
        }
    }

    public class DataTerm extends Term implements Comparable<DataTerm>
    {
        private final TokenTree perBlockIndex;

        protected DataTerm(MappedBuffer content, OnDiskIndexBuilder.TermSize size, TokenTree perBlockIndex)
        {
            super(content, size, hasMarkedPartials);
            this.perBlockIndex = perBlockIndex;
        }

        public RangeIterator<Long, Token> getTokens()
        {
            final long blockEnd = FBUtilities.align(content.position(), OnDiskIndexBuilder.BLOCK_SIZE);

            if (isSparse())
                return new PrefetchedTokensIterator(getSparseTokens());

            long offset = blockEnd + 4 + content.getInt(getDataOffset() + 1);
            return new TokenTree(descriptor, indexFile.duplicate().position(offset)).iterator(keyFetcher);
        }

        public boolean isSparse()
        {
            return content.get(getDataOffset()) > 0;
        }

        public NavigableMap<Long, Token> getSparseTokens()
        {
            long ptrOffset = getDataOffset();

            byte size = content.get(ptrOffset);

            assert size > 0;

            NavigableMap<Long, Token> individualTokens = new TreeMap<>();
            for (int i = 0; i < size; i++)
            {
                Token token = perBlockIndex.get(content.getLong(ptrOffset + 1 + (8 * i)), keyFetcher);

                assert token != null;
                individualTokens.put(token.get(), token);
            }

            return individualTokens;
        }

        public int compareTo(DataTerm other)
        {
            return other == null ? 1 : compareTo(comparator, other.getTerm());
        }
    }

    protected static class PointerTerm extends Term
    {
        public PointerTerm(MappedBuffer content, OnDiskIndexBuilder.TermSize size, boolean hasMarkedPartials)
        {
            super(content, size, hasMarkedPartials);
        }

        public int getBlock()
        {
            return content.getInt(getDataOffset());
        }
    }

    private static class PrefetchedTokensIterator extends RangeIterator<Long, Token>
    {
        private final NavigableMap<Long, Token> tokens;
        private PeekingIterator<Token> currentIterator;

        public PrefetchedTokensIterator(NavigableMap<Long, Token> tokens)
        {
            super(tokens.firstKey(), tokens.lastKey(), tokens.size());
            this.tokens = tokens;
            this.currentIterator = Iterators.peekingIterator(tokens.values().iterator());
        }

        protected Token computeNext()
        {
            return currentIterator != null && currentIterator.hasNext()
                    ? currentIterator.next()
                    : endOfData();
        }

        protected void performSkipTo(Long nextToken)
        {
            currentIterator = Iterators.peekingIterator(tokens.tailMap(nextToken, true).values().iterator());
        }

        public void close() throws IOException
        {
            endOfData();
        }
    }

    public AbstractType<?> getComparator()
    {
        return comparator;
    }

    public String getIndexPath()
    {
        return indexPath;
    }

    private class TermIterator extends AbstractGuavaIterator<DataTerm>
    {
        private final Expression e;
        private final IteratorOrder order;

        protected OnDiskBlock<DataTerm> currentBlock;
        protected int blockIndex, offset;

        private boolean checkLower = true, checkUpper = true;

        public TermIterator(int startBlock, Expression expression, IteratorOrder order)
        {
            this.e = expression;
            this.order = order;
            this.blockIndex = startBlock;

            nextBlock();
        }

        protected DataTerm computeNext()
        {
            for (;;)
            {
                if (currentBlock == null)
                    return endOfData();

                if (offset >= 0 && offset < currentBlock.termCount())
                {
                    DataTerm currentTerm = currentBlock.getTerm(nextOffset());

                    // we need to step over all of the partial terms, in PREFIX mode,
                    // encountered by the query until upper-bound tells us to stop
                    if (e.getOp() == Op.PREFIX && currentTerm.isPartial())
                        continue;

                    // haven't reached the start of the query range yet, let's
                    // keep skip the current term until lower bound is satisfied
                    if (checkLower && !e.isLowerSatisfiedBy(currentTerm))
                        continue;

                    // flip the flag right on the first bounds match
                    // to avoid expensive comparisons
                    checkLower = false;

                    if (checkUpper && !e.isUpperSatisfiedBy(currentTerm))
                        return endOfData();

                    return currentTerm;
                }

                nextBlock();
            }
        }

        protected void nextBlock()
        {
            currentBlock = null;

            if (blockIndex < 0 || blockIndex >= dataLevel.blockCount)
                return;

            currentBlock = dataLevel.getBlock(nextBlockIndex());
            offset = checkLower ? order.startAt(currentBlock, e) : currentBlock.minOffset(order);

            // let's check the last term of the new block right away
            // if expression's upper bound is satisfied by it such means that we can avoid
            // doing any expensive upper bound checks for that block.
            checkUpper = e.hasUpper() && !e.isUpperSatisfiedBy(currentBlock.getTerm(currentBlock.maxOffset(order)));
        }

        protected int nextBlockIndex()
        {
            int current = blockIndex;
            blockIndex += order.step;
            return current;
        }

        protected int nextOffset()
        {
            int current = offset;
            offset += order.step;
            return current;
        }
    }
}
