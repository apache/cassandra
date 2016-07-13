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
import java.util.*;

import org.apache.cassandra.index.sasi.*;
import org.apache.cassandra.index.sasi.utils.AbstractIterator;
import org.apache.cassandra.index.sasi.utils.CombinedValue;
import org.apache.cassandra.index.sasi.utils.MappedBuffer;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.utils.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import static org.apache.cassandra.index.sasi.disk.TokenTreeBuilder.ENTRY_TOKEN_OFFSET;
import static org.apache.cassandra.index.sasi.disk.TokenTreeBuilder.LEAF_ENTRY_BYTES;
import static org.apache.cassandra.index.sasi.disk.TokenTreeBuilder.EntryType;

// Note: all of the seek-able offsets contained in TokenTree should be sizeof(long)
// even if currently only lower int portion of them if used, because that makes
// it possible to switch to mmap implementation which supports long positions
// without any on-disk format changes and/or re-indexing if one day we'll have a need to.
public class TokenTree
{
    private static final int LONG_BYTES = Long.SIZE / 8;
    private static final int INT_BYTES = Integer.SIZE / 8;
    private static final int SHORT_BYTES = Short.SIZE / 8;

    private final Descriptor descriptor;
    private final MappedBuffer file;
    private final long startPos;
    private final long treeMinToken;
    private final long treeMaxToken;
    private final long tokenCount;

    @VisibleForTesting
    protected TokenTree(MappedBuffer tokenTree)
    {
        this(Descriptor.CURRENT, tokenTree);
    }

    public TokenTree(Descriptor d, MappedBuffer tokenTree)
    {
        descriptor = d;
        file = tokenTree;
        startPos = file.position();

        file.position(startPos + TokenTreeBuilder.SHARED_HEADER_BYTES);

        if (!validateMagic())
            throw new IllegalArgumentException("invalid token tree");

        tokenCount = file.getLong();
        treeMinToken = file.getLong();
        treeMaxToken = file.getLong();
    }

    public long getCount()
    {
        return tokenCount;
    }

    public RangeIterator<Long, Token> iterator(KeyFetcher keyFetcher)
    {
        return new TokenTreeIterator(file.duplicate(), keyFetcher);
    }

    public OnDiskToken get(final long searchToken, KeyFetcher keyFetcher)
    {
        seekToLeaf(searchToken, file);
        long leafStart = file.position();
        short leafSize = file.getShort(leafStart + 1); // skip the info byte

        file.position(leafStart + TokenTreeBuilder.BLOCK_HEADER_BYTES); // skip to tokens
        short tokenIndex = searchLeaf(searchToken, leafSize);

        file.position(leafStart + TokenTreeBuilder.BLOCK_HEADER_BYTES);

        OnDiskToken token = OnDiskToken.getTokenAt(file, tokenIndex, leafSize, keyFetcher);
        return token.get().equals(searchToken) ? token : null;
    }

    private boolean validateMagic()
    {
        switch (descriptor.version.toString())
        {
            case Descriptor.VERSION_AA:
                return true;
            case Descriptor.VERSION_AB:
                return TokenTreeBuilder.AB_MAGIC == file.getShort();
            default:
                return false;
        }
    }

    // finds leaf that *could* contain token
    private void seekToLeaf(long token, MappedBuffer file)
    {
        // this loop always seeks forward except for the first iteration
        // where it may seek back to the root
        long blockStart = startPos;
        while (true)
        {
            file.position(blockStart);

            byte info = file.get();
            boolean isLeaf = (info & 1) == 1;

            if (isLeaf)
            {
                file.position(blockStart);
                break;
            }

            short tokenCount = file.getShort();

            long minToken = file.getLong();
            long maxToken = file.getLong();

            long seekBase = blockStart + TokenTreeBuilder.BLOCK_HEADER_BYTES;
            if (minToken > token)
            {
                // seek to beginning of child offsets to locate first child
                file.position(seekBase + tokenCount * LONG_BYTES);
                blockStart = (startPos + (int) file.getLong());
            }
            else if (maxToken < token)
            {
                // seek to end of child offsets to locate last child
                file.position(seekBase + (2 * tokenCount) * LONG_BYTES);
                blockStart = (startPos + (int) file.getLong());
            }
            else
            {
                // skip to end of block header/start of interior block tokens
                file.position(seekBase);

                short offsetIndex = searchBlock(token, tokenCount, file);

                // file pointer is now at beginning of offsets
                if (offsetIndex == tokenCount)
                    file.position(file.position() + (offsetIndex * LONG_BYTES));
                else
                    file.position(file.position() + ((tokenCount - offsetIndex - 1) + offsetIndex) * LONG_BYTES);

                blockStart = (startPos + (int) file.getLong());
            }
        }
    }

    private short searchBlock(long searchToken, short tokenCount, MappedBuffer file)
    {
        short offsetIndex = 0;
        for (int i = 0; i < tokenCount; i++)
        {
            long readToken = file.getLong();
            if (searchToken < readToken)
                break;

            offsetIndex++;
        }

        return offsetIndex;
    }

    private short searchLeaf(long searchToken, short tokenCount)
    {
        long base = file.position();

        int start = 0;
        int end = tokenCount;
        int middle = 0;

        while (start <= end)
        {
            middle = start + ((end - start) >> 1);

            // each entry is 16 bytes wide, token is in bytes 4-11
            long token = file.getLong(base + (middle * AbstractTokenTreeBuilder.LEAF_ENTRY_BYTES + AbstractTokenTreeBuilder.ENTRY_TOKEN_OFFSET));

            if (token == searchToken)
                break;

            if (token < searchToken)
                start = middle + 1;
            else
                end = middle - 1;
        }

        return (short) middle;
    }

    public class TokenTreeIterator extends RangeIterator<Long, Token>
    {
        private final KeyFetcher keyFetcher;
        private final MappedBuffer file;

        private long currentLeafStart;
        private int currentTokenIndex;

        private long leafMinToken;
        private long leafMaxToken;
        private short leafSize;

        protected boolean firstIteration = true;
        private boolean lastLeaf;

        TokenTreeIterator(MappedBuffer file, KeyFetcher keyFetcher)
        {
            super(treeMinToken, treeMaxToken, tokenCount);

            this.file = file;
            this.keyFetcher = keyFetcher;
        }

        protected Token computeNext()
        {
            maybeFirstIteration();

            if (currentTokenIndex >= leafSize && lastLeaf)
                return endOfData();

            if (currentTokenIndex < leafSize) // tokens remaining in this leaf
            {
                return getTokenAt(currentTokenIndex++);
            }
            else // no more tokens remaining in this leaf
            {
                assert !lastLeaf;

                seekToNextLeaf();
                setupBlock();
                return computeNext();
            }
        }

        protected void performSkipTo(Long nextToken)
        {
            maybeFirstIteration();

            if (nextToken <= leafMaxToken) // next is in this leaf block
            {
                searchLeaf(nextToken);
            }
            else // next is in a leaf block that needs to be found
            {
                seekToLeaf(nextToken, file);
                setupBlock();
                findNearest(nextToken);
            }
        }

        private void setupBlock()
        {
            currentLeafStart = file.position();
            currentTokenIndex = 0;

            lastLeaf = (file.get() & (1 << TokenTreeBuilder.LAST_LEAF_SHIFT)) > 0;
            leafSize = file.getShort();

            leafMinToken = file.getLong();
            leafMaxToken = file.getLong();

            // seek to end of leaf header/start of data
            file.position(currentLeafStart + TokenTreeBuilder.BLOCK_HEADER_BYTES);
        }

        private void findNearest(Long next)
        {
            if (next > leafMaxToken && !lastLeaf)
            {
                seekToNextLeaf();
                setupBlock();
                findNearest(next);
            }
            else if (next > leafMinToken)
                searchLeaf(next);
        }

        private void searchLeaf(long next)
        {
            for (int i = currentTokenIndex; i < leafSize; i++)
            {
                if (compareTokenAt(currentTokenIndex, next) >= 0)
                    break;

                currentTokenIndex++;
            }
        }

        private int compareTokenAt(int idx, long toToken)
        {
            return Long.compare(file.getLong(getTokenPosition(idx)), toToken);
        }

        private Token getTokenAt(int idx)
        {
            return OnDiskToken.getTokenAt(file, idx, leafSize, keyFetcher);
        }

        private long getTokenPosition(int idx)
        {
            // skip 2 byte entry header to get position pointing directly at the entry's token
            return OnDiskToken.getEntryPosition(idx, file) + ENTRY_TOKEN_OFFSET;
        }

        private void seekToNextLeaf()
        {
            file.position(currentLeafStart + TokenTreeBuilder.BLOCK_BYTES);
        }

        public void close() throws IOException
        {
            // nothing to do here
        }

        private void maybeFirstIteration()
        {
            // seek to the first token only when requested for the first time,
            // highly predictable branch and saves us a lot by not traversing the tree
            // on creation time because it's not at all required.
            if (!firstIteration)
                return;

            seekToLeaf(treeMinToken, file);
            setupBlock();
            firstIteration = false;
        }
    }

    public static class OnDiskToken extends Token
    {
        private final Set<TokenInfo> info = new HashSet<>(2);

        private final Set<RowKey> loadedKeys = new TreeSet<>(RowKey.COMPARATOR);

        public OnDiskToken(MappedBuffer buffer, long position, short leafSize, KeyFetcher keyFetcher)
        {
            super(buffer.getLong(position + ENTRY_TOKEN_OFFSET));
            info.add(new TokenInfo(buffer, position, leafSize, keyFetcher));
        }

        public void merge(CombinedValue<Long> other)
        {
            if (!(other instanceof Token))
                return;

            Token o = (Token) other;
            if (token != o.token)
                throw new IllegalArgumentException(String.format("%s != %s", token, o.token));

            if (o instanceof OnDiskToken)
            {
                info.addAll(((OnDiskToken) other).info);
            }
            else
            {
                Iterators.addAll(loadedKeys, o.iterator());
            }
        }

        public Iterator<RowKey> iterator()
        {
            List<Iterator<RowKey>> keys = new ArrayList<>(info.size());

            for (TokenInfo i : info)
                keys.add(i.iterator());

            if (!loadedKeys.isEmpty())
                keys.add(loadedKeys.iterator());

            return MergeIterator.get(keys, RowKey.COMPARATOR, new MergeIterator.Reducer<RowKey, RowKey>()
            {
                RowKey reduced = null;

                public boolean trivialReduceIsTrivial()
                {
                    return true;
                }

                public void reduce(int idx, RowKey current)
                {
                    reduced = current;
                }

                protected RowKey getReduced()
                {
                    return reduced;
                }
            });
        }

        public Set<RowOffset> getOffsets()
        {
            Set<RowOffset> offsets = new HashSet<>(4);
            for (TokenInfo i : info)
            {
                for (RowOffset offset : i.fetchOffsets())
                    offsets.add(offset);
            }

            return offsets;
        }

        public static OnDiskToken getTokenAt(MappedBuffer buffer, int idx, short leafSize, KeyFetcher keyFetcher)
        {
            return new OnDiskToken(buffer, getEntryPosition(idx, buffer), leafSize, keyFetcher);
        }

        private static long getEntryPosition(int idx, MappedBuffer file)
        {
            // skip n entries, to the entry with the given index
            return file.position() + (idx * LEAF_ENTRY_BYTES);
        }
    }

    private static class TokenInfo
    {
        private final MappedBuffer buffer;
        private final KeyFetcher keyFetcher;

        private final long position;
        private final short leafSize;

        public TokenInfo(MappedBuffer buffer, long position, short leafSize, KeyFetcher keyFetcher)
        {
            this.keyFetcher = keyFetcher;
            this.buffer = buffer;
            this.position = position;
            this.leafSize = leafSize;
        }

        public Iterator<RowKey> iterator()
        {
            return new KeyIterator(keyFetcher, fetchOffsets());
        }

        public int hashCode()
        {
            return new HashCodeBuilder().append(position).append(leafSize).append(keyFetcher).build();
        }

        public boolean equals(Object other)
        {
            if (!(other instanceof TokenInfo))
                return false;

            TokenInfo o = (TokenInfo) other;
            return keyFetcher == o.keyFetcher && position == o.position;

        }

        private List<RowOffset> fetchOffsets()
        {
            short info = buffer.getShort(position);
            EntryType type = EntryType.of(info & TokenTreeBuilder.ENTRY_TYPE_MASK);

            List<RowOffset> rowOffsets = new LinkedList<>();
            switch (type)
            {
                case SIMPLE:
                    long partitionOffset = buffer.getLong(position + SHORT_BYTES + LONG_BYTES);
                    long rowOffset = buffer.getLong(position + SHORT_BYTES + (2 * LONG_BYTES));

                    rowOffsets.add(new RowOffset(partitionOffset, rowOffset));
                    break;
                case PACKED:
                    long partitionOffset1 = buffer.getInt(position + SHORT_BYTES + LONG_BYTES);
                    long rowOffset1 = buffer.getInt(position + SHORT_BYTES + LONG_BYTES + INT_BYTES);

                    long partitionOffset2 = buffer.getInt(position + SHORT_BYTES + 2 * LONG_BYTES);
                    long rowOffset2 = buffer.getInt(position + SHORT_BYTES + 2 * LONG_BYTES + INT_BYTES);

                    rowOffsets.add(new RowOffset(partitionOffset1, rowOffset1));
                    rowOffsets.add(new RowOffset(partitionOffset2, rowOffset2));
                    break;
                case OVERFLOW:
                    long collisionOffset = buffer.getLong(position + SHORT_BYTES + LONG_BYTES);
                    long count = buffer.getLong(position + SHORT_BYTES + (2 * LONG_BYTES));

                    // Skip leaves and collision offsets that do not belong to current token
                    long offsetPos = buffer.position() + leafSize * LEAF_ENTRY_BYTES + collisionOffset * 2 * LONG_BYTES;

                    for (int i = 0; i < count; i++)
                        rowOffsets.add(new RowOffset(buffer.getLong(offsetPos + 2 * i * LONG_BYTES),
                                                     buffer.getLong(offsetPos + 2 * i * LONG_BYTES + LONG_BYTES)));
                    break;
                default:
                    throw new IllegalStateException("Unknown entry type: " + type);
            }

            return rowOffsets;
        }
    }

    private static class KeyIterator extends AbstractIterator<RowKey>
    {
        private final KeyFetcher keyFetcher;
        private final List<RowOffset> offsets;
        private int index = 0;

        public KeyIterator(KeyFetcher keyFetcher, List<RowOffset> offsets)
        {
            this.keyFetcher = keyFetcher;
            this.offsets = offsets;
        }

        public RowKey computeNext()
        {
            if (index < offsets.size())
            {
                RowOffset offset = offsets.get(index++);
                return keyFetcher.getRowKey(offset.partitionOffset, offset.rowOffset);
            }
            else
            {
                return endOfData();
            }
        }
    }
}
