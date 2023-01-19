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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sasi.utils.CombinedValue;
import org.apache.cassandra.index.sasi.utils.MappedBuffer;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.utils.AbstractGuavaIterator;
import org.apache.cassandra.utils.MergeIterator;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import static org.apache.cassandra.index.sasi.disk.TokenTreeBuilder.EntryType;

// Note: all of the seek-able offsets contained in TokenTree should be sizeof(long)
// even if currently only lower int portion of them if used, because that makes
// it possible to switch to mmap implementation which supports long positions
// without any on-disk format changes and/or re-indexing if one day we'll have a need to.
public class TokenTree
{
    private static final int LONG_BYTES = Long.SIZE / 8;
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

    public RangeIterator<Long, Token> iterator(Function<Long, DecoratedKey> keyFetcher)
    {
        return new TokenTreeIterator(file.duplicate(), keyFetcher);
    }

    public OnDiskToken get(final long searchToken, Function<Long, DecoratedKey> keyFetcher)
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
            long token = file.getLong(base + (middle * (2 * LONG_BYTES) + 4));

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
        private final Function<Long, DecoratedKey> keyFetcher;
        private final MappedBuffer file;

        private long currentLeafStart;
        private int currentTokenIndex;

        private long leafMinToken;
        private long leafMaxToken;
        private short leafSize;

        protected boolean firstIteration = true;
        private boolean lastLeaf;

        TokenTreeIterator(MappedBuffer file, Function<Long, DecoratedKey> keyFetcher)
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
            // skip 4 byte entry header to get position pointing directly at the entry's token
            return OnDiskToken.getEntryPosition(idx, file) + (2 * SHORT_BYTES);
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
        private final Set<DecoratedKey> loadedKeys = new TreeSet<>(DecoratedKey.comparator);

        public OnDiskToken(MappedBuffer buffer, long position, short leafSize, Function<Long, DecoratedKey> keyFetcher)
        {
            super(buffer.getLong(position + (2 * SHORT_BYTES)));
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

        public Iterator<DecoratedKey> iterator()
        {
            List<Iterator<DecoratedKey>> keys = new ArrayList<>(info.size());

            for (TokenInfo i : info)
                keys.add(i.iterator());

            if (!loadedKeys.isEmpty())
                keys.add(loadedKeys.iterator());

            return MergeIterator.get(keys, DecoratedKey.comparator, new MergeIterator.Reducer<DecoratedKey, DecoratedKey>()
            {
                DecoratedKey reduced = null;

                public boolean trivialReduceIsTrivial()
                {
                    return true;
                }

                public void reduce(int idx, DecoratedKey current)
                {
                    reduced = current;
                }

                protected DecoratedKey getReduced()
                {
                    return reduced;
                }
            });
        }

        public LongSet getOffsets()
        {
            LongSet offsets = new LongHashSet(4);
            for (TokenInfo i : info)
            {
                for (long offset : i.fetchOffsets())
                    offsets.add(offset);
            }

            return offsets;
        }

        public static OnDiskToken getTokenAt(MappedBuffer buffer, int idx, short leafSize, Function<Long, DecoratedKey> keyFetcher)
        {
            return new OnDiskToken(buffer, getEntryPosition(idx, buffer), leafSize, keyFetcher);
        }

        private static long getEntryPosition(int idx, MappedBuffer file)
        {
            // info (4 bytes) + token (8 bytes) + offset (4 bytes) = 16 bytes
            return file.position() + (idx * (2 * LONG_BYTES));
        }
    }

    private static class TokenInfo
    {
        private final MappedBuffer buffer;
        private final Function<Long, DecoratedKey> keyFetcher;

        private final long position;
        private final short leafSize;

        public TokenInfo(MappedBuffer buffer, long position, short leafSize, Function<Long, DecoratedKey> keyFetcher)
        {
            this.keyFetcher = keyFetcher;
            this.buffer = buffer;
            this.position = position;
            this.leafSize = leafSize;
        }

        public Iterator<DecoratedKey> iterator()
        {
            return new KeyIterator(keyFetcher, fetchOffsets());
        }

        public int hashCode()
        {
            return new HashCodeBuilder().append(keyFetcher).append(position).append(leafSize).build();
        }

        public boolean equals(Object other)
        {
            if (!(other instanceof TokenInfo))
                return false;

            TokenInfo o = (TokenInfo) other;
            return keyFetcher == o.keyFetcher && position == o.position;
        }

        private long[] fetchOffsets()
        {
            short info = buffer.getShort(position);
            // offset extra is unsigned short (right-most 16 bits of 48 bits allowed for an offset)
            int offsetExtra = buffer.getShort(position + SHORT_BYTES) & 0xFFFF;
            // is the it left-most (32-bit) base of the actual offset in the index file
            int offsetData = buffer.getInt(position + (2 * SHORT_BYTES) + LONG_BYTES);

            EntryType type = EntryType.of(info & TokenTreeBuilder.ENTRY_TYPE_MASK);

            switch (type)
            {
                case SIMPLE:
                    return new long[] { offsetData };

                case OVERFLOW:
                    long[] offsets = new long[offsetExtra]; // offsetShort contains count of tokens
                    long offsetPos = (buffer.position() + (2 * (leafSize * LONG_BYTES)) + (offsetData * LONG_BYTES));

                    for (int i = 0; i < offsetExtra; i++)
                        offsets[i] = buffer.getLong(offsetPos + (i * LONG_BYTES));

                    return offsets;

                case FACTORED:
                    return new long[] { (((long) offsetData) << Short.SIZE) + offsetExtra };

                case PACKED:
                    return new long[] { offsetExtra, offsetData };

                default:
                    throw new IllegalStateException("Unknown entry type: " + type);
            }
        }
    }

    private static class KeyIterator extends AbstractGuavaIterator<DecoratedKey>
    {
        private final Function<Long, DecoratedKey> keyFetcher;
        private final long[] offsets;
        private int index = 0;

        public KeyIterator(Function<Long, DecoratedKey> keyFetcher, long[] offsets)
        {
            this.keyFetcher = keyFetcher;
            this.offsets = offsets;
        }

        public DecoratedKey computeNext()
        {
            return index < offsets.length ? keyFetcher.apply(offsets[index++]) : endOfData();
        }
    }
}
