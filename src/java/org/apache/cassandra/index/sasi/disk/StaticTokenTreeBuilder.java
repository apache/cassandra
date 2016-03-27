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
import java.util.Iterator;
import java.util.SortedMap;

import org.apache.cassandra.index.sasi.utils.CombinedTerm;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Pair;

import com.carrotsearch.hppc.LongSet;
import com.google.common.collect.Iterators;

/**
 * Intended usage of this class is to be used in place of {@link DynamicTokenTreeBuilder}
 * when multiple index segments produced by {@link PerSSTableIndexWriter} are stitched together
 * by {@link PerSSTableIndexWriter#complete()}.
 *
 * This class uses the RangeIterator, now provided by
 * {@link CombinedTerm#getTokenIterator()}, to iterate the data twice.
 * The first iteration builds the tree with leaves that contain only enough
 * information to build the upper layers -- these leaves do not store more
 * than their minimum and maximum tokens plus their total size, which makes them
 * un-serializable.
 *
 * When the tree is written to disk the final layer is not
 * written. Its at this point the data is iterated once again to write
 * the leaves to disk. This (logarithmically) reduces copying of the
 * token values while building and writing upper layers of the tree,
 * removes the use of SortedMap when combining SAs, and relies on the
 * memory mapped SAs otherwise, greatly improving performance and no
 * longer causing OOMs when TokenTree sizes are big.
 *
 * See https://issues.apache.org/jira/browse/CASSANDRA-11383 for more details.
 */
public class StaticTokenTreeBuilder extends AbstractTokenTreeBuilder
{
    private final CombinedTerm combinedTerm;

    public StaticTokenTreeBuilder(CombinedTerm term)
    {
        combinedTerm = term;
    }

    public void add(Long token, long keyPosition)
    {
        throw new UnsupportedOperationException();
    }

    public void add(SortedMap<Long, LongSet> data)
    {
        throw new UnsupportedOperationException();
    }

    public void add(Iterator<Pair<Long, LongSet>> data)
    {
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty()
    {
        return combinedTerm.getTokenIterator().getCount() == 0;
    }

    public Iterator<Pair<Long, LongSet>> iterator()
    {
        Iterator<Token> iterator = combinedTerm.getTokenIterator();
        return new AbstractIterator<Pair<Long, LongSet>>()
        {
            protected Pair<Long, LongSet> computeNext()
            {
                if (!iterator.hasNext())
                    return endOfData();

                Token token = iterator.next();
                return Pair.create(token.get(), token.getOffsets());
            }
        };
    }

    public long getTokenCount()
    {
        return combinedTerm.getTokenIterator().getCount();
    }

    @Override
    public void write(DataOutputPlus out) throws IOException
    {
        // if the root is not a leaf then none of the leaves have been written (all are PartialLeaf)
        // so write out the last layer of the tree by converting PartialLeaf to StaticLeaf and
        // iterating the data once more
        super.write(out);
        if (root.isLeaf())
            return;

        RangeIterator<Long, Token> tokens = combinedTerm.getTokenIterator();
        ByteBuffer blockBuffer = ByteBuffer.allocate(BLOCK_BYTES);
        Iterator<Node> leafIterator = leftmostLeaf.levelIterator();
        while (leafIterator.hasNext())
        {
            Leaf leaf = (Leaf) leafIterator.next();
            Leaf writeableLeaf = new StaticLeaf(Iterators.limit(tokens, leaf.tokenCount()), leaf);
            writeableLeaf.serialize(-1, blockBuffer);
            flushBuffer(blockBuffer, out, true);
        }

    }

    protected void constructTree()
    {
        RangeIterator<Long, Token> tokens = combinedTerm.getTokenIterator();

        tokenCount = tokens.getCount();
        treeMinToken = tokens.getMinimum();
        treeMaxToken = tokens.getMaximum();
        numBlocks = 1;

        if (tokenCount <= TOKENS_PER_BLOCK)
        {
            leftmostLeaf = new StaticLeaf(tokens, tokens.getMinimum(), tokens.getMaximum(), tokens.getCount(), true);
            rightmostLeaf = leftmostLeaf;
            root = leftmostLeaf;
        }
        else
        {
            root = new InteriorNode();
            rightmostParent = (InteriorNode) root;

            // build all the leaves except for maybe
            // the last leaf which is not completely full .
            // This loop relies on the fact that multiple index segments
            // will never have token intersection for a single term,
            // because it's impossible to encounter the same value for
            // the same column multiple times in a single key/sstable.
            Leaf lastLeaf = null;
            long numFullLeaves = tokenCount / TOKENS_PER_BLOCK;
            for (long i = 0; i < numFullLeaves; i++)
            {
                Long firstToken = tokens.next().get();
                for (int j = 1; j < (TOKENS_PER_BLOCK - 1); j++)
                    tokens.next();

                Long lastToken = tokens.next().get();
                Leaf leaf = new PartialLeaf(firstToken, lastToken, TOKENS_PER_BLOCK);

                if (lastLeaf == null)
                    leftmostLeaf = leaf;
                else
                    lastLeaf.next = leaf;

                rightmostParent.add(leaf);
                lastLeaf = rightmostLeaf = leaf;
                numBlocks++;
            }

            // build the last leaf out of any remaining tokens if necessary
            // safe downcast since TOKENS_PER_BLOCK is an int
            int remainingTokens = (int) (tokenCount % TOKENS_PER_BLOCK);
            if (remainingTokens != 0)
            {
                Long firstToken = tokens.next().get();
                Long lastToken = firstToken;
                while (tokens.hasNext())
                    lastToken = tokens.next().get();

                Leaf leaf = new PartialLeaf(firstToken, lastToken, remainingTokens);
                rightmostParent.add(leaf);
                lastLeaf.next = rightmostLeaf = leaf;
                numBlocks++;
            }
        }
    }

    // This denotes the leaf which only has min/max and token counts
    // but doesn't have any associated data yet, so it can't be serialized.
    private class PartialLeaf extends Leaf
    {
        private final int size;
        public PartialLeaf(Long min, Long max, int count)
        {
            super(min, max);
            size = count;
        }

        public int tokenCount()
        {
            return size;
        }

        public void serializeData(ByteBuffer buf)
        {
            throw new UnsupportedOperationException();
        }

        public boolean isSerializable()
        {
            return false;
        }
    }

    // This denotes the leaf which has been filled with data and is ready to be serialized
    private class StaticLeaf extends Leaf
    {
        private final Iterator<Token> tokens;
        private final int count;
        private final boolean isLast;

        public StaticLeaf(Iterator<Token> tokens, Leaf leaf)
        {
            this(tokens, leaf.smallestToken(), leaf.largestToken(), leaf.tokenCount(), leaf.isLastLeaf());
        }

        public StaticLeaf(Iterator<Token> tokens, Long min, Long max, long count, boolean isLastLeaf)
        {
            super(min, max);

            this.count = (int) count; // downcast is safe since leaf size is always < Integer.MAX_VALUE
            this.tokens = tokens;
            this.isLast = isLastLeaf;
        }

        public boolean isLastLeaf()
        {
            return isLast;
        }

        public int tokenCount()
        {
            return count;
        }

        public void serializeData(ByteBuffer buf)
        {
            while (tokens.hasNext())
            {
                Token entry = tokens.next();
                createEntry(entry.get(), entry.getOffsets()).serialize(buf);
            }
        }

        public boolean isSerializable()
        {
            return true;
        }
    }
}
