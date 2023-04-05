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
package org.apache.cassandra.index.sasi.conf.view;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.index.sasi.SSTableIndex;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.utils.trie.KeyAnalyzer;
import org.apache.cassandra.index.sasi.utils.trie.PatriciaTrie;
import org.apache.cassandra.index.sasi.utils.trie.Trie;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

import com.google.common.collect.Sets;

/**
 * This class is an extension over RangeTermTree for string terms,
 * it is required because interval tree can't handle matching if search is on the
 * prefix of min/max of the range, so for ascii/utf8 fields we build an additional
 * prefix trie (including both min/max terms of the index) and do union of the results
 * of the prefix tree search and results from the interval tree lookup.
 */
public class PrefixTermTree extends RangeTermTree
{
    private final OnDiskIndexBuilder.Mode mode;
    private final Trie<ByteBuffer, Set<SSTableIndex>> trie;

    public PrefixTermTree(ByteBuffer min, ByteBuffer max,
                          Trie<ByteBuffer, Set<SSTableIndex>> trie,
                          IntervalTree<Term, SSTableIndex, Interval<Term, SSTableIndex>> ranges,
                          OnDiskIndexBuilder.Mode mode,
                          AbstractType<?> comparator)
    {
        super(min, max, ranges, comparator);

        this.mode = mode;
        this.trie = trie;
    }

    public Set<SSTableIndex> search(Expression e)
    {
        Map<ByteBuffer, Set<SSTableIndex>> indexes = (e == null || e.lower == null || mode == OnDiskIndexBuilder.Mode.CONTAINS)
                                                        ? trie : trie.prefixMap(e.lower.value);

        Set<SSTableIndex> view = new HashSet<>(indexes.size());
        indexes.values().forEach(view::addAll);
        return Sets.union(view, super.search(e));
    }

    public static class Builder extends RangeTermTree.Builder
    {
        private final PatriciaTrie<ByteBuffer, Set<SSTableIndex>> trie;

        protected Builder(OnDiskIndexBuilder.Mode mode, final AbstractType<?> comparator)
        {
            super(mode, comparator);
            trie = new PatriciaTrie<>(new ByteBufferKeyAnalyzer(comparator));

        }

        public void addIndex(SSTableIndex index)
        {
            super.addIndex(index);
            addTerm(index.minTerm(), index);
            addTerm(index.maxTerm(), index);
        }

        public TermTree build()
        {
            return new PrefixTermTree(min, max, trie, IntervalTree.build(intervals), mode, comparator);
        }

        private void addTerm(ByteBuffer term, SSTableIndex index)
        {
            Set<SSTableIndex> indexes = trie.get(term);
            if (indexes == null)
                trie.put(term, (indexes = new HashSet<>()));

            indexes.add(index);
        }
    }

    private static class ByteBufferKeyAnalyzer implements KeyAnalyzer<ByteBuffer>
    {
        private final AbstractType<?> comparator;

        public ByteBufferKeyAnalyzer(AbstractType<?> comparator)
        {
            this.comparator = comparator;
        }

        /**
         * A bit mask where the first bit is 1 and the others are zero
         */
        private static final int MSB = 1 << Byte.SIZE-1;

        public int compare(ByteBuffer a, ByteBuffer b)
        {
            return comparator.compare(a, b);
        }

        public int lengthInBits(ByteBuffer o)
        {
            return o.remaining() * Byte.SIZE;
        }

        public boolean isBitSet(ByteBuffer key, int bitIndex)
        {
            if (bitIndex >= lengthInBits(key))
                return false;

            int index = bitIndex / Byte.SIZE;
            int bit = bitIndex % Byte.SIZE;
            return (key.get(index) & mask(bit)) != 0;
        }

        public int bitIndex(ByteBuffer key, ByteBuffer otherKey)
        {
            int length = Math.max(key.remaining(), otherKey.remaining());

            boolean allNull = true;
            for (int i = 0; i < length; i++)
            {
                byte b1 = valueAt(key, i);
                byte b2 = valueAt(otherKey, i);

                if (b1 != b2)
                {
                    int xor = b1 ^ b2;
                    for (int j = 0; j < Byte.SIZE; j++)
                    {
                        if ((xor & mask(j)) != 0)
                            return (i * Byte.SIZE) + j;
                    }
                }

                if (b1 != 0)
                    allNull = false;
            }

            return allNull ? KeyAnalyzer.NULL_BIT_KEY : KeyAnalyzer.EQUAL_BIT_KEY;
        }

        public boolean isPrefix(ByteBuffer key, ByteBuffer prefix)
        {
            if (key.remaining() < prefix.remaining())
                return false;

            for (int i = 0; i < prefix.remaining(); i++)
            {
                if (key.get(i) != prefix.get(i))
                    return false;
            }

            return true;
        }

        /**
         * Returns the {@code byte} value at the given index.
         */
        private byte valueAt(ByteBuffer value, int index)
        {
            return index >= 0 && index < value.remaining() ? value.get(index) : 0;
        }

        /**
         * Returns a bit mask where the given bit is set
         */
        private int mask(int bit)
        {
            return MSB >>> bit;
        }
    }
}
