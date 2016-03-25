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
package org.apache.cassandra.index.sasi.sa;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import org.apache.cassandra.index.sasi.disk.DynamicTokenTreeBuilder;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Pair;

import com.google.common.base.Charsets;
import net.mintern.primitive.Primitive;

public class SuffixSA extends SA<CharBuffer>
{
    public SuffixSA(AbstractType<?> comparator, OnDiskIndexBuilder.Mode mode)
    {
        super(comparator, mode);
    }

    protected Term<CharBuffer> getTerm(ByteBuffer termValue, TokenTreeBuilder tokens)
    {
        return new CharTerm(charCount, Charsets.UTF_8.decode(termValue.duplicate()), tokens);
    }

    public TermIterator finish()
    {
        return new SASuffixIterator();
    }

    private class SASuffixIterator extends TermIterator
    {

        private static final int COMPLETE_BIT = 31;

        private final long[] suffixes;

        private int current = 0;
        private IndexedTerm lastProcessedSuffix;
        private TokenTreeBuilder container;

        public SASuffixIterator()
        {
            // each element has term index and char position encoded as two 32-bit integers
            // to avoid binary search per suffix while sorting suffix array.
            suffixes = new long[charCount];

            long termIndex = -1, currentTermLength = -1;
            boolean isComplete = false;
            for (int i = 0; i < charCount; i++)
            {
                if (i >= currentTermLength || currentTermLength == -1)
                {
                    Term currentTerm = terms.get((int) ++termIndex);
                    currentTermLength = currentTerm.getPosition() + currentTerm.length();
                    isComplete = true;
                }

                suffixes[i] = (termIndex << 32) | i;
                if (isComplete)
                    suffixes[i] |= (1L << COMPLETE_BIT);

                isComplete = false;
            }

            Primitive.sort(suffixes, (a, b) -> {
                Term aTerm = terms.get((int) (a >>> 32));
                Term bTerm = terms.get((int) (b >>> 32));
                return comparator.compare(aTerm.getSuffix(clearCompleteBit(a) - aTerm.getPosition()),
                                          bTerm.getSuffix(clearCompleteBit(b) - bTerm.getPosition()));
            });
        }

        private int clearCompleteBit(long value)
        {
            return (int) (value & ~(1L << COMPLETE_BIT));
        }

        private Pair<IndexedTerm, TokenTreeBuilder> suffixAt(int position)
        {
            long index = suffixes[position];
            Term term = terms.get((int) (index >>> 32));
            boolean isPartitial = (index & ((long) 1 << 31)) == 0;
            return Pair.create(new IndexedTerm(term.getSuffix(clearCompleteBit(index) - term.getPosition()), isPartitial), term.getTokens());
        }

        public ByteBuffer minTerm()
        {
            return suffixAt(0).left.getBytes();
        }

        public ByteBuffer maxTerm()
        {
            return suffixAt(suffixes.length - 1).left.getBytes();
        }

        protected Pair<IndexedTerm, TokenTreeBuilder> computeNext()
        {
            while (true)
            {
                if (current >= suffixes.length)
                {
                    if (lastProcessedSuffix == null)
                        return endOfData();

                    Pair<IndexedTerm, TokenTreeBuilder> result = finishSuffix();

                    lastProcessedSuffix = null;
                    return result;
                }

                Pair<IndexedTerm, TokenTreeBuilder> suffix = suffixAt(current++);

                if (lastProcessedSuffix == null)
                {
                    lastProcessedSuffix = suffix.left;
                    container = new DynamicTokenTreeBuilder(suffix.right);
                }
                else if (comparator.compare(lastProcessedSuffix.getBytes(), suffix.left.getBytes()) == 0)
                {
                    lastProcessedSuffix = suffix.left;
                    container.add(suffix.right);
                }
                else
                {
                    Pair<IndexedTerm, TokenTreeBuilder> result = finishSuffix();

                    lastProcessedSuffix = suffix.left;
                    container = new DynamicTokenTreeBuilder(suffix.right);

                    return result;
                }
            }
        }

        private Pair<IndexedTerm, TokenTreeBuilder> finishSuffix()
        {
            return Pair.create(lastProcessedSuffix, container.finish());
        }
    }
}
