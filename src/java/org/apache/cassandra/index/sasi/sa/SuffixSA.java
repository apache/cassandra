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
        private final long[] suffixes;

        private int current = 0;
        private ByteBuffer lastProcessedSuffix;
        private TokenTreeBuilder container;

        public SASuffixIterator()
        {
            // each element has term index and char position encoded as two 32-bit integers
            // to avoid binary search per suffix while sorting suffix array.
            suffixes = new long[charCount];

            long termIndex = -1, currentTermLength = -1;
            for (int i = 0; i < charCount; i++)
            {
                if (i >= currentTermLength || currentTermLength == -1)
                {
                    Term currentTerm = terms.get((int) ++termIndex);
                    currentTermLength = currentTerm.getPosition() + currentTerm.length();
                }

                suffixes[i] = (termIndex << 32) | i;
            }

            Primitive.sort(suffixes, (a, b) -> {
                Term aTerm = terms.get((int) (a >>> 32));
                Term bTerm = terms.get((int) (b >>> 32));
                return comparator.compare(aTerm.getSuffix(((int) a) - aTerm.getPosition()),
                                          bTerm.getSuffix(((int) b) - bTerm.getPosition()));
            });
        }

        private Pair<ByteBuffer, TokenTreeBuilder> suffixAt(int position)
        {
            long index = suffixes[position];
            Term term = terms.get((int) (index >>> 32));
            return Pair.create(term.getSuffix(((int) index) - term.getPosition()), term.getTokens());
        }

        public ByteBuffer minTerm()
        {
            return suffixAt(0).left;
        }

        public ByteBuffer maxTerm()
        {
            return suffixAt(suffixes.length - 1).left;
        }

        protected Pair<ByteBuffer, TokenTreeBuilder> computeNext()
        {
            while (true)
            {
                if (current >= suffixes.length)
                {
                    if (lastProcessedSuffix == null)
                        return endOfData();

                    Pair<ByteBuffer, TokenTreeBuilder> result = finishSuffix();

                    lastProcessedSuffix = null;
                    return result;
                }

                Pair<ByteBuffer, TokenTreeBuilder> suffix = suffixAt(current++);

                if (lastProcessedSuffix == null)
                {
                    lastProcessedSuffix = suffix.left;
                    container = new TokenTreeBuilder(suffix.right.getTokens());
                }
                else if (comparator.compare(lastProcessedSuffix, suffix.left) == 0)
                {
                    lastProcessedSuffix = suffix.left;
                    container.add(suffix.right.getTokens());
                }
                else
                {
                    Pair<ByteBuffer, TokenTreeBuilder> result = finishSuffix();

                    lastProcessedSuffix = suffix.left;
                    container = new TokenTreeBuilder(suffix.right.getTokens());

                    return result;
                }
            }
        }

        private Pair<ByteBuffer, TokenTreeBuilder> finishSuffix()
        {
            return Pair.create(lastProcessedSuffix, container.finish());
        }
    }
}
