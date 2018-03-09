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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Pair;

public class IntegralSA extends SA<ByteBuffer>
{
    public IntegralSA(AbstractType<?> comparator, OnDiskIndexBuilder.Mode mode)
    {
        super(comparator, mode);
    }

    public Term<ByteBuffer> getTerm(ByteBuffer termValue, TokenTreeBuilder tokens)
    {
        return new ByteTerm(charCount, termValue, tokens);
    }

    public TermIterator finish()
    {
        return new IntegralSuffixIterator();
    }


    private class IntegralSuffixIterator extends TermIterator
    {
        private final Iterator<Term<ByteBuffer>> termIterator;

        public IntegralSuffixIterator()
        {
            Collections.sort(terms, new Comparator<Term<?>>()
            {
                public int compare(Term<?> a, Term<?> b)
                {
                    return a.compareTo(comparator, b);
                }
            });

            termIterator = terms.iterator();
        }

        public ByteBuffer minTerm()
        {
            return terms.get(0).getTerm();
        }

        public ByteBuffer maxTerm()
        {
            return terms.get(terms.size() - 1).getTerm();
        }

        protected Pair<IndexedTerm, TokenTreeBuilder> computeNext()
        {
            if (!termIterator.hasNext())
                return endOfData();

            Term<ByteBuffer> term = termIterator.next();
            return Pair.create(new IndexedTerm(term.getTerm(), false), term.getTokens().finish());
        }
    }
}
