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

package org.apache.cassandra.index.sai.disk.v1.bbtree;

import java.util.Iterator;
import java.util.Map;

import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.index.sai.utils.TermsIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * A helper class for mapping disparate iterators to a single iterator for consumption by the
 * {@link BlockBalancedTreeWriter}
 */
public interface BlockBalancedTreeIterator extends Iterator<Pair<byte[], PostingList>>
{
    static BlockBalancedTreeIterator fromTermsIterator(final TermsIterator termsIterator, IndexTermType indexTermType)
    {
        return new BlockBalancedTreeIterator()
        {
            final byte[] scratch = new byte[indexTermType.fixedSizeOf()];

            @Override
            public boolean hasNext()
            {
                return termsIterator.hasNext();
            }

            @Override
            public Pair<byte[], PostingList> next()
            {
                ByteComparable term = termsIterator.next();
                ByteSourceInverse.copyBytes(term.asComparableBytes(ByteComparable.Version.OSS50), scratch);
                return Pair.create(scratch, termsIterator.postings());
            }
        };
    }

    static BlockBalancedTreeIterator fromTrieIterator(Iterator<Map.Entry<ByteComparable, PackedLongValues.Builder>> iterator, int bytesPerValue)
    {
        return new BlockBalancedTreeIterator()
        {
            final byte[] scratch = new byte[bytesPerValue];

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public Pair<byte[], PostingList> next()
            {
                Map.Entry<ByteComparable, PackedLongValues.Builder> entry = iterator.next();
                ByteSourceInverse.copyBytes(entry.getKey().asComparableBytes(ByteComparable.Version.OSS50), scratch);
                PackedLongValues postings = entry.getValue().build();
                PackedLongValues.Iterator postingsIterator = postings.iterator();
                return Pair.create(scratch, new PostingList() {
                    @Override
                    public long nextPosting()
                    {
                        if (postingsIterator.hasNext())
                            return postingsIterator.next();
                        return END_OF_STREAM;
                    }

                    @Override
                    public long size()
                    {
                        return postings.size();
                    }

                    @Override
                    public long advance(long targetRowID)
                    {
                        throw new UnsupportedOperationException();
                    }
                });
            }
        };
    }
}
