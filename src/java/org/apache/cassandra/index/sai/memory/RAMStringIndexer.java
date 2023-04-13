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
package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.TermsIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;

/**
 * Indexes strings into an on-heap inverted index to be flushed to an on-disk index later.
 * The flushing process uses the {@link TermsIterator} interface to iterate over the
 * indexed terms.
 */
@NotThreadSafe
public class RAMStringIndexer
{
    private final BytesRefHash termsHash;
    private final RAMPostingSlices slices;
    private final Counter bytesUsed;
    
    private int rowCount = 0;
    private int[] lastRowIdPerTerm = new int[RAMPostingSlices.DEFAULT_TERM_DICT_SIZE];

    public RAMStringIndexer()
    {
        bytesUsed = Counter.newCounter();
        ByteBlockPool termsPool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(bytesUsed));
        termsHash = new BytesRefHash(termsPool);
        slices = new RAMPostingSlices(bytesUsed);
    }

    public long estimatedBytesUsed()
    {
        return bytesUsed.get();
    }

    public boolean isEmpty()
    {
        return rowCount == 0;
    }

    /**
     * EXPENSIVE OPERATION due to sorting the terms, only call once.
     */
    public TermsIterator getTermsWithPostings()
    {
        final int[] sortedTermIDs = termsHash.sort();

        final int valueCount = termsHash.size();
        final ByteSliceReader sliceReader = new ByteSliceReader();

        return new TermsIterator()
        {
            // This is the position of the current term in the sorted terms array
            private int position = 0;
            private final BytesRef br = new BytesRef();

            @Override
            public ByteBuffer getMinTerm()
            {
                BytesRef term = new BytesRef();
                int minTermID = sortedTermIDs[0];
                termsHash.get(minTermID, term);
                return ByteBuffer.wrap(term.bytes, term.offset, term.length);
            }

            @Override
            public ByteBuffer getMaxTerm()
            {
                BytesRef term = new BytesRef();
                int maxTermID = sortedTermIDs[valueCount-1];
                termsHash.get(maxTermID, term);
                return ByteBuffer.wrap(term.bytes, term.offset, term.length);
            }

            public void close() {}

            @Override
            public PostingList postings()
            {
                // ordinal should already have been advanced in next()
                int termID = sortedTermIDs[position - 1];
                return slices.postingList(termID, sliceReader);
            }

            @Override
            public boolean hasNext() {
                return position < valueCount;
            }

            @Override
            public ByteComparable next()
            {
                if (!hasNext())
                    throw new NoSuchElementException();

                termsHash.get(sortedTermIDs[position], br);
                position++;
                return asByteComparable(br.bytes, br.offset, br.length);
            }

            private ByteComparable asByteComparable(byte[] bytes, int offset, int length)
            {
                return v -> ByteSource.fixedLength(bytes, offset, length);
            }
        };
    }

    public long add(BytesRef term, int segmentRowId)
    {
        long startBytes = estimatedBytesUsed();
        int termID = termsHash.add(term);

        if (termID >= 0)
        {
            // first time seeing this term, so create its first slice.
            slices.createNewSlice(termID);
        }
        else
        {
            termID = (-termID) - 1;
        }

        if (termID >= lastRowIdPerTerm.length - 1)
        {
            lastRowIdPerTerm = ArrayUtil.grow(lastRowIdPerTerm, termID + 1);
        }

        int delta = segmentRowId - lastRowIdPerTerm[termID];

        lastRowIdPerTerm[termID] = segmentRowId;

        slices.writeVInt(termID, delta);

        long allocatedBytes = estimatedBytesUsed() - startBytes;

        rowCount++;

        return allocatedBytes;
    }
}
