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
package org.apache.cassandra.index.sai.disk.v1.segment;

import java.util.NoSuchElementException;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * Holds one {@link PackedLongValues} section per {@link PostingType}, used as the trie-node payload in
 * {@link SegmentTrieBuffer}.
 * <p>
 * Currently two sections are active: {@code EXACT(0)} and {@code PREFIX(1)}. {@code SUFFIX(2)} is reserved —
 * set {@link #FILTER_TYPES} to 3 when suffix search is added.
 */
public class PackedLongValuesList implements Accountable
{
    /** Increment to 3 when SUFFIX is implemented. */
    static final int FILTER_TYPES = 2;

    private final PackedLongValues exact;
    private final PackedLongValues prefix;

    private PackedLongValuesList(PackedLongValues exact, PackedLongValues prefix)
    {
        this.exact = exact;
        this.prefix = prefix;
    }

    /** Number of exact-match postings (= prefixIndex in the V2 on-disk format). */
    public int exactCount()
    {
        return (int) exact.size();
    }

    /** Number of prefix postings. */
    public int prefixCount()
    {
        return (int) prefix.size();
    }

    /** Total postings (= suffixIndex in the V2 on-disk format). */
    public int totalCount()
    {
        return exactCount() + prefixCount();
    }

    @Override
    public long ramBytesUsed()
    {
        return exact.ramBytesUsed() + prefix.ramBytesUsed();
    }

    /** Iterator over the exact-section row IDs, ascending. */
    public PackedLongValues.Iterator exactIterator()
    {
        return exact.iterator();
    }

    /** Iterator over the prefix-section row IDs, ascending. */
    public PackedLongValues.Iterator prefixIterator()
    {
        return prefix.iterator();
    }

    /**
     * Iterator emitting, in order: {@code exactCount}, {@code totalCount}, all exact row IDs, then all prefix
     * row IDs. This header-then-sections layout lets {@link SegmentTrieBuffer} expose the node payload as a
     * single {@link org.apache.cassandra.index.sai.postings.PostingList}.
     */
    public Iterator iterator()
    {
        return new Iterator();
    }

    public final class Iterator
    {
        private int headerIdx = 0;
        private final PackedLongValues.Iterator exactIt = exact.iterator();
        private final PackedLongValues.Iterator prefixIt = prefix.iterator();

        public boolean hasNext()
        {
            return headerIdx < 2 || exactIt.hasNext() || prefixIt.hasNext();
        }

        public long next()
        {
            if (headerIdx == 0)
            {
                headerIdx++;
                return exactCount();
            }
            if (headerIdx == 1)
            {
                headerIdx++;
                return totalCount();
            }
            if (exactIt.hasNext())
                return exactIt.next();
            if (prefixIt.hasNext())
                return prefixIt.next();
            throw new NoSuchElementException();
        }
    }

    public static class Builder implements Accountable
    {
        private final PackedLongValues.Builder exactBuilder = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
        private final PackedLongValues.Builder prefixBuilder = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);

        /**
         * @param rowId segment row ID
         * @param type  the {@link PostingType} determining which section receives the row ID
         */
        public Builder add(long rowId, PostingType type)
        {
            switch (type)
            {
                case EXACT:
                    exactBuilder.add(rowId);
                    break;
                case PREFIX:
                    prefixBuilder.add(rowId);
                    break;
                default:
                    throw new IllegalArgumentException("Unhandled PostingType: " + type);
            }
            return this;
        }

        public PackedLongValuesList build()
        {
            return new PackedLongValuesList(exactBuilder.build(), prefixBuilder.build());
        }

        @Override
        public long ramBytesUsed()
        {
            return exactBuilder.ramBytesUsed() + prefixBuilder.ramBytesUsed();
        }
    }
}
