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

package org.apache.cassandra.index.sai.disk.v1.sortedterms;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.index.sai.disk.v1.trie.TriePrefixSearcher;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class SortedTermsTrieSearcher implements AutoCloseable
{
    private final List<TrieSegment> segments;

    public SortedTermsTrieSearcher(Rebufferer source, SortedTermsMeta meta)
    {
        this.segments = new ArrayList<>(meta.segments.size());
        for (SortedTermsMeta.SegmentMeta segmentMeta : meta.segments)
            this.segments.add(new TrieSegment(source, segmentMeta));

    }

    /**
     * Search the segments for a prefix. The search will return the first payload value after the prefix or
     * -1 if the prefix wasn't found.
     *
     * @param prefix The {@link ByteComparable} containing the prefix
     * @return A {@link long} containing the payload value or -1.
     */
    public long prefixSearch(ByteComparable prefix)
    {
        for (TrieSegment segment : segments)
        {
            if (segment.includesTerm(prefix))
                return segment.prefixSearch(prefix);
        }
        return -1l;
    }

    @Override
    public void close() throws Exception
    {
        segments.forEach(TrieSegment::close);
    }

    private static class TrieSegment implements AutoCloseable
    {
        private final TriePrefixSearcher prefixSearcher;
        private final SortedTermsMeta.SegmentMeta meta;

        TrieSegment(Rebufferer source, SortedTermsMeta.SegmentMeta meta)
        {
            this.meta = meta;
            this.prefixSearcher = new TriePrefixSearcher(source, meta.trieFilePointer);
        }

        boolean includesTerm(ByteComparable term)
        {
            return ByteComparable.compare(term, v -> ByteSource.fixedLength(meta.minimumTerm.bytes), ByteComparable.Version.OSS50) >= 0 &&
                   ByteComparable.compare(term, v -> ByteSource.fixedLength(meta.maximumTerm.bytes), ByteComparable.Version.OSS50) <= 0;
        }

        long prefixSearch(ByteComparable term)
        {
            return prefixSearcher.prefixSearch(term.asComparableBytes(Walker.BYTE_COMPARABLE_VERSION));
        }

        @Override
        public void close()
        {
            prefixSearcher.close();
        }
    }
}
