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
package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.bti.RowIndexReader.IndexInfo;
import org.apache.cassandra.io.tries.IncrementalTrieWriter;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Preparer / writer of row index tries that can be read by {@link RowIndexReader} and iterated by
 * {@link RowIndexReverseIterator}.
 * <p>
 * Uses {@link IncrementalTrieWriter} to build a trie of index section separators of the shortest possible length such
 * that {@code prevMax < separator <= nextMin}.
 */
class RowIndexWriter implements AutoCloseable
{
    private final ClusteringComparator comparator;
    private final IncrementalTrieWriter<IndexInfo> trie;
    private ByteComparable prevMax = null;
    private ByteComparable prevSep = null;

    RowIndexWriter(ClusteringComparator comparator, DataOutputPlus out, Version version)
    {
        this.comparator = comparator;
        this.trie = IncrementalTrieWriter.open(RowIndexReader.getSerializer(version), out);
    }

    void reset()
    {
        prevMax = null;
        prevSep = null;
        trie.reset();
    }

    @Override
    public void close()
    {
        trie.close();
    }

    void add(ClusteringPrefix<?> firstName, ClusteringPrefix<?> lastName, IndexInfo info) throws IOException
    {
        assert info.openDeletion != null;
        ByteComparable sep = prevMax == null
                             ? ByteComparable.EMPTY
                             : ByteComparable.separatorGt(prevMax, comparator.asByteComparable(firstName));
        trie.add(sep, info);
        prevSep = sep;
        prevMax = comparator.asByteComparable(lastName);
    }

    public long complete(long endPos) throws IOException
    {
        // Add a separator after the last section, so that greater inputs can be quickly rejected.
        // To maximize its efficiency we add it with the length of the last added separator.
        int i = 0;
        ByteSource max = prevMax.asComparableBytes(Walker.BYTE_COMPARABLE_VERSION);
        ByteSource sep = prevSep.asComparableBytes(Walker.BYTE_COMPARABLE_VERSION);
        int c;
        while ((c = max.next()) == sep.next() && c != ByteSource.END_OF_STREAM)
            ++i;
        assert c != ByteSource.END_OF_STREAM : "Corrupted row order, max=" + prevMax;

        trie.add(nudge(prevMax, i), new IndexInfo(endPos, DeletionTime.LIVE));

        return trie.complete();
    }

    /**
     * Produces a source that is slightly greater than argument with length at least nudgeAt.
     */
    private ByteComparable nudge(ByteComparable value, int nudgeAt)
    {
        return version -> new ByteSource()
        {
            private final ByteSource v = value.asComparableBytes(version);
            private int cur = 0;

            @Override
            public int next()
            {
                int b = ByteSource.END_OF_STREAM;
                if (cur <= nudgeAt)
                {
                    b = v.next();
                    if (cur == nudgeAt)
                    {
                        if (b < 0xFF)
                            ++b;
                        else
                            return b;  // can't nudge here, increase next instead (eventually will be -1)
                    }
                }
                ++cur;
                return b;
            }
        };
    }
}
