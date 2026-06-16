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
package org.apache.cassandra.index.sai.disk.v1.trie;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.mutable.MutableLong;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentWriter;
import org.apache.cassandra.index.sai.postings.IntArrayPostingList;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.IndexEntry;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;

/**
 * Builds an on-disk inverted index structure: terms dictionary and postings lists.
 */
@NotThreadSafe
public class LiteralIndexWriter implements SegmentWriter
{
    /** Attribute key on TERMS_DATA marking a segment as using the V2 (prefix-enabled) postings format. */
    public static final String POSTINGS_FORMAT = "postings_format";
    public static final String POSTINGS_FORMAT_V2 = "v2";

    private final IndexDescriptor indexDescriptor;
    private final IndexIdentifier indexIdentifier;
    private long postingsAdded;

    public LiteralIndexWriter(IndexDescriptor indexDescriptor, IndexIdentifier indexIdentifier)
    {
        this.indexDescriptor = indexDescriptor;
        this.indexIdentifier = indexIdentifier;
    }

    @Override
    public SegmentMetadata.ComponentMetadataMap writeCompleteSegment(Iterator<IndexEntry> iterator) throws IOException
    {
        return writeCompleteSegment(iterator, false);
    }

    /**
     * Writes the terms dictionary and postings lists for a segment.
     *
     * @param iterator      sorted entries. When {@code prefixEnabled} is false each entry's posting list holds raw
     *                      row IDs. When true, each entry's posting list emits {@code exactCount}, {@code totalCount},
     *                      then all exact row IDs followed by all prefix row IDs
     *                      (see {@link org.apache.cassandra.index.sai.disk.v1.segment.SegmentTrieBuffer}).
     * @param prefixEnabled when true, eligible nodes are written using the V2 posting list format and the segment is
     *                      tagged with {@code postings_format = v2}
     */
    public SegmentMetadata.ComponentMetadataMap writeCompleteSegment(Iterator<IndexEntry> iterator, boolean prefixEnabled) throws IOException
    {
        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();

        final int minimumLeaves = prefixEnabled ? CassandraRelevantProperties.SAI_MINIMUM_POSTINGS_LEAVES.getInt()
                                                 : Integer.MAX_VALUE;

        try (TrieTermsDictionaryWriter termsDictionaryWriter = new TrieTermsDictionaryWriter(indexDescriptor, indexIdentifier);
             PostingsWriter postingsWriter = new PostingsWriter(indexDescriptor, indexIdentifier))
        {
            // Terms and postings writers are opened in append mode with pointers at the end of their respective files.
            long termsOffset = termsDictionaryWriter.getStartOffset();
            long postingsOffset = postingsWriter.getStartOffset();

            while (iterator.hasNext())
            {
                IndexEntry indexEntry = iterator.next();
                try (PostingList postings = indexEntry.postingList)
                {
                    if (!prefixEnabled)
                    {
                        long offset = postingsWriter.write(postings);
                        termsDictionaryWriter.add(indexEntry.term, offset);
                        continue;
                    }

                    // V2: the posting list emits exactCount, totalCount, then exact rows followed by prefix rows.
                    int exactCount = (int) postings.nextPosting();
                    int totalCount = (int) postings.nextPosting();
                    int prefixCount = totalCount - exactCount;

                    boolean isTerminal = exactCount > 0;
                    boolean writePrefixSection = prefixCount >= minimumLeaves;

                    if (!isTerminal && !writePrefixSection)
                    {
                        // Pure intermediate node below the prefix threshold: no on-disk entry (descent will reach leaves).
                        drain(postings, totalCount);
                        continue;
                    }

                    int[] exactRows = drainSortedInts(postings, exactCount);

                    int[] prefixRows = null;
                    if (writePrefixSection && prefixCount > 0)
                        prefixRows = drainSortedInts(postings, prefixCount);
                    else
                        drain(postings, prefixCount);

                    PostingList exactPostings = exactCount > 0 ? new IntArrayPostingList(exactRows) : null;
                    PostingList prefixPostings = prefixRows != null ? new IntArrayPostingList(prefixRows) : null;

                    long offset = postingsWriter.writeV2(exactPostings, prefixPostings);
                    termsDictionaryWriter.add(indexEntry.term, offset);
                }
            }
            postingsAdded = postingsWriter.getTotalPostings();
            MutableLong footerPointer = new MutableLong();
            long termsRoot = termsDictionaryWriter.complete(footerPointer);
            postingsWriter.complete();

            long termsLength = termsDictionaryWriter.getFilePointer() - termsOffset;
            long postingsLength = postingsWriter.getFilePointer() - postingsOffset;

            Map<String, String> map = new HashMap<>(2);
            map.put(SAICodecUtils.FOOTER_POINTER, footerPointer.getValue().toString());
            if (prefixEnabled)
                map.put(POSTINGS_FORMAT, POSTINGS_FORMAT_V2);

            // Postings list file pointers are stored directly in TERMS_DATA, so a root is not needed.
            components.put(IndexComponent.POSTING_LISTS, -1, postingsOffset, postingsLength);
            components.put(IndexComponent.TERMS_DATA, termsRoot, termsOffset, termsLength, map);
        }
        return components;
    }

    private static int[] drainSortedInts(PostingList postings, int count) throws IOException
    {
        int[] rows = new int[count];
        for (int i = 0; i < count; i++)
            rows[i] = (int) postings.nextPosting();
        // Rows accumulated at a prefix node may arrive out of order (e.g. the memtable flush path adds in term
        // order). Posting lists must be ascending, so sort before writing. Already-sorted input (the SSTable build
        // path) makes this a no-op.
        Arrays.sort(rows);
        return rows;
    }

    private static void drain(PostingList postings, int count) throws IOException
    {
        for (int i = 0; i < count; i++)
            postings.nextPosting();
    }

    @Override
    public long getNumberOfRows()
    {
        return postingsAdded;
    }
}
