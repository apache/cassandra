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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.mutable.MutableLong;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentWriter;
import org.apache.cassandra.index.sai.utils.IndexEntry;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.postings.PostingList;

/**
 * Builds an on-disk inverted index structure: terms dictionary and postings lists.
 */
@NotThreadSafe
public class LiteralIndexWriter implements SegmentWriter
{
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
        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();

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
                    long offset = postingsWriter.write(postings);
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

            // Postings list file pointers are stored directly in TERMS_DATA, so a root is not needed.
            components.put(IndexComponent.POSTING_LISTS, -1, postingsOffset, postingsLength);
            components.put(IndexComponent.TERMS_DATA, termsRoot, termsOffset, termsLength, map);
        }
        return components;
    }

    @Override
    public long getNumberOfRows()
    {
        return postingsAdded;
    }
}
