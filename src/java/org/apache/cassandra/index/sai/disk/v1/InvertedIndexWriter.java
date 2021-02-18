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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.mutable.MutableLong;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Builds an on-disk inverted index structure: terms dictionary and postings lists.
 */
@NotThreadSafe
public class InvertedIndexWriter implements Closeable
{
    private final TrieTermsDictionaryWriter termsDictionaryWriter;
    private final PostingsWriter postingsWriter;
    private long postingsAdded;

    public InvertedIndexWriter(IndexComponents indexComponents, boolean segmented) throws IOException
    {
        this.termsDictionaryWriter = new TrieTermsDictionaryWriter(indexComponents, segmented);
        this.postingsWriter = new PostingsWriter(indexComponents, segmented);
    }

    /**
     * Appends a set of terms and associated postings to their respective overall SSTable component files.
     *
     * @param terms an iterator of terms with their associated postings
     *
     * @return metadata describing the location of this inverted index in the overall SSTable
     *         terms and postings component files
     */
    public SegmentMetadata.ComponentMetadataMap writeAll(TermsIterator terms) throws IOException
    {
        // Terms and postings writers are opened in append mode with pointers at the end of their respective files.
        long termsOffset = termsDictionaryWriter.getStartOffset();
        long postingsOffset = postingsWriter.getStartOffset();

        while (terms.hasNext())
        {
            ByteComparable term = terms.next();
            try (PostingList postings = terms.postings())
            {
                final long offset = postingsWriter.write(postings);
                if (offset >= 0)
                    termsDictionaryWriter.add(term, offset);
            }
        }
        postingsAdded = postingsWriter.getTotalPostings();
        MutableLong footerPointer = new MutableLong();
        long termsRoot = termsDictionaryWriter.complete(footerPointer);
        postingsWriter.complete();

        long termsLength = termsDictionaryWriter.getFilePointer() - termsOffset;
        long postingsLength = postingsWriter.getFilePointer() - postingsOffset;

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();

        Map<String,String> map = new HashMap<>(2);
        map.put(SAICodecUtils.FOOTER_POINTER, "" + footerPointer.getValue());

        // Postings list file pointers are stored directly in TERMS_DATA, so a root is not needed.
        components.put(IndexComponents.NDIType.POSTING_LISTS, -1, postingsOffset, postingsLength);
        components.put(IndexComponents.NDIType.TERMS_DATA, termsRoot, termsOffset, termsLength, map);

        return components;
    }

    @Override
    public void close() throws IOException
    {
        postingsWriter.close();
        termsDictionaryWriter.close();
    }

    /**
     * @return total number of row IDs added to posting lists
     */
    public long getPostingsCount()
    {
        return postingsAdded;
    }
}
