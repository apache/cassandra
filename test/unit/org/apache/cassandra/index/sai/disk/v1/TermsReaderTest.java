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

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.apache.cassandra.index.sai.disk.InvertedIndexBuilder.buildStringTermsEnum;
import static org.apache.cassandra.index.sai.metrics.QueryEventListeners.NO_OP_TRIE_LISTENER;

public class TermsReaderTest extends NdiRandomizedTest
{
    @Test
    public void testTermQueriesAgainstShortPostingLists() throws IOException
    {
        testTermQueries(randomIntBetween(5, 10), randomIntBetween(5, 10));
    }

    @Test
    public void testTermQueriesAgainstLongPostingLists() throws  IOException
    {
        testTermQueries(randomIntBetween(512, 1024), randomIntBetween(1024, 2048));
    }

    @Test
    public void testTermsIteration() throws IOException
    {
        doTestTermsIteration();
    }

    private void doTestTermsIteration() throws IOException
    {
        final int terms = 70, postings = 2;
        final IndexComponents indexComponents = newIndexComponents();
        final List<Pair<ByteComparable, IntArrayList>> termsEnum = buildTermsEnum(terms, postings);

        SegmentMetadata.ComponentMetadataMap indexMetas;
        try (InvertedIndexWriter writer = new InvertedIndexWriter(indexComponents, false))
        {
            indexMetas = writer.writeAll(new MemtableTermsIterator(null, null, termsEnum.iterator()));
        }

        FileHandle termsData = indexComponents.createFileHandle(indexComponents.termsData);
        FileHandle postingLists = indexComponents.createFileHandle(indexComponents.postingLists);

        long termsFooterPointer = Long.parseLong(indexMetas.get(IndexComponents.NDIType.TERMS_DATA).attributes.get(SAICodecUtils.FOOTER_POINTER));

        try (TermsReader reader = new TermsReader(indexComponents, termsData, postingLists,
                                                  indexMetas.get(indexComponents.termsData.ndiType).root, termsFooterPointer))
        {
            try (TermsIterator actualTermsEnum = reader.allTerms(0, NO_OP_TRIE_LISTENER))
            {
                int i = 0;
                for (ByteComparable term = actualTermsEnum.next(); term != null; term = actualTermsEnum.next())
                {
                    final ByteComparable expected = termsEnum.get(i++).left;
                    assertEquals(0, ByteComparable.compare(expected, term, ByteComparable.Version.OSS41));
                }
            }
        }
    }

    private void testTermQueries(int numTerms, int numPostings) throws IOException
    {
        final IndexComponents indexComponents = newIndexComponents();
        final List<Pair<ByteComparable, IntArrayList>> termsEnum = buildTermsEnum(numTerms, numPostings);

        SegmentMetadata.ComponentMetadataMap indexMetas;
        try (InvertedIndexWriter writer = new InvertedIndexWriter(indexComponents, false))
        {
            indexMetas = writer.writeAll(new MemtableTermsIterator(null, null, termsEnum.iterator()));
        }

        FileHandle termsData = indexComponents.createFileHandle(indexComponents.termsData);
        FileHandle postingLists = indexComponents.createFileHandle(indexComponents.postingLists);

        long termsFooterPointer = Long.parseLong(indexMetas.get(IndexComponents.NDIType.TERMS_DATA).attributes.get(SAICodecUtils.FOOTER_POINTER));

        try (TermsReader reader = new TermsReader(indexComponents, termsData, postingLists,
                                                  indexMetas.get(indexComponents.termsData.ndiType).root, termsFooterPointer))
        {
            for (Pair<ByteComparable, IntArrayList> pair : termsEnum)
            {
                final byte[] bytes = ByteSourceInverse.readBytes(pair.left.asComparableBytes(ByteComparable.Version.OSS41));
                try (PostingList actualPostingList = reader.exactMatch(ByteComparable.fixedLength(bytes), NO_OP_TRIE_LISTENER, new QueryContext()))
                {
                    final IntArrayList expectedPostingList = pair.right;

                    assertNotNull(actualPostingList);
                    assertEquals(expectedPostingList.size(), actualPostingList.size());

                    for (int i = 0; i < expectedPostingList.size(); ++i)
                    {
                        final long expectedRowID = expectedPostingList.get(i);
                        long result = actualPostingList.nextPosting();
                        assertEquals(expectedRowID, result);
                    }

                    long lastResult = actualPostingList.nextPosting();
                    assertEquals(PostingList.END_OF_STREAM, lastResult);
                }

                // test skipping
                try (PostingList actualPostingList = reader.exactMatch(ByteComparable.fixedLength(bytes), NO_OP_TRIE_LISTENER, new QueryContext()))
                {
                    final IntArrayList expectedPostingList = pair.right;
                    // test skipping to the last block
                    final int idxToSkip = numPostings - 2;
                    // tokens are equal to their corresponding row IDs
                    final int tokenToSkip = expectedPostingList.get(idxToSkip);

                    long advanceResult = actualPostingList.advance(tokenToSkip);
                    assertEquals(tokenToSkip, advanceResult);

                    for (int i = idxToSkip + 1; i < expectedPostingList.size(); ++i)
                    {
                        final long expectedRowID = expectedPostingList.get(i);
                        long result = actualPostingList.nextPosting();
                        assertEquals(expectedRowID, result);
                    }

                    long lastResult = actualPostingList.nextPosting();
                    assertEquals(PostingList.END_OF_STREAM, lastResult);
                }
            }
        }
    }

    private List<Pair<ByteComparable, IntArrayList>> buildTermsEnum(int terms, int postings)
    {
        return buildStringTermsEnum(terms, postings, () -> randomSimpleString(4, 10), () -> nextInt(0, Integer.MAX_VALUE));
    }
}
