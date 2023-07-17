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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.index.sai.utils.SegmentMemoryLimiter;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.junit.Assert.assertEquals;

public class SortedTermsTrieSearcherTest extends SAIRandomizedTester
{
    private IndexDescriptor indexDescriptor;

    @Before
    public void setup() throws Exception
    {
        indexDescriptor = newIndexDescriptor();
    }

    @After
    public void reset()
    {
        SegmentMemoryLimiter.reset();
    }

    @Test
    public void singleSegmentTest() throws Exception
    {
        List<byte[]> terms = new ArrayList<>();
        writeTerms(indexDescriptor, terms);

        withTrieSearcher(indexDescriptor, searcher ->
        {
            for (long index = 0; index < terms.size(); index++)
            {
                byte[] term = terms.get((int)index);
                long result = searcher.prefixSearch(v -> ByteSource.fixedLength(term));
                assertEquals(index, result);
            }
        });
    }

    @Test
    public void multiSegmentTest() throws Exception
    {
        SegmentMemoryLimiter.setLimitBytes(10);

        List<byte[]> terms = new ArrayList<>();
        writeTerms(indexDescriptor, terms);

        withTrieSearcher(indexDescriptor, searcher ->
        {
            for (long index = 0; index < terms.size(); index++)
            {
                byte[] term = terms.get((int)index);
                long result = searcher.prefixSearch(v -> ByteSource.fixedLength(term));
                assertEquals(index, result);
            }
        });
    }

    // This tests that if we look for a value that doesn't exist in the trie we get the next value returned
    @Test
    public void missingValueTest() throws Exception
    {
        List<byte[]> terms = new ArrayList<>();
        writeTerms(indexDescriptor, terms);

        withTrieSearcher(indexDescriptor, searcher ->
        {
            for (long index = 0; index < terms.size() - 1; index++)
            {
                ByteBuffer buffer = Int32Type.instance.decompose((int) index * 2 + 1);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS50);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);
                long result = searcher.prefixSearch(v -> ByteSource.fixedLength(bytes));
                assertEquals(index + 1, result);
            }
        });
    }

    private void writeTerms(IndexDescriptor indexDescriptor, List<byte[]> terms) throws IOException
    {
        try (MetadataWriter metadataWriter = new MetadataWriter(indexDescriptor.openPerSSTableOutput(IndexComponent.GROUP_META)))
        {
            IndexOutputWriter trieWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEY_TRIE);
            IndexOutputWriter bytesWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEY_BLOCKS);
            NumericValuesWriter blockFPWriter = new NumericValuesWriter(indexDescriptor.componentName(IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS),
                                                                        indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS),
                                                                        metadataWriter, true);
            try (SortedTermsWriter writer = new SortedTermsWriter(indexDescriptor.componentName(IndexComponent.PRIMARY_KEY_BLOCKS),
                                                                  metadataWriter,
                                                                  bytesWriter,
                                                                  blockFPWriter,
                                                                  trieWriter))
            {
                for (int x = 0; x < 4000; x+=2)
                {
                    ByteBuffer buffer = Int32Type.instance.decompose(x);
                    ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS50);
                    byte[] bytes = ByteSourceInverse.readBytes(byteSource);
                    terms.add(bytes);

                    writer.add(ByteComparable.fixedLength(bytes));
                }
            }
        }
    }

    @FunctionalInterface
    public interface ThrowingConsumer<T> {
        void accept(T t) throws IOException;
    }

    private void withTrieSearcher(IndexDescriptor indexDescriptor, ThrowingConsumer<SortedTermsTrieSearcher> testCode) throws IOException
    {
        MetadataSource metadataSource = MetadataSource.loadGroupMetadata(indexDescriptor);
        SortedTermsMeta sortedTermsMeta = new SortedTermsMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.PRIMARY_KEY_BLOCKS)));
        try (FileHandle trieData = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_TRIE))
        {
            SortedTermsTrieSearcher searcher = new SortedTermsTrieSearcher(trieData.instantiateRebufferer(null), sortedTermsMeta);
            testCode.accept(searcher);
        }
    }
}
