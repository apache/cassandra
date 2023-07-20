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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.utils.SegmentMemoryLimiter;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.junit.Assert.assertEquals;

public class SortedTermsTrieSearcherTest extends AbstractSortedTermsTester
{
    @Test
    public void singleSegmentTest() throws Exception
    {
        List<byte[]> terms = new ArrayList<>();
        writeTerms(writer ->
        {
            for (int x = 0; x < 4000; x+=2)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(x);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS50);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);
                terms.add(bytes);

                writer.add(ByteComparable.fixedLength(bytes));
            }
        });

        withTrieSearcher(searcher ->
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
        writeTerms(writer ->
        {
            for (int x = 0; x < 4000; x+=2)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(x);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS50);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);
                terms.add(bytes);

                writer.add(ByteComparable.fixedLength(bytes));
            }
        });

        withTrieSearcher(searcher ->
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
        writeTerms(writer ->
        {
            for (int x = 0; x < 4000; x+=2)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(x);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS50);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);
                terms.add(bytes);

                writer.add(ByteComparable.fixedLength(bytes));
            }
        });

        withTrieSearcher(searcher ->
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
}
