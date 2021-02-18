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
package org.apache.cassandra.index.sai.disk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertTrue;

public class TermsIteratorMergerTest extends SAITester
{
    @Test
    public void testMerger() throws Exception
    {
        final TermsIterator[] iterators = new TermsIterator[getRandom().nextIntBetween(2, 50)];

        TreeMap<String, List<Long>> expected = new TreeMap<>();
        for (int termCount = 0; termCount < getRandom().nextIntBetween(10, 50); termCount++)
        {
            List<Long> postings = new ArrayList<>();
            for (int postingsCount = 0; postingsCount < getRandom().nextIntBetween(200, 500); postingsCount++)
                postings.add(Long.valueOf(getRandom().nextIntBetween(0, 10000)));
            postings.sort(Long::compareTo);
            expected.put(getRandom().nextAsciiString(5, 20), postings);
        }

        String[] expectedTerms = expected.keySet().toArray(new String[] {});
        for (int termIteratorCount = 0; termIteratorCount < iterators.length; termIteratorCount++)
        {
            TreeMap<ByteBuffer, Long[]> termMap = new TreeMap<>();
            for (int termCount = 0; termCount < getRandom().nextIntBetween(2, expected.size() - 1); termCount++)
            {
                String term = expectedTerms[getRandom().nextIntBetween(0, expected.size() - 1)];
                Long[] expectedPostings = expected.get(term).toArray(new Long[] {});
                List<Long> postings = new ArrayList<>();
                for (int postingsCount = 0; postingsCount < getRandom().nextIntBetween(50, 150); postingsCount++)
                {
                    while (true)
                    {
                        long posting = expectedPostings[getRandom().nextIntBetween(0, expectedPostings.length - 1)];
                        if (!postings.contains(posting))
                        {
                            postings.add(posting);
                            break;
                        }
                    }
                }
                postings.sort(Long::compareTo);
                termMap.put(UTF8Type.instance.decompose(term), postings.toArray(new Long[] {}));
                iterators[termIteratorCount] = new TermsIteratorImpl(termMap);
            }
        }

        TermsIteratorMerger merger = new TermsIteratorMerger(iterators, UTF8Type.instance);
        while (merger.hasNext())
        {
            String term = UTF8Type.instance.compose(UTF8Type.instance.fromComparableBytes(merger.next().asPeekableBytes(ByteComparable.Version.OSS41), ByteComparable.Version.OSS41));
            assertTrue(expected.containsKey(term));
            List<Long> expectedPostings = expected.get(term);
            PostingList postings = merger.postings();
            long lastRowId = Long.MIN_VALUE;
            while (true)
            {
                long rowId = postings.nextPosting();
                if (rowId == PostingList.END_OF_STREAM) break;
                assertTrue(rowId > lastRowId);
                lastRowId = rowId;
                assertTrue(expectedPostings.contains(rowId));
            }
        }
    }

    public static class TermsIteratorImpl implements TermsIterator
    {
        final TreeMap<ByteBuffer, Long[]> map;
        final Iterator<Map.Entry<ByteBuffer, Long[]>> iterator;
        Map.Entry<ByteBuffer, Long[]> current;

        public TermsIteratorImpl(TreeMap<ByteBuffer, Long[]> map)
        {
            this.map = map;
            iterator = map.entrySet().iterator();
        }

        @Override
        public PostingList postings() throws IOException
        {

            return new PostingList()
            {
                int index = 0;
                Long[] postings = current.getValue();

                @Override
                public long nextPosting() throws IOException
                {
                    if (index == postings.length)
                        return END_OF_STREAM;
                    return postings[index++];
                }

                @Override
                public long size()
                {
                    return postings.length;
                }

                @Override
                public long advance(long targetRowID) throws IOException
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public ByteBuffer getMinTerm()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer getMaxTerm()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {}

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public ByteComparable next()
        {
            current = iterator.next();
            return version -> UTF8Type.instance.asComparableBytes(current.getKey(), version);
        }
    }
}
