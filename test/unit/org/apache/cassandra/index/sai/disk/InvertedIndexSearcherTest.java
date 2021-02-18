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
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.InvertedIndexWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListeners;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.LongArrays;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.apache.cassandra.Util.dk;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class InvertedIndexSearcherTest extends NdiRandomizedTest
{
    private static final SSTableContext.KeyFetcher KEY_FETCHER = new SSTableContext.KeyFetcher() {
        @Override
        public DecoratedKey apply(RandomAccessReader reader, long keyOffset)
        {
            return dk(String.format("pkvalue_%07d", keyOffset));
        }

        @Override
        public RandomAccessReader createReader()
        {
            return null;
        }
    };

    @BeforeClass
    public static void setupCQLTester()
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void testEqQueriesAgainstStringIndex() throws Exception
    {
        doTestEqQueriesAgainstStringIndex();
    }

    private void doTestEqQueriesAgainstStringIndex() throws Exception
    {
        final int numTerms = randomIntBetween(64, 512), numPostings = randomIntBetween(256, 1024);
        final List<Pair<ByteComparable, IntArrayList>> termsEnum = buildTermsEnum(numTerms, numPostings);

        try (IndexSearcher searcher = buildIndexAndOpenSearcher(numTerms, numPostings, termsEnum))
        {
            for (int t = 0; t < numTerms; ++t)
            {
                try (RangeIterator results = searcher.search(new Expression(SAITester.createColumnContext("meh", UTF8Type.instance))
                        .add(Operator.EQ, wrap(termsEnum.get(t).left)), SSTableQueryContext.forTest(), false))
                {
                    assertEquals(results.getMinimum(), results.getCurrent());
                    assertTrue(results.hasNext());

                    for (int p = 0; p < numPostings; ++p)
                    {
                        final long expectedToken = termsEnum.get(t).right.get(p);
                        assertTrue(results.hasNext());
                        final long actualToken = results.next().get();
                        assertEquals(expectedToken, actualToken);
                    }
                    assertFalse(results.hasNext());
                }

                try (RangeIterator results = searcher.search(new Expression(SAITester.createColumnContext("meh", UTF8Type.instance))
                        .add(Operator.EQ, wrap(termsEnum.get(t).left)), SSTableQueryContext.forTest(), false))
                {
                    assertEquals(results.getMinimum(), results.getCurrent());
                    assertTrue(results.hasNext());

                    // test skipping to the last block
                    final int idxToSkip = numPostings - 7;
                    // tokens are equal to their corresponding row IDs
                    final long tokenToSkip = termsEnum.get(t).right.get(idxToSkip);
                    results.skipTo(tokenToSkip);

                    for (int p = idxToSkip; p < numPostings; ++p)
                    {
                        final long expectedToken = termsEnum.get(t).right.get(p);
                        final long actualToken = results.next().get();
                        assertEquals(expectedToken, actualToken);
                    }
                }
            }

            // try searching for terms that weren't indexed
            final String tooLongTerm = randomSimpleString(10, 12);
            RangeIterator results = searcher.search(new Expression(SAITester.createColumnContext("meh", UTF8Type.instance))
                                                                .add(Operator.EQ, UTF8Type.instance.decompose(tooLongTerm)), SSTableQueryContext.forTest(), false);
            assertFalse(results.hasNext());

            final String tooShortTerm = randomSimpleString(1, 2);
            results = searcher.search(new Expression(SAITester.createColumnContext("meh", UTF8Type.instance))
                                                      .add(Operator.EQ, UTF8Type.instance.decompose(tooShortTerm)), SSTableQueryContext.forTest(), false);
            assertFalse(results.hasNext());
        }
    }

    @Test
    public void testUnsupportedOperator() throws Exception
    {
        final int numTerms = randomIntBetween(5, 15), numPostings = randomIntBetween(5, 20);
        final List<Pair<ByteComparable, IntArrayList>> termsEnum = buildTermsEnum(numTerms, numPostings);

        try (IndexSearcher searcher = buildIndexAndOpenSearcher(numTerms, numPostings, termsEnum))
        {
            searcher.search(new Expression(SAITester.createColumnContext("meh", UTF8Type.instance)).add(Operator.GT, UTF8Type.instance.decompose("a")), SSTableQueryContext.forTest(), false);

            fail("Expect IllegalArgumentException thrown, but didn't");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    private IndexSearcher buildIndexAndOpenSearcher(int terms, int postings, List<Pair<ByteComparable, IntArrayList>> termsEnum) throws IOException
    {
        final int size = terms * postings;
        final IndexComponents indexComponents = newIndexComponents();

        SegmentMetadata.ComponentMetadataMap indexMetas;
        try (InvertedIndexWriter writer = new InvertedIndexWriter(indexComponents, false))
        {
            indexMetas = writer.writeAll(new MemtableTermsIterator(null, null, termsEnum.iterator()));
        }

        final SegmentMetadata segmentMetadata = new SegmentMetadata(0,
                                                                    size,
                                                                    0,
                                                                    Long.MAX_VALUE,
                                                                    new BufferDecoratedKey(DatabaseDescriptor.getPartitioner().getMinimumToken(), ByteBufferUtil.bytes(1)),
                                                                    new BufferDecoratedKey(DatabaseDescriptor.getPartitioner().getMaximumToken(), ByteBufferUtil.bytes(2)),
                                                                    wrap(termsEnum.get(0).left),
                                                                    wrap(termsEnum.get(terms - 1).left),
                                                                    indexMetas);

        try (SSTableIndex.PerIndexFiles indexFiles = new SSTableIndex.PerIndexFiles(indexComponents, true))
        {
            final LongArray.Factory factory = () -> LongArrays.identity().build();
            Segment segment = new Segment(factory, factory, KEY_FETCHER, indexFiles, segmentMetadata, UTF8Type.instance);
            final IndexSearcher searcher = IndexSearcher.open(segment, QueryEventListeners.NO_OP_TRIE_LISTENER);
            assertThat(searcher, is(instanceOf(InvertedIndexSearcher.class)));
            return searcher;
        }
    }

    private List<Pair<ByteComparable, IntArrayList>> buildTermsEnum(int terms, int postings)
    {
        return InvertedIndexBuilder.buildStringTermsEnum(terms, postings, () -> randomSimpleString(3, 5), () -> nextInt(0, Integer.MAX_VALUE));
    }

    private ByteBuffer wrap(ByteComparable bc)
    {
        return ByteBuffer.wrap(ByteSourceInverse.readBytes(bc.asComparableBytes(ByteComparable.Version.OSS41)));
    }
}
