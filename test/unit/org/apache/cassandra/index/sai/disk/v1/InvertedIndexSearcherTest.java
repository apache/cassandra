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
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.memory.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.segment.IndexSegmentSearcher;
import org.apache.cassandra.index.sai.disk.v1.segment.LiteralIndexSegmentSearcher;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.trie.LiteralIndexWriter;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class InvertedIndexSearcherTest extends SAIRandomizedTester
{
    public static final PrimaryKeyMap TEST_PRIMARY_KEY_MAP = new PrimaryKeyMap()
    {
        private final PrimaryKey.Factory primaryKeyFactory = new PrimaryKey.Factory(new ClusteringComparator());

        @Override
        public PrimaryKey primaryKeyFromRowId(long sstableRowId)
        {
            return primaryKeyFactory.createTokenOnly(new Murmur3Partitioner.LongToken(sstableRowId));
        }

        @Override
        public long rowIdFromPrimaryKey(PrimaryKey key)
        {
            return key.token().getLongValue();
        }

        @Override
        public long count()
        {
            return Long.MAX_VALUE;
        }
    };
    public static final PrimaryKeyMap.Factory TEST_PRIMARY_KEY_MAP_FACTORY = () -> TEST_PRIMARY_KEY_MAP;

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
        QueryContext context = mock(QueryContext.class);
        final int numTerms = getRandom().nextIntBetween(64, 512), numPostings = getRandom().nextIntBetween(256, 1024);
        final List<Pair<ByteComparable, LongArrayList>> termsEnum = buildTermsEnum(numTerms, numPostings);

        try (IndexSegmentSearcher searcher = buildIndexAndOpenSearcher(numTerms, numPostings, termsEnum))
        {
            for (int t = 0; t < numTerms; ++t)
            {
                try (KeyRangeIterator results = searcher.search(new Expression(SAITester.createIndexContext("meh", UTF8Type.instance))
                        .add(Operator.EQ, wrap(termsEnum.get(t).left)), context))
                {
                    assertEquals(results.getMinimum(), results.getCurrent());
                    assertTrue(results.hasNext());

                    for (int p = 0; p < numPostings; ++p)
                    {
                        final long expectedToken = termsEnum.get(t).right.get(p);
                        assertTrue(results.hasNext());
                        final long actualToken = results.next().token().getLongValue();
                        assertEquals(expectedToken, actualToken);
                    }
                    assertFalse(results.hasNext());
                }

                try (KeyRangeIterator results = searcher.search(new Expression(SAITester.createIndexContext("meh", UTF8Type.instance))
                        .add(Operator.EQ, wrap(termsEnum.get(t).left)), context))
                {
                    assertEquals(results.getMinimum(), results.getCurrent());
                    assertTrue(results.hasNext());

                    // test skipping to the last block
                    final int idxToSkip = numPostings - 7;
                    // tokens are equal to their corresponding row IDs
                    final long tokenToSkip = termsEnum.get(t).right.get(idxToSkip);
                    results.skipTo(SAITester.TEST_FACTORY.createTokenOnly(new Murmur3Partitioner.LongToken(tokenToSkip)));

                    for (int p = idxToSkip; p < numPostings; ++p)
                    {
                        final long expectedToken = termsEnum.get(t).right.get(p);
                        final long actualToken = results.next().token().getLongValue();
                        assertEquals(expectedToken, actualToken);
                    }
                }
            }

            // try searching for terms that weren't indexed
            final String tooLongTerm = randomSimpleString(10, 12);
            KeyRangeIterator results = searcher.search(new Expression(SAITester.createIndexContext("meh", UTF8Type.instance))
                                                                .add(Operator.EQ, UTF8Type.instance.decompose(tooLongTerm)), context);
            assertFalse(results.hasNext());

            final String tooShortTerm = randomSimpleString(1, 2);
            results = searcher.search(new Expression(SAITester.createIndexContext("meh", UTF8Type.instance))
                                                      .add(Operator.EQ, UTF8Type.instance.decompose(tooShortTerm)), context);
            assertFalse(results.hasNext());
        }
    }

    @Test
    public void testUnsupportedOperator() throws Exception
    {
        QueryContext context = mock(QueryContext.class);

        final int numTerms = getRandom().nextIntBetween(5, 15), numPostings = getRandom().nextIntBetween(5, 20);
        final List<Pair<ByteComparable, LongArrayList>> termsEnum = buildTermsEnum(numTerms, numPostings);

        try (IndexSegmentSearcher searcher = buildIndexAndOpenSearcher(numTerms, numPostings, termsEnum))
        {
            searcher.search(new Expression(SAITester.createIndexContext("meh", UTF8Type.instance))
                            .add(Operator.GT, UTF8Type.instance.decompose("a")), context);

            fail("Expect IllegalArgumentException thrown, but didn't");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    private IndexSegmentSearcher buildIndexAndOpenSearcher(int terms, int postings, List<Pair<ByteComparable, LongArrayList>> termsEnum) throws IOException
    {
        final int size = terms * postings;
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, UTF8Type.instance);

        SegmentMetadata.ComponentMetadataMap indexMetas;
        try (LiteralIndexWriter writer = new LiteralIndexWriter(indexDescriptor, indexContext))
        {
            indexMetas = writer.writeCompleteSegment(new MemtableTermsIterator(null, null, termsEnum.iterator()));
        }

        final SegmentMetadata segmentMetadata = new SegmentMetadata(0,
                                                                    size,
                                                                    0,
                                                                    Long.MAX_VALUE,
                                                                    SAITester.TEST_FACTORY.createTokenOnly(DatabaseDescriptor.getPartitioner().getMinimumToken()),
                                                                    SAITester.TEST_FACTORY.createTokenOnly(DatabaseDescriptor.getPartitioner().getMaximumToken()),
                                                                    wrap(termsEnum.get(0).left),
                                                                    wrap(termsEnum.get(terms - 1).left),
                                                                    indexMetas);

        try (PerColumnIndexFiles indexFiles = new PerColumnIndexFiles(indexDescriptor, indexContext))
        {
            final IndexSegmentSearcher searcher = IndexSegmentSearcher.open(TEST_PRIMARY_KEY_MAP_FACTORY,
                                                                            indexFiles,
                                                                            segmentMetadata,
                                                                            SAITester.createIndexContext(index, UTF8Type.instance));
            assertThat(searcher, is(instanceOf(LiteralIndexSegmentSearcher.class)));
            return searcher;
        }
    }

    private List<Pair<ByteComparable, LongArrayList>> buildTermsEnum(int terms, int postings)
    {
        return InvertedIndexBuilder.buildStringTermsEnum(terms, postings, () -> randomSimpleString(3, 5), () -> nextInt(0, Integer.MAX_VALUE));
    }

    private ByteBuffer wrap(ByteComparable bc)
    {
        return ByteBuffer.wrap(ByteSourceInverse.readBytes(bc.asComparableBytes(ByteComparable.Version.OSS50)));
    }
}
