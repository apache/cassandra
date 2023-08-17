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
package org.apache.cassandra.index.sai.disk.v1.bbtree;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.Assert;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.segment.NumericIndexSegmentSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.segment.IndexSegmentSearcher;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.memory.MemtableTermsIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TermsIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.AbstractGuavaIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BlockBalancedTreeIndexBuilder
{
    public static final PrimaryKeyMap TEST_PRIMARY_KEY_MAP = new PrimaryKeyMap()
    {
        private final PrimaryKey.Factory primaryKeyFactory = new PrimaryKey.Factory(null);

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


    private static final BigDecimal ONE_TENTH = BigDecimal.valueOf(1, 1);

    private final IndexDescriptor indexDescriptor;
    private final AbstractType<?> type;
    private final AbstractGuavaIterator<Pair<ByteComparable, LongArrayList>> terms;
    private final int size;
    private final int minSegmentRowId;
    private final int maxSegmentRowId;

    public BlockBalancedTreeIndexBuilder(IndexDescriptor indexDescriptor,
                                         AbstractType<?> type,
                                         AbstractGuavaIterator<Pair<ByteComparable, LongArrayList>> terms,
                                         int size,
                                         int minSegmentRowId,
                                         int maxSegmentRowId)
    {
        this.indexDescriptor = indexDescriptor;
        this.type = type;
        this.terms = terms;
        this.size = size;
        this.minSegmentRowId = minSegmentRowId;
        this.maxSegmentRowId = maxSegmentRowId;
    }

    NumericIndexSegmentSearcher flushAndOpen() throws IOException
    {
        final TermsIterator termEnum = new MemtableTermsIterator(null, null, terms);
        final SegmentMetadata metadata;

        IndexContext columnContext = SAITester.createIndexContext("test", Int32Type.instance);
        NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                           columnContext,
                                                           TypeUtil.fixedSizeOf(type),
                                                           maxSegmentRowId);
        final SegmentMetadata.ComponentMetadataMap indexMetas = writer.writeCompleteSegment(BlockBalancedTreeIterator.fromTermsIterator(termEnum, type));
        metadata = new SegmentMetadata(0,
                                       size,
                                       minSegmentRowId,
                                       maxSegmentRowId,
                                       // min/max is unused for now
                                       SAITester.TEST_FACTORY.createTokenOnly(Murmur3Partitioner.instance.decorateKey(UTF8Type.instance.fromString("a")).getToken()),
                                       SAITester.TEST_FACTORY.createTokenOnly(Murmur3Partitioner.instance.decorateKey(UTF8Type.instance.fromString("b")).getToken()),
                                       UTF8Type.instance.fromString("c"),
                                       UTF8Type.instance.fromString("d"),
                                       indexMetas);

        try (PerColumnIndexFiles indexFiles = new PerColumnIndexFiles(indexDescriptor, SAITester.createIndexContext("test", Int32Type.instance)))
        {
            IndexSegmentSearcher searcher = IndexSegmentSearcher.open(TEST_PRIMARY_KEY_MAP_FACTORY, indexFiles, metadata, columnContext);
            assertThat(searcher, is(instanceOf(NumericIndexSegmentSearcher.class)));
            return (NumericIndexSegmentSearcher) searcher;
        }
    }

    /**
     * Returns a k-d tree index where:
     * 1. term values have 32b
     * 2. term value is equal to {@code startTermInclusive} + row id;
     * 3. tokens and offsets are equal to row id;
     */
    public static IndexSegmentSearcher buildInt32Searcher(IndexDescriptor indexDescriptor, int startTermInclusive, int endTermExclusive)
    throws IOException
    {
        final int size = endTermExclusive - startTermInclusive;
        Assert.assertTrue(size > 0);
        BlockBalancedTreeIndexBuilder indexBuilder = new BlockBalancedTreeIndexBuilder(indexDescriptor,
                                                                                       Int32Type.instance,
                                                                                       singleOrd(int32Range(startTermInclusive, endTermExclusive), Int32Type.instance, startTermInclusive, size),
                                                                                       size,
                                                                                       startTermInclusive,
                                                                                       endTermExclusive);
        return indexBuilder.flushAndOpen();
    }

    public static IndexSegmentSearcher buildDecimalSearcher(IndexDescriptor indexDescriptor, BigDecimal startTermInclusive, BigDecimal endTermExclusive)
    throws IOException
    {
        BigDecimal bigDifference = endTermExclusive.subtract(startTermInclusive);
        int size = bigDifference.intValueExact() * 10;
        Assert.assertTrue(size > 0);
        BlockBalancedTreeIndexBuilder indexBuilder = new BlockBalancedTreeIndexBuilder(indexDescriptor,
                                                                                       DecimalType.instance,
                                                                                       singleOrd(decimalRange(startTermInclusive, endTermExclusive), DecimalType.instance, startTermInclusive.intValueExact() * 10, size),
                                                                                       size,
                                                                 startTermInclusive.intValueExact() * 10,
                                                                 endTermExclusive.intValueExact() * 10);
        return indexBuilder.flushAndOpen();
    }

    public static IndexSegmentSearcher buildBigIntegerSearcher(IndexDescriptor indexDescriptor, BigInteger startTermInclusive, BigInteger endTermExclusive)
    throws IOException
    {
        BigInteger bigDifference = endTermExclusive.subtract(startTermInclusive);
        int size = bigDifference.intValueExact();
        Assert.assertTrue(size > 0);
        BlockBalancedTreeIndexBuilder indexBuilder = new BlockBalancedTreeIndexBuilder(indexDescriptor,
                                                                                       IntegerType.instance,
                                                                                       singleOrd(bigIntegerRange(startTermInclusive, endTermExclusive), IntegerType.instance, startTermInclusive.intValueExact(), size),
                                                                                       size,
                                                                                       startTermInclusive.intValueExact(),
                                                                                       endTermExclusive.intValueExact());
        return indexBuilder.flushAndOpen();
    }

    /**
     * Returns a k-d tree index where:
     * 1. term values have 64b
     * 2. term value is equal to {@code startTermInclusive} + row id;
     * 3. tokens and offsets are equal to row id;
     */
    public static IndexSegmentSearcher buildLongSearcher(IndexDescriptor indexDescriptor, long startTermInclusive, long endTermExclusive)
    throws IOException
    {
        final long size = endTermExclusive - startTermInclusive;
        Assert.assertTrue(size > 0);
        BlockBalancedTreeIndexBuilder indexBuilder = new BlockBalancedTreeIndexBuilder(indexDescriptor,
                                                                                       LongType.instance,
                                                                                       singleOrd(longRange(startTermInclusive, endTermExclusive), LongType.instance, Math.toIntExact(startTermInclusive), Math.toIntExact(size)),
                                                                                       Math.toIntExact(size),
                                                                                       Math.toIntExact(startTermInclusive),
                                                                                       Math.toIntExact(endTermExclusive));
        return indexBuilder.flushAndOpen();
    }

    /**
     * Returns a k-d tree index where:
     * 1. term values have 16b
     * 2. term value is equal to {@code startTermInclusive} + row id;
     * 3. tokens and offsets are equal to row id;
     */
    public static IndexSegmentSearcher buildShortSearcher(IndexDescriptor indexDescriptor, short startTermInclusive, short endTermExclusive)
    throws IOException
    {
        final int size = endTermExclusive - startTermInclusive;
        Assert.assertTrue(size > 0);
        BlockBalancedTreeIndexBuilder indexBuilder = new BlockBalancedTreeIndexBuilder(indexDescriptor,
                                                                                       ShortType.instance,
                                                                                       singleOrd(shortRange(startTermInclusive, endTermExclusive), ShortType.instance, startTermInclusive, size),
                                                                                       size,
                                                                                       startTermInclusive,
                                                                                       endTermExclusive);
        return indexBuilder.flushAndOpen();
    }

    /**
     * Returns inverted index where each posting list contains exactly one element equal to the terms ordinal number +
     * given offset.
     */
    public static AbstractGuavaIterator<Pair<ByteComparable, LongArrayList>> singleOrd(Iterator<ByteBuffer> terms, AbstractType<?> type, int segmentRowIdOffset, int size)
    {
        return new AbstractGuavaIterator<Pair<ByteComparable, LongArrayList>>()
        {
            private long currentTerm = 0;
            private int currentSegmentRowId = segmentRowIdOffset;

            @Override
            protected Pair<ByteComparable, LongArrayList> computeNext()
            {
                if (currentTerm++ >= size)
                {
                    return endOfData();
                }

                LongArrayList postings = new LongArrayList();
                postings.add(currentSegmentRowId++);
                assertTrue(terms.hasNext());

                final ByteSource encoded = TypeUtil.asComparableBytes(terms.next(), type, ByteComparable.Version.OSS50);
                return Pair.create(v -> encoded, postings);
            }
        };
    }

    /**
     * Returns sequential ordered encoded ints from {@code startInclusive} (inclusive) to {@code endExclusive}
     * (exclusive) by an incremental step of {@code 1}.
     */
    public static Iterator<ByteBuffer> int32Range(int startInclusive, int endExclusive)
    {
        return IntStream.range(startInclusive, endExclusive)
                        .mapToObj(Int32Type.instance::decompose)
                        .collect(Collectors.toList())
                        .iterator();
    }

    /**
     * Returns sequential ordered encoded longs from {@code startInclusive} (inclusive) to {@code endExclusive}
     * (exclusive) by an incremental step of {@code 1}.
     */
    public static Iterator<ByteBuffer> longRange(long startInclusive, long endExclusive)
    {
        return LongStream.range(startInclusive, endExclusive)
                         .mapToObj(LongType.instance::decompose)
                         .collect(Collectors.toList())
                         .iterator();
    }

    public static Iterator<ByteBuffer> decimalRange(final BigDecimal startInclusive, final BigDecimal endExclusive)
    {
        int n = endExclusive.subtract(startInclusive).intValueExact() * 10;
        final Supplier<BigDecimal> generator = new Supplier<BigDecimal>() {
            BigDecimal current = startInclusive;

            @Override
            public BigDecimal get() {
                BigDecimal result = current;
                current = current.add(ONE_TENTH);
                return result;
            }
        };
        return Stream.generate(generator)
                     .limit(n)
                     .map(bd -> TypeUtil.asIndexBytes(DecimalType.instance.decompose(bd), DecimalType.instance))
                     .collect(Collectors.toList())
                     .iterator();
    }

    public static Iterator<ByteBuffer> bigIntegerRange(final BigInteger startInclusive, final BigInteger endExclusive)
    {
        int n = endExclusive.subtract(startInclusive).intValueExact();
        final Supplier<BigInteger> generator = new Supplier<BigInteger>() {
            BigInteger current = startInclusive;

            @Override
            public BigInteger get() {
                BigInteger result = current;
                current = current.add(BigInteger.ONE);
                return result;
            }
        };
        return Stream.generate(generator)
                     .limit(n)
                     .map(bd -> TypeUtil.asIndexBytes(IntegerType.instance.decompose(bd), IntegerType.instance))
                     .collect(Collectors.toList())
                     .iterator();
    }


    /**
     * Returns sequential ordered encoded shorts from {@code startInclusive} (inclusive) to {@code endExclusive}
     * (exclusive) by an incremental step of {@code 1}.
     */
    public static Iterator<ByteBuffer> shortRange(short startInclusive, short endExclusive)
    {
        return IntStream.range(startInclusive, endExclusive)
                        .mapToObj(i -> ShortType.instance.decompose((short) i))
                        .collect(Collectors.toList())
                        .iterator();
    }
}
