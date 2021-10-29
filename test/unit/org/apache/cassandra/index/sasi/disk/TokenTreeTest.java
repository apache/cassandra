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
package org.apache.cassandra.index.sasi.disk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder.EntryType;
import org.apache.cassandra.index.sasi.utils.CombinedTerm;
import org.apache.cassandra.index.sasi.utils.CombinedValue;
import org.apache.cassandra.index.sasi.utils.MappedBuffer;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;

import org.junit.Assert;

import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.google.common.base.Function;

public class TokenTreeTest
{
    private static final Function<Long, DecoratedKey> KEY_CONVERTER = new KeyConverter();

    static LongSet singleOffset = new LongHashSet();
    static LongSet bigSingleOffset = new LongHashSet();
    static LongSet shortPackableCollision = new LongHashSet();
    static LongSet intPackableCollision = new LongHashSet();
    static LongSet multiCollision =  new LongHashSet();
    static LongSet unpackableCollision = new LongHashSet();

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
        singleOffset.add(1);
        bigSingleOffset.add(2147521562L);
        shortPackableCollision.add(2L); shortPackableCollision.add(3L);
        intPackableCollision.add(6L); intPackableCollision.add(((long) Short.MAX_VALUE) + 1);
        multiCollision.add(3L); multiCollision.add(4L); multiCollision.add(5L);
        unpackableCollision.add(((long) Short.MAX_VALUE) + 1); unpackableCollision.add(((long) Short.MAX_VALUE) + 2);
    }



    final static SortedMap<Long, LongSet> simpleTokenMap = new TreeMap<Long, LongSet>()
    {{
            put(1L, bigSingleOffset); put(3L, shortPackableCollision); put(4L, intPackableCollision); put(6L, singleOffset);
            put(9L, multiCollision); put(10L, unpackableCollision); put(12L, singleOffset); put(13L, singleOffset);
            put(15L, singleOffset); put(16L, singleOffset); put(20L, singleOffset); put(22L, singleOffset);
            put(25L, singleOffset); put(26L, singleOffset); put(27L, singleOffset); put(28L, singleOffset);
            put(40L, singleOffset); put(50L, singleOffset); put(100L, singleOffset); put(101L, singleOffset);
            put(102L, singleOffset); put(103L, singleOffset); put(108L, singleOffset); put(110L, singleOffset);
            put(112L, singleOffset); put(115L, singleOffset); put(116L, singleOffset); put(120L, singleOffset);
            put(121L, singleOffset); put(122L, singleOffset); put(123L, singleOffset); put(125L, singleOffset);
    }};

    final static SortedMap<Long, LongSet> bigTokensMap = new TreeMap<Long, LongSet>()
    {{
            for (long i = 0; i < 1000000; i++)
                put(i, singleOffset);
    }};

    @FunctionalInterface
    private static interface CheckedConsumer<C> {
        public void accept(C c) throws Exception;
    }

    final static List<SortedMap<Long, LongSet>> tokenMaps = Arrays.asList(simpleTokenMap, bigTokensMap);
    private void forAllTokenMaps(CheckedConsumer<SortedMap<Long, LongSet>> c) throws Exception {
        for (SortedMap<Long, LongSet> tokens : tokenMaps)
            c.accept(tokens);
    }

    final static SequentialWriterOption DEFAULT_OPT = SequentialWriterOption.newBuilder().bufferSize(4096).build();

    @Test
    public void testSerializedSizeDynamic() throws Exception
    {
        forAllTokenMaps(tokens -> testSerializedSize(new DynamicTokenTreeBuilder(tokens)));
    }

    @Test
    public void testSerializedSizeStatic() throws Exception
    {
        forAllTokenMaps(tokens -> testSerializedSize(new StaticTokenTreeBuilder(new FakeCombinedTerm(tokens))));
    }


    public void testSerializedSize(final TokenTreeBuilder builder) throws Exception
    {
        builder.finish();
        final File treeFile = FileUtils.createTempFile("token-tree-size-test", "tt");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.write(writer);
            writer.sync();
        }

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        Assert.assertEquals((int) reader.bytesRemaining(), builder.serializedSize());
        reader.close();
    }

    @Test
    public void buildSerializeAndIterateDynamic() throws Exception
    {
        forAllTokenMaps(tokens -> buildSerializeAndIterate(new DynamicTokenTreeBuilder(tokens), tokens));
    }

    @Test
    public void buildSerializeAndIterateStatic() throws Exception
    {
        forAllTokenMaps(tokens ->
                        buildSerializeAndIterate(new StaticTokenTreeBuilder(new FakeCombinedTerm(tokens)), tokens));
    }


    public void buildSerializeAndIterate(TokenTreeBuilder builder, SortedMap<Long, LongSet> tokenMap) throws Exception
    {

        builder.finish();
        final File treeFile = FileUtils.createTempFile("token-tree-iterate-test1", "tt");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.write(writer);
            writer.sync();
        }

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        final TokenTree tokenTree = new TokenTree(new MappedBuffer(reader));

        final Iterator<Token> tokenIterator = tokenTree.iterator(KEY_CONVERTER);
        final Iterator<Map.Entry<Long, LongSet>> listIterator = tokenMap.entrySet().iterator();
        while (tokenIterator.hasNext() && listIterator.hasNext())
        {
            Token treeNext = tokenIterator.next();
            Map.Entry<Long, LongSet> listNext = listIterator.next();

            Assert.assertEquals(listNext.getKey(), treeNext.get());
            Assert.assertEquals(convert(listNext.getValue()), convert(treeNext));
        }

        Assert.assertFalse("token iterator not finished", tokenIterator.hasNext());
        Assert.assertFalse("list iterator not finished", listIterator.hasNext());

        reader.close();
    }

    @Test
    public void buildSerializeAndGetDynamic() throws Exception
    {
        buildSerializeAndGet(false);
    }

    @Test
    public void buildSerializeAndGetStatic() throws Exception
    {
        buildSerializeAndGet(true);
    }

    public void buildSerializeAndGet(boolean isStatic) throws Exception
    {
        final long tokMin = 0;
        final long tokMax = 1000;

        final TokenTree tokenTree = generateTree(tokMin, tokMax, isStatic);

        for (long i = 0; i <= tokMax; i++)
        {
            TokenTree.OnDiskToken result = tokenTree.get(i, KEY_CONVERTER);
            Assert.assertNotNull("failed to find object for token " + i, result);

            LongSet found = result.getOffsets();
            Assert.assertEquals(1, found.size());
            Assert.assertEquals(i, found.toArray()[0]);
        }

        Assert.assertNull("found missing object", tokenTree.get(tokMax + 10, KEY_CONVERTER));
    }

    @Test
    public void buildSerializeIterateAndSkipDynamic() throws Exception
    {
        forAllTokenMaps(tokens -> buildSerializeIterateAndSkip(new DynamicTokenTreeBuilder(tokens), tokens));
    }

    @Test
    public void buildSerializeIterateAndSkipStatic() throws Exception
    {
        forAllTokenMaps(tokens ->
                        buildSerializeIterateAndSkip(new StaticTokenTreeBuilder(new FakeCombinedTerm(tokens)), tokens));
    }

    // works with maps other than bigTokensMap but skips to a rather large token
    // so likely for maps other than bigTokensMap skipping is not tested by this.
    public void buildSerializeIterateAndSkip(TokenTreeBuilder builder, SortedMap<Long, LongSet> tokens) throws Exception
    {
        builder.finish();
        final File treeFile = FileUtils.createTempFile("token-tree-iterate-test2", "tt");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.write(writer);
            writer.sync();
        }

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        final TokenTree tokenTree = new TokenTree(new MappedBuffer(reader));

        final RangeIterator<Long, Token> treeIterator = tokenTree.iterator(KEY_CONVERTER);
        final RangeIterator<Long, TokenWithOffsets> listIterator = new EntrySetSkippableIterator(tokens);

        long lastToken = 0L;
        while (treeIterator.hasNext() && lastToken < 12)
        {
            Token treeNext = treeIterator.next();
            TokenWithOffsets listNext = listIterator.next();

            Assert.assertEquals(listNext.token, (lastToken = treeNext.get()));
            Assert.assertEquals(convert(listNext.offsets), convert(treeNext));
        }

        treeIterator.skipTo(100548L);
        listIterator.skipTo(100548L);

        while (treeIterator.hasNext() && listIterator.hasNext())
        {
            Token treeNext = treeIterator.next();
            TokenWithOffsets listNext = listIterator.next();

            Assert.assertEquals(listNext.token, (long) treeNext.get());
            Assert.assertEquals(convert(listNext.offsets), convert(treeNext));

        }

        Assert.assertFalse("Tree iterator not completed", treeIterator.hasNext());
        Assert.assertFalse("List iterator not completed", listIterator.hasNext());

        reader.close();
    }

    @Test
    public void skipPastEndDynamic() throws Exception
    {
        skipPastEnd(new DynamicTokenTreeBuilder(simpleTokenMap), simpleTokenMap);
    }

    @Test
    public void skipPastEndStatic() throws Exception
    {
        skipPastEnd(new StaticTokenTreeBuilder(new FakeCombinedTerm(simpleTokenMap)), simpleTokenMap);
    }

    public void skipPastEnd(TokenTreeBuilder builder, SortedMap<Long, LongSet> tokens) throws Exception
    {
        builder.finish();
        final File treeFile = FileUtils.createTempFile("token-tree-skip-past-test", "tt");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.write(writer);
            writer.sync();
        }

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        final RangeIterator<Long, Token> tokenTree = new TokenTree(new MappedBuffer(reader)).iterator(KEY_CONVERTER);

        tokenTree.skipTo(tokens.lastKey() + 10);
    }

    @Test
    public void testTokenMergeDyanmic() throws Exception
    {
        testTokenMerge(false);
    }

    @Test
    public void testTokenMergeStatic() throws Exception
    {
        testTokenMerge(true);
    }

    public void testTokenMerge(boolean isStatic) throws Exception
    {
        final long min = 0, max = 1000;

        // two different trees with the same offsets
        TokenTree treeA = generateTree(min, max, isStatic);
        TokenTree treeB = generateTree(min, max, isStatic);

        RangeIterator<Long, Token> a = treeA.iterator(new KeyConverter());
        RangeIterator<Long, Token> b = treeB.iterator(new KeyConverter());

        long count = min;
        while (a.hasNext() && b.hasNext())
        {
            final Token tokenA = a.next();
            final Token tokenB = b.next();

            // merging of two OnDiskToken
            tokenA.merge(tokenB);
            // merging with RAM Token with different offset
            tokenA.merge(new TokenWithOffsets(tokenA.get(), convert(count + 1)));
            // and RAM token with the same offset
            tokenA.merge(new TokenWithOffsets(tokenA.get(), convert(count)));

            // should fail when trying to merge different tokens
            try
            {
                tokenA.merge(new TokenWithOffsets(tokenA.get() + 1, convert(count)));
                Assert.fail();
            }
            catch (IllegalArgumentException e)
            {
                // expected
            }

            final Set<Long> offsets = new TreeSet<>();
            for (DecoratedKey key : tokenA)
                 offsets.add(LongType.instance.compose(key.getKey()));

            Set<Long> expected = new TreeSet<>();
            {
                expected.add(count);
                expected.add(count + 1);
            }

            Assert.assertEquals(expected, offsets);
            count++;
        }

        Assert.assertEquals(max, count - 1);
    }

    @Test
    public void testEntryTypeOrdinalLookup()
    {
        Assert.assertEquals(EntryType.SIMPLE, EntryType.of(EntryType.SIMPLE.ordinal()));
        Assert.assertEquals(EntryType.PACKED, EntryType.of(EntryType.PACKED.ordinal()));
        Assert.assertEquals(EntryType.FACTORED, EntryType.of(EntryType.FACTORED.ordinal()));
        Assert.assertEquals(EntryType.OVERFLOW, EntryType.of(EntryType.OVERFLOW.ordinal()));
    }

    @Test
    public void testMergingOfEqualTokenTrees() throws Exception
    {
        testMergingOfEqualTokenTrees(simpleTokenMap);
        testMergingOfEqualTokenTrees(bigTokensMap);
    }

    public void testMergingOfEqualTokenTrees(SortedMap<Long, LongSet> tokensMap) throws Exception
    {
        TokenTreeBuilder tokensA = new DynamicTokenTreeBuilder(tokensMap);
        TokenTreeBuilder tokensB = new DynamicTokenTreeBuilder(tokensMap);

        TokenTree a = buildTree(tokensA);
        TokenTree b = buildTree(tokensB);

        TokenTreeBuilder tokensC = new StaticTokenTreeBuilder(new CombinedTerm(null, null)
        {
            public RangeIterator<Long, Token> getTokenIterator()
            {
                RangeIterator.Builder<Long, Token> union = RangeUnionIterator.builder();
                union.add(a.iterator(new KeyConverter()));
                union.add(b.iterator(new KeyConverter()));

                return union.build();
            }
        });

        TokenTree c = buildTree(tokensC);
        Assert.assertEquals(tokensMap.size(), c.getCount());

        Iterator<Token> tokenIterator = c.iterator(KEY_CONVERTER);
        Iterator<Map.Entry<Long, LongSet>> listIterator = tokensMap.entrySet().iterator();
        while (tokenIterator.hasNext() && listIterator.hasNext())
        {
            Token treeNext = tokenIterator.next();
            Map.Entry<Long, LongSet> listNext = listIterator.next();

            Assert.assertEquals(listNext.getKey(), treeNext.get());
            Assert.assertEquals(convert(listNext.getValue()), convert(treeNext));
        }

        for (Map.Entry<Long, LongSet> entry : tokensMap.entrySet())
        {
            TokenTree.OnDiskToken result = c.get(entry.getKey(), KEY_CONVERTER);
            Assert.assertNotNull("failed to find object for token " + entry.getKey(), result);

            LongSet found = result.getOffsets();
            Assert.assertEquals(entry.getValue(), found);
        }
    }


    private static TokenTree buildTree(TokenTreeBuilder builder) throws Exception
    {
        builder.finish();
        final File treeFile = FileUtils.createTempFile("token-tree-", "db");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.write(writer);
            writer.sync();
        }

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        return new TokenTree(new MappedBuffer(reader));
    }

    private static class EntrySetSkippableIterator extends RangeIterator<Long, TokenWithOffsets>
    {
        private final PeekingIterator<Map.Entry<Long, LongSet>> elements;

        EntrySetSkippableIterator(SortedMap<Long, LongSet> elms)
        {
            super(elms.firstKey(), elms.lastKey(), elms.size());
            elements = Iterators.peekingIterator(elms.entrySet().iterator());
        }

        @Override
        public TokenWithOffsets computeNext()
        {
            if (!elements.hasNext())
                return endOfData();

            Map.Entry<Long, LongSet> next = elements.next();
            return new TokenWithOffsets(next.getKey(), next.getValue());
        }

        @Override
        protected void performSkipTo(Long nextToken)
        {
            while (elements.hasNext())
            {
                if (Long.compare(elements.peek().getKey(), nextToken) >= 0)
                {
                    break;
                }

                elements.next();
            }
        }

        @Override
        public void close() throws IOException
        {
            // nothing to do here
        }
    }

    public static class FakeCombinedTerm extends CombinedTerm
    {
        private final SortedMap<Long, LongSet> tokens;

        public FakeCombinedTerm(SortedMap<Long, LongSet> tokens)
        {
            super(null, null);
            this.tokens = tokens;
        }

        public RangeIterator<Long, Token> getTokenIterator()
        {
            return new TokenMapIterator(tokens);
        }
    }

    public static class TokenMapIterator extends RangeIterator<Long, Token>
    {
        public final Iterator<Map.Entry<Long, LongSet>> iterator;

        public TokenMapIterator(SortedMap<Long, LongSet> tokens)
        {
            super(tokens.firstKey(), tokens.lastKey(), tokens.size());
            iterator = tokens.entrySet().iterator();
        }

        public Token computeNext()
        {
            if (!iterator.hasNext())
                return endOfData();

            Map.Entry<Long, LongSet> entry = iterator.next();
            return new TokenWithOffsets(entry.getKey(), entry.getValue());
        }

        public void close() throws IOException
        {

        }

        public void performSkipTo(Long next)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class TokenWithOffsets extends Token
    {
        private final LongSet offsets;

        public TokenWithOffsets(long token, final LongSet offsets)
        {
            super(token);
            this.offsets = offsets;
        }

        @Override
        public LongSet getOffsets()
        {
            return offsets;
        }

        @Override
        public void merge(CombinedValue<Long> other)
        {}

        @Override
        public int compareTo(CombinedValue<Long> o)
        {
            return Long.compare(token, o.get());
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof TokenWithOffsets))
                return false;

            TokenWithOffsets o = (TokenWithOffsets) other;
            return token == o.token && offsets.equals(o.offsets);
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder().append(token).build();
        }

        @Override
        public String toString()
        {
            return String.format("TokenValue(token: %d, offsets: %s)", token, offsets);
        }

        @Override
        public Iterator<DecoratedKey> iterator()
        {
            List<DecoratedKey> keys = new ArrayList<>(offsets.size());
            for (LongCursor offset : offsets)
                 keys.add(dk(offset.value));

            return keys.iterator();
        }
    }

    private static Set<DecoratedKey> convert(LongSet offsets)
    {
        Set<DecoratedKey> keys = new HashSet<>();
        for (LongCursor offset : offsets)
            keys.add(KEY_CONVERTER.apply(offset.value));

        return keys;
    }

    private static Set<DecoratedKey> convert(Token results)
    {
        Set<DecoratedKey> keys = new HashSet<>();
        for (DecoratedKey key : results)
            keys.add(key);

        return keys;
    }

    private static LongSet convert(long... values)
    {
        LongSet result = new LongHashSet(values.length);
        for (long v : values)
            result.add(v);

        return result;
    }

    private static class KeyConverter implements Function<Long, DecoratedKey>
    {
        @Override
        public DecoratedKey apply(Long offset)
        {
            return dk(offset);
        }
    }

    private static DecoratedKey dk(Long token)
    {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(token);
        buf.flip();
        Long hashed = MurmurHash.hash2_64(buf, buf.position(), buf.remaining(), 0);
        return new BufferDecoratedKey(new Murmur3Partitioner.LongToken(hashed), buf);
    }

    private static TokenTree generateTree(final long minToken, final long maxToken, boolean isStatic) throws IOException
    {
        final SortedMap<Long, LongSet> toks = new TreeMap<Long, LongSet>()
        {{
                for (long i = minToken; i <= maxToken; i++)
                {
                    LongSet offsetSet = new LongHashSet();
                    offsetSet.add(i);
                    put(i, offsetSet);
                }
        }};

        final TokenTreeBuilder builder = isStatic ? new StaticTokenTreeBuilder(new FakeCombinedTerm(toks)) : new DynamicTokenTreeBuilder(toks);
        builder.finish();
        final File treeFile = FileUtils.createTempFile("token-tree-get-test", "tt");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.write(writer);
            writer.sync();
        }

        RandomAccessReader reader = null;

        try
        {
            reader = RandomAccessReader.open(treeFile);
            return new TokenTree(new MappedBuffer(reader));
        }
        finally
        {
            FileUtils.closeQuietly(reader);
        }
    }
}
