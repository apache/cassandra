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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Version.OSS50;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.compare;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TrieTermsDictionaryTest extends SAIRandomizedTester
{
    private IndexDescriptor indexDescriptor;
    private IndexIdentifier indexIdentifier;

    @Before
    public void setup() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
        indexIdentifier = createIndexIdentifier("test", "test", newIndex());
    }

    @Test
    public void testExactMatch() throws Exception
    {
        doTestExactMatch();
    }

    private void doTestExactMatch() throws Exception
    {
        long fp;
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(indexDescriptor, indexIdentifier))
        {
            writer.add(asByteComparable("ab"), 0);
            writer.add(asByteComparable("abb"), 1);
            writer.add(asByteComparable("abc"), 2);
            writer.add(asByteComparable("abcd"), 3);
            writer.add(asByteComparable("abd"), 4);
            fp = writer.complete(new MutableLong());
        }

        try (FileHandle input = indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, indexIdentifier);
             TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(input.instantiateRebufferer(null), fp))
        {
            assertEquals(TrieTermsDictionaryReader.NOT_FOUND, reader.exactMatch(asByteComparable("a")));
            assertEquals(0, reader.exactMatch(asByteComparable("ab")));
            assertEquals(2, reader.exactMatch(asByteComparable("abc")));
            assertEquals(TrieTermsDictionaryReader.NOT_FOUND, reader.exactMatch(asByteComparable("abca")));
            assertEquals(1, reader.exactMatch(asByteComparable("abb")));
            assertEquals(TrieTermsDictionaryReader.NOT_FOUND, reader.exactMatch(asByteComparable("abba")));
        }
    }

    @Test
    public void testTermEnum() throws IOException
    {
        final List<ByteComparable> byteComparables = generateSortedByteComparables();

        long fp;
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(indexDescriptor, indexIdentifier))
        {
            for (int i = 0; i < byteComparables.size(); ++i)
            {
                writer.add(byteComparables.get(i), i);
            }
            fp = writer.complete(new MutableLong());
        }

        try (FileHandle input = indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, indexIdentifier);
             TrieTermsIterator iterator = new TrieTermsIterator(input.instantiateRebufferer(null), fp))
        {
            final Iterator<ByteComparable> expected = byteComparables.iterator();
            int offset = 0;
            while (iterator.hasNext())
            {
                assertTrue(expected.hasNext());
                final Pair<ByteComparable, Long> actual = iterator.next();

                assertEquals(0, compare(expected.next(), actual.left, OSS50));
                assertEquals(offset++, actual.right.longValue());
            }
            assertFalse(expected.hasNext());
        }
    }

    @Test
    public void testMinMaxTerm() throws IOException
    {
        final List<ByteComparable> byteComparables = generateSortedByteComparables();

        long fp;
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(indexDescriptor, indexIdentifier))
        {
            for (int i = 0; i < byteComparables.size(); ++i)
            {
                writer.add(byteComparables.get(i), i);
            }
            fp = writer.complete(new MutableLong());
        }

        try (FileHandle input = indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, indexIdentifier);
             TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(input.instantiateRebufferer(null), fp))
        {
            final ByteComparable expectedMaxTerm = byteComparables.get(byteComparables.size() - 1);
            final ByteComparable actualMaxTerm = reader.getMaxTerm();
            assertEquals(0, compare(expectedMaxTerm, actualMaxTerm, OSS50));

            final ByteComparable expectedMinTerm = byteComparables.get(0);
            final ByteComparable actualMinTerm = reader.getMinTerm();
            assertEquals(0, compare(expectedMinTerm, actualMinTerm, OSS50));
        }
    }

    private List<ByteComparable> generateSortedByteComparables()
    {
        final int numKeys = getRandom().nextIntBetween(16, 512);
        final List<String> randomStrings = Stream.generate(() -> randomSimpleString(4, 48))
                                                 .limit(numKeys)
                                                 .sorted()
                                                 .collect(Collectors.toList());

        // Get rid of any duplicates otherwise the tests will fail.
        return randomStrings.stream()
                            .filter(string -> Collections.frequency(randomStrings, string) == 1)
                            .map(this::asByteComparable)
                            .collect(Collectors.toList());
    }

    private ByteComparable asByteComparable(String s)
    {
        return ByteComparable.fixedLength(ByteBufferUtil.bytes(s));
    }
}
