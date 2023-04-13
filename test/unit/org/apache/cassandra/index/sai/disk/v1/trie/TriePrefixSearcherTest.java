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

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.io.tries.IncrementalDeepTrieWriterPageAware;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryReader.trieSerializer;
import static org.junit.Assert.assertEquals;

public class TriePrefixSearcherTest extends SAIRandomizedTester
{
    private IndexDescriptor indexDescriptor;

    @Before
    public void createIndexDescriptor() throws Throwable
    {
        indexDescriptor =  newIndexDescriptor();
    }

    @Test
    public void completeValueTest() throws Throwable
    {
        long trieFilePointer = createSimpleTrie(indexDescriptor);

        try (FileHandle trieFileHandle = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_TRIE);
             TriePrefixSearcher searcher = new TriePrefixSearcher(trieFileHandle.instantiateRebufferer(null), trieFilePointer))
        {
            assertEquals(1L, searcher.prefixSearch(UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString("abcdef"), ByteComparable.Version.OSS42)));
            assertEquals(2L, searcher.prefixSearch(UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString("abdefg"), ByteComparable.Version.OSS42)));
            assertEquals(3L, searcher.prefixSearch(UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString("abdfgh"), ByteComparable.Version.OSS42)));
        }
    }

    @Test
    public void prefixValueTest() throws Throwable
    {
        long trieFilePointer = createSimpleTrie(indexDescriptor);

        try (FileHandle trieFileHandle = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_TRIE);
             TriePrefixSearcher searcher = new TriePrefixSearcher(trieFileHandle.instantiateRebufferer(null), trieFilePointer))
        {
            assertEquals(1L, searcher.prefixSearch(UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString(""), ByteComparable.Version.OSS42)));
            assertEquals(1L, searcher.prefixSearch(UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString("a"), ByteComparable.Version.OSS42)));
            assertEquals(1L, searcher.prefixSearch(UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString("ab"), ByteComparable.Version.OSS42)));
            assertEquals(1L, searcher.prefixSearch(UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString("abc"), ByteComparable.Version.OSS42)));
            assertEquals(2L, searcher.prefixSearch(UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString("abd"), ByteComparable.Version.OSS42)));
            assertEquals(2L, searcher.prefixSearch(UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString("abde"), ByteComparable.Version.OSS42)));
            assertEquals(3L, searcher.prefixSearch(UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString("abdf"), ByteComparable.Version.OSS42)));
        }
    }

    @Test
    public void unknownValueTest() throws Throwable
    {
        long trieFilePointer = createSimpleTrie(indexDescriptor);

        try (FileHandle trieFileHandle = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_TRIE);
             TriePrefixSearcher searcher = new TriePrefixSearcher(trieFileHandle.instantiateRebufferer(null), trieFilePointer))
        {
            assertEquals(-1L, searcher.prefixSearch(UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString("b"), ByteComparable.Version.OSS42)));
        }
    }

    @Test
    public void completeMultiPartTest() throws Throwable
    {
        long trieFilePointer = createMultiPartTrie(indexDescriptor);

        try (FileHandle trieFileHandle = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_TRIE);
             TriePrefixSearcher searcher = new TriePrefixSearcher(trieFileHandle.instantiateRebufferer(null), trieFilePointer))
        {
            assertEquals(1L, searcher.prefixSearch(createMultiPart(ByteComparable.Version.OSS42, "abc", "def", "ghi")));
            assertEquals(2L, searcher.prefixSearch(createMultiPart(ByteComparable.Version.OSS42, "abc", "def", "jkl")));
            assertEquals(3L, searcher.prefixSearch(createMultiPart(ByteComparable.Version.OSS42, "abc", "ghi", "jkl")));
            assertEquals(4L, searcher.prefixSearch(createMultiPart(ByteComparable.Version.OSS42, "def", "ghi", "jkl")));
        }
    }

    @Test
    public void prefixMultiPartTest() throws Throwable
    {
        long trieFilePointer = createMultiPartTrie(indexDescriptor);

        try (FileHandle trieFileHandle = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_TRIE);
             TriePrefixSearcher searcher = new TriePrefixSearcher(trieFileHandle.instantiateRebufferer(null), trieFilePointer))
        {
            assertEquals(1L, searcher.prefixSearch(createMultiPart(ByteComparable.Version.OSS42, "ab")));
            assertEquals(1L, searcher.prefixSearch(createMultiPart(ByteComparable.Version.OSS42, "abc")));
            assertEquals(1L, searcher.prefixSearch(createMultiPart(ByteComparable.Version.OSS42, "abc", "de")));
            assertEquals(1L, searcher.prefixSearch(createMultiPart(ByteComparable.Version.OSS42, "abc", "def")));
            assertEquals(1L, searcher.prefixSearch(createMultiPart(ByteComparable.Version.OSS42, "abc", "def", "gh")));
            assertEquals(2L, searcher.prefixSearch(createMultiPart(ByteComparable.Version.OSS42, "abc", "def", "jk")));
            assertEquals(3L, searcher.prefixSearch(createMultiPart(ByteComparable.Version.OSS42, "abc", "ghi")));
            assertEquals(4L, searcher.prefixSearch(createMultiPart(ByteComparable.Version.OSS42, "d")));
        }
    }

    private long createSimpleTrie(IndexDescriptor indexDescriptor) throws Throwable
    {
        try (IndexOutputWriter trieOutput = indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEY_TRIE);
        IncrementalDeepTrieWriterPageAware<Long> trieWriter = new IncrementalDeepTrieWriterPageAware<>(trieSerializer, trieOutput.asSequentialWriter()))
        {
            trieWriter.add(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString("abcdef"), v), 1L);
            trieWriter.add(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString("abdefg"), v), 2L);
            trieWriter.add(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString("abdfgh"), v), 3L);
            return trieWriter.complete();
        }
    }

    private long createMultiPartTrie(IndexDescriptor indexDescriptor) throws Throwable
    {
        try (IndexOutputWriter trieOutput = indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEY_TRIE);
             IncrementalDeepTrieWriterPageAware<Long> trieWriter = new IncrementalDeepTrieWriterPageAware<>(trieSerializer, trieOutput.asSequentialWriter()))
        {
            trieWriter.add(v -> createMultiPart(v, "abc", "def", "ghi"), 1L);
            trieWriter.add(v -> createMultiPart(v, "abc", "def", "jkl"), 2L);
            trieWriter.add(v -> createMultiPart(v, "abc", "ghi", "jkl"), 3L);
            trieWriter.add(v -> createMultiPart(v, "def", "ghi", "jkl"), 4L);
            return trieWriter.complete();
        }
    }

    private ByteSource createMultiPart(ByteComparable.Version version, String... parts)
    {
        ByteSource [] byteSources = new ByteSource[parts.length];
        for (int index = 0; index < parts.length; index++)
            byteSources[index] = UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString(parts[index]), version);
        return ByteSource.withTerminator(ByteSource.TERMINATOR, byteSources);
    }



}
