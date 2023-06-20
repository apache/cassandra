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
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.io.tries.IncrementalDeepTrieWriterPageAware;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.store.IndexInput;

import static org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryReader.trieSerializer;

public class TrieValidationTest extends SAIRandomizedTester
{
    private IndexDescriptor indexDescriptor;

    @Before
    public void createIndexDescriptor() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
    }

    @Test
    public void testHeaderValidation() throws Throwable
    {
        createSimpleTrie(indexDescriptor);
        try (IndexInput input = indexDescriptor.openPerSSTableInput(IndexComponent.PRIMARY_KEY_TRIE))
        {
            SAICodecUtils.validate(input);
        }
    }

    @Test
    public void testChecksumValidation() throws Throwable
    {
        createSimpleTrie(indexDescriptor);
        try (IndexInput input = indexDescriptor.openPerSSTableInput(IndexComponent.PRIMARY_KEY_TRIE))
        {
            SAICodecUtils.validateChecksum(input);
        }
    }

    private static void createSimpleTrie(IndexDescriptor indexDescriptor) throws Throwable
    {
        try (IndexOutputWriter trieOutput = indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEY_TRIE);
             IncrementalDeepTrieWriterPageAware<Long> trieWriter = new IncrementalDeepTrieWriterPageAware<>(trieSerializer, trieOutput.asSequentialWriter()))
        {
            SAICodecUtils.writeHeader(trieOutput);
            trieWriter.add(v -> createMultiPart(v, "abc", "def", "ghi"), 1L);
            trieWriter.add(v -> createMultiPart(v, "abc", "def", "jkl"), 2L);
            trieWriter.add(v -> createMultiPart(v, "abc", "ghi", "jkl"), 3L);
            trieWriter.add(v -> createMultiPart(v, "def", "ghi", "jkl"), 4L);
            trieWriter.add(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString("abcdef"), v), 5L);
            trieWriter.add(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString("abdefg"), v), 6L);
            trieWriter.add(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString("abdfgh"), v), 7L);
            trieWriter.complete();
            SAICodecUtils.writeFooter(trieOutput);
        }
    }

    private static ByteSource createMultiPart(ByteComparable.Version version, String... parts)
    {
        ByteSource [] byteSources = new ByteSource[parts.length];
        for (int index = 0; index < parts.length; index++)
            byteSources[index] = UTF8Type.instance.asComparableBytes(UTF8Type.instance.fromString(parts[index]), version);
        return ByteSource.withTerminator(ByteSource.TERMINATOR, byteSources);
    }

}
