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

package org.apache.cassandra.io.compress;


import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionaryCache;
import org.apache.cassandra.db.compression.ZstdCompressionDictionary;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.schema.CompressionParams;

import static org.apache.cassandra.Util.spinAssertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompressionMetadataTest
{
    File chunksIndexFile = new File("/path/to/metadata");
    CompressionParams params = CompressionParams.zstd();
    long dataLength = 1000;
    long compressedFileLength = 100;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private CompressionMetadata newCompressionMetadata(Memory memory)
    {
        return new CompressionMetadata(chunksIndexFile,
                                       params,
                                       memory,
                                       memory.size(),
                                       dataLength,
                                       compressedFileLength,
                                       null);
    }

    @Test
    public void testMemoryIsFreed()
    {
        Memory memory = Memory.allocate(10);
        CompressionMetadata cm = newCompressionMetadata(memory);

        cm.close();
        assertThat(cm.isCleanedUp()).isTrue();
        assertThatExceptionOfType(AssertionError.class).isThrownBy(memory::size);
    }

    @Test
    public void testMemoryIsShared()
    {
        Memory memory = Memory.allocate(10);
        CompressionMetadata cm = newCompressionMetadata(memory);

        CompressionMetadata copy = cm.sharedCopy();
        assertThat(copy).isNotSameAs(cm);

        cm.close();
        assertThat(cm.isCleanedUp()).isFalse();
        assertThat(copy.isCleanedUp()).isFalse();
        assertThat(memory.size()).isEqualTo(10); // expected that no expection is thrown since memory should not be released yet

        copy.close();
        assertThat(cm.isCleanedUp()).isTrue();
        assertThat(copy.isCleanedUp()).isTrue();
        assertThatExceptionOfType(AssertionError.class).isThrownBy(memory::size);
    }

    /**
     * Test that simulates the Zstd with dictionary → LZ4 schema change scenario. (CASSANDRA-21047)
     */
    @Test
    public void testDictionaryRefCountingDuringSchemaChange()
    {
        // Step 1: Create a dictionary and add it to cache (simulating table with Zstd compression)
        CompressionDictionary.DictId dictId = new CompressionDictionary.DictId(CompressionDictionary.Kind.ZSTD, 1L);
        byte[] dictBytes = "sample dictionary data for compression".getBytes();
        ZstdCompressionDictionary dictionary = new ZstdCompressionDictionary(dictId, dictBytes);

        assertThat(dictionary.selfRef().globalCount()).isOne();
        CompressionDictionaryCache cache = new CompressionDictionaryCache();
        cache.add(dictionary);

        // Verify dictionary is in cache
        CompressionDictionary cachedDict = cache.get(dictId);
        assertThat(cachedDict)
        .as("Dictionary should be cached")
        .isNotNull()
        .isSameAs(dictionary);
        assertThat(dictionary.selfRef().globalCount()).isOne();

        // Step 2: Open SSTable with dictionary (simulating CompressionMetadata)
        Memory memory = Memory.allocate(100);
        CompressionMetadata metadata = new CompressionMetadata(
        chunksIndexFile,
        params,
        memory,
        memory.size(),
        dataLength,
        compressedFileLength,
        dictionary  // CompressionMetadata takes a reference
        );
        // RC is incremented
        assertThat(dictionary.selfRef().globalCount()).isEqualTo(2);

        // Verify we can get the compressor (which uses the dictionary)
        ICompressor compressor = metadata.compressor();
        assertThat(compressor)
        .as("Should be able to get compressor with dictionary")
        .isNotNull();
        // RC is incremented
        assertThat(dictionary.selfRef().globalCount()).isEqualTo(3);

        // Step 3: Schema change - close cache (simulating switch from Zstd to LZ4)
        // This releases the cache's reference to the dictionary
        cache.close();
        // RC is decremented
        spinAssertEquals(2, () -> dictionary.selfRef().globalCount());

        // Step 4: Verify dictionary is still usable via CompressionMetadata
        // This is the key test - before the fix, this would fail with "Dictionary has been closed"
        // compressor should return the cached compressor, does not change the reference count
        ICompressor compressorAfterCacheClosed = metadata.compressor();
        assertThat(compressorAfterCacheClosed)
        .as("Compressor should still be accessible after cache is closed")
        .isNotNull()
        .isSameAs(compressor);  // Should return the same cached compressor

        // Verify dictionary methods still work
        assertThat(dictionary.dictionaryForDecompression())
        .as("Dictionary decompression should still work")
        .isNotNull();

        // Step 5: Close CompressionMetadata (simulating SSTable being compacted away)
        metadata.close();
        // RC is decremented
        assertThat(dictionary.selfRef().globalCount()).isOne();

        // Step 6: Dictionary is still usable because ZstdDictionaryCompressor cache holds a reference
        // This is expected behavior - the compressor cache is global and may keep dictionaries
        // alive for reuse across SSTables
        assertThat(dictionary.dictionaryForDecompression())
        .as("Dictionary should still work due to compressor cache")
        .isNotNull();

        ZstdDictionaryCompressor.invalidateCache();
        // RC is decremented
        spinAssertEquals(0, () -> dictionary.selfRef().globalCount());
        assertThatThrownBy(dictionary::dictionaryForDecompression)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Dictionary has been released");
    }

    /**
     * Test multiple CompressionMetadata instances sharing the same dictionary
     * and surviving cache eviction. (CASSANDRA-21047)
     */
    @Test
    public void testMultipleMetadataInstancesSharingDictionary()
    {
        // Create dictionary and cache
        CompressionDictionary.DictId dictId = new CompressionDictionary.DictId(CompressionDictionary.Kind.ZSTD, 2L);
        byte[] dictBytes = "shared dictionary data".getBytes();
        ZstdCompressionDictionary dictionary = new ZstdCompressionDictionary(dictId, dictBytes);

        assertThat(dictionary.selfRef().globalCount()).isOne();
        CompressionDictionaryCache cache = new CompressionDictionaryCache();
        cache.add(dictionary);

        // Create multiple CompressionMetadata instances (simulating multiple SSTables)
        Memory memory1 = Memory.allocate(100);
        CompressionMetadata metadata1 = new CompressionMetadata(
        chunksIndexFile, params, memory1, memory1.size(),
        dataLength, compressedFileLength, dictionary
        );
        assertThat(dictionary.selfRef().globalCount()).isEqualTo(2);

        Memory memory2 = Memory.allocate(100);
        CompressionMetadata metadata2 = new CompressionMetadata(
        chunksIndexFile, params, memory2, memory2.size(),
        dataLength, compressedFileLength, dictionary
        );
        assertThat(dictionary.selfRef().globalCount()).isEqualTo(3);

        Memory memory3 = Memory.allocate(100);
        CompressionMetadata metadata3 = new CompressionMetadata(
        chunksIndexFile, params, memory3, memory3.size(),
        dataLength, compressedFileLength, dictionary
        );
        assertThat(dictionary.selfRef().globalCount()).isEqualTo(4);

        // All should be able to get compressors
        assertThat(metadata1.compressor()).isNotNull();
        assertThat(metadata2.compressor()).isNotNull();
        assertThat(metadata3.compressor()).isNotNull();
        assertThat(dictionary.selfRef().globalCount()).isEqualTo(5);

        // Close cache (schema change)
        cache.close();
        spinAssertEquals(4, () -> dictionary.selfRef().globalCount());

        // All metadata instances should still work
        assertThat(metadata1.compressor()).isNotNull();
        assertThat(metadata2.compressor()).isNotNull();
        assertThat(metadata3.compressor()).isNotNull();

        // Close metadata instances one by one
        metadata1.close();
        assertThat(dictionary.selfRef().globalCount()).isEqualTo(3);
        assertThat(metadata2.compressor())
        .as("Other metadata should still work")
        .isNotNull();
        assertThat(metadata3.compressor())
        .as("Other metadata should still work")
        .isNotNull();

        metadata2.close();
        assertThat(dictionary.selfRef().globalCount()).isEqualTo(2);
        assertThat(metadata3.compressor())
        .as("Last metadata should still work")
        .isNotNull();

        // Close last instance - now dictionary should be released
        metadata3.close();
        assertThat(dictionary.selfRef().globalCount()).isEqualTo(1);

        ZstdDictionaryCompressor.invalidateCache();
        // RC is decremented
        spinAssertEquals(0, () -> dictionary.selfRef().globalCount());
        assertThatThrownBy(dictionary::dictionaryForDecompression)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Dictionary has been released");
    }
}
