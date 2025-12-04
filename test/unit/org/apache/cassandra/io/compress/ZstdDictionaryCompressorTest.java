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

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictTrainer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compression.ZstdCompressionDictionary;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import static org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import static org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.fail;

public class ZstdDictionaryCompressorTest
{
    private static final int TEST_DATA_SIZE = 1024;
    private static final String REPEATED_PATTERN = "The quick brown fox jumps over the lazy dog. ";

    private static byte[] testData;
    private static byte[] compressibleData;
    private static ZstdCompressionDictionary testDictionary;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        testData = new byte[TEST_DATA_SIZE];
        new Random(42).nextBytes(testData);

        // Generate compressible data
        StringBuilder sb = new StringBuilder();
        while (sb.length() < TEST_DATA_SIZE)
        {
            sb.append(REPEATED_PATTERN);
        }
        compressibleData = sb.substring(0, TEST_DATA_SIZE).getBytes();
        testDictionary = createTestDictionary();
    }

    @AfterClass
    public static void tearDown()
    {
        if (testDictionary != null)
        {
            testDictionary.selfRef().close();
        }
    }

    @Test
    public void testCreateWithOptions()
    {
        Map<String, String> options = Map.of(ZstdCompressor.COMPRESSION_LEVEL_OPTION_NAME, "5");

        ZstdDictionaryCompressor compressor = ZstdDictionaryCompressor.create(options);
        assertThat(compressor).isNotNull();
        assertThat(compressor.compressionLevel()).isEqualTo(5);
        assertThat(compressor.dictionary()).isNull(); // No dictionary should be set
    }

    @Test
    public void testCreateWithEmptyOptions()
    {
        ZstdDictionaryCompressor compressor = ZstdDictionaryCompressor.create(Collections.emptyMap());
        assertThat(compressor).isNotNull();
        assertThat(compressor.compressionLevel()).isEqualTo(ZstdCompressor.DEFAULT_COMPRESSION_LEVEL);
    }

    @Test
    public void testCreateWithDictionary()
    {
        ZstdDictionaryCompressor compressor = ZstdDictionaryCompressor.create(testDictionary);
        assertThat(compressor).isNotNull();
        assertThat(compressor.compressionLevel()).isEqualTo(ZstdCompressor.DEFAULT_COMPRESSION_LEVEL);
        assertThat(compressor.dictionary()).isSameAs(testDictionary);
    }

    @Test
    public void testCreateWithInvalidCompressionLevel()
    {
        String invalidLevel = String.valueOf(Zstd.maxCompressionLevel() + 1);
        Map<String, String> options = Map.of(ZstdCompressor.COMPRESSION_LEVEL_OPTION_NAME, invalidLevel);

        assertThatThrownBy(() -> ZstdDictionaryCompressor.create(options))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(ZstdCompressor.COMPRESSION_LEVEL_OPTION_NAME + '=' + invalidLevel + " is invalid");
    }

    @Test
    public void testCompressDecompressWithDictionary() throws IOException
    {
        ZstdDictionaryCompressor compressor = ZstdDictionaryCompressor.create(testDictionary);

        ByteBuffer input = ByteBuffer.allocateDirect(compressibleData.length);
        input.put(compressibleData);
        input.flip();

        ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.initialCompressedBufferLength(compressibleData.length));

        // Compress
        compressor.compress(input, compressed);
        compressed.flip();

        assertThat(compressed.remaining())
        .as("Data should be compressed")
        .isLessThan(compressibleData.length);

        // Decompress
        ByteBuffer decompressed = ByteBuffer.allocateDirect(compressibleData.length);
        compressed.rewind();
        compressor.uncompress(compressed, decompressed);
        decompressed.flip();

        // Verify roundtrip
        byte[] result = new byte[decompressed.remaining()];
        decompressed.get(result);
        assertThat(result).isEqualTo(compressibleData);
    }

    @Test
    public void testCompressDecompressWithoutDictionary() throws IOException
    {
        // Test fallback behavior when no dictionary is provided
        ZstdDictionaryCompressor compressor = ZstdDictionaryCompressor.create(Collections.emptyMap());

        ByteBuffer input = ByteBuffer.allocateDirect(testData.length);
        input.put(testData);
        input.flip();

        ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.initialCompressedBufferLength(testData.length));

        // Compress
        compressor.compress(input, compressed);
        compressed.flip();

        // Decompress
        ByteBuffer decompressed = ByteBuffer.allocateDirect(testData.length);
        compressed.rewind();
        compressor.uncompress(compressed, decompressed);
        decompressed.flip();

        // Verify roundtrip
        byte[] result = new byte[decompressed.remaining()];
        decompressed.get(result);
        assertThat(result).isEqualTo(testData);
    }

    @Test
    public void testCompressDecompressByteArray() throws IOException
    {
        ZstdDictionaryCompressor compressor = ZstdDictionaryCompressor.create(testDictionary);

        // Test byte array compression/decompression using direct buffers
        ByteBuffer input = ByteBuffer.allocateDirect(compressibleData.length);
        input.put(compressibleData);
        input.flip();

        ByteBuffer output = ByteBuffer.allocateDirect(compressor.initialCompressedBufferLength(compressibleData.length));

        compressor.compress(input, output);
        int compressedLength = output.position();

        // Extract compressed data to byte array for array-based decompression test
        byte[] compressed = new byte[compressedLength];
        output.flip();
        output.get(compressed);

        // Decompress using byte array method
        byte[] decompressed = new byte[compressibleData.length];
        int decompressedLength = compressor.uncompress(compressed, 0, compressedLength, decompressed, 0);

        assertThat(decompressedLength).isEqualTo(compressibleData.length);
        assertThat(decompressed).isEqualTo(compressibleData);
    }

    @Test
    public void testDictionaryCompressionImprovement()
    {
        // Test that dictionary compression provides better compression ratio
        ZstdDictionaryCompressor dictCompressor = ZstdDictionaryCompressor.create(testDictionary);
        ZstdDictionaryCompressor noDictCompressor = ZstdDictionaryCompressor.create(Collections.emptyMap());

        ByteBuffer input1 = ByteBuffer.allocateDirect(compressibleData.length);
        input1.put(compressibleData);
        input1.flip();

        ByteBuffer input2 = ByteBuffer.allocateDirect(compressibleData.length);
        input2.put(compressibleData);
        input2.flip();

        ByteBuffer dictCompressed = ByteBuffer.allocateDirect(dictCompressor.initialCompressedBufferLength(compressibleData.length));
        ByteBuffer noDictCompressed = ByteBuffer.allocateDirect(noDictCompressor.initialCompressedBufferLength(compressibleData.length));

        try
        {
            dictCompressor.compress(input1, dictCompressed);
            noDictCompressor.compress(input2, noDictCompressed);

            dictCompressed.flip();
            noDictCompressed.flip();

            // Dictionary compression should achieve better compression ratio for repetitive data
            assertThat(dictCompressed.remaining())
            .as("Dictionary compression should achieve better compression ratio")
            .isLessThanOrEqualTo(noDictCompressed.remaining());
        }
        catch (IOException e)
        {
            fail("Compression should not fail: " + e.getMessage());
        }
    }

    @Test
    public void testCompressorCaching()
    {
        // Test that same dictionary returns same compressor instance
        ZstdDictionaryCompressor compressor1 = ZstdDictionaryCompressor.create(testDictionary);
        ZstdDictionaryCompressor compressor2 = ZstdDictionaryCompressor.create(testDictionary);

        assertThat(compressor1)
        .as("Same dictionary should return cached compressor instance")
        .isSameAs(compressor2);
    }

    @Test
    public void testGetOrCopyWithDictionary()
    {
        ZstdDictionaryCompressor originalCompressor = ZstdDictionaryCompressor.create(Collections.emptyMap());
        ZstdDictionaryCompressor dictCompressor = originalCompressor.getOrCopyWithDictionary(testDictionary);

        assertThat(dictCompressor)
        .as("Should return different compressor instance")
        .isNotSameAs(originalCompressor);
        assertThat(dictCompressor.dictionary())
        .as("Should have the provided dictionary")
        .isSameAs(testDictionary);
        assertThat(dictCompressor.compressionLevel())
        .as("Should preserve compression level")
        .isEqualTo(originalCompressor.compressionLevel());
    }

    @Test
    public void testGetOrCopyWithSameDictionary()
    {
        ZstdDictionaryCompressor originalCompressor = ZstdDictionaryCompressor.create(testDictionary);
        ZstdDictionaryCompressor sameCompressor = originalCompressor.getOrCopyWithDictionary(testDictionary);

        assertThat(sameCompressor)
        .as("Same dictionary should return same compressor")
        .isSameAs(originalCompressor);
    }

    @Test
    public void testClosedDictionaryHandling()
    {
        ZstdDictionaryCompressor.invalidateCache();
        ZstdCompressionDictionary closedDict = createTestDictionary();
        closedDict.selfRef().close();

        // This should throw IllegalStateException
        assertThatThrownBy(() -> ZstdDictionaryCompressor.create(closedDict))
        .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testCompressionWithNullDictionary() throws IOException
    {
        // Test that null dictionary falls back to standard compression
        ZstdDictionaryCompressor compressor = ZstdDictionaryCompressor.create((ZstdCompressionDictionary) null);

        ByteBuffer input = ByteBuffer.allocateDirect(testData.length);
        input.put(testData);
        input.flip();

        ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.initialCompressedBufferLength(testData.length));

        // Should not throw exception, should fall back to standard Zstd
        compressor.compress(input, compressed);
        compressed.flip();

        ByteBuffer decompressed = ByteBuffer.allocateDirect(testData.length);
        compressed.rewind();
        compressor.uncompress(compressed, decompressed);
        decompressed.flip();

        byte[] result = new byte[decompressed.remaining()];
        decompressed.get(result);
        assertThat(result)
        .as("Null dictionary should fall back to standard compression")
        .isEqualTo(testData);
    }

    @Test
    public void testDecompressionFailureHandling()
    {
        ZstdDictionaryCompressor compressor = ZstdDictionaryCompressor.create(testDictionary);

        // Create invalid compressed data
        byte[] invalidData = new byte[10];
        new Random().nextBytes(invalidData);

        byte[] output = new byte[100];

        assertThatThrownBy(() -> compressor.uncompress(invalidData, 0, invalidData.length, output, 0))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Decompression failed");
    }

    @Test
    public void testAcceptableDictionaryKind()
    {
        ZstdDictionaryCompressor compressor = ZstdDictionaryCompressor.create(Collections.emptyMap());
        assertThat(compressor.acceptableDictionaryKind())
        .as("Should accept ZSTD dictionary kind")
        .isEqualTo(Kind.ZSTD);
    }

    @Test
    public void testEmptyDataCompression() throws IOException
    {
        ZstdDictionaryCompressor compressor = ZstdDictionaryCompressor.create(testDictionary);

        byte[] emptyData = new byte[0];
        ByteBuffer input = ByteBuffer.allocateDirect(emptyData.length + 1); // Allocate at least 1 byte for direct buffer
        input.put(emptyData);
        input.flip();

        ByteBuffer compressed = ByteBuffer.allocateDirect(Math.max(1, compressor.initialCompressedBufferLength(0)));

        compressor.compress(input, compressed);
        compressed.flip();

        ByteBuffer decompressed = ByteBuffer.allocateDirect(1); // Allocate at least 1 byte for direct buffer
        compressed.rewind();
        compressor.uncompress(compressed, decompressed);

        assertThat(decompressed.position())
        .as("Should have written nothing for empty data")
        .isEqualTo(0);
    }

    private static ZstdCompressionDictionary createTestDictionary()
    {
        try
        {
            int sampleSize = 100 * 1024;
            int dictSize = 6 * 1024;
            // Create a simple dictionary from repetitive data
            ZstdDictTrainer trainer = new ZstdDictTrainer(sampleSize, dictSize, 3);

            for (int i = 0; i < 1000; i++)
            {
                trainer.addSample(compressibleData);
            }

            byte[] dictBytes = trainer.trainSamples();
            DictId dictId = new DictId(Kind.ZSTD, 1);

            return new ZstdCompressionDictionary(dictId, dictBytes);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to create test dictionary", e);
        }
    }
}
