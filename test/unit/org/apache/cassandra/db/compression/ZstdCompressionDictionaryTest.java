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

package org.apache.cassandra.db.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.io.compress.ZstdCompressorBase;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ZstdCompressionDictionaryTest
{
    private static final byte[] SAMPLE_DICT_DATA = createSampleDictionaryData();
    private static final DictId SAMPLE_DICT_ID = new DictId(Kind.ZSTD, 123456789L);

    private ZstdCompressionDictionary dictionary;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        dictionary = new ZstdCompressionDictionary(SAMPLE_DICT_ID, SAMPLE_DICT_DATA);
        dictionary.initRefLazily();
    }

    @Test
    public void testEqualsAndHashCode()
    {
        ZstdCompressionDictionary dictionary2 = new ZstdCompressionDictionary(SAMPLE_DICT_ID, SAMPLE_DICT_DATA);
        ZstdCompressionDictionary differentIdDict = new ZstdCompressionDictionary(
        new DictId(Kind.ZSTD, 987654321L), SAMPLE_DICT_DATA);

        assertThat(dictionary)
        .as("Dictionaries with same ID should be equal")
        .isEqualTo(dictionary2);

        assertThat(dictionary.hashCode())
        .as("Hash codes should be equal for same ID")
        .isEqualTo(dictionary2.hashCode());

        assertThat(dictionary)
        .as("Dictionaries with different IDs should not be equal")
        .isNotEqualTo(differentIdDict);
    }

    @Test
    public void testDictionaryForCompression()
    {
        int compressionLevel = 3;
        ZstdDictCompress compressDict = dictionary.dictionaryForCompression(compressionLevel);

        assertThat(compressDict)
        .as("Compression dictionary should not be null")
        .isNotNull();

        // Calling again should return the same cached instance
        ZstdDictCompress compressDict2 = dictionary.dictionaryForCompression(compressionLevel);
        assertThat(compressDict2)
        .as("Second call should return cached instance")
        .isSameAs(compressDict);
    }

    @Test
    public void testDictionaryForCompressionMultipleLevels()
    {
        ZstdDictCompress level1 = dictionary.dictionaryForCompression(1);
        ZstdDictCompress level3 = dictionary.dictionaryForCompression(3);
        ZstdDictCompress level6 = dictionary.dictionaryForCompression(6);

        assertThat(level1)
        .as("Level 1 compression dictionary should not be null")
        .isNotNull();

        assertThat(level3)
        .as("Level 3 compression dictionary should not be null")
        .isNotNull();

        assertThat(level6)
        .as("Level 6 compression dictionary should not be null")
        .isNotNull();

        assertThat(level1)
        .as("Different compression levels should have different instances")
        .isNotSameAs(level3);

        assertThat(level3)
        .as("Different compression levels should have different instances")
        .isNotSameAs(level6);
    }

    @Test
    public void testDictionaryForDecompression()
    {
        ZstdDictDecompress decompressDict = dictionary.dictionaryForDecompression();

        assertThat(decompressDict)
        .as("Decompression dictionary should not be null")
        .isNotNull();

        ZstdDictDecompress decompressDict2 = dictionary.dictionaryForDecompression();
        assertThat(decompressDict2)
        .as("Second call should return cached instance")
        .isSameAs(decompressDict);
    }

    @Test
    public void testInvalidCompressionLevel()
    {
        // Test with various invalid compression levels
        assertThatThrownBy(() -> dictionary.dictionaryForCompression(ZstdCompressorBase.FAST_COMPRESSION_LEVEL - 1))
        .as("Negative compression level should throw exception")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is invalid");

        assertThatThrownBy(() -> dictionary.dictionaryForCompression(100))
        .as("Too high compression level should throw exception")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is invalid");
    }

    @Test
    public void testDictionaryClose()
    {
        // Access some dictionaries first
        dictionary.dictionaryForCompression(3);
        dictionary.dictionaryForDecompression();

        // Release the self-reference
        dictionary.selfRef().release();

        // After releasing selfRef, tryRef should return null
        Ref<ZstdCompressionDictionary> ref = dictionary.tryRef();
        assertThat(ref)
        .as("tryRef should return null after dictionary is released")
        .isNull();
    }

    @Test
    public void testDictionaryAccessWithoutReference()
    {
        // Create a new dictionary for this test
        ZstdCompressionDictionary testDict = new ZstdCompressionDictionary(
            new DictId(Kind.ZSTD, 999999L),
            SAMPLE_DICT_DATA
        );

        testDict.initRefLazily();

        // Access some dictionaries first to initialize them
        testDict.dictionaryForCompression(3);
        testDict.dictionaryForDecompression();

        // Release the self-reference to simulate dictionary cleanup
        testDict.selfRef().release();

        // Verify tryRef returns null after release
        assertThat(testDict.tryRef())
        .as("tryRef should return null after dictionary is released")
        .isNull();

        // Accessing dictionary methods without a valid reference should throw IllegalStateException
        // This protects against use-after-free bugs in both development and production
        assertThatThrownBy(() -> testDict.dictionaryForCompression(3))
        .as("Should throw exception when accessing released dictionary")
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Dictionary has been released");

        assertThatThrownBy(() -> testDict.dictionaryForDecompression())
        .as("Should throw exception when accessing released dictionary")
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Dictionary has been released");
    }

    @Test
    public void testTryRef()
    {
        Ref<ZstdCompressionDictionary> ref = dictionary.tryRef();

        assertThat(ref)
        .as("tryRef should return non-null reference")
        .isNotNull();

        assertThat(ref.get())
        .as("Reference should point to same dictionary")
        .isSameAs(dictionary);

        ref.release();
    }

    @Test
    public void testMultipleReferences()
    {
        Ref<ZstdCompressionDictionary> ref1 = dictionary.ref();
        Ref<ZstdCompressionDictionary> ref2 = dictionary.ref();
        Ref<ZstdCompressionDictionary> ref3 = dictionary.tryRef();

        assertThat(ref1.get())
        .as("All references should point to same dictionary")
        .isSameAs(dictionary);

        assertThat(ref2.get())
        .as("All references should point to same dictionary")
        .isSameAs(dictionary);

        assertThat(ref3.get())
        .as("All references should point to same dictionary")
        .isSameAs(dictionary);

        // Dictionary should still be accessible
        assertThat(dictionary.dictionaryForCompression(3))
        .as("Dictionary should still be accessible with multiple refs")
        .isNotNull();

        ref1.release();
        ref2.release();
        ref3.release();
    }

    @Test
    public void testReferenceAfterClose()
    {
        dictionary.selfRef().release();

        assertThatThrownBy(() -> dictionary.ref())
        .as("Should not be able to get reference after release")
        .isInstanceOf(AssertionError.class);

        Ref<ZstdCompressionDictionary> tryRef = dictionary.tryRef();
        assertThat(tryRef)
        .as("tryRef should return null after release")
        .isNull();
    }

    @Test
    public void testConcurrentAccess() throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        AtomicInteger successCount = new AtomicInteger(0);
        int numTasks = 100;

        try
        {
            Future<?>[] futures = new Future[numTasks];

            for (int i = 0; i < numTasks; i++)
            {
                final int level = (i % 6) + 1; // Compression levels 1-6
                futures[i] = executor.submit(() -> {
                    try
                    {
                        Ref<ZstdCompressionDictionary> ref = dictionary.ref();
                        ZstdDictCompress compressDict = ref.get().dictionaryForCompression(level);
                        ZstdDictDecompress decompressDict = ref.get().dictionaryForDecompression();

                        assertThat(compressDict).isNotNull();
                        assertThat(decompressDict).isNotNull();

                        successCount.incrementAndGet();
                        ref.release();
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                });
            }

            // Wait for all tasks to complete
            for (Future<?> future : futures)
            {
                future.get(5, TimeUnit.SECONDS);
            }

            assertThat(successCount.get())
            .as("All concurrent accesses should succeed")
            .isEqualTo(numTasks);
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSerializeDeserialize() throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dictionary.serialize(dos);
        dos.flush();

        byte[] serializedData = baos.toByteArray();
        assertThat(serializedData.length)
        .as("Serialized data should not be empty")
        .isGreaterThan(0);

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(serializedData);
        DataInputStream dis = new DataInputStream(bais);

        CompressionDictionary deserializedDict = CompressionDictionary.deserialize(dis, null);

        assertThat(deserializedDict)
        .as("Deserialized dictionary should not be null")
        .isNotNull();

        assertThat(deserializedDict.dictId())
        .as("Deserialized dictionary ID should match")
        .isEqualTo(dictionary.dictId());

        assertThat(deserializedDict.kind())
        .as("Deserialized dictionary kind should match")
        .isEqualTo(dictionary.kind());

        assertThat(deserializedDict.rawDictionary())
        .as("Deserialized dictionary data should match")
        .isEqualTo(dictionary.rawDictionary());
    }

    @Test
    public void testSerializeDeserializeWithManager() throws Exception
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dictionary.serialize(dos);
        dos.flush();

        byte[] serializedData = baos.toByteArray();

        // First deserialization should create and cache the dictionary
        ByteArrayInputStream bais1 = new ByteArrayInputStream(serializedData);
        DataInputStream dis1 = new DataInputStream(bais1);
        CompressionDictionary dict1 = CompressionDictionary.deserialize(dis1, null);

        // Second deserialization should return cached instance
        ByteArrayInputStream bais2 = new ByteArrayInputStream(serializedData);
        DataInputStream dis2 = new DataInputStream(bais2);
        CompressionDictionary dict2 = CompressionDictionary.deserialize(dis2, null);

        assertThat(dict1)
        .as("Both deserializations should return identical dictionary")
        .isNotNull()
        .isEqualTo(dict2);
    }

    @Test
    public void testDeserializeCorruptedData() throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Write corrupted data (wrong checksum)
        dos.writeByte(Kind.ZSTD.ordinal());
        dos.writeLong(SAMPLE_DICT_ID.id);
        dos.writeInt(SAMPLE_DICT_DATA.length);
        dos.write(SAMPLE_DICT_DATA);
        dos.writeInt(0xDEADBEEF); // Wrong checksum
        dos.flush();

        byte[] corruptedData = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(corruptedData);
        DataInputStream dis = new DataInputStream(bais);

        assertThatThrownBy(() -> CompressionDictionary.deserialize(dis, null))
        .as("Should throw exception for corrupted data")
        .isInstanceOf(IOException.class)
        .hasMessageContaining("checksum does not match");
    }

    private static byte[] createSampleDictionaryData()
    {
        // Create sample dictionary data that could be used for compression
        String sampleText = "The quick brown fox jumps over the lazy dog. ";
        return sampleText.repeat(100).getBytes();
    }
}
