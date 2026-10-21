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

package org.apache.cassandra.test.microbench;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import com.github.luben.zstd.ZstdDictTrainer;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.db.compression.ZstdCompressionDictionary;
import org.apache.cassandra.io.compress.ZstdDictionaryCompressor;

// The full bench takes over 20 minutes to finish
@State(Scope.Benchmark)
public abstract class ZstdDictionaryCompressorBenchBase
{
    @Param({"4096", "16384", "65536"})
    protected int dataSize;

    @Param({"CASSANDRA_LIKE", "COMPRESSIBLE", "MIXED"})
    protected DataType dataType;

    @Param({"0", "65536"})
    protected int dictionarySize;

    // @Param({"3", "5", "7"})
    @Param({"3"})
    protected int compressionLevel;

    protected byte[] inputData;
    protected ByteBuffer inputBuffer;
    protected ByteBuffer compressedBuffer;
    protected ByteBuffer decompressedBuffer;
    protected ZstdDictionaryCompressor compressor;
    protected ZstdDictionaryCompressor noDictCompressor;
    protected ZstdCompressionDictionary dictionary;

    public enum DataType
    {
        CASSANDRA_LIKE, COMPRESSIBLE, MIXED
    }

    @Setup(Level.Trial)
    public void setupTrial()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException
    {
        Random random = new Random(42);

        // Generate test data based on type
        inputData = generateTestData(dataType, dataSize, random);

        // Create direct ByteBuffers (required by ZSTD)
        inputBuffer = ByteBuffer.allocateDirect(dataSize);
        inputBuffer.put(inputData);
        inputBuffer.flip();

        // Allocate buffers with extra space for compression overhead
        int maxCompressedSize = dataSize + 1024;
        compressedBuffer = ByteBuffer.allocateDirect(maxCompressedSize);
        decompressedBuffer = ByteBuffer.allocateDirect(dataSize);

        // Create dictionary if needed
        if (dictionarySize != 0)
        {
            dictionary = createDictionary(dataType, dictionarySize, random);
            Map<String, String> options = Map.of("compression_level", String.valueOf(compressionLevel));
            compressor = ZstdDictionaryCompressor.create(options).getOrCopyWithDictionary(dictionary);
        }
        else
        {
            Map<String, String> options = Map.of("compression_level", String.valueOf(compressionLevel));
            compressor = ZstdDictionaryCompressor.create(options);
        }

        // Always create a no-dictionary compressor for comparison
        Map<String, String> options = Map.of("compression_level", String.valueOf(compressionLevel));
        noDictCompressor = ZstdDictionaryCompressor.create(options);
    }

    @TearDown(Level.Iteration)
    public void tearDown()
    {
        if (dictionary != null)
        {
            dictionary.close();
            dictionary = null;
        }
        ZstdDictionaryCompressor.invalidateCache();
    }

    protected byte[] generateTestData(DataType type, int size, Random random)
    {
        byte[] data = new byte[size];

        switch (type)
        {
            case CASSANDRA_LIKE:
                generateCassandraLikeData(data, random);
                break;

            case COMPRESSIBLE:
                generateCompressibleData(data, random);
                break;

            case MIXED:
                generateMixedData(data, random);
                break;
        }

        return data;
    }

    private void generateCassandraLikeData(byte[] data, Random random)
    {
        StringBuilder sb = new StringBuilder();
        String[] patterns = {
            "user_id_", "timestamp_", "session_", "event_type_",
            "metadata_", "value_", "status_", "location_"
        };

        while (sb.length() < data.length)
        {
            String pattern = patterns[random.nextInt(patterns.length)];
            sb.append(pattern).append(UUID.randomUUID().toString()).append("|");
            sb.append("timestamp:").append(System.currentTimeMillis() + random.nextInt(86400000)).append("|");
            sb.append("value:").append(random.nextDouble()).append("|");
            sb.append("count:").append(random.nextInt(1000)).append("\n");
        }

        byte[] generated = sb.substring(0, Math.min(data.length, sb.length())).getBytes();
        System.arraycopy(generated, 0, data, 0, generated.length);

        // Fill remaining space with random data if needed
        if (generated.length < data.length)
        {
            byte[] remaining = new byte[data.length - generated.length];
            random.nextBytes(remaining);
            System.arraycopy(remaining, 0, data, generated.length, remaining.length);
        }
    }

    private void generateCompressibleData(byte[] data, Random random)
    {
        String pattern = "The quick brown fox jumps over the lazy dog. This is a highly compressible pattern that repeats. ";
        byte[] patternBytes = pattern.getBytes();

        for (int i = 0; i < data.length; i++)
        {
            data[i] = patternBytes[i % patternBytes.length];
        }

        // Add some randomness (10%)
        for (int i = 0; i < data.length / 10; i++)
        {
            data[random.nextInt(data.length)] = (byte) random.nextInt(256);
        }
    }

    private void generateMixedData(byte[] data, Random random)
    {
        int quarter = data.length / 4;

        // 25% random
        random.nextBytes(data);

        // 25% compressible
        byte[] compressible = new byte[quarter];
        generateCompressibleData(compressible, random);
        System.arraycopy(compressible, 0, data, quarter, quarter);

        // 50% Cassandra-like
        byte[] cassandraLike = new byte[data.length - 2 * quarter];
        generateCassandraLikeData(cassandraLike, random);
        System.arraycopy(cassandraLike, 0, data, 2 * quarter, cassandraLike.length);
    }

    private ZstdCompressionDictionary createDictionary(DataType dataType, int dictSize, Random random)
    {
        // Generate training samples
        byte[][] samples = new byte[100][];
        int totalSampleSize = 0;
        for (int i = 0; i < samples.length; i++)
        {
            samples[i] = generateTestData(dataType, Math.min(1024, dataSize), random);
            totalSampleSize += samples[i].length;
        }

        // Train dictionary
        ZstdDictTrainer trainer = new ZstdDictTrainer(totalSampleSize, dictSize);
        for (byte[] sample : samples)
        {
            trainer.addSample(sample);
        }

        byte[] dictData = trainer.trainSamples();
        DictId dictId = new DictId(Kind.ZSTD, 0);
        return new ZstdCompressionDictionary(dictId, dictData);
    }
}
