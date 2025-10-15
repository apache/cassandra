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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.cassandra.utils.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ZstdDictionaryTrainerTest
{
    private static final String TEST_KEYSPACE = "test_ks";
    private static final String TEST_TABLE = "test_table";
    private static final String SAMPLE_DATA = "The quick brown fox jumps over the lazy dog. ";
    private static final int COMPRESSION_LEVEL = 3;

    private CompressionDictionaryTrainingConfig testConfig;
    private ZstdDictionaryTrainer trainer;
    private Consumer<CompressionDictionary> mockCallback;
    private AtomicReference<CompressionDictionary> callbackResult;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        testConfig = CompressionDictionaryTrainingConfig.builder()
                                                        .maxDictionarySize(1024)          // Small for testing
                                                        .maxTotalSampleSize(10 * 1024)    // 10KB total
                                                        .samplingRate(1)                  // 100% sampling for predictable tests
                                                        .build();

        callbackResult = new AtomicReference<>();
        mockCallback = callbackResult::set;

        trainer = new ZstdDictionaryTrainer(TEST_KEYSPACE, TEST_TABLE, testConfig, COMPRESSION_LEVEL);
        trainer.setDictionaryTrainedListener(mockCallback);
    }

    @After
    public void tearDown() throws Exception
    {
        if (trainer != null)
        {
            trainer.close();
        }

        // Clean up any dictionary created in callback
        CompressionDictionary dict = callbackResult.get();
        if (dict != null)
        {
            dict.close();
            callbackResult.set(null);
        }
    }

    @Test
    public void testTrainerInitialState()
    {
        assertThat(trainer.getTrainingState().getStatus())
        .as("Initial status should be NOT_STARTED")
        .isEqualTo(TrainingStatus.NOT_STARTED);
        assertThat(trainer.isReady())
        .as("Should not be ready initially")
        .isFalse();
        assertThat(trainer.kind())
        .as("Should return ZSTD kind")
        .isEqualTo(Kind.ZSTD);
    }

    @Test
    public void testTrainerStart()
    {
        // Auto start depends on configuration - test both scenarios
        boolean started = trainer.start(false);
        if (started)
        {
            assertThat(trainer.getTrainingState().getStatus())
            .as("Status should be SAMPLING if auto-start enabled")
            .isEqualTo(TrainingStatus.SAMPLING);
        }
        else
        {
            assertThat(trainer.getTrainingState().getStatus())
            .as("Status should remain NOT_STARTED if auto-start disabled")
            .isEqualTo(TrainingStatus.NOT_STARTED);
        }
    }

    @Test
    public void testTrainerStartManual()
    {
        assertThat(trainer.start(true))
        .as("Manual training should start successfully")
        .isTrue();
        assertThat(trainer.getTrainingState().getStatus())
        .as("Status should be SAMPLING after start")
        .isEqualTo(TrainingStatus.SAMPLING);
        assertThat(trainer.isReady())
        .as("Should not be ready immediately after start")
        .isFalse();
    }

    @Test
    public void testTrainerStartMultipleTimes()
    {
        assertThat(trainer.start(true))
        .as("First start (manual training) should succeed")
        .isTrue();
        Object firstTrainer = trainer.trainer();
        assertThat(firstTrainer).isNotNull();
        assertThat(trainer.start(true))
        .as("Second start (manual training) should suceed and reset")
        .isTrue();
        Object secondTrainer = trainer.trainer();
        assertThat(secondTrainer).isNotNull().isNotSameAs(firstTrainer);
        assertThat(trainer.start(false))
        .as("Third start (not manual training) should fail")
        .isFalse();
    }

    @Test
    public void testTrainerCloseIdempotent()
    {
        trainer.start(true);
        trainer.close();
        trainer.close(); // Should not throw
        trainer.close(); // Should not throw

        assertThat(trainer.getTrainingState().getStatus())
        .as("Status should remain NOT_STARTED after multiple closes")
        .isEqualTo(TrainingStatus.NOT_STARTED);
    }

    @Test
    public void testTrainerReset()
    {
        trainer.start(true);
        addSampleData(1000); // Add some samples

        assertThat(trainer.getTrainingState().getSampleCount())
        .as("Should have samples before reset")
        .isGreaterThan(0);

        trainer.reset();
        assertThat(trainer.getTrainingState().getStatus())
        .as("Status should be NOT_STARTED after reset")
        .isEqualTo(TrainingStatus.NOT_STARTED);
        assertThat(trainer.getTrainingState().getSampleCount())
        .as("Sample count should be 0 after reset")
        .isEqualTo(0);
        assertThat(trainer.isReady())
        .as("Should not be ready after reset")
        .isFalse();
    }

    @Test
    public void testStartAfterClose()
    {
        trainer.start(true);
        trainer.close();

        assertThat(trainer.start(true))
        .as("Should not start after close")
        .isFalse();
        assertThat(trainer.getTrainingState().getStatus())
        .as("Status should remain NOT_STARTED")
        .isEqualTo(TrainingStatus.NOT_STARTED);
    }

    @Test
    public void testShouldSample()
    {
        trainer.start(true);
        // With sampling rate 1 (100%), should always return true
        for (int i = 0; i < 10; i++)
        {
            assertThat(trainer.shouldSample())
            .as("Should sample with rate 1")
            .isTrue();
        }
    }

    @Test
    public void testShouldSampleWithLowRate()
    {
        // Test with lower sampling rate
        CompressionDictionaryTrainingConfig lowSamplingConfig =
        CompressionDictionaryTrainingConfig.builder()
                                           .maxDictionarySize(1024)
                                           .maxTotalSampleSize(10 * 1024)
                                           .samplingRate(0.001f) // 0.1% sampling
                                           .build();

        try (ZstdDictionaryTrainer lowSamplingTrainer = new ZstdDictionaryTrainer(TEST_KEYSPACE, TEST_TABLE,
                                                                                  lowSamplingConfig, COMPRESSION_LEVEL))
        {
            lowSamplingTrainer.setDictionaryTrainedListener(mockCallback);
            // With very low sampling rate, should mostly return false
            int sampleCount = 0;
            int iterations = 1000;
            for (int i = 0; i < iterations; i++)
            {
                if (lowSamplingTrainer.shouldSample())
                {
                    sampleCount++;
                }
            }

            // Should be roughly 0.1% (1 out of 1000), allow some variance
            assertThat(sampleCount)
            .as("Sample rate should be low")
            .isLessThan(iterations / 10);
        }
    }

    @Test
    public void testAddSample()
    {
        trainer.start(true);

        assertThat(trainer.getTrainingState().getSampleCount())
        .as("Initial sample count should be 0")
        .isEqualTo(0);

        ByteBuffer sample = ByteBuffer.wrap(SAMPLE_DATA.getBytes());
        trainer.addSample(sample);

        assertThat(trainer.getTrainingState().getSampleCount())
        .as("Sample count should be 1 after adding one sample")
        .isEqualTo(1);
        assertThat(trainer.getTrainingState().getStatus())
        .as("Status should be SAMPLING")
        .isEqualTo(TrainingStatus.SAMPLING);
        assertThat(trainer.isReady())
        .as("Should not be ready with single small sample")
        .isFalse();
    }

    @Test
    public void testAddSampleBeforeStart()
    {
        // Should not accept samples before start
        ByteBuffer sample = ByteBuffer.wrap(SAMPLE_DATA.getBytes());
        trainer.addSample(sample);

        assertThat(trainer.getTrainingState().getStatus())
        .as("Status should remain NOT_STARTED")
        .isEqualTo(TrainingStatus.NOT_STARTED);
        assertThat(trainer.isReady())
        .as("Should not be ready")
        .isFalse();
    }

    @Test
    public void testAddSampleAfterClose()
    {
        trainer.start(true);
        trainer.close();

        ByteBuffer sample = ByteBuffer.wrap(SAMPLE_DATA.getBytes());
        trainer.addSample(sample);

        assertThat(trainer.getTrainingState().getStatus())
        .as("Status should remain NOT_STARTED after close")
        .isEqualTo(TrainingStatus.NOT_STARTED);
        assertThat(trainer.isReady())
        .as("Should not be ready after close")
        .isFalse();
    }

    @Test
    public void testAddNullSample()
    {
        trainer.start(true);
        trainer.addSample(null); // Should not throw

        assertThat(trainer.getTrainingState().getStatus())
        .as("Status should remain SAMPLING")
        .isEqualTo(TrainingStatus.SAMPLING);
        assertThat(trainer.isReady())
        .as("Should not be ready with null sample")
        .isFalse();
    }

    @Test
    public void testAddEmptySample()
    {
        trainer.start(true);
        ByteBuffer empty = ByteBuffer.allocate(0);
        trainer.addSample(empty); // Should not throw

        assertThat(trainer.getTrainingState().getStatus())
        .as("Status should remain SAMPLING")
        .isEqualTo(TrainingStatus.SAMPLING);
        assertThat(trainer.isReady())
        .as("Should not be ready with empty sample")
        .isFalse();
    }

    @Test
    public void testIsReady()
    {
        trainer.start(true);
        assertThat(trainer.isReady())
        .as("Should not be ready initially")
        .isFalse();

        addSampleData(testConfig.acceptableTotalSampleSize / 2);
        assertThat(trainer.isReady())
        .as("Should not be ready with insufficient samples")
        .isFalse();

        addSampleData(testConfig.acceptableTotalSampleSize);
        assertThat(trainer.isReady())
        .as("Should be ready after enough samples")
        .isTrue();

        trainer.close();

        assertThat(trainer.isReady())
        .as("Should not be ready when closed")
        .isFalse();
    }

    @Test
    public void testTrainDictionaryWithInsufficientSampleCount()
    {
        trainer.start(true);

        // Add sufficient data size but only 5 samples (less than minimum 10)
        for (int i = 0; i < 5; i++)
        {
            ByteBuffer largeSample = ByteBuffer.wrap(new byte[testConfig.acceptableTotalSampleSize / 5]);
            trainer.addSample(largeSample);
        }

        assertThat(trainer.getTrainingState().getSampleCount())
        .as("Should have 5 samples")
        .isEqualTo(5);
        assertThat(trainer.isReady())
        .as("Should not be ready with insufficient sample count")
        .isFalse();

        // Trying to train without force should fail
        assertThatThrownBy(() -> trainer.trainDictionary(false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Trainer is not ready");

        // Force training should fail with insufficient samples
        assertThatThrownBy(() -> trainer.trainDictionary(true))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Insufficient samples for training: 5 (minimum required: 10)");
    }

    @Test
    public void testTrainDictionaryWithSufficientSampleCount()
    {
        trainer.start(true);

        // Add 15 samples with sufficient total size
        for (int i = 0; i < 15; i++)
        {
            ByteBuffer sample = ByteBuffer.wrap(new byte[testConfig.acceptableTotalSampleSize / 15 + 1]);
            trainer.addSample(sample);
        }

        assertThat(trainer.getTrainingState().getSampleCount()).isEqualTo(15);
        assertThat(trainer.isReady()).isTrue();

        // Training should succeed
        CompressionDictionary dictionary = trainer.trainDictionary(false);
        assertThat(dictionary).as("Dictionary should be created").isNotNull();
        assertThat(trainer.getTrainingState().getStatus()).isEqualTo(TrainingStatus.COMPLETED);
    }

    @Test
    public void testTrainDictionaryAsync() throws Exception
    {
        Future<CompressionDictionary> future = startTraining(true, false, testConfig.acceptableTotalSampleSize);
        CompressionDictionary dictionary = future.get(5, TimeUnit.SECONDS);

        assertThat(dictionary).as("Dictionary should not be null").isNotNull();
        assertThat(trainer.getTrainingState().getStatus()).as("Status should be COMPLETED").isEqualTo(TrainingStatus.COMPLETED);

        // Verify callback was called
        assertThat(callbackResult.get()).as("Callback should have been called").isNotNull();
        assertThat(callbackResult.get().dictId()).as("Callback should receive same dictionary").isEqualTo(dictionary.dictId());
    }

    @Test
    public void testTrainDictionaryAsyncForce() throws Exception
    {
        // Don't add enough samples
        Future<CompressionDictionary> future = startTraining(true, true, 512);
        CompressionDictionary dictionary = future.get(1, TimeUnit.SECONDS);
        assertThat(dictionary)
        .as("Forced async training should produce dictionary")
        .isNotNull();
    }

    @Test
    public void testTrainDictionaryAsyncForceFailsWithNoData() throws Exception
    {
        AtomicReference<CompressionDictionary> dictRef = new AtomicReference<>();
        Future<CompressionDictionary> result = startTraining(true, true, 0)
                                                          .addCallback((dict, t) -> dictRef.set(dict));

        assertThat(result.isDone() && result.cause() != null)
        .as("Result should be completed exceptionally")
        .isTrue();
        assertThat(trainer.getTrainingState().getStatus())
        .as("Status should be FAILED")
        .isEqualTo(TrainingStatus.FAILED);
        assertThat(dictRef.get())
        .as("Dictionary reference should be null")
        .isNull();
    }

    @Test
    public void testDictionaryTrainedListener()
    {
        trainer.start(true);
        addSampleData(testConfig.acceptableTotalSampleSize);

        // Train dictionary synchronously - callback should be called
        CompressionDictionary dictionary = trainer.trainDictionary(false);

        // Verify callback was invoked with the dictionary
        assertThat(callbackResult.get()).as("Callback should have been called").isNotNull();
        assertThat(callbackResult.get().dictId().id)
        .as("Callback should receive correct dictionary ID")
        .isEqualTo(dictionary.dictId().id);
        assertThat(callbackResult.get().kind())
        .as("Callback should receive correct dictionary kind")
        .isEqualTo(dictionary.kind());
    }

    @Test
    public void testMonotonicDictionaryIds()
    {
        long now = Clock.Global.currentTimeMillis();
        long id1 = ZstdDictionaryTrainer.makeDictionaryId(now, 100L);
        long hourLater= now + TimeUnit.HOURS.toMillis(1);
        long id2 = ZstdDictionaryTrainer.makeDictionaryId(hourLater, 200L);
        long id3 = ZstdDictionaryTrainer.makeDictionaryId(now, 200L);

        assertThat(id2)
        .as("Dictionary IDs should be monotonic over time")
        .isGreaterThan(id1)
        .isGreaterThan(id3);

        assertThat(id3).isNotEqualTo(id1).isNotEqualTo(id2);
    }

    @Test
    public void testIsCompatibleWith()
    {
        CompressionParams compatibleParams = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true,
                                                                    Map.of("compression_level", "3"));

        assertThat(trainer.isCompatibleWith(compatibleParams))
        .as("Should be compatible with same compression level")
        .isTrue();


        CompressionParams incompatibleParams = CompressionParams.lz4();

        assertThat(trainer.isCompatibleWith(incompatibleParams))
        .as("Should not be compatible with different compressor")
        .isFalse();

        CompressionParams differentLevelParams = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true,
                                                                        Map.of("compression_level", "4"));

        assertThat(trainer.isCompatibleWith(differentLevelParams))
        .as("Should not be compatible with different compression level")
        .isFalse();

        CompressionParams disabledParams = CompressionParams.noCompression();

        assertThat(trainer.isCompatibleWith(disabledParams))
        .as("Should not be compatible with disabled compression")
        .isFalse();
    }

    @Test
    public void testUpdateSamplingRate()
    {
        trainer.start(true);

        // Test updating to different valid sampling rates
        trainer.updateSamplingRate(10);

        // With sampling rate 10 (10%), should mostly return false
        int sampleCount = 0;
        int iterations = 1000;
        for (int i = 0; i < iterations; i++)
        {
            if (trainer.shouldSample())
            {
                sampleCount++;
            }
        }

        // Should be roughly 10% (1 out of 10), allow some variance
        assertThat(sampleCount)
        .as("Sample rate should be approximately 10%")
        .isGreaterThan(iterations / 20) // at least 5%
        .isLessThan(iterations / 5);    // at most 20%

        // Test updating to 100% sampling
        trainer.updateSamplingRate(1);

        // Should always sample now
        for (int i = 0; i < 10; i++)
        {
            assertThat(trainer.shouldSample())
            .as("Should always sample with rate 1")
            .isTrue();
        }
    }

    @Test
    public void testUpdateSamplingRateValidation()
    {
        trainer.start(true);

        // Test invalid sampling rates
        assertThatThrownBy(() -> trainer.updateSamplingRate(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Sampling rate must be positive");

        assertThatThrownBy(() -> trainer.updateSamplingRate(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Sampling rate must be positive");

        assertThatThrownBy(() -> trainer.updateSamplingRate(-100))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Sampling rate must be positive");
    }

    @Test
    public void testUpdateSamplingRateBeforeStart()
    {
        // Should be able to update sampling rate even before start
        trainer.updateSamplingRate(5);

        trainer.start(true);

        // Verify the updated rate is used after start
        int sampleCount = 0;
        int iterations = 1000;
        for (int i = 0; i < iterations; i++)
        {
            if (trainer.shouldSample())
            {
                sampleCount++;
            }
        }

        // Should be roughly 20% (1 out of 5), allow some variance
        assertThat(sampleCount)
        .as("Sample rate should be approximately 20%")
        .isGreaterThan(iterations / 10) // at least 10%
        .isLessThan(iterations / 2);    // at most 50%
    }

    private Future<CompressionDictionary> startTraining(boolean manualTraining, boolean forceTrain, int sampleSize) throws Exception
    {
        trainer.start(manualTraining);
        if (sampleSize > 0)
        {
            addSampleData(sampleSize);
        }

        if (forceTrain)
        {
            assertThat(trainer.isReady())
            .as("Trainer should not be ready to train due to lack of samples")
            .isFalse();
        }

        CountDownLatch latch = new CountDownLatch(1);
        Future<CompressionDictionary> future = trainer.trainDictionaryAsync(forceTrain)
                                                                 .addCallback((dict, throwable) -> latch.countDown());
        assertThat(latch.await(10, TimeUnit.SECONDS))
        .as("Training should complete within timeout")
        .isTrue();
        return future;
    }

    private void addSampleData(int totalSize)
    {
        byte[] sampleBytes = SAMPLE_DATA.getBytes();
        int samplesNeeded = (totalSize + sampleBytes.length - 1) / sampleBytes.length; // Round up

        for (int i = 0; i < samplesNeeded; i++)
        {
            ByteBuffer sample = ByteBuffer.wrap(sampleBytes);
            trainer.addSample(sample);
        }
    }

    @Test
    public void testStatisticsMethods()
    {
        assertThat(trainer.getTrainingState().getSampleCount())
        .as("Initial sample count should be 0")
        .isEqualTo(0);

        assertThat(trainer.getTrainingState().getTotalSampleSize())
        .as("Initial total sample size should be 0")
        .isEqualTo(0);

        // Start training
        trainer.start(true);

        // Add some samples
        byte[] sampleBytes = SAMPLE_DATA.getBytes();
        int sampleSize = sampleBytes.length;
        int numSamples = 5;

        for (int i = 0; i < numSamples; i++)
        {
            trainer.addSample(ByteBuffer.wrap(sampleBytes));
        }

        assertThat(trainer.getTrainingState().getSampleCount())
        .as("Sample count should be updated after adding samples")
        .isEqualTo(numSamples);

        assertThat(trainer.getTrainingState().getTotalSampleSize())
        .as("Total sample size should match number of samples times sample size")
        .isEqualTo((long) numSamples * sampleSize);

        trainer.reset();

        assertThat(trainer.getTrainingState().getSampleCount())
        .as("Sample count should be 0 after reset")
        .isEqualTo(0);

        assertThat(trainer.getTrainingState().getTotalSampleSize())
        .as("Total sample size should be 0 after reset")
        .isEqualTo(0);
    }
}
