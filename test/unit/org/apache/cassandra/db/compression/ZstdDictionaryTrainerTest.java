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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.Future;

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
                                                        .build();

        callbackResult = new AtomicReference<>();
        Consumer<CompressionDictionary> mockCallback = callbackResult::set;

        trainer = new ZstdDictionaryTrainer(TEST_KEYSPACE, TEST_TABLE, COMPRESSION_LEVEL);
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
        boolean started = trainer.start(testConfig);
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
        assertThat(trainer.start(testConfig))
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
        assertThat(trainer.start(testConfig))
        .as("First start (manual training) should succeed")
        .isTrue();
        Object firstTrainer = trainer.trainer();
        assertThat(firstTrainer).isNotNull();
        assertThat(trainer.start(testConfig))
        .as("Second start (manual training) should suceed and reset")
        .isTrue();
        Object secondTrainer = trainer.trainer();
        assertThat(secondTrainer).isNotNull().isNotSameAs(firstTrainer);
    }

    @Test
    public void testTrainerCloseIdempotent()
    {
        trainer.start(testConfig);
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
        trainer.start(testConfig);
        addSampleData(1000); // Add some samples

        assertThat(trainer.getTrainingState().getSampleCount())
        .as("Should have samples before reset")
        .isGreaterThan(0);

        trainer.reset(testConfig);
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
        trainer.start(testConfig);
        trainer.close();

        assertThat(trainer.start(testConfig))
        .as("Should not start after close")
        .isFalse();
        assertThat(trainer.getTrainingState().getStatus())
        .as("Status should remain NOT_STARTED")
        .isEqualTo(TrainingStatus.NOT_STARTED);
    }

    @Test
    public void testAddSample()
    {
        trainer.start(testConfig);

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
        trainer.start(testConfig);
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
        trainer.start(testConfig);
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
        trainer.start(testConfig);
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
        trainer.start(testConfig);
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
        trainer.start(testConfig);

        // Add sufficient data size but only 5 samples (less than minimum 11)
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

        // Trying to train without force should fail with detailed message
        assertThatThrownBy(() -> trainer.trainDictionary(false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Trainer is not ready")
        .hasMessageContaining("insufficient samples collected")
        .hasMessageContaining("have 5/11 samples")
        .hasMessageContaining("Use --force to train anyway");

        // Force training should fail with insufficient samples (below absolute minimum)
        assertThatThrownBy(() -> trainer.trainDictionary(true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Insufficient samples for training: 5 (minimum required: 11)");
    }

    @Test
    public void testTrainDictionaryWithSufficientSampleCount()
    {
        trainer.start(testConfig);

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
        Future<CompressionDictionary> future = startTraining(false, testConfig);
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
        Future<CompressionDictionary> future = startTraining(true, testConfig, 512);
        CompressionDictionary dictionary = future.get(1, TimeUnit.SECONDS);
        assertThat(dictionary)
        .as("Forced async training should produce dictionary")
        .isNotNull();
    }

    @Test
    public void testTrainDictionaryAsyncForceFailsWithNoData() throws Exception
    {
        AtomicReference<CompressionDictionary> dictRef = new AtomicReference<>();
        Future<CompressionDictionary> result = startTraining(true, testConfig, 0)
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
        trainer.start(testConfig);
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
    public void testTrainDictionaryNotInitialized()
    {
        // Try to train without starting
        assertThatThrownBy(() -> trainer.trainDictionary(false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Trainer is not ready")
        .hasMessageContaining("trainer not initialized")
        .hasMessageContaining("call start() first");
    }

    @Test
    public void testTrainDictionaryClosed()
    {
        trainer.start(testConfig);
        addSampleData(testConfig.acceptableTotalSampleSize);
        trainer.close();

        // Try to train after closing
        assertThatThrownBy(() -> trainer.trainDictionary(false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Trainer is not ready")
        .hasMessageContaining("trainer is closed");
    }

    @Test
    public void testTrainDictionaryInsufficientSampleSize()
    {
        trainer.start(testConfig);

        // Add enough samples (15) but with insufficient total size
        for (int i = 0; i < 15; i++)
        {
            ByteBuffer smallSample = ByteBuffer.wrap(new byte[10]);
            trainer.addSample(smallSample);
        }

        assertThat(trainer.getTrainingState().getSampleCount())
        .as("Should have 15 samples")
        .isEqualTo(15);
        assertThat(trainer.getTrainingState().getTotalSampleSize())
        .as("Total sample size should be small")
        .isLessThan(testConfig.acceptableTotalSampleSize);
        assertThat(trainer.isReady())
        .as("Should not be ready with insufficient sample size")
        .isFalse();

        // Trying to train without force should fail with detailed message
        assertThatThrownBy(() -> trainer.trainDictionary(false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Trainer is not ready")
        .hasMessageContaining("insufficient sample size")
        .hasMessageContaining("have 150 bytes/8 KiB")
        .hasMessageContaining("Use --force to train anyway");
    }

    @Test
    public void testTrainDictionaryInsufficientBothSampleCountAndSize()
    {
        trainer.start(testConfig);

        // Add only 3 samples with small size
        for (int i = 0; i < 3; i++)
        {
            ByteBuffer smallSample = ByteBuffer.wrap(new byte[10]);
            trainer.addSample(smallSample);
        }

        assertThat(trainer.getTrainingState().getSampleCount())
        .as("Should have 3 samples")
        .isEqualTo(3);
        assertThat(trainer.isReady())
        .as("Should not be ready with insufficient samples and size")
        .isFalse();

        // Trying to train without force should fail with detailed message showing both issues
        assertThatThrownBy(() -> trainer.trainDictionary(false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Trainer is not ready")
        .hasMessageContaining("insufficient samples collected")
        .hasMessageContaining("have 3/11 samples, 30 bytes/8 KiB")
        .hasMessageContaining("Use --force to train anyway");
    }

    private Future<CompressionDictionary> startTraining(boolean forceTrain, CompressionDictionaryTrainingConfig config, int sampleSize) throws Exception
    {
        trainer.start(config);
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

    private Future<CompressionDictionary> startTraining(boolean forceTrain, CompressionDictionaryTrainingConfig config) throws Exception
    {
        return startTraining(forceTrain, config, config.acceptableTotalSampleSize);
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
        trainer.start(testConfig);

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

        trainer.reset(testConfig);

        assertThat(trainer.getTrainingState().getSampleCount())
        .as("Sample count should be 0 after reset")
        .isEqualTo(0);

        assertThat(trainer.getTrainingState().getTotalSampleSize())
        .as("Total sample size should be 0 after reset")
        .isEqualTo(0);
    }
}
