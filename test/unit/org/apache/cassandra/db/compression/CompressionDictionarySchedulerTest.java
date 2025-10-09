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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.Util.spinUntilTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompressionDictionarySchedulerTest
{
    private static final String TEST_NAME = "compression_dict_scheduler_test_";
    private static final String KEYSPACE = TEST_NAME + "keyspace";
    private static final String TABLE = "test_table";

    private CompressionDictionaryScheduler scheduler;
    private TestDictionaryTrainer testTrainer;
    private ZstdCompressionDictionary testDictionary;
    private ICompressionDictionaryCache testCache;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        ServerTestUtils.prepareServerNoRegister();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1));
    }

    @Before
    public void setUp()
    {
        testTrainer = new TestDictionaryTrainer();
        testDictionary = createTestDictionary();
        testCache = new CompressionDictionaryCache();
        scheduler = new CompressionDictionaryScheduler(KEYSPACE, TABLE, testCache, true);
    }

    @After
    public void tearDown() throws Exception
    {
        if (scheduler != null)
        {
            scheduler.close();
        }
        if (testDictionary != null)
        {
            testDictionary.close();
        }
        if (testCache != null)
        {
            testCache.close();
        }
    }

    @Test
    public void testScheduleManualTraining()
    {
        testManualTraining(false, new ManualTrainingOptions(600));
    }

    @Test
    public void testScheduleManualTrainingWithCustomDuration()
    {
        testManualTraining(true, new ManualTrainingOptions(1));
    }

    @Test
    public void testConcurrentTraining()
    {
        ManualTrainingOptions options = new ManualTrainingOptions(600);

        testTrainer.setReady(true);
        testTrainer.setTrainingResult(ImmediateFuture.success(testDictionary));

        // Schedule first training
        scheduler.scheduleManualTraining(options, testTrainer);

        // Attempt to schedule second training should fail
        assertThatThrownBy(() -> scheduler.scheduleManualTraining(options, testTrainer))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Training already in progress");
    }

    @Test
    public void testManualTrainingFailure()
    {
        ManualTrainingOptions options = new ManualTrainingOptions(600);

        testTrainer.setReady(true);
        testTrainer.setTrainingResult(ImmediateFuture.failure(new RuntimeException("Training failed")));

        scheduler.scheduleManualTraining(options, testTrainer);

        // Expect the trainer to fail
        spinUntilTrue(() -> testTrainer.getTrainingStatus() == TrainingStatus.FAILED, 5);
    }

    @Test
    public void testTrainerNotStarted()
    {
        ManualTrainingOptions options = new ManualTrainingOptions(600);

        testTrainer.setTrainingStatus(TrainingStatus.NOT_STARTED);

        scheduler.scheduleManualTraining(options, testTrainer);
        assertThat((Object) scheduler.scheduledManualTrainingTask()).isNotNull();

        // Expect the manual training task to be cleaned up
        spinUntilTrue(() -> scheduler.scheduledManualTrainingTask() == null, 5);
    }

    private void testManualTraining(boolean expectForceTraining, ManualTrainingOptions trainOptions)
    {
        boolean ready = !expectForceTraining;
        testTrainer.setReady(ready);
        testTrainer.setTrainingResult(ImmediateFuture.success(testDictionary));
        AtomicReference<CompressionDictionary> dictHolder = new AtomicReference<>();
        testTrainer.setDictionaryTrainedListener(dictHolder::set);

        assertThat(dictHolder.get())
        .as("No dictionary is available before training")
        .isNull();

        scheduler.scheduleManualTraining(trainOptions, testTrainer);

        // Wait until dictionary is trained and notified
        spinUntilTrue(() -> dictHolder.get() == testDictionary, 5);
        assertThat(testTrainer.isForceTrained).isEqualTo(expectForceTraining);

        assertThat(testTrainer.getTrainDictionaryAsyncCallCount())
        .as("trainDictionaryAsync should be called")
        .isGreaterThan(0);
    }

    private static ZstdCompressionDictionary createTestDictionary()
    {
        byte[] dictBytes = "test dictionary data for scheduler testing".getBytes();
        DictId dictId = new DictId(Kind.ZSTD, Clock.Global.currentTimeMillis());
        return new ZstdCompressionDictionary(dictId, dictBytes);
    }

    /**
     * Test implementation of dictionary trainer
     */
    private static class TestDictionaryTrainer implements ICompressionDictionaryTrainer
    {
        public volatile boolean isForceTrained = false;
        private final AtomicInteger trainDictionaryAsyncCallCount = new AtomicInteger(0);
        private volatile TrainingStatus trainingStatus = TrainingStatus.SAMPLING;
        private volatile boolean ready = false;
        private volatile Future<CompressionDictionary> trainingResult = null;
        private volatile Consumer<CompressionDictionary> onDictionaryTrained = null;

        @Override
        public boolean shouldSample()
        {
            return true;
        }

        @Override
        public void addSample(java.nio.ByteBuffer sample)
        {
            // No-op for testing
        }

        @Override
        public CompressionDictionary trainDictionary(boolean force)
        {
            throw new RuntimeException("Not expected to be called in test");
        }

        @Override
        public Future<CompressionDictionary> trainDictionaryAsync(boolean force)
        {
            trainDictionaryAsyncCallCount.incrementAndGet();
            isForceTrained = force;
            if (trainingResult != null)
            {
                if (trainingResult.isDone() && trainingResult.cause() != null)
                {
                    trainingStatus = TrainingStatus.FAILED;
                }
                else
                {
                    trainingStatus = TrainingStatus.COMPLETED;
                    try
                    {
                        onDictionaryTrained.accept(trainingResult.get());
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                }
                return trainingResult;
            }

            return ImmediateFuture.success(createTestDictionary());
        }

        @Override
        public boolean isReady()
        {
            return ready;
        }

        @Override
        public void reset()
        {
            trainingStatus = TrainingStatus.NOT_STARTED;
            ready = false;
        }

        @Override
        public TrainingStatus getTrainingStatus()
        {
            return trainingStatus;
        }

        @Override
        public boolean start(boolean manualTraining)
        {
            if (trainingStatus == TrainingStatus.NOT_STARTED)
            {
                trainingStatus = TrainingStatus.SAMPLING;
                return true;
            }
            return false;
        }

        @Override
        public Kind kind()
        {
            return Kind.ZSTD;
        }

        @Override
        public boolean isCompatibleWith(CompressionParams newParams)
        {
            return true; // Simplified for testing
        }

        @Override
        public void close()
        {
            trainingStatus = TrainingStatus.NOT_STARTED;
        }

        @Override
        public void setDictionaryTrainedListener(Consumer<CompressionDictionary> listener)
        {
            this.onDictionaryTrained = listener;
        }

        @Override
        public void updateSamplingRate(int newSamplingRate)
        {
            // not used in test
        }

        @Override
        public long getSampleCount()
        {
            return 0;
        }

        @Override
        public long getTotalSampleSize()
        {
            return 0;
        }

        // Test helper methods
        public void setReady(boolean ready)
        {
            this.ready = ready;
        }

        public void setTrainingStatus(TrainingStatus status)
        {
            this.trainingStatus = status;
        }

        public void setTrainingResult(Future<CompressionDictionary> result)
        {
            this.trainingResult = result;
        }

        public int getTrainDictionaryAsyncCallCount()
        {
            return trainDictionaryAsyncCallCount.get();
        }
    }
}
