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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictTrainer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.io.compress.IDictionaryCompressor;
import org.apache.cassandra.io.compress.ZstdDictionaryCompressor;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.Clock;

/**
 * Zstd implementation of dictionary trainer with lifecycle management.
 */
public class ZstdDictionaryTrainer implements ICompressionDictionaryTrainer
{
    private static final Logger logger = LoggerFactory.getLogger(ZstdDictionaryTrainer.class);

    private final String keyspaceName;
    private final String tableName;
    private final CompressionDictionaryTrainingConfig config;
    private final AtomicLong totalSampleSize;
    private final AtomicLong sampleCount;
    private final int compressionLevel; // optimal if using the same level for training as when compressing.

    // Sampling rate can be updated during training
    private volatile int samplingRate;

    // Minimum number of samples required by ZSTD library
    private static final int MIN_SAMPLES_REQUIRED = 10;

    private volatile Consumer<CompressionDictionary> dictionaryTrainedListener;
    // TODO: manage the samples in this class for auto-train (follow-up). The ZstdDictTrainer cannot be re-used for multiple training runs.
    private ZstdDictTrainer zstdTrainer;
    private volatile boolean closed = false;
    private volatile TrainingStatus currentTrainingStatus;

    public ZstdDictionaryTrainer(String keyspaceName, String tableName,
                                 CompressionDictionaryTrainingConfig config,
                                 int compressionLevel)
    {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.config = config;
        this.totalSampleSize = new AtomicLong(0);
        this.sampleCount = new AtomicLong(0);
        this.compressionLevel = compressionLevel;
        this.samplingRate = config.samplingRate;
        this.currentTrainingStatus = TrainingStatus.NOT_STARTED;
    }

    @Override
    public boolean shouldSample()
    {
        return zstdTrainer != null && ThreadLocalRandom.current().nextInt(samplingRate) == 0;
    }

    @Override
    public void addSample(ByteBuffer sample)
    {
        if (closed || sample == null || !sample.hasRemaining() || zstdTrainer == null)
            return;

        byte[] sampleBytes = new byte[sample.remaining()];
        sample.duplicate().get(sampleBytes);

        if (zstdTrainer.addSample(sampleBytes))
        {
            // Update the totalSampleSize and sampleCount if the sample is added
            totalSampleSize.addAndGet(sampleBytes.length);
            sampleCount.incrementAndGet();
        }
    }

    @Override
    public CompressionDictionary trainDictionary(boolean force)
    {
        boolean isReady = isReady();
        if (!force && !isReady)
        {
            currentTrainingStatus = TrainingStatus.FAILED;
            throw new IllegalStateException("Trainer is not ready");
        }

        long currentSampleCount = sampleCount.get();
        if (currentSampleCount < MIN_SAMPLES_REQUIRED) // minimum samples should be required even if force training
        {
            currentTrainingStatus = TrainingStatus.FAILED;
            String errorMsg = String.format("Insufficient samples for training: %d (minimum required: %d)",
                                            currentSampleCount, MIN_SAMPLES_REQUIRED);
            throw new IllegalStateException(errorMsg);
        }

        currentTrainingStatus = TrainingStatus.TRAINING;
        try
        {
            logger.debug("Training with sample count: {}, sample size: {}, isReady: {}",
                        currentSampleCount, totalSampleSize.get(), isReady);
            byte[] dictBytes = zstdTrainer.trainSamples();
            long zstdDictId = Zstd.getDictIdFromDict(dictBytes);
            DictId dictId = new DictId(Kind.ZSTD, makeDictionaryId(Clock.Global.currentTimeMillis(), zstdDictId));
            currentTrainingStatus = TrainingStatus.COMPLETED;
            logger.debug("New dictionary is trained with {}", dictId);
            CompressionDictionary dictionary = new ZstdCompressionDictionary(dictId, dictBytes);
            notifyDictionaryTrainedListener(dictionary);
            return dictionary;
        }
        catch (Exception e)
        {
            currentTrainingStatus = TrainingStatus.FAILED;
            throw new RuntimeException("Failed to train Zstd dictionary", e);
        }
    }

    @Override
    public boolean isReady()
    {
        return currentTrainingStatus != TrainingStatus.TRAINING
               && !closed
               && zstdTrainer != null
               && totalSampleSize.get() >= config.acceptableTotalSampleSize
               && sampleCount.get() > MIN_SAMPLES_REQUIRED;
    }

    @Override
    public TrainingStatus getTrainingStatus()
    {
        return currentTrainingStatus;
    }

    @Override
    public boolean start(boolean manualTraining)
    {
        if (closed || !(manualTraining || shouldAutoStartTraining()))
            return false;

        try
        {
            // reset on starting; a new zstdTrainer instance is created during reset
            reset();
            logger.info("Started dictionary training for {}.{}", keyspaceName, tableName);
            currentTrainingStatus = TrainingStatus.SAMPLING;
            return true;
        }
        catch (Exception e)
        {
            logger.warn("Failed to create ZstdDictTrainer for {}.{}", keyspaceName, tableName, e);
            currentTrainingStatus = TrainingStatus.FAILED;
        }
        return false;
    }

    /**
     * Determines if training should auto-start based on configuration.
     */
    private boolean shouldAutoStartTraining()
    {
        return DatabaseDescriptor.getCompressionDictionaryTrainingAutoTrainEnabled();
    }

    @Override
    public void reset()
    {
        if (closed)
        {
            return;
        }

        currentTrainingStatus = TrainingStatus.NOT_STARTED;
        synchronized (this)
        {
            totalSampleSize.set(0);
            sampleCount.set(0);
            zstdTrainer = new ZstdDictTrainer(config.maxTotalSampleSize, config.maxDictionarySize, compressionLevel);
        }
    }

    @Override
    public Kind kind()
    {
        return Kind.ZSTD;
    }

    @Override
    public void setDictionaryTrainedListener(Consumer<CompressionDictionary> listener)
    {
        this.dictionaryTrainedListener = listener;
    }

    @Override
    public void updateSamplingRate(int newSamplingRate)
    {
        if (newSamplingRate <= 0)
        {
            throw new IllegalArgumentException("Sampling rate must be positive, got: " + newSamplingRate);
        }
        this.samplingRate = newSamplingRate;
        logger.debug("Updated sampling rate to {} for {}.{}", newSamplingRate, keyspaceName, tableName);
    }

    /**
     * Notifies the registered listener that a dictionary has been trained.
     *
     * @param dictionary the newly trained dictionary
     */
    private void notifyDictionaryTrainedListener(CompressionDictionary dictionary)
    {
        Consumer<CompressionDictionary> listener = this.dictionaryTrainedListener;
        if (listener != null)
        {
            try
            {
                listener.accept(dictionary);
            }
            catch (Exception e)
            {
                logger.warn("Error notifying dictionary trained listener for {}.{}", keyspaceName, tableName, e);
            }
        }
    }

    @Override
    public boolean isCompatibleWith(CompressionParams newParams)
    {
        if (!newParams.isDictionaryCompressionEnabled())
        {
            return false;
        }

        IDictionaryCompressor newCompressor = (IDictionaryCompressor) newParams.getSstableCompressor();

        // Check if the compressor type is compatible with this trainer
        if (newCompressor.acceptableDictionaryKind() != Kind.ZSTD)
        {
            return false;
        }

        ZstdDictionaryCompressor zstdDictionaryCompressor = (ZstdDictionaryCompressor) newCompressor;
        // For Zstd compressors, check if compression level matches
        return this.compressionLevel == zstdDictionaryCompressor.compressionLevel();
    }

    @Override
    public void close()
    {
        if (closed)
            return;

        closed = true;
        currentTrainingStatus = TrainingStatus.NOT_STARTED;

        synchronized (this)
        {
            // Permanent shutdown: clear all state and prevent restart
            totalSampleSize.set(0);
            sampleCount.set(0);
            zstdTrainer = null;
        }

        logger.info("Permanently closed dictionary trainer for {}.{}", keyspaceName, tableName);
    }

    /**
     * Creates a monotonically increasing dictionary ID by combining timestamp and dictionary ID.
     * <p>
     * The resulting dictionary ID has the following structure:
     * - Upper 32 bits: timestamp in minutes (signed int)
     * - Lower 32 bits: Zstd dictionary ID (unsigned int, passed as long due to Java limitations)
     * <p>
     * This ensures dictionary IDs are monotonically increasing over time, which helps to identify
     * the latest dictionary.
     * <p>
     * The implementation assumes that dictionary training frequency is significantly larger than
     * every minute, which a healthy system should do. In the scenario when multiple dictionaries
     * are trained in the same minute (only possible using manual training), there should not be
     * correctness concerns since the dictionary is attached to the SSTables, but leads to performance
     * hit from having too many dictionary. Therefore, such scenario should be avoided at the best.
     *
     * @param currentTimeMillis the current time in milliseconds
     * @param dictId            dictionary ID (unsigned 32-bit value represented as long)
     * @return combined dictionary ID that is monotonically increasing over time
     */
    static long makeDictionaryId(long currentTimeMillis, long dictId)
    {
        // timestamp in minutes since Unix epoch. Good until year 6053
        long timestampMinutes = currentTimeMillis / 1000 / 60;
        // Convert timestamp to long and shift to upper 32 bits
        long combined = timestampMinutes << 32;

        // Add the unsigned int (already as long) to lower 32 bits
        combined |= (dictId & 0xFFFFFFFFL);

        return combined;
    }

    @VisibleForTesting
    long getSampleCount()
    {
        return sampleCount.get();
    }

    @VisibleForTesting
    Object trainer()
    {
        return zstdTrainer;
    }
}
