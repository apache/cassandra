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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictTrainer;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;

/**
 * Zstd implementation of dictionary trainer with lifecycle management.
 */
public class ZstdDictionaryTrainer implements ICompressionDictionaryTrainer
{
    private static final Logger logger = LoggerFactory.getLogger(ZstdDictionaryTrainer.class);

    private final String keyspaceName;
    private final String tableName;
    private volatile CompressionDictionaryTrainingConfig config;
    private final AtomicLong totalSampleSize;
    private final AtomicLong sampleCount;
    private final int compressionLevel; // optimal if using the same level for training as when compressing.

    // Minimum number of samples required by ZSTD library
    private static final int MIN_SAMPLES_REQUIRED = 11;

    private volatile Consumer<CompressionDictionary> dictionaryTrainedListener;
    // TODO: manage the samples in this class for auto-train (follow-up). The ZstdDictTrainer cannot be re-used for multiple training runs.
    private volatile ZstdDictTrainer zstdTrainer;
    private volatile boolean closed = false;
    private volatile TrainingStatus currentTrainingStatus;
    private volatile String failureMessage;

    public ZstdDictionaryTrainer(String keyspaceName, String tableName, int compressionLevel)
    {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.totalSampleSize = new AtomicLong(0);
        this.sampleCount = new AtomicLong(0);
        this.compressionLevel = compressionLevel;
        this.currentTrainingStatus = TrainingStatus.NOT_STARTED;
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
            failureMessage = buildNotReadyMessage();
            currentTrainingStatus = TrainingStatus.FAILED;
            throw new IllegalStateException(failureMessage);
        }

        long currentSampleCount = sampleCount.get();
        if (currentSampleCount < MIN_SAMPLES_REQUIRED) // minimum samples should be required even if force training
        {
            failureMessage = String.format("Insufficient samples for training: %d (minimum required: %d)",
                                           currentSampleCount, MIN_SAMPLES_REQUIRED);
            currentTrainingStatus = TrainingStatus.FAILED;
            throw new IllegalStateException(failureMessage);
        }

        currentTrainingStatus = TrainingStatus.TRAINING;
        failureMessage = null; // Clear any previous failure message
        try
        {
            logger.debug("Training with sample count: {}, sample size: {}, isReady: {}",
                         currentSampleCount, totalSampleSize.get(), isReady);
            byte[] dictBytes = zstdTrainer.trainSamples();
            long zstdDictId = Zstd.getDictIdFromDict(dictBytes);
            DictId dictId = new DictId(Kind.ZSTD, makeDictionaryId(Clock.Global.currentTimeMillis(), zstdDictId));
            currentTrainingStatus = TrainingStatus.COMPLETED;
            logger.debug("New dictionary is trained with {}", dictId);
            int checksum = CompressionDictionary.calculateChecksum((byte) dictId.kind.ordinal(), dictId.id, dictBytes);
            CompressionDictionary dictionary = Kind.ZSTD.createDictionary(dictId, dictBytes, checksum);
            notifyDictionaryTrainedListener(dictionary);
            return dictionary;
        }
        catch (Exception e)
        {
            failureMessage = "Failed to train Zstd dictionary: " + e.getMessage();
            currentTrainingStatus = TrainingStatus.FAILED;
            throw new RuntimeException(failureMessage, e);
        }
    }

    @Override
    public Future<CompressionDictionary> trainDictionaryAsync(boolean force)
    {
        DictionaryTrainingTask task = new DictionaryTrainingTask(force);
        ScheduledExecutors.nonPeriodicTasks.execute(task);
        return task;
    }

    /**
     * Async task for training dictionary that handles failures without routing through JVMStabilityInspector.
     * Follows the pattern used by repair tasks (ValidationTask, SyncTask, etc.):
     * - Extends AsyncFuture and implements Runnable
     * - Never throws exceptions from run()
     * - Calls trySuccess()/tryFailure() to complete the future
     *
     * This ensures validation failures are handled cleanly without ERROR logging to JVMStabilityInspector,
     * while unexpected errors are still properly logged.
     */
    private class DictionaryTrainingTask extends AsyncFuture<CompressionDictionary> implements Runnable
    {
        private final boolean force;

        DictionaryTrainingTask(boolean force)
        {
            this.force = force;
        }

        @Override
        public void run()
        {
            try
            {
                CompressionDictionary dict = trainDictionary(force);
                trySuccess(dict);
            }
            catch (IllegalStateException e)
            {
                logger.debug("Dictionary training validation failed for {}.{}: {}",
                             keyspaceName, tableName, e.getMessage());
                tryFailure(e);
            }
            catch (Throwable t)
            {
                // Unexpected failures - log at error level for visibility
                logger.error("Unexpected error during dictionary training for {}.{}",
                             keyspaceName, tableName, t);
                tryFailure(t);
            }
        }
    }

    /**
     * Builds a detailed message explaining why the trainer is not ready.
     *
     * @return error message with details about what conditions are not met
     */
    private String buildNotReadyMessage()
    {
        StringBuilder message = new StringBuilder("Trainer is not ready");

        if (closed)
        {
            message.append(": trainer is closed");
            return message.toString();
        }

        if (currentTrainingStatus == TrainingStatus.TRAINING)
        {
            message.append(": training is already in progress");
            return message.toString();
        }

        if (zstdTrainer == null)
        {
            message.append(": trainer not initialized (call start() first)");
            return message.toString();
        }

        if (config == null)
        {
            message.append(": configuration not initialized (call start() first)");
            return message.toString();
        }

        long currentSampleCount = sampleCount.get();
        long currentTotalSampleSize = totalSampleSize.get();

        // Check both sample count and total sample size
        boolean hasEnoughSamples = currentSampleCount >= MIN_SAMPLES_REQUIRED;
        boolean hasEnoughSampleSize = currentTotalSampleSize >= config.acceptableTotalSampleSize;

        if (!hasEnoughSamples && !hasEnoughSampleSize)
        {
            message.append(String.format(": insufficient samples collected (have %d/%d samples, %s/%s). Use --force to train anyway.",
                                         currentSampleCount, MIN_SAMPLES_REQUIRED,
                                         FileUtils.stringifyFileSize(currentTotalSampleSize, true),
                                         FileUtils.stringifyFileSize(config.acceptableTotalSampleSize, true)));
        }
        else if (!hasEnoughSamples)
        {
            message.append(String.format(": insufficient sample count (have %d/%d samples). Use --force to train anyway.",
                                         currentSampleCount, MIN_SAMPLES_REQUIRED));
        }
        else if (!hasEnoughSampleSize)
        {
            message.append(String.format(": insufficient sample size (have %s/%s). Use --force to train anyway.",
                                         FileUtils.stringifyFileSize(currentTotalSampleSize, true),
                                         FileUtils.stringifyFileSize(config.acceptableTotalSampleSize, true)));
        }

        return message.toString();
    }

    @Override
    public boolean isReady()
    {
        return currentTrainingStatus != TrainingStatus.TRAINING
               && !closed
               && zstdTrainer != null
               && config != null
               && totalSampleSize.get() >= config.acceptableTotalSampleSize
               && sampleCount.get() >= MIN_SAMPLES_REQUIRED;
    }

    @Override
    public TrainingState getTrainingState()
    {
        long currentSampleCount = sampleCount.get();
        long currentTotalSampleSize = totalSampleSize.get();

        switch (currentTrainingStatus)
        {
            case NOT_STARTED:
                return TrainingState.notStarted();
            case SAMPLING:
                return TrainingState.sampling(currentSampleCount, currentTotalSampleSize);
            case TRAINING:
                return TrainingState.training(currentSampleCount, currentTotalSampleSize);
            case COMPLETED:
                return TrainingState.completed(currentSampleCount, currentTotalSampleSize);
            case FAILED:
                return TrainingState.failed(failureMessage, currentSampleCount, currentTotalSampleSize);
            default:
                throw new IllegalStateException("Unknown training status: " + currentTrainingStatus);
        }
    }

    @Override
    public boolean start(CompressionDictionaryTrainingConfig trainingConfig)
    {
        if (closed)
            return false;

        try
        {
            // reset on starting; a new zstdTrainer instance is created during reset
            reset(trainingConfig);
            currentTrainingStatus = TrainingStatus.SAMPLING;
            failureMessage = null; // Clear any previous failure message
            return true;
        }
        catch (Exception e)
        {
            String message = String.format("Failed to create %s for %s.%s, reason: %s",
                                           ZstdDictTrainer.class.getSimpleName(),
                                           keyspaceName,
                                           tableName,
                                           e.getMessage());

            logger.warn(message);
            failureMessage = message;
            currentTrainingStatus = TrainingStatus.FAILED;
        }
        return false;
    }

    @Override
    public void reset(CompressionDictionaryTrainingConfig trainingConfig)
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
            try
            {
                zstdTrainer = new ZstdDictTrainer(trainingConfig.maxTotalSampleSize, trainingConfig.maxDictionarySize, compressionLevel);
            }
            catch (Throwable t)
            {
                throw new IllegalStateException(t);
            }

            config = trainingConfig;
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
     * Custom epoch for dictionary ID timestamps: October 20, 2025 00:00:00 UTC
     * Calculated as: Instant.parse("2025-10-20T00:00:00Z").toEpochMilli()
     * This allows using signed 32-bit seconds for ±68 years (~1957 to ~2093)
     */
    public static final long CUSTOM_EPOCH_MILLIS = 1760889600000L;

    /**
     * Creates a monotonically increasing dictionary ID by combining timestamp and Zstd dictionary ID.
     * This is a public API to support external dictionary imports.
     * <p>
     * The resulting dictionary ID has the following structure:
     * - Upper 32 bits (first 4 bytes): timestamp in seconds since custom epoch (signed int)
     * - Lower 32 bits (last 4 bytes): Zstd dictionary ID (unsigned int, passed as long)
     * <p>
     * Custom epoch: October 20, 2025 00:00:00 UTC ({@link #CUSTOM_EPOCH_MILLIS})
     * <p>
     * Using signed 32-bit seconds allows representing ±2,147,483,648 seconds (±68.1 years),
     * giving a valid range from ~1957 to ~2093, which is sufficient for the software's lifespan.
     * <p>
     * This ensures dictionary IDs are monotonically increasing over time, helping to identify
     * the latest dictionary.
     *
     * @param currentTimeMillis the current time in milliseconds since Unix epoch
     * @param dictId            Zstd dictionary ID (unsigned 32-bit value represented as long)
     * @return combined dictionary ID that is monotonically increasing over time
     */
    public static long makeDictionaryId(long currentTimeMillis, long dictId)
    {
        // timestamp in seconds since custom epoch
        long timestampSeconds = (currentTimeMillis - CUSTOM_EPOCH_MILLIS) / 1000;
        // Shift timestamp to upper 32 bits
        long combined = timestampSeconds << 32;

        // Add the unsigned int (already as long) to lower 32 bits
        combined |= (dictId & 0xFFFFFFFFL);

        return combined;
    }

    @VisibleForTesting
    Object trainer()
    {
        return zstdTrainer;
    }
}
