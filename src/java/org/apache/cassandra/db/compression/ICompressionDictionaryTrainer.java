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
import java.util.function.Consumer;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.IDictionaryCompressor;
import org.apache.cassandra.schema.CompressionParams;

/**
 * Interface for training compression dictionaries from sample data.
 * <p>
 * Implementations handle:
 * - Sample collection and management
 * - Dictionary training lifecycle
 * - Asynchronous training execution
 * - Training status tracking
 */
public interface ICompressionDictionaryTrainer extends AutoCloseable
{
    /**
     * Starts the trainer for collecting samples.
     *
     * @param manualTraining true if this is manual training, false for automatic
     * @return true if the trainer is started; otherwise false. The trainer is started
     *         in any of those conditions: 1. trainer closed; 2. not requested for
     *         either manual or auto training; 3. failed to start
     */
    boolean start(boolean manualTraining);

    /**
     * @return true if the trainer is ready to take a new sample; otherwise, false
     */
    boolean shouldSample();

    /**
     * Adds a sample to the training dataset.
     *
     * @param sample the sample data to add for training
     */
    void addSample(ByteBuffer sample);

    /**
     * Trains and produces a compression dictionary from collected samples synchronously.
     *
     * @param force force the dictionary training even if there are not enough samples;
     *              otherwise, dictionary training won't start if the trainer is not ready
     * @return the trained compression dictionary
     */
    CompressionDictionary trainDictionary(boolean force);

    /**
     * Trains and produces a compression dictionary from collected samples asynchronously.
     *
     * @param force force the dictionary training even if there are not enough samples
     * @return Future that completes when training is done
     */
    default Future<CompressionDictionary> trainDictionaryAsync(boolean force)
    {
        return ScheduledExecutors.nonPeriodicTasks.submit(() -> trainDictionary(force));
    }

    /**
     * @return true if enough samples have been collected for training
     */
    boolean isReady();

    /**
     * Clears all collected samples and resets trainer state.
     */
    void reset();

    /**
     * @return the current training status
     */
    TrainingStatus getTrainingStatus();

    /**
     * @return the compression algorithm kind this trainer supports
     */
    CompressionDictionary.Kind kind();

    /**
     * Determines if this trainer is compatible with the given compression parameters.
     * This method allows the trainer to decide whether it can continue operating
     * with new compression parameters or if a new trainer instance is needed.
     *
     * @param newParams the new compression parameters to check compatibility against
     * @return true if this trainer is compatible with the new parameters, false otherwise
     */
    boolean isCompatibleWith(CompressionParams newParams);

    /**
     * Sets the listener for dictionary training events.
     *
     * @param listener the listener to be notified when dictionaries are trained, null to remove listener
     */
    void setDictionaryTrainedListener(Consumer<CompressionDictionary> listener);

    /**
     * Updates the sampling rate for this trainer.
     *
     * @param newSamplingRate the new sampling rate. For exmaple, 1 = sample every time (100%),
     *                        2 = expect sample 1/2 of data (50%), n = expect sample 1/n of data
     */
    void updateSamplingRate(int newSamplingRate);

    /**
     * Gets the number of samples collected so far during training.
     *
     * @return the number of samples collected, or 0 if training hasn't started
     */
    long getSampleCount();

    /**
     * Gets the total size of samples collected so far during training.
     *
     * @return the total sample size in bytes, or 0 if training hasn't started
     */
    long getTotalSampleSize();

    /**
     * Factory method to create appropriate trainer based on compression parameters.
     *
     * @param keyspaceName the keyspace name for logging
     * @param tableName the table name for logging
     * @param params the compression parameters
     * @param config the training configuration
     * @return a dictionary trainer for the specified compression algorithm
     * @throws IllegalArgumentException if no dictionary trainer is available for the compression algorithm
     */
    static ICompressionDictionaryTrainer create(String keyspaceName,
                                                String tableName,
                                                CompressionParams params,
                                                CompressionDictionaryTrainingConfig config)
    {
        ICompressor compressor = params.getSstableCompressor();
        if (!(compressor instanceof IDictionaryCompressor))
        {
            throw new IllegalArgumentException("Compressor does not support dictionary training: " + params.getSstableCompressor());
        }

        IDictionaryCompressor dictionaryCompressor = (IDictionaryCompressor) compressor;
        return dictionaryCompressor.acceptableDictionaryKind().createTrainer(keyspaceName, tableName, config, compressor);
    }

    enum TrainingStatus
    {
        NOT_STARTED,
        SAMPLING,
        TRAINING,
        COMPLETED,
        FAILED;
    }
}
