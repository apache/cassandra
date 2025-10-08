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
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.MBeanWrapper.OnException;

public class CompressionDictionaryManager implements CompressionDictionaryManagerMBean,
                                                     ICompressionDictionaryCache,
                                                     ICompressionDictionaryEventHandler,
                                                     AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(CompressionDictionaryManager.class);

    private final String keyspaceName;
    private final String tableName;
    private volatile boolean mbeanRegistered;
    private volatile boolean isEnabled;

    // Components
    private final ICompressionDictionaryEventHandler eventHandler;
    private final ICompressionDictionaryCache cache;
    private final ICompressionDictionaryScheduler scheduler;
    private ICompressionDictionaryTrainer trainer = null;

    public CompressionDictionaryManager(ColumnFamilyStore columnFamilyStore, boolean registerBookkeeping)
    {
        this.keyspaceName = columnFamilyStore.keyspace.getName();
        this.tableName = columnFamilyStore.getTableName();

        this.isEnabled = columnFamilyStore.metadata().params.compression.isDictionaryCompressionEnabled();
        this.cache = new CompressionDictionaryCache();
        this.eventHandler = new CompressionDictionaryEventHandler(columnFamilyStore, cache);
        this.scheduler = new CompressionDictionaryScheduler(keyspaceName, tableName, cache, isEnabled);
        if (isEnabled)
        {
            // Initialize components
            this.trainer = ICompressionDictionaryTrainer.create(keyspaceName, tableName,
                                                                columnFamilyStore.metadata().params.compression,
                                                                createTrainingConfig());
            trainer.setDictionaryTrainedListener(this::handleNewDictionary);

            scheduler.scheduleRefreshTask();

            trainer.start(false);
        }

        if (registerBookkeeping)
        {
            MBeanWrapper.instance.registerMBean(this, mbeanName(keyspaceName, tableName));
        }
        mbeanRegistered = registerBookkeeping;
    }

    static String mbeanName(String keyspaceName, String tableName)
    {
        return MBEAN_NAME + ",keyspace=" + keyspaceName + ",table=" + tableName;
    }

    public boolean isEnabled()
    {
        return isEnabled;
    }

    /**
     * Reloads dictionary management configuration when compression parameters change.
     * This method enables or disables dictionary compression based on the new parameters,
     * and properly manages the lifecycle of training and refresh tasks.
     *
     * @param newParams the new compression parameters to apply
     */
    public synchronized void maybeReloadFromSchema(CompressionParams newParams)
    {
        this.isEnabled = newParams.isDictionaryCompressionEnabled();
        scheduler.setEnabled(isEnabled);
        if (isEnabled)
        {
            // Check if we need a new trainer due to compression parameter changes
            boolean needsNewTrainer = shouldCreateNewTrainer(newParams);

            if (needsNewTrainer)
            {
                // The manual training should be cancelled if a new trainer is needed
                scheduler.cancelManualTraining();
                // Close existing trainer and create a new one
                if (trainer != null)
                {
                    try
                    {
                        trainer.close();
                    }
                    catch (Exception e)
                    {
                        logger.warn("Failed to close existing trainer for {}.{}", keyspaceName, tableName, e);
                    }
                }

                trainer = ICompressionDictionaryTrainer.create(keyspaceName, tableName, newParams, createTrainingConfig());
                trainer.setDictionaryTrainedListener(this::handleNewDictionary);
            }

            scheduler.scheduleRefreshTask();

            // Start trainer if it exists
            if (trainer != null)
            {
                trainer.start(false);
            }
            return;
        }

        // Clean up when dictionary compression is disabled
        try
        {
            close();
        }
        catch (Exception e)
        {
            logger.warn("Failed to close CompressionDictionaryManager on disabling " +
                        "dictionary-based compression for table {}.{}", keyspaceName, tableName);
        }
    }

    /**
     * Adds a sample to the dictionary trainer for learning compression patterns.
     * Samples are randomly selected to avoid bias and improve dictionary quality.
     *
     * @param sample the sample data to potentially add for training
     */
    public void addSample(ByteBuffer sample)
    {
        ICompressionDictionaryTrainer dictionaryTrainer = trainer;
        if (dictionaryTrainer != null && dictionaryTrainer.shouldSample())
        {
            dictionaryTrainer.addSample(sample);
        }
    }
    
    @Nullable
    @Override
    public CompressionDictionary getCurrent()
    {
        return cache.getCurrent();
    }

    @Override
    public CompressionDictionary get(CompressionDictionary.DictId dictId)
    {
        return cache.get(dictId);
    }

    @Override
    public void add(CompressionDictionary compressionDictionary)
    {
        cache.add(compressionDictionary);
    }

    @Override
    public void setCurrentIfNewer(@Nullable CompressionDictionary dictionary)
    {
        cache.setCurrentIfNewer(dictionary);
    }

    @Override
    public void onNewDictionaryTrained(CompressionDictionary.DictId dictionaryId)
    {
        eventHandler.onNewDictionaryTrained(dictionaryId);
    }

    @Override
    public void onNewDictionaryAvailable(CompressionDictionary.DictId dictionaryId)
    {
        eventHandler.onNewDictionaryAvailable(dictionaryId);
    }

    @Override
    public synchronized void train(Map<String, String> options)
    {
        // Validate table supports dictionary compression
        if (!isEnabled)
        {
            throw new UnsupportedOperationException("Table " + keyspaceName + '.' + tableName + " does not support dictionary compression");
        }

        if (trainer == null)
        {
            throw new IllegalStateException("Dictionary trainer is not available for table " + keyspaceName + '.' + tableName);
        }

        // Parse and validate training options
        ManualTrainingOptions trainingOptions = ManualTrainingOptions.fromStringMap(options);

        trainer.start(true);
        scheduler.scheduleManualTraining(trainingOptions, trainer);
    }

    @Override
    public String getTrainingStatus()
    {
        ICompressionDictionaryTrainer dictionaryTrainer = trainer;
        if (dictionaryTrainer == null)
        {
            return TrainingStatus.NOT_STARTED.toString();
        }
        return dictionaryTrainer.getTrainingStatus().toString();
    }

    @Override
    public void updateSamplingRate(int samplingRate)
    {
        ICompressionDictionaryTrainer dictionaryTrainer = trainer;
        if (dictionaryTrainer == null)
        {
            throw new IllegalArgumentException("Dictionary trainer is not available for table " + keyspaceName + '.' + tableName);
        }
        dictionaryTrainer.updateSamplingRate(samplingRate);
    }

    /**
     * Close all the resources. The method can be called multiple times.
     */
    @Override
    public synchronized void close()
    {
        unregisterMbean();
        if (trainer != null)
        {
            closeQuitely(trainer, "CompressionDictionaryTrainer");
            trainer = null;
        }
        closeQuitely(cache, "CompressionDictionaryCache");
        closeQuitely(scheduler, "CompressionDictionaryScheduler");
    }

    private void handleNewDictionary(CompressionDictionary dictionary)
    {
        // sequence meatters; persist the new dictionary before broadcasting to others.
        storeDictionary(dictionary);
        onNewDictionaryTrained(dictionary.dictId());
    }

    private CompressionDictionaryTrainingConfig createTrainingConfig()
    {
        return CompressionDictionaryTrainingConfig
               .builder()
               .maxDictionarySize(DatabaseDescriptor.getCompressionDictionaryTrainingMaxDictionarySize())
               .maxTotalSampleSize(DatabaseDescriptor.getCompressionDictionaryTrainingMaxTotalSampleSize())
               .samplingRate(DatabaseDescriptor.getCompressionDictionaryTrainingSamplingRate())
               .build();
    }

    private void storeDictionary(CompressionDictionary dictionary)
    {
        if (!isEnabled)
        {
            return;
        }

        SystemDistributedKeyspace.storeCompressionDictionary(keyspaceName, tableName, dictionary);
        cache.setCurrentIfNewer(dictionary);
    }

    /**
     * Determines if a new trainer should be created based on compression parameter changes.
     * A new trainer is needed when no existing trainer exists or when the existing trainer
     * is not compatible with the new compression parameters.
     *
     * The method is (and should be) only invoked inside {@link #maybeReloadFromSchema(CompressionParams)},
     * which is guarded by synchronized.
     *
     * @param newParams the new compression parameters
     * @return true if a new trainer should be created
     */
    private boolean shouldCreateNewTrainer(CompressionParams newParams)
    {
        if (trainer == null)
        {
            return true;
        }

        return !trainer.isCompatibleWith(newParams);
    }

    private void unregisterMbean()
    {
        if (mbeanRegistered)
        {
            MBeanWrapper.instance.unregisterMBean(mbeanName(keyspaceName, tableName), OnException.IGNORE);
            mbeanRegistered = true;
        }
    }

    private void closeQuitely(AutoCloseable closeable, String objectName)
    {
        try
        {
            closeable.close();
        }
        catch (Exception exception)
        {
            logger.warn("Failed closing {}", objectName, exception);
        }
    }

    @VisibleForTesting
    boolean isReady()
    {
        return trainer != null && trainer.isReady();
    }

    @VisibleForTesting
    ICompressionDictionaryTrainer trainer()
    {
        return trainer;
    }
}
