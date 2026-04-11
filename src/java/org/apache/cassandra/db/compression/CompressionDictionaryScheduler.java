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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.SystemDistributedKeyspace;

/**
 * Manages scheduled tasks for compression dictionary operations.
 * <p>
 * This class handles:
 * - Periodic refresh of dictionaries from system tables
 * - Manual training task scheduling and monitoring
 * - Cleanup of scheduled tasks
 */
public class CompressionDictionaryScheduler implements ICompressionDictionaryScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(CompressionDictionaryScheduler.class);

    private final String keyspaceName;
    private final String tableName;
    private final String tableId;
    private final ICompressionDictionaryCache cache;
    private final AtomicBoolean trainingInProgress = new AtomicBoolean(false);
    private final AtomicReference<TrainingState> lastTrainingState = new AtomicReference<>(TrainingState.notStarted());
    private volatile ICompressionDictionaryTrainer activeTrainer;

    private volatile ScheduledFuture<?> scheduledRefreshTask;
    private volatile boolean isEnabled;

    public CompressionDictionaryScheduler(String keyspaceName,
                                          String tableName,
                                          String tableId,
                                          ICompressionDictionaryCache cache,
                                          boolean isEnabled)
    {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.tableId = tableId;
        this.cache = cache;
        this.isEnabled = isEnabled;
    }

    /**
     * Schedules the periodic dictionary refresh task if not already scheduled.
     */
    public void scheduleRefreshTask()
    {
        if (scheduledRefreshTask != null)
            return;

        this.scheduledRefreshTask = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(
        this::refreshDictionaryFromSystemTable,
        DatabaseDescriptor.getCompressionDictionaryRefreshInitialDelaySeconds(),
        DatabaseDescriptor.getCompressionDictionaryRefreshIntervalSeconds(),
        TimeUnit.SECONDS
        );
    }

    @Override
    public void scheduleSSTableBasedTraining(ColumnFamilyStore.RefViewFragment refViewFragment,
                                             CompressionParams compressionParams,
                                             CompressionDictionaryTrainingConfig config,
                                             Consumer<CompressionDictionary> listener,
                                             boolean force)
    {
        if (!trainingInProgress.compareAndSet(false, true))
        {
            refViewFragment.close();
            throw new IllegalStateException("Training already in progress for table " + keyspaceName + '.' + tableName);
        }

        ICompressionDictionaryTrainer trainer;

        try
        {
            trainer = ICompressionDictionaryTrainer.create(keyspaceName, tableName, compressionParams);
            trainer.setDictionaryTrainedListener(listener);
        }
        catch (Throwable t)
        {
            trainingInProgress.set(false);
            refViewFragment.close();
            throw t;
        }

        if (trainer.start(config))
        {
            activeTrainer = trainer;
            lastTrainingState.set(trainer.getTrainingState());
            logger.info("Starting SSTable-based dictionary training for {}.{} from {} SSTables",
                        keyspaceName, tableName, refViewFragment.sstables.size());

            SSTableSamplingTask task = new SSTableSamplingTask(refViewFragment, trainer, config, force);
            // trainer is eventually closed here, as well as indicating
            // in manualTrainingInProgress that it was finished
            ScheduledExecutors.nonPeriodicTasks.submit(task);
        }
        else
        {
            finishTraining(trainer.getTrainingState());
            cleanup(refViewFragment, trainer);
        }
    }

    /**
     * Cancels the in-progress manual training task.
     */
    private void finishTraining(TrainingState trainingState)
    {
        lastTrainingState.set(trainingState);
        activeTrainer = null;
        trainingInProgress.compareAndSet(true, false);
    }

    /**
     * Sets the enabled state of the scheduler. When disabled, refresh tasks will not execute.
     *
     * @param enabled whether the scheduler should be enabled
     */
    @Override
    public void setEnabled(boolean enabled)
    {
        this.isEnabled = enabled;
    }

    @Override
    public TrainingState getLastTrainingState()
    {
        ICompressionDictionaryTrainer trainer = activeTrainer;
        if (trainer != null)
            return trainer.getTrainingState();
        return lastTrainingState.get();
    }

    /**
     * Refreshes dictionary from system table and updates the cache.
     * This method is called periodically by the scheduled refresh task.
     */
    private void refreshDictionaryFromSystemTable()
    {
        try
        {
            if (!isEnabled)
            {
                return;
            }

            CompressionDictionary dictionary = SystemDistributedKeyspace.retrieveLatestCompressionDictionary(keyspaceName, tableName, tableId);
            cache.add(dictionary);
        }
        catch (Exception e)
        {
            logger.warn("Failed to refresh compression dictionary for {}.{}",
                        keyspaceName, tableName, e);
        }
    }

    @Override
    public void close()
    {
        if (scheduledRefreshTask != null)
        {
            scheduledRefreshTask.cancel(false);
            scheduledRefreshTask = null;
        }

        finishTraining(TrainingState.notStarted());
    }

    /**
     * Task that samples chunks from existing SSTables and triggers training.
     * Acquires references to SSTables to prevent them from being deleted during sampling.
     */
    private class SSTableSamplingTask implements Runnable
    {
        private final ColumnFamilyStore.RefViewFragment refViewFragment;
        private final ICompressionDictionaryTrainer trainer;
        private final CompressionDictionaryTrainingConfig config;
        private final boolean force;

        private SSTableSamplingTask(ColumnFamilyStore.RefViewFragment refViewFragment,
                                    ICompressionDictionaryTrainer trainer,
                                    CompressionDictionaryTrainingConfig config,
                                    boolean force)
        {
            this.refViewFragment = refViewFragment;
            this.trainer = trainer;
            this.config = config;
            this.force = force;
        }

        @Override
        public void run()
        {
            try
            {
                logger.info("Sampling chunks from {} SSTables for {}.{}",
                            refViewFragment.sstables.size(), keyspaceName, tableName);

                // Sample chunks from SSTables and add to trainer
                SSTableChunkSampler.sampleFromSSTables(refViewFragment.sstables, trainer, config);

                logger.info("Completed sampling for {}.{}, now training dictionary",
                            keyspaceName, tableName);

                // Use the force parameter from the task
                trainer.trainDictionaryAsync(force)
                       .addCallback((dictionary, throwable) -> {
                           if (throwable != null)
                           {
                               logger.error("SSTable-based dictionary training failed for {}.{}: {}",
                                            keyspaceName, tableName, throwable.getMessage());
                           }
                           else
                           {
                               logger.info("SSTable-based dictionary training completed for {}.{}",
                                           keyspaceName, tableName);
                           }

                           finishTraining(trainer.getTrainingState());
                           cleanup(refViewFragment, trainer);
                       });
            }
            catch (Exception e)
            {
                logger.error("Failed to sample from SSTables for {}.{}", keyspaceName, tableName, e);
                finishTraining(trainer.getTrainingState());
                cleanup(refViewFragment, trainer);
            }
        }
    }

    private void cleanup(ColumnFamilyStore.RefViewFragment refViewFragment, ICompressionDictionaryTrainer trainer)
    {
        try
        {
            trainer.close();
        }
        catch (Throwable t)
        {
            logger.debug("Unable to close trainer.", t);
        }
        refViewFragment.close();
    }

    @VisibleForTesting
    boolean isTrainingRunning()
    {
        return trainingInProgress.get();
    }
}
