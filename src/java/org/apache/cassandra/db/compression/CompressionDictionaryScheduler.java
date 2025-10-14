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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.utils.concurrent.Ref;

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
    private final ICompressionDictionaryCache cache;

    private volatile ScheduledFuture<?> scheduledRefreshTask;
    private volatile ScheduledFuture<?> scheduledManualTrainingTask;
    private volatile boolean isEnabled;

    public CompressionDictionaryScheduler(String keyspaceName,
                                          String tableName,
                                          ICompressionDictionaryCache cache,
                                          boolean isEnabled)
    {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
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
    public void scheduleSSTableBasedTraining(ICompressionDictionaryTrainer trainer,
                                             Set<SSTableReader> sstables,
                                             CompressionDictionaryTrainingConfig config)
    {
        if (scheduledManualTrainingTask != null)
        {
            throw new IllegalStateException("Training already in progress for table " + keyspaceName + '.' + tableName);
        }

        logger.info("Starting SSTable-based dictionary training for {}.{} from {} SSTables",
                    keyspaceName, tableName, sstables.size());

        // Run the SSTableSamplingTask asynchronously
        // Use a dummy scheduled task to track that training is in progress
        SSTableSamplingTask task = new SSTableSamplingTask(sstables, trainer, config);
        ScheduledExecutors.nonPeriodicTasks.submit(task);

        // Set a placeholder task so status checks know training is in progress
        scheduledManualTrainingTask = ScheduledExecutors.scheduledTasks.schedule(() -> {}, 1, TimeUnit.HOURS);
    }

    /**
     * Cancels the in-progress manual training task.
     */
    private void cancelManualTraining()
    {
        ScheduledFuture<?> future = scheduledManualTrainingTask;
        if (future != null)
        {
            future.cancel(false);
        }
        scheduledManualTrainingTask = null;
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

            CompressionDictionary dictionary = SystemDistributedKeyspace.retrieveLatestCompressionDictionary(keyspaceName, tableName);
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

        if (scheduledManualTrainingTask != null)
        {
            scheduledManualTrainingTask.cancel(false);
            scheduledManualTrainingTask = null;
        }
    }

    /**
     * Task that samples chunks from existing SSTables and triggers training.
     * Acquires references to SSTables to prevent them from being deleted during sampling.
     */
    private class SSTableSamplingTask implements Runnable
    {
        private final Set<SSTableReader> sstables;
        private final ICompressionDictionaryTrainer trainer;
        private final CompressionDictionaryTrainingConfig config;
        private final List<Ref<SSTableReader>> sstableRefs;

        private SSTableSamplingTask(Set<SSTableReader> sstables,
                                    ICompressionDictionaryTrainer trainer,
                                    CompressionDictionaryTrainingConfig config)
        {
            this.trainer = trainer;
            this.config = config;

            // Acquire references to all SSTables to prevent deletion during sampling
            this.sstableRefs = new ArrayList<>();
            Set<SSTableReader> referencedSSTables = new HashSet<>();

            for (SSTableReader sstable : sstables)
            {
                Ref<SSTableReader> ref = sstable.tryRef();
                if (ref != null)
                {
                    sstableRefs.add(ref);
                    referencedSSTables.add(sstable);
                }
                else
                {
                    logger.debug("Couldn't acquire reference to SSTable {}. It may have been removed.",
                                 sstable.descriptor);
                }
            }

            this.sstables = referencedSSTables;
        }

        @Override
        public void run()
        {
            try
            {
                if (sstables.isEmpty())
                {
                    logger.warn("No SSTables available for sampling in {}.{}", keyspaceName, tableName);
                    cancelManualTraining();
                    return;
                }

                logger.info("Sampling chunks from {} SSTables for {}.{}",
                            sstables.size(), keyspaceName, tableName);

                // Sample chunks from SSTables and add to trainer
                SSTableChunkSampler.sampleFromSSTables(sstables, trainer, config);

                logger.info("Completed sampling for {}.{}, now training dictionary",
                            keyspaceName, tableName);

                // force=true for manual training
                trainer.trainDictionaryAsync(true)
                       .addCallback((dictionary, throwable) -> {
                           cancelManualTraining();
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
                       });
            }
            catch (Exception e)
            {
                logger.error("Failed to sample from SSTables for {}.{}", keyspaceName, tableName, e);
                cancelManualTraining();
            }
            finally
            {
                // Release all SSTable references
                for (Ref<SSTableReader> ref : sstableRefs)
                {
                    ref.release();
                }
            }
        }
    }

    @VisibleForTesting
    ScheduledFuture<?> scheduledManualTrainingTask()
    {
        return scheduledManualTrainingTask;
    }
}
