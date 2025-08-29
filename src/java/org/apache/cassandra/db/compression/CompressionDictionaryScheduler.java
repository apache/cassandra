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

import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;
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
    public void scheduleManualTraining(Map<String, String> options, ICompressionDictionaryTrainer trainer)
    {
        if (scheduledManualTrainingTask != null)
        {
            throw new IllegalStateException("Training already in progress for table " + keyspaceName + '.' + tableName);
        }

        // Parse max sampling duration from options (default from configuration)
        int maxSamplingDurationSeconds = DatabaseDescriptor.getCompressionDictionaryTrainingManualSamplingDurationSeconds();
        if (options.containsKey("maxSamplingDurationSeconds"))
        {
            String durationStr = options.get("maxSamplingDurationSeconds");
            try
            {
                maxSamplingDurationSeconds = Integer.parseInt(durationStr);
            }
            catch (NumberFormatException e)
            {
                logger.warn("Invalid maxSamplingDurationSeconds value: {}, using default: {}",
                            durationStr, maxSamplingDurationSeconds);
            }
        }

        logger.info("Starting manual dictionary training for {}.{} with max sampling duration: {} seconds",
                    keyspaceName, tableName, maxSamplingDurationSeconds);

        long deadlineMillis = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(maxSamplingDurationSeconds);

        ManualTrainingTask task = new ManualTrainingTask(deadlineMillis, trainer);

        // Check every second whether it gets enough samples and completes training
        scheduledManualTrainingTask = ScheduledExecutors.scheduledTasks
                                      .scheduleWithFixedDelay(task, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void cancelManualTraining()
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
            cache.setCurrentIfNewer(dictionary);
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

    private class ManualTrainingTask implements Runnable
    {
        private final long deadlineMillis;
        private final ICompressionDictionaryTrainer trainer;
        private boolean isTraining = false;

        private ManualTrainingTask(long deadlineMillis, ICompressionDictionaryTrainer trainer)
        {
            this.deadlineMillis = deadlineMillis;
            this.trainer = trainer;
        }

        @Override
        public void run()
        {
            if (trainer.getTrainingStatus() == TrainingStatus.NOT_STARTED)
            {
                logger.warn("Trainer is not started. Stop training dictionary for table {}.{}", keyspaceName, tableName);
                cancelManualTraining();
                return;
            }

            long now = System.currentTimeMillis();
            // Force training if there are not enough samples, but we have hit the max sampling duration
            boolean reachedDeadline = now >= deadlineMillis;
            if (!isTraining && (trainer.isReady() || reachedDeadline))
            {
                // Set isTraining to only enter the branch once
                isTraining = true;
                trainer.trainDictionaryAsync(reachedDeadline)
                       .whenComplete((dictionary, throwable) -> {
                           cancelManualTraining();
                           if (throwable != null)
                           {
                               logger.error("Manual dictionary training failed for {}.{}", keyspaceName, tableName, throwable);
                           }
                           else
                           {
                               logger.info("Manual dictionary training completed for {}.{}", keyspaceName, tableName);
                           }
                       });
            }
        }
    }

    @VisibleForTesting
    ScheduledFuture<?> scheduledManualTrainingTask()
    {
        return scheduledManualTrainingTask;
    }
}
