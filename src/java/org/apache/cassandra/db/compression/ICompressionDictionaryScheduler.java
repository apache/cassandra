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

/**
 * Interface for managing scheduled tasks for compression dictionary operations.
 * <p>
 * Implementations handle:
 * - Periodic refresh of dictionaries from system tables
 * - Manual training task scheduling and monitoring
 * - Cleanup of scheduled tasks
 */
public interface ICompressionDictionaryScheduler extends AutoCloseable
{
    /**
     * Schedules the periodic dictionary refresh task if not already scheduled.
     */
    void scheduleRefreshTask();

    /**
     * Schedules manual training with the specified options.
     *
     * @param options parsed and validated training options
     * @param trainer the trainer to use
     * @throws IllegalStateException if training is already in progress
     */
    void scheduleManualTraining(ManualTrainingOptions options, ICompressionDictionaryTrainer trainer);

    /**
     * Cancel the in-progress manual training
     */
    void cancelManualTraining();

    /**
     * Sets the enabled state of the scheduler. When disabled, refresh tasks will not execute.
     *
     * @param enabled whether the scheduler should be enabled
     */
    void setEnabled(boolean enabled);
}
