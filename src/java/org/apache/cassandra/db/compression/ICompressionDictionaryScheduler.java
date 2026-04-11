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

import java.util.function.Consumer;

import org.apache.cassandra.db.ColumnFamilyStore.RefViewFragment;
import org.apache.cassandra.schema.CompressionParams;

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
     * Schedules SSTable-based training that samples from existing SSTables.
     *
     * A caller of this method should ensure that SSTables referred in {@code refViewFragment} are closed
     * eventually, either directly at the end of that method or by other means, when training is running
     * asynchronously.
     *
     * A caller of this method might assume that {@code trainer} might be closed after this method finishes, either
     * directly in this method or indirectly when training is running asynchronously.
     *
     * @param refViewFragment the view of SSTables to sample from
     * @param compressionParams parameters for compression
     * @param config the training configuration
     * @param listener listener invoked when a dictionary is trained
     * @param force force the dictionary training even if there are not enough samples
     * @throws IllegalStateException if training is already in progress
     */
    void scheduleSSTableBasedTraining(RefViewFragment refViewFragment,
                                      CompressionParams compressionParams,
                                      CompressionDictionaryTrainingConfig config,
                                      Consumer<CompressionDictionary> listener,
                                      boolean force);

    /**
     * Sets the enabled state of the scheduler. When disabled, refresh tasks will not execute.
     *
     * @param enabled whether the scheduler should be enabled
     */
    void setEnabled(boolean enabled);

    TrainingState getLastTrainingState();
}
