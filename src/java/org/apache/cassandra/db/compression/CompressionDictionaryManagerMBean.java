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

public interface CompressionDictionaryManagerMBean
{
    String MBEAN_NAME = "org.apache.cassandra.db.compression:type=CompressionDictionaryManager";

    /**
     * Starts sampling and training for this table.
     * 
     * @param options options for the training process (currently unused, reserved for future extensions)
     * @throws UnsupportedOperationException if table doesn't support dictionary compression
     * @throws IllegalStateException if training is already in progress for this table
     */
    void train(Map<String, String> options);

    /**
     * Gets the current training status for this table.
     * Enables async polling for status/completion.
     *
     * @return training status as string: "Not started", "In progress", "Completed", or "Failed"
     */
    String getTrainingStatus();

    /**
     * Updates the sampling rate for the trainer.
     *
     * @param samplingRate the new sampling rate. For exmaple, 1 = sample every time (100%);
     *                     2 = expect sample 1/2 of data (50%), n = expect sample 1/n of data
     * @throws IllegalArgumentException if sampling rate is invalid or trainer is not available
     */
    void updateSamplingRate(int samplingRate);
}
