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

public interface CompressionDictionaryManagerMBean
{
    String MBEAN_NAME = "org.apache.cassandra.db.compression:type=CompressionDictionaryManager";

    /**
     * Starts training from existing SSTables for this table.
     * Samples chunks from all live SSTables and trains a compression dictionary.
     * If no SSTables are available, automatically flushes the memtable first.
     *
     * @throws UnsupportedOperationException if table doesn't support dictionary compression
     * @throws IllegalStateException if training is already in progress for this table or no SSTables available after flush
     */
    void train();

    /**
     * Gets the current training status for this table.
     * Enables async polling for status/completion.
     *
     * @return training status as string: "Not started", "In progress", "Completed", or "Failed"
     */
    String getTrainingStatus();

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
}
