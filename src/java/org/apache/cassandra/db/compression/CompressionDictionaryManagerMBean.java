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

import javax.annotation.Nullable;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

public interface CompressionDictionaryManagerMBean
{
    String MBEAN_NAME = "org.apache.cassandra.db.compression:type=CompressionDictionaryManager";

    /**
     * Starts training from existing SSTables for this table.
     * Samples chunks from all live SSTables and trains a compression dictionary.
     * If no SSTables are available, automatically flushes the memtable first.
     * This operation runs synchronously and blocks until training completes.
     *
     * @param force force the dictionary training even if there are not enough samples;
     *              otherwise, dictionary training won't start if the trainer is not ready
     * @throws UnsupportedOperationException if table doesn't support dictionary compression
     * @throws IllegalStateException         if no SSTables available after flush
     */
    void train(boolean force);

    /**
     * Gets the current training state for this table.
     * Returns a snapshot of {@link TrainingState} as JMX CompositeData.
     *
     * @return CompositeData representing {@link TrainingState}
     */
    CompositeData getTrainingState();

    /**
     * Lists compression dictionaries for given keyspace and table. Returned compression dictionaries
     * do not contain raw dictionary bytes.
     *
     * @return compression dictionaries for given keyspace and table
     */
    TabularData listCompressionDictionaries();

    /**
     * Get latest compression dictionary.
     *
     * @return CompositeData representing compression dictionary or null if not found
     */
    @Nullable
    CompositeData getCompressionDictionary();

    /**
     * Get compression dictionary.
     *
     * @param dictId id of compression dictionary to get
     * @return CompositeData representing compression dictionary or null of not found
     */
    @Nullable
    CompositeData getCompressionDictionary(long dictId);

    /**
     * Import a compression dictionary.
     *
     * @param dictionary compression dictionary to import
     * @throws IllegalArgumentException when dictionary to import is older (based on dictionary id) than
     * the latest compression dictionary for given table, or when dictionary data are invalid
     * @throws IllegalStateException    if underlying table does not support dictionary compression or
     *                                  kind of dictionary to import does not match kind of dictionary table
     *                                  is configured for
     */
    void importCompressionDictionary(CompositeData dictionary);
}
