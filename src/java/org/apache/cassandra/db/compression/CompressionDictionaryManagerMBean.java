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
     * Training runs asynchronously, this method returns as soon as it is scheduled; {@link #getTrainingState()}
     * reports its progress.
     *
     * @param force      force the dictionary training even if there are not enough samples;
     *                   otherwise, dictionary training won't start if the trainer is not ready
     * @param parameters parameters of training process
     * @throws UnsupportedOperationException if table doesn't support dictionary compression
     * @throws IllegalArgumentException      if a training parameter is invalid, or the minimum training frequency has
     *                                       not passed since the latest dictionary was created
     * @throws IllegalStateException         if no SSTables available after flush, a training is already in progress,
     *                                       or the latest dictionary of the table is local to a node and has to be
     *                                       promoted or discarded first
     */
    void train(boolean force, Map<String, String> parameters);

    /**
     * Starts training from existing SSTables for this table.
     * Samples chunks from all live SSTables and trains a compression dictionary.
     * If no SSTables are available, automatically flushes the memtable first.
     * Training runs asynchronously, this method returns as soon as it is scheduled; {@link #getTrainingState()}
     * reports its progress.
     * <p>
     * Training parameters will be taken from CQL's compression section of a given table training is conducted on.
     *
     * @param force force the dictionary training even if there are not enough samples;
     *              otherwise, dictionary training won't start if the trainer is not ready
     * @throws UnsupportedOperationException if table doesn't support dictionary compression
     * @throws IllegalArgumentException      if a training parameter is invalid, or the minimum training frequency has
     *                                       not passed since the latest dictionary was created
     * @throws IllegalStateException         if no SSTables available after flush, a training is already in progress,
     *                                       or the latest dictionary of the table is local to a node and has to be
     *                                       promoted or discarded first
     */
    default void train(boolean force)
    {
        train(force, Map.of());
    }

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
     * Import a compression dictionary used by all nodes. Imported dictionary will be durably persisted to
     * system_distributed keyspace.
     *
     * @param dictionary compression dictionary to import
     * @throws IllegalArgumentException when dictionary to import is not newer (based on dictionary id) than
     *                                  the latest compression dictionary for given table, kind of dictionary to
     *                                  import does not match kind of dictionary table is configured for, or when
     *                                  dictionary data are invalid
     * @throws IllegalStateException    if underlying table does not support dictionary compression, or the latest
     *                                  dictionary of the table is local to a node and has to be promoted or
     *                                  discarded first
     */
    default void importCompressionDictionary(CompositeData dictionary)
    {
        importCompressionDictionary(dictionary, true);
    }

    /**
     * Import a compression dictionary.
     *
     * @param dictionary compression dictionary to import
     * @param global     whether all nodes use the dictionary, or only this node until the dictionary is promoted
     * @throws IllegalArgumentException when dictionary to import is not newer (based on dictionary id) than
     *                                  the latest compression dictionary for given table, kind of dictionary to
     *                                  import does not match kind of dictionary table is configured for, or when
     *                                  dictionary data are invalid
     * @throws IllegalStateException    if underlying table does not support dictionary compression, or the latest
     *                                  dictionary of the table is local to a node and has to be promoted or
     *                                  discarded first
     */
    void importCompressionDictionary(CompositeData dictionary, boolean global);

    /**
     * Promotes the latest dictionary from local to global one. That means that it will
     * set node_id column of the latest dictionary to null and announce the dictionary to all nodes, which start to
     * use it right away, or on their next dictionary refreshment round when the announcement does not reach them.
     * A locally-trained or locally-imported dictionary
     * (done via nodetool compressiondictionary train|import --local) will cause that dictionary to be used for
     * compression only on a node it was trained or imported on. The calling of this method, on any node, will promote
     * it among all nodes.
     *
     * @throws IllegalStateException if no dictionary is found, the latest one is already used by all nodes
     *                               or it was deleted before it could be promoted
     */
    void promoteDictionary();

    /**
     * Discards the latest dictionary when it is local to a node, i.e. trained or imported with --local and not promoted.
     * It is deleted from system_distributed and its node compresses with the newest dictionary used by all nodes again,
     * or with none when there is no such dictionary. It can be called on any node, the node of the dictionary is
     * notified. SSTables written with a discarded dictionary remain readable.
     *
     * @throws IllegalStateException if no dictionary is found, the latest one is used by all nodes, or it was promoted
     *                               or deleted before it could be discarded
     */
    void discardDictionary();
}
