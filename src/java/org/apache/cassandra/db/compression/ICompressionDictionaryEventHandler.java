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

public interface ICompressionDictionaryEventHandler
{
    /**
     * Invoked when a dictionary is trained or imported, and when a dictionary is promoted. The dictionary is announced
     * by {@link CompressionDictionaryUpdateMessage} to the other nodes of its scope, as no node outside of it uses it;
     * a dictionary scoped to this node only is not announced.
     * @param dictionaryId dictionary id
     * @param scope scope of the dictionary, after the promotion for a promoted one
     */
    void onNewDictionaryTrained(CompressionDictionary.DictId dictionaryId, CompressionDictionaryScope scope);

    /**
     * Invoked when a dictionary scoped narrower than the cluster is discarded. The other nodes of its scope are
     * notified by {@link CompressionDictionaryUpdateMessage}, as no other node uses it; a node which left the cluster
     * is not.
     * @param dictionaryId id of the discarded dictionary
     * @param scope scope of the discarded dictionary
     */
    void onDictionaryDiscarded(CompressionDictionary.DictId dictionaryId, CompressionDictionaryScope scope);

    /**
     * Invoked on the node which promoted a dictionary and on a node which received
     * {@link CompressionDictionaryUpdateMessage}, sent when another node trained, imported, promoted or discarded a
     * dictionary whose scope contains the receiving node. The node refreshes its current dictionary asynchronously to
     * the newest dictionary whose scope contains it, which is not necessarily the given one. When the refresh fails,
     * the periodic refresh does it later.
     * @param dictionaryId id of the trained, imported, promoted or discarded dictionary, used for logging only
     */
    void onNewDictionaryAvailable(CompressionDictionary.DictId dictionaryId);
}
