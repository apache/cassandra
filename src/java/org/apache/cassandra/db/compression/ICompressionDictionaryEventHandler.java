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

import org.apache.cassandra.tcm.membership.NodeId;

public interface ICompressionDictionaryEventHandler
{
    /**
     * Invoked when a dictionary is trained or imported, and when a dictionary is promoted. A dictionary used by all
     * nodes is announced to all other nodes by {@link CompressionDictionaryUpdateMessage}, a dictionary local to this
     * node is not announced as no other node uses it.
     * @param dictionaryId dictionary id
     * @param global whether the dictionary is used by all nodes or only by the node it was trained or imported on
     */
    void onNewDictionaryTrained(CompressionDictionary.DictId dictionaryId, boolean global);

    /**
     * Invoked when a dictionary local to another node is discarded. Only that node is notified, by
     * {@link CompressionDictionaryUpdateMessage}, as no other node uses it; a node which left the cluster is not.
     * @param dictionaryId id of the discarded dictionary
     * @param owner the node the discarded dictionary was local to
     */
    void onDictionaryDiscarded(CompressionDictionary.DictId dictionaryId, NodeId owner);

    /**
     * Invoked on the node which promoted a dictionary and on a node which received
     * {@link CompressionDictionaryUpdateMessage}, sent when another node trained, imported or promoted a dictionary
     * used by all nodes, or discarded a dictionary local to the receiving node. The node refreshes its current
     * dictionary asynchronously to the newest dictionary used by all nodes or local to it, which is not necessarily
     * the given one. When the refresh fails, the periodic refresh does it later.
     * @param dictionaryId id of the trained, imported, promoted or discarded dictionary, used for logging only
     */
    void onNewDictionaryAvailable(CompressionDictionary.DictId dictionaryId);
}
