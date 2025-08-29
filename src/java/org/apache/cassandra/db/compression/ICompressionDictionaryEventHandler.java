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
     * Invoked when a new dictionary is trained
     * @param dictionaryId dictionary id
     */
    void onNewDictionaryTrained(CompressionDictionary.DictId dictionaryId);

    /**
     * Invoked when {@link CompressionDictionaryUpdateMessage} is received indicating
     * a dictionary is trained and local node should retrieve the specified dictionary
     * @param dictionaryId dictionary id
     */
    void onNewDictionaryAvailable(CompressionDictionary.DictId dictionaryId);
}
