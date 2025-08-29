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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.schema.Schema;

public class CompressionDictionaryUpdateVerbHandler implements IVerbHandler<CompressionDictionaryUpdateMessage>
{
    private static final Logger logger = LoggerFactory.getLogger(CompressionDictionaryUpdateVerbHandler.class);
    public static final CompressionDictionaryUpdateVerbHandler instance = new CompressionDictionaryUpdateVerbHandler();

    private CompressionDictionaryUpdateVerbHandler() {}

    @Override
    public void doVerb(Message<CompressionDictionaryUpdateMessage> message)
    {
        CompressionDictionaryUpdateMessage payload = message.payload;

        try
        {
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(payload.tableId);
            if (cfs == null)
            {
                logger.warn("Received dictionary update for unknown table with tableId {}", payload.tableId);
                return;
            }

            logger.debug("Received dictionary update notification for {}.{} with dictionaryId {}",
                         cfs.keyspace, cfs.name, payload.dictionaryId);
            CompressionDictionaryManager manager = cfs.compressionDictionaryManager();
            manager.onNewDictionaryAvailable(payload.dictionaryId);
        }
        catch (Exception e)
        {
            logger.error("Failed to process dictionary update notification for tableId {}",
                         payload.tableId, e);
        }
    }
}
