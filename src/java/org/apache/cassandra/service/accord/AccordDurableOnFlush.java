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

package org.apache.cassandra.service.accord;

import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.schema.TableMetadata;

class AccordDurableOnFlush implements Consumer<TableMetadata>
{
    private static final Logger logger = LoggerFactory.getLogger(AccordDurableOnFlush.class);

    private Int2ObjectHashMap<RedundantBefore> commandStores = new Int2ObjectHashMap<>();

    AccordDurableOnFlush()
    {
    }

    synchronized boolean add(int commandStoreId, RedundantBefore reportOnFlush)
    {
        if (commandStores == null)
            return false;
        commandStores.merge(commandStoreId, reportOnFlush, RedundantBefore::merge);
        return true;
    }

    @Override
    public void accept(TableMetadata metadata)
    {
        Int2ObjectHashMap<RedundantBefore> notify;
        synchronized (this)
        {
            notify = commandStores;
            commandStores = null;
        }
        CommandStores commandStores = AccordService.instance().node().commandStores();
        for (Map.Entry<Integer, RedundantBefore> e : notify.entrySet())
        {
            RedundantBefore durable = e.getValue();
            notify(metadata, commandStores.forId(e.getKey()), durable);
        }
    }

    static void notify(TableMetadata metadata, CommandStore commandStore, RedundantBefore report)
    {
        logger.debug("Reporting flush of {}/{}; reporting {} to {}", metadata.id, metadata, report, commandStore);
        commandStore.execute((PreLoadContext.Empty) () -> "Report Durable", safeStore -> {
            safeStore.upsertRedundantBefore(report);
        });
    }
}
