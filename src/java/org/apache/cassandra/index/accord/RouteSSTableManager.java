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

package org.apache.cassandra.index.accord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import accord.local.MaxDecidedRX;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.ByteArrayUtil;

import static org.apache.cassandra.index.accord.IndexDescriptor.IndexComponent.CINTIA_SORTED_LIST;

public class RouteSSTableManager implements SSTableManager
{
    private final Map<SSTableReader, SSTableIndex> sstables = new HashMap<>();

    @Override
    public synchronized void onSSTableChanged(Collection<SSTableReader> removed, Iterable<SSTableReader> added)
    {
        //TODO (performance): most added tables will have 0 segmenets, so exclude those from search
        removed.forEach(s -> {
            SSTableIndex index = sstables.remove(s);
            if (index != null)
            {
                index.close();
                index.id.deleteIndex();
            }
        });

        List<SSTableReader> notComplete = null;
        for (SSTableReader sstable : added)
        {
            IndexDescriptor id = IndexDescriptor.create(sstable);
            if (!id.isIndexBuildComplete())
            {
                if (notComplete == null) notComplete = new ArrayList<>();
                notComplete.add(sstable);
                continue;
            }
            try
            {
                sstables.put(sstable, SSTableIndex.create(id));
            }
            catch (IOException e)
            {
                if (notComplete == null) notComplete = new ArrayList<>();
                notComplete.add(sstable);
            }
        }
        if (notComplete != null)
            throw new IllegalArgumentException("SStables were added without an index... " + notComplete);
    }

    @Override
    public synchronized boolean isIndexComplete(SSTableReader reader)
    {
        return sstables.containsKey(reader);
    }

    @Override
    public synchronized void search(int storeId, TableId tableId,
                                    byte[] start, byte[] end,
                                    TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX,
                                    Consumer<ByteBuffer> onMatch)
    {
        Key group = new Key(storeId, tableId);
        for (SSTableIndex index : sstables.values())
        {
            try
            {
                index.search(group, start, end, minTxnId, maxTxnId, decidedRX, onMatch);
            }
            catch (Throwable t)
            {
                File file = index.id.fileFor(CINTIA_SORTED_LIST);
                throw new FSReadError("Failed to search range index " + file + " for (" + ByteArrayUtil.bytesToHex(start) + "..." + ByteArrayUtil.bytesToHex(end) + "]", t, file);
            }
        }
    }

    @Override
    public synchronized void search(int storeId, TableId tableId, byte[] key, TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX, Consumer<ByteBuffer> onMatch)
    {
        Key group = new Key(storeId, tableId);
        for (SSTableIndex index : sstables.values())
            index.search(group, key, minTxnId, maxTxnId, decidedRX, onMatch);
    }
}
