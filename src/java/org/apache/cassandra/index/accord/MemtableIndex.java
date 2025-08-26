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
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import accord.local.MaxDecidedRX;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.schema.TableId;

public class MemtableIndex
{
    private final RangeMemoryIndex memoryIndex = new RangeMemoryIndex();
    private final LongAdder writeCount = new LongAdder();
    private final LongAdder estimatedMemoryUsed = new LongAdder();

    public long writeCount()
    {
        return writeCount.sum();
    }

    public long estimatedMemoryUsed()
    {
        return estimatedMemoryUsed.sum();
    }

    public boolean isEmpty()
    {
        return memoryIndex.isEmpty();
    }

    public long index(DecoratedKey key, Clustering<?> clustering, ByteBuffer value)
    {
        if (value == null || value.remaining() == 0)
            return 0;
        long size = memoryIndex.add(key, clustering, value);
        writeCount.increment();
        estimatedMemoryUsed.add(size);
        return size;
    }

    public Segment write(IndexDescriptor id) throws IOException
    {
        return memoryIndex.write(id);
    }

    public void search(int storeId, TableId tableId, byte[] start, byte[] end,
                       TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX,
                       Consumer<ByteBuffer> onMatch)
    {
        memoryIndex.search(storeId, tableId,
                           start, end,
                           minTxnId, maxTxnId, decidedRX,
                           onMatch);
    }

    public void search(int storeId, TableId tableId, byte[] key,
                       TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX,
                       Consumer<ByteBuffer> onMatch)
    {
        memoryIndex.search(storeId, tableId, key,
                           minTxnId, maxTxnId, decidedRX,
                           onMatch);
    }
}
