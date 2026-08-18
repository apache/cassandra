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

package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.AbstractCompactionController;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.TimeUUID;

abstract class AbstractCompactionPipeline extends CompactionInfo.Holder implements AutoCloseable {
    // Which pipeline each compaction selected, as a delta across one compaction. Read by
    // CompactionPipelineCounts (test), which documents why supportability alone cannot establish it.
    private static final LongAdder CURSOR_PIPELINES_CREATED = new LongAdder();
    private static final LongAdder ITERATOR_PIPELINES_CREATED = new LongAdder();

    static long cursorPipelinesCreated()
    {
        return CURSOR_PIPELINES_CREATED.sum();
    }

    static long iteratorPipelinesCreated()
    {
        return ITERATOR_PIPELINES_CREATED.sum();
    }

    static AbstractCompactionPipeline create(
        CompactionTask task,
        OperationType type,
        AbstractCompactionStrategy.ScannerList scanners,
        AbstractCompactionController controller,
        long nowInSec,
        TimeUUID compactionId)
    {
        if (DatabaseDescriptor.cursorCompactionEnabled()) {
            if (CursorCompactor.isSupported(scanners, controller))
            {
                CURSOR_PIPELINES_CREATED.increment();
                return new CursorCompactionPipeline(task, type, scanners, controller, nowInSec, compactionId);
            }
        }
        ITERATOR_PIPELINES_CREATED.increment();
        return new IteratorCompactionPipeline(task, type, scanners, controller, nowInSec, compactionId);
    }

    abstract boolean processNextPartitionKey() throws IOException;

    public abstract long[] getMergedRowCounts();

    public abstract long getTotalSourceCQLRows();

    public abstract long getTotalKeysWritten();

    public abstract long getTotalBytesScanned();

    public abstract AutoCloseable openWriterResource(ColumnFamilyStore cfs,
                                                     Directories directories,
                                                     ILifecycleTransaction transaction,
                                                     Set<SSTableReader> nonExpiredSSTables);

    @Override
    public abstract void close() throws IOException;

    public abstract Collection<SSTableReader> finishWriting();

    public abstract long estimatedKeys();

    public abstract void stop();
}
