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
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.TimeUUID;

abstract class AbstractCompactionPipeline extends CompactionInfo.Holder implements AutoCloseable {
    // Pipeline selection counts, read only by CompactionPipelineCounts in the test tree.
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

    final CompactionTask task;
    long totalKeysWritten;
    CompactionAwareWriter writer;

    AbstractCompactionPipeline(CompactionTask task)
    {
        this.task = task;
    }

    /**
     * The object that does the work. It owns the progress and the stop flag this pipeline reports,
     * so a subclass must not keep a second copy of either.
     */
    abstract CompactionInfo.Holder delegate();

    @Override
    public CompactionInfo getCompactionInfo()
    {
        return delegate().getCompactionInfo();
    }

    @Override
    public boolean isGlobal()
    {
        return delegate().isGlobal();
    }

    public void stop()
    {
        delegate().stop();
    }

    abstract boolean processNextPartitionKey() throws IOException;

    public abstract long[] getMergedRowCounts();

    public abstract long getTotalSourceCQLRows();

    public abstract long getTotalBytesScanned();

    @Override
    public abstract void close() throws IOException;

    public AutoCloseable openWriterResource(ColumnFamilyStore cfs,
                                            Directories directories,
                                            ILifecycleTransaction transaction,
                                            Set<SSTableReader> nonExpiredSSTables)
    {
        this.writer = task.getCompactionAwareWriter(cfs, directories, transaction, nonExpiredSSTables);
        return writer;
    }

    public Collection<SSTableReader> finishWriting()
    {
        return writer.finish();
    }

    public long estimatedKeys()
    {
        return writer.estimatedKeys();
    }

    public long getTotalKeysWritten()
    {
        return totalKeysWritten;
    }
}
