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

package org.apache.cassandra.db.compaction.writers;

import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.concurrent.Transactional;


/**
 * Class that abstracts away the actual writing of files to make it possible to use CompactionTask for more
 * use cases.
 */
public abstract class CompactionAwareWriter extends Transactional.AbstractTransactional implements Transactional
{
    protected final ColumnFamilyStore cfs;
    protected final Set<SSTableReader> nonExpiredSSTables;
    protected final long estimatedTotalKeys;
    protected final long maxAge;
    protected final long minRepairedAt;
    protected final SSTableRewriter sstableWriter;

    public CompactionAwareWriter(ColumnFamilyStore cfs, Set<SSTableReader> allSSTables, Set<SSTableReader> nonExpiredSSTables, boolean offline)
    {
        this.cfs = cfs;
        this.nonExpiredSSTables = nonExpiredSSTables;
        this.estimatedTotalKeys = SSTableReader.getApproximateKeyCount(nonExpiredSSTables);
        this.maxAge = CompactionTask.getMaxDataAge(nonExpiredSSTables);
        this.minRepairedAt = CompactionTask.getMinRepairedAt(nonExpiredSSTables);
        this.sstableWriter = new SSTableRewriter(cfs, allSSTables, maxAge, offline);
    }

    /**
     * Writes a row in an implementation specific way
     * @param row the row to append
     * @return true if the row was written, false otherwise
     */
    public abstract boolean append(AbstractCompactedRow row);

    @Override
    protected Throwable doAbort(Throwable accumulate)
    {
        return sstableWriter.abort(accumulate);
    }

    @Override
    protected Throwable doCleanup(Throwable accumulate)
    {
        return accumulate;
    }

    @Override
    protected Throwable doCommit(Throwable accumulate)
    {
        return sstableWriter.commit(accumulate);
    }

    @Override
    protected void doPrepare()
    {
        sstableWriter.prepareToCommit();
    }

    /**
     * we are done, return the finished sstables so that the caller can mark the old ones as compacted
     * @return all the written sstables sstables
     */
    @Override
    public List<SSTableReader> finish()
    {
        super.finish();
        return sstableWriter.finished();
    }

    /**
     * estimated number of keys we should write
     */
    public long estimatedKeys()
    {
        return estimatedTotalKeys;
    }

    /**
     * The directories we can write to
     */
    public Directories getDirectories()
    {
        return cfs.directories;
    }

    /**
     * Return a directory where we can expect expectedWriteSize to fit.
     */
    public Directories.DataDirectory getWriteDirectory(long expectedWriteSize)
    {
        Directories.DataDirectory directory = getDirectories().getWriteableLocation(expectedWriteSize);
        if (directory == null)
            throw new RuntimeException("Insufficient disk space to write " + expectedWriteSize + " bytes");

        return directory;
    }
}