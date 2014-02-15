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
package org.apache.cassandra.streaming;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

/**
 * Task that manages receiving files for the session for certain ColumnFamily.
 */
public class StreamReceiveTask extends StreamTask
{
    // number of files to receive
    private final int totalFiles;
    // total size of files to receive
    private final long totalSize;
    private volatile boolean aborted;

    //  holds references to SSTables received
    protected Collection<SSTableWriter> sstables;

    public StreamReceiveTask(StreamSession session, UUID cfId, int totalFiles, long totalSize)
    {
        super(session, cfId);
        this.totalFiles = totalFiles;
        this.totalSize = totalSize;
        this.sstables = new ArrayList<>(totalFiles);
    }

    /**
     * Process received file.
     *
     * @param sstable SSTable file received.
     */
    public void received(SSTableWriter sstable)
    {
        assert cfId.equals(sstable.metadata.cfId);
        assert !aborted;

        sstables.add(sstable);
        if (sstables.size() == totalFiles)
            complete();
    }

    public int getTotalNumberOfFiles()
    {
        return totalFiles;
    }

    public long getTotalSize()
    {
        return totalSize;
    }

    private void complete()
    {
        if (!sstables.isEmpty())
            StorageService.tasks.submit(new OnCompletionRunnable(this));
    }

    private static class OnCompletionRunnable implements Runnable
    {
        private final StreamReceiveTask task;

        public OnCompletionRunnable(StreamReceiveTask task)
        {
            this.task = task;
        }

        public void run()
        {
            Pair<String, String> kscf = Schema.instance.getCF(task.cfId);
            ColumnFamilyStore cfs = Keyspace.open(kscf.left).getColumnFamilyStore(kscf.right);

            StreamLockfile lockfile = new StreamLockfile(cfs.directories.getCompactionLocationAsFile(), UUID.randomUUID());
            lockfile.create(task.sstables);
            List<SSTableReader> readers = new ArrayList<>();
            for (SSTableWriter writer : task.sstables)
                readers.add(writer.closeAndOpenReader());
            lockfile.delete();

            if (!SSTableReader.acquireReferences(readers))
                throw new AssertionError("We shouldn't fail acquiring a reference on a sstable that has just been transferred");
            try
            {
                // add sstables and build secondary indexes
                cfs.addSSTables(readers);
                cfs.indexManager.maybeBuildSecondaryIndexes(readers, cfs.indexManager.allIndexesNames());
            }
            finally
            {
                SSTableReader.releaseReferences(readers);
            }

            task.session.taskCompleted(task);
        }
    }

    public void abort()
    {
        aborted = true;
        Runnable r = new Runnable()
        {
            public void run()
            {
                for (SSTableWriter writer : sstables)
                    writer.abort();
            }
        };
        StorageService.tasks.submit(r);
    }
}
