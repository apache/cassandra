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
package org.apache.cassandra.io.sstable.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class RangeAwareSSTableWriter implements SSTableMultiWriter
{
    private final List<PartitionPosition> boundaries;
    private final Directories.DataDirectory[] directories;
    private final int sstableLevel;
    private final long estimatedKeys;
    private final long repairedAt;
    private final UUID pendingRepair;
    private final SSTableFormat.Type format;
    private final SerializationHeader header;
    private final LifecycleTransaction txn;
    private int currentIndex = -1;
    public final ColumnFamilyStore cfs;
    private final List<SSTableMultiWriter> finishedWriters = new ArrayList<>();
    private final List<SSTableReader> finishedReaders = new ArrayList<>();
    private SSTableMultiWriter currentWriter = null;

    public RangeAwareSSTableWriter(ColumnFamilyStore cfs, long estimatedKeys, long repairedAt, UUID pendingRepair, SSTableFormat.Type format, int sstableLevel, long totalSize, LifecycleTransaction txn, SerializationHeader header) throws IOException
    {
        directories = cfs.getDirectories().getWriteableLocations();
        this.sstableLevel = sstableLevel;
        this.cfs = cfs;
        this.estimatedKeys = estimatedKeys / directories.length;
        this.repairedAt = repairedAt;
        this.pendingRepair = pendingRepair;
        this.format = format;
        this.txn = txn;
        this.header = header;
        boundaries = StorageService.getDiskBoundaries(cfs, directories);
        if (boundaries == null)
        {
            Directories.DataDirectory localDir = cfs.getDirectories().getWriteableLocation(totalSize);
            if (localDir == null)
                throw new IOException(String.format("Insufficient disk space to store %s",
                                                    FBUtilities.prettyPrintMemory(totalSize)));
            Descriptor desc = cfs.newSSTableDescriptor(cfs.getDirectories().getLocationForDisk(localDir), format);
            currentWriter = cfs.createSSTableMultiWriter(desc, estimatedKeys, repairedAt, pendingRepair, sstableLevel, header, txn);
        }
    }

    private void maybeSwitchWriter(DecoratedKey key)
    {
        if (boundaries == null)
            return;

        boolean switched = false;
        while (currentIndex < 0 || key.compareTo(boundaries.get(currentIndex)) > 0)
        {
            switched = true;
            currentIndex++;
        }

        if (switched)
        {
            if (currentWriter != null)
                finishedWriters.add(currentWriter);

            Descriptor desc = cfs.newSSTableDescriptor(cfs.getDirectories().getLocationForDisk(directories[currentIndex]), format);
            currentWriter = cfs.createSSTableMultiWriter(desc, estimatedKeys, repairedAt, pendingRepair, sstableLevel, header, txn);
        }
    }

    public boolean append(UnfilteredRowIterator partition)
    {
        maybeSwitchWriter(partition.partitionKey());
        return currentWriter.append(partition);
    }

    @Override
    public Collection<SSTableReader> finish(long repairedAt, long maxDataAge, boolean openResult)
    {
        if (currentWriter != null)
            finishedWriters.add(currentWriter);
        currentWriter = null;
        for (SSTableMultiWriter writer : finishedWriters)
        {
            if (writer.getFilePointer() > 0)
                finishedReaders.addAll(writer.finish(repairedAt, maxDataAge, openResult));
            else
                SSTableMultiWriter.abortOrDie(writer);
        }
        return finishedReaders;
    }

    @Override
    public Collection<SSTableReader> finish(boolean openResult)
    {
        if (currentWriter != null)
            finishedWriters.add(currentWriter);
        currentWriter = null;
        for (SSTableMultiWriter writer : finishedWriters)
        {
            if (writer.getFilePointer() > 0)
                finishedReaders.addAll(writer.finish(openResult));
            else
                SSTableMultiWriter.abortOrDie(writer);
        }
        return finishedReaders;
    }

    @Override
    public Collection<SSTableReader> finished()
    {
        return finishedReaders;
    }

    @Override
    public SSTableMultiWriter setOpenResult(boolean openResult)
    {
        finishedWriters.forEach((w) -> w.setOpenResult(openResult));
        currentWriter.setOpenResult(openResult);
        return this;
    }

    public String getFilename()
    {
        return String.join("/", cfs.keyspace.getName(), cfs.getTableName());
    }

    @Override
    public long getFilePointer()
    {
        return currentWriter.getFilePointer();
    }

    @Override
    public TableId getTableId()
    {
        return currentWriter.getTableId();
    }

    @Override
    public Throwable commit(Throwable accumulate)
    {
        if (currentWriter != null)
            finishedWriters.add(currentWriter);
        currentWriter = null;
        for (SSTableMultiWriter writer : finishedWriters)
            accumulate = writer.commit(accumulate);
        return accumulate;
    }

    @Override
    public Throwable abort(Throwable accumulate)
    {
        if (currentWriter != null)
            finishedWriters.add(currentWriter);
        currentWriter = null;
        for (SSTableMultiWriter finishedWriter : finishedWriters)
            accumulate = finishedWriter.abort(accumulate);

        return accumulate;
    }

    @Override
    public void prepareToCommit()
    {
        if (currentWriter != null)
            finishedWriters.add(currentWriter);
        currentWriter = null;
        finishedWriters.forEach(SSTableMultiWriter::prepareToCommit);
    }

    @Override
    public void close()
    {
        if (currentWriter != null)
            finishedWriters.add(currentWriter);
        currentWriter = null;
        finishedWriters.forEach(SSTableMultiWriter::close);
    }
}
