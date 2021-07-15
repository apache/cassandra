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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Transactional;


/**
 * Class that abstracts away the actual writing of files to make it possible to use CompactionTask for more
 * use cases.
 */
public abstract class CompactionAwareWriter extends Transactional.AbstractTransactional implements Transactional
{
    protected static final Logger logger = LoggerFactory.getLogger(CompactionAwareWriter.class);

    protected final ColumnFamilyStore cfs;
    protected final Directories directories;
    protected final Set<SSTableReader> nonExpiredSSTables;
    protected final long estimatedTotalKeys;
    protected final long maxAge;
    protected final long minRepairedAt;
    protected final UUID pendingRepair;
    protected final boolean isTransient;

    protected final SSTableRewriter sstableWriter;
    protected final LifecycleTransaction txn;
    private final List<Directories.DataDirectory> locations;
    private final List<PartitionPosition> diskBoundaries;
    private int locationIndex;
    protected Directories.DataDirectory currentDirectory;

    public CompactionAwareWriter(ColumnFamilyStore cfs,
                                 Directories directories,
                                 LifecycleTransaction txn,
                                 Set<SSTableReader> nonExpiredSSTables,
                                 boolean keepOriginals)
    {
        this.cfs = cfs;
        this.directories = directories;
        this.nonExpiredSSTables = nonExpiredSSTables;
        this.txn = txn;

        estimatedTotalKeys = SSTableReader.getApproximateKeyCount(nonExpiredSSTables);
        maxAge = CompactionTask.getMaxDataAge(nonExpiredSSTables);
        sstableWriter = SSTableRewriter.construct(cfs, txn, keepOriginals, maxAge);
        minRepairedAt = CompactionTask.getMinRepairedAt(nonExpiredSSTables);
        pendingRepair = CompactionTask.getPendingRepair(nonExpiredSSTables);
        isTransient = CompactionTask.getIsTransient(nonExpiredSSTables);
        DiskBoundaries db = cfs.getDiskBoundaries();
        diskBoundaries = db.getPositions();
        locations = db.directories;
        locationIndex = -1;
    }

    @Override
    protected Throwable doAbort(Throwable accumulate)
    {
        return sstableWriter.abort(accumulate);
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
    public Collection<SSTableReader> finish()
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
     * Writes a partition in an implementation specific way
     * @param partition the partition to append
     * @return true if the partition was written, false otherwise
     */
    public boolean append(UnfilteredRowIterator partition)
    {
        maybeSwitchWriter(partition.partitionKey());
        return sstableWriter.append(partition) != null;
    }

    @Override
    protected Throwable doPostCleanup(Throwable accumulate)
    {
        sstableWriter.close();
        return super.doPostCleanup(accumulate);
    }

    /**
     * Switches the writer if necessary, i.e. if the new key should be placed in a different data directory, or if the
     * specific strategy has decided a new sstable is needed.
     *
     * Guaranteed to be called before the first call to realAppend.
     */
    protected void maybeSwitchWriter(DecoratedKey key)
    {
        if (maybeSwitchLocation(key))
            return;

        if (shouldSwitchWriterInCurrentLocation(key))
            switchCompactionWriter(currentDirectory);
    }

    /**
     * Switches the file location and writer and returns true if the new key should be placed in a different data
     * directory.
     */
    protected boolean maybeSwitchLocation(DecoratedKey key)
    {
        if (diskBoundaries == null)
        {
            if (locationIndex < 0)
            {
                Directories.DataDirectory defaultLocation = getWriteDirectory(nonExpiredSSTables, cfs.getExpectedCompactedFileSize(nonExpiredSSTables, OperationType.UNKNOWN));
                switchCompactionWriter(defaultLocation);
                locationIndex = 0;
                return true;
            }
            return false;
        }

        if (locationIndex > -1 && key.compareTo(diskBoundaries.get(locationIndex)) < 0)
            return false;

        int prevIdx = locationIndex;
        while (locationIndex == -1 || key.compareTo(diskBoundaries.get(locationIndex)) > 0)
            locationIndex++;
        Directories.DataDirectory newLocation = locations.get(locationIndex);
        if (prevIdx >= 0)
            logger.debug("Switching write location from {} to {}", locations.get(prevIdx), newLocation);
        switchCompactionWriter(newLocation);
        return true;
    }

    /**
     * Returns true if the writer should be switched for reasons other than switching to a new data directory
     * (e.g. because an sstable size limit has been reached).
     */
    protected abstract boolean shouldSwitchWriterInCurrentLocation(DecoratedKey key);

    /**
     * Implementations of this method should finish the current sstable writer and start writing to this directory.
     *
     * Called once before starting to append and then whenever we see a need to start writing to another directory.
     * @param directory
     */
    protected void switchCompactionWriter(Directories.DataDirectory directory)
    {
        currentDirectory = directory;
        PartitionPosition diskBoundary = diskBoundaries != null && locationIndex > -1
                                         ? diskBoundaries.get(locationIndex)
                                         : null;
        sstableWriter.switchWriter(sstableWriter(directory, diskBoundary));
    }

    @SuppressWarnings("resource")
    protected SSTableWriter sstableWriter(Directories.DataDirectory directory, PartitionPosition diskBoundary)
    {
        return SSTableWriter.create(cfs.newSSTableDescriptor(getDirectories().getLocationForDisk(directory)),
                                    estimatedTotalKeys,
                                    minRepairedAt,
                                    pendingRepair,
                                    isTransient,
                                    cfs.metadata,
                                    new MetadataCollector(txn.originals(), cfs.metadata().comparator),
                                    SerializationHeader.make(cfs.metadata(), nonExpiredSSTables),
                                    cfs.indexManager.listIndexGroups(),
                                    txn);
    }

    /**
     * The directories we can write to
     */
    public Directories getDirectories()
    {
        return directories;
    }

    /**
     * Return a directory where we can expect expectedWriteSize to fit.
     *
     * @param sstables the sstables to compact
     * @return
     */
    public Directories.DataDirectory getWriteDirectory(Iterable<SSTableReader> sstables, long estimatedWriteSize)
    {
        Descriptor descriptor = null;
        for (SSTableReader sstable : sstables)
        {
            if (descriptor == null)
                descriptor = sstable.descriptor;
            if (!descriptor.directory.equals(sstable.descriptor.directory))
            {
                logger.trace("All sstables not from the same disk - putting results in {}", descriptor.directory);
                break;
            }
        }
        Directories.DataDirectory d = getDirectories().getDataDirectoryForFile(descriptor);
        if (d != null)
        {
            long availableSpace = d.getAvailableSpace();
            if (availableSpace < estimatedWriteSize)
                throw new RuntimeException(String.format("Not enough space to write %s to %s (%s available)",
                                                         FBUtilities.prettyPrintMemory(estimatedWriteSize),
                                                         d.location,
                                                         FBUtilities.prettyPrintMemory(availableSpace)));
            logger.trace("putting compaction results in {}", descriptor.directory);
            return d;
        }
        d = getDirectories().getWriteableLocation(estimatedWriteSize);
        if (d == null)
            throw new RuntimeException(String.format("Not enough disk space to store %s",
                                                     FBUtilities.prettyPrintMemory(estimatedWriteSize)));
        return d;
    }

    public CompactionAwareWriter setRepairedAt(long repairedAt)
    {
        this.sstableWriter.setRepairedAt(repairedAt);
        return this;
    }

    public long bytesWritten()
    {
        return sstableWriter.bytesWritten();
    }
}
