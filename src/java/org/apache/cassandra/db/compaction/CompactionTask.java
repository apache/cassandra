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

import java.io.File;
import java.io.IOException;
import java.util.*;

import com.google.common.base.Throwables;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager.CompactionExecutorStatsCollector;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.utils.CloseableIterator;

public class CompactionTask extends AbstractCompactionTask
{
    protected static final Logger logger = LoggerFactory.getLogger(CompactionTask.class);
    protected final int gcBefore;
    protected static long totalBytesCompacted = 0;
    private Set<SSTableReader> toCompact;
    private CompactionExecutorStatsCollector collector;

    public CompactionTask(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, final int gcBefore)
    {
        super(cfs, sstables);
        this.gcBefore = gcBefore;
        toCompact = new HashSet<SSTableReader>(sstables);
    }

    public static synchronized long addToTotalBytesCompacted(long bytesCompacted)
    {
        return totalBytesCompacted += bytesCompacted;
    }

    protected int executeInternal(CompactionExecutorStatsCollector collector)
    {
        this.collector = collector;
        run();
        return toCompact.size();
    }

    public long getExpectedWriteSize()
    {
        return cfs.getExpectedCompactedFileSize(toCompact, compactionType);
    }

    public boolean reduceScopeForLimitedSpace()
    {
        if (partialCompactionsAcceptable() && toCompact.size() > 1)
        {
            // Try again w/o the largest one.
            logger.warn("insufficient space to compact all requested files " + StringUtils.join(toCompact, ", "));
            // Note that we have removed files that are still marked as compacting.
            // This suboptimal but ok since the caller will unmark all the sstables at the end.
            return toCompact.remove(cfs.getMaxSizeFile(toCompact));
        }
        else
        {
            return false;
        }
    }

    /**
     * For internal use and testing only.  The rest of the system should go through the submit* methods,
     * which are properly serialized.
     * Caller is in charge of marking/unmarking the sstables as compacting.
     */
    protected void runWith(File sstableDirectory) throws Exception
    {
        // The collection of sstables passed may be empty (but not null); even if
        // it is not empty, it may compact down to nothing if all rows are deleted.
        assert sstables != null && sstableDirectory != null;

        if (DatabaseDescriptor.isSnapshotBeforeCompaction())
            cfs.snapshotWithoutFlush(System.currentTimeMillis() + "-compact-" + cfs.columnFamily);

        // sanity check: all sstables must belong to the same cfs
        for (SSTableReader sstable : toCompact)
            assert sstable.descriptor.cfname.equals(cfs.columnFamily);

        CompactionController controller = new CompactionController(cfs, toCompact, gcBefore);
        // new sstables from flush can be added during a compaction, but only the compaction can remove them,
        // so in our single-threaded compaction world this is a valid way of determining if we're compacting
        // all the sstables (that existed when we started)
        logger.info("Compacting {}", toCompact);

        long startTime = System.currentTimeMillis();
        long totalkeysWritten = 0;

        AbstractCompactionStrategy strategy = cfs.getCompactionStrategy();
        long estimatedTotalKeys = Math.max(DatabaseDescriptor.getIndexInterval(), SSTableReader.getApproximateKeyCount(toCompact));
        long estimatedSSTables = Math.max(1, SSTable.getTotalBytes(toCompact) / strategy.getMaxSSTableSize());
        long keysPerSSTable = (long) Math.ceil((double) estimatedTotalKeys / estimatedSSTables);
        if (logger.isDebugEnabled())
            logger.debug("Expected bloom filter size : " + keysPerSSTable);

        AbstractCompactionIterable ci = DatabaseDescriptor.isMultithreadedCompaction()
                                      ? new ParallelCompactionIterable(compactionType, strategy.getScanners(toCompact), controller)
                                      : new CompactionIterable(compactionType, strategy.getScanners(toCompact), controller);
        CloseableIterator<AbstractCompactedRow> iter = ci.iterator();
        Map<DecoratedKey, RowIndexEntry> cachedKeys = new HashMap<DecoratedKey, RowIndexEntry>();

        // we can't preheat until the tracker has been set. This doesn't happen until we tell the cfs to
        // replace the old entries.  Track entries to preheat here until then.
        Map<Descriptor, Map<DecoratedKey, RowIndexEntry>> cachedKeyMap =  new HashMap<Descriptor, Map<DecoratedKey, RowIndexEntry>>();

        Collection<SSTableReader> sstables = new ArrayList<SSTableReader>();
        Collection<SSTableWriter> writers = new ArrayList<SSTableWriter>();

        if (collector != null)
            collector.beginCompaction(ci);
        try
        {
            if (!iter.hasNext())
            {
                // don't mark compacted in the finally block, since if there _is_ nondeleted data,
                // we need to sync it (via closeAndOpen) first, so there is no period during which
                // a crash could cause data loss.
                cfs.markCompacted(toCompact, compactionType);
                return;
            }

            SSTableWriter writer = cfs.createCompactionWriter(keysPerSSTable, sstableDirectory, toCompact);
            writers.add(writer);
            while (iter.hasNext())
            {
                if (ci.isStopRequested())
                    throw new CompactionInterruptedException(ci.getCompactionInfo());

                AbstractCompactedRow row = iter.next();
                if (row.isEmpty())
                {
                    controller.invalidateCachedRow(row.key);
                    row.close();
                    continue;
                }

                // If the row is cached, we call removeDeleted on at read time it to have coherent query returns,
                // but if the row is not pushed out of the cache, obsolete tombstones will persist indefinitely.
                controller.removeDeletedInCache(row.key);

                RowIndexEntry indexEntry = writer.append(row);
                totalkeysWritten++;

                if (DatabaseDescriptor.getPreheatKeyCache())
                {
                    for (SSTableReader sstable : toCompact)
                    {
                        if (sstable.getCachedPosition(row.key, false) != null)
                        {
                            cachedKeys.put(row.key, indexEntry);
                            break;
                        }
                    }
                }

                if (newSSTableSegmentThresholdReached(writer))
                {
                    // tmp = false because later we want to query it with descriptor from SSTableReader
                    cachedKeyMap.put(writer.descriptor.asTemporary(false), cachedKeys);
                    writer = cfs.createCompactionWriter(keysPerSSTable, sstableDirectory, toCompact);
                    writers.add(writer);
                    cachedKeys = new HashMap<DecoratedKey, RowIndexEntry>();
                }
            }

            if (writer.getFilePointer() > 0)
            {
                cachedKeyMap.put(writer.descriptor.asTemporary(false), cachedKeys);
            }
            else
            {
                writer.abort();
                writers.remove(writer);
            }

            long maxAge = getMaxDataAge(toCompact);
            for (SSTableWriter completedWriter : writers)
                sstables.add(completedWriter.closeAndOpenReader(maxAge));
        }
        catch (Throwable t)
        {
            for (SSTableWriter writer : writers)
                writer.abort();
            // also remove already completed SSTables
            for (SSTableReader sstable : sstables)
            {
                sstable.markCompacted();
                sstable.releaseReference();
            }
            throw Throwables.propagate(t);
        }
        finally
        {
            controller.close();

            try
            {
                iter.close();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

            if (collector != null)
                collector.finishCompaction(ci);
        }

        cfs.replaceCompactedSSTables(toCompact, sstables, compactionType);
        // TODO: this doesn't belong here, it should be part of the reader to load when the tracker is wired up
        for (SSTableReader sstable : sstables)
        {
            for (Map.Entry<DecoratedKey, RowIndexEntry> entry : cachedKeyMap.get(sstable.descriptor).entrySet())
               sstable.cacheKey(entry.getKey(), entry.getValue());
        }

        if (logger.isInfoEnabled())
        {
            // log a bunch of statistics about the result
            long dTime = System.currentTimeMillis() - startTime;
            long startsize = SSTable.getTotalBytes(toCompact);
            long endsize = SSTable.getTotalBytes(sstables);
            double ratio = (double)endsize / (double)startsize;

            StringBuilder builder = new StringBuilder();
            for (SSTableReader reader : sstables)
                builder.append(reader.descriptor.baseFilename()).append(",");

            double mbps = dTime > 0 ? (double)endsize/(1024*1024)/((double)dTime/1000) : 0;
            long totalSourceRows = 0;
            long[] counts = ci.getMergedRowCounts();
            StringBuilder mergeSummary = new StringBuilder(counts.length * 10);
            for (int i = 0; i < counts.length; i++)
            {
                int rows = i + 1;
                long count = counts[i];
                totalSourceRows += rows * count;
                mergeSummary.append(String.format("%d:%d, ", rows, count));
            }

            logger.info(String.format("Compacted %d sstables to [%s].  %,d bytes to %,d (~%d%% of original) in %,dms = %fMB/s.  %,d total rows, %,d unique.  Row merge counts were {%s}",
                                      toCompact.size(), builder.toString(), startsize, endsize, (int) (ratio * 100), dTime, mbps, totalSourceRows, totalkeysWritten, mergeSummary.toString()));
            logger.debug(String.format("CF Total Bytes Compacted: %,d", CompactionTask.addToTotalBytesCompacted(endsize)));
        }
    }

    protected boolean partialCompactionsAcceptable()
    {
        return !isUserDefined;
    }

    // extensibility point for other strategies that may want to limit the upper bounds of the sstable segment size
    protected boolean newSSTableSegmentThresholdReached(SSTableWriter writer)
    {
        return false;
    }

    public static long getMaxDataAge(Collection<SSTableReader> sstables)
    {
        long max = 0;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.maxDataAge > max)
                max = sstable.maxDataAge;
        }
        return max;
    }
}
