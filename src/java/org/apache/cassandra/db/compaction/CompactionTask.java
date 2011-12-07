/**
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
import java.util.*;
import java.util.Map.Entry;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionManager.CompactionExecutorStatsCollector;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

public class CompactionTask extends AbstractCompactionTask
{
    protected static final Logger logger = LoggerFactory.getLogger(CompactionTask.class);
    protected String compactionFileLocation;
    protected final int gcBefore;
    protected boolean isUserDefined;
    protected static long totalBytesCompacted = 0;

    public CompactionTask(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, final int gcBefore)
    {
        super(cfs, sstables);
        compactionFileLocation = null;
        this.gcBefore = gcBefore;
        this.isUserDefined = false;
    }

    public static synchronized long addToTotalBytesCompacted(long bytesCompacted)
    {
        return totalBytesCompacted += bytesCompacted;
    }

    /**
     * For internal use and testing only.  The rest of the system should go through the submit* methods,
     * which are properly serialized.
     * Caller is in charge of marking/unmarking the sstables as compacting.
     */
    public int execute(CompactionExecutorStatsCollector collector) throws IOException
    {
        // The collection of sstables passed may be empty (but not null); even if
        // it is not empty, it may compact down to nothing if all rows are deleted.
        assert sstables != null;

        Set<SSTableReader> toCompact = new HashSet<SSTableReader>(sstables);
        if (!isCompactionInteresting(toCompact))
            return 0;

        if (compactionFileLocation == null)
            compactionFileLocation = cfs.table.getDataFileLocation(cfs.getExpectedCompactedFileSize(toCompact));
        if (partialCompactionsAcceptable())
        {
            // If the compaction file path is null that means we have no space left for this compaction.
            // Try again w/o the largest one.
            if (compactionFileLocation == null)
            {
                while (compactionFileLocation == null && toCompact.size() > 1)
                {
                    logger.warn("insufficient space to compact all requested files " + StringUtils.join(toCompact, ", "));
                    // Note that we have removed files that are still marked as compacting. This suboptimal but ok since the caller will unmark all
                    // the sstables at the end.
                    toCompact.remove(cfs.getMaxSizeFile(toCompact));
                    compactionFileLocation = cfs.table.getDataFileLocation(cfs.getExpectedCompactedFileSize(toCompact));
                }
            }

            if (compactionFileLocation == null)
            {
                logger.warn("insufficient space to compact even the two smallest files, aborting");
                return 0;
            }
        }

        if (DatabaseDescriptor.isSnapshotBeforeCompaction())
            cfs.table.snapshot(System.currentTimeMillis() + "-" + "compact-" + cfs.columnFamily);

        // sanity check: all sstables must belong to the same cfs
        for (SSTableReader sstable : toCompact)
            assert sstable.descriptor.cfname.equals(cfs.columnFamily);

        CompactionController controller = new CompactionController(cfs, toCompact, gcBefore, isUserDefined);
        // new sstables from flush can be added during a compaction, but only the compaction can remove them,
        // so in our single-threaded compaction world this is a valid way of determining if we're compacting
        // all the sstables (that existed when we started)
        logger.info("Compacting {}", toCompact);

        long startTime = System.currentTimeMillis();
        long totalkeysWritten = 0;

        long estimatedTotalKeys = Math.max(DatabaseDescriptor.getIndexInterval(), SSTableReader.getApproximateKeyCount(toCompact));
        long estimatedSSTables = Math.max(1, SSTable.getTotalBytes(toCompact) / cfs.getCompactionStrategy().getMaxSSTableSize());
        long keysPerSSTable = (long) Math.ceil((double) estimatedTotalKeys / estimatedSSTables);
        if (logger.isDebugEnabled())
            logger.debug("Expected bloom filter size : " + keysPerSSTable);

        AbstractCompactionIterable ci = DatabaseDescriptor.isMultithreadedCompaction()
                                      ? new ParallelCompactionIterable(OperationType.COMPACTION, toCompact, controller)
                                      : new CompactionIterable(OperationType.COMPACTION, toCompact, controller);
        CloseableIterator<AbstractCompactedRow> iter = ci.iterator();
        Iterator<AbstractCompactedRow> nni = Iterators.filter(iter, Predicates.notNull());
        Map<DecoratedKey, Long> cachedKeys = new HashMap<DecoratedKey, Long>();

        // we can't preheat until the tracker has been set. This doesn't happen until we tell the cfs to
        // replace the old entries.  Track entries to preheat here until then.
        Map<SSTableReader, Map<DecoratedKey, Long>> cachedKeyMap =  new HashMap<SSTableReader, Map<DecoratedKey, Long>>();

        Collection<SSTableReader> sstables = new ArrayList<SSTableReader>();
        Collection<SSTableWriter> writers = new ArrayList<SSTableWriter>();

        if (collector != null)
            collector.beginCompaction(ci);
        try
        {
            if (!nni.hasNext())
            {
                // don't mark compacted in the finally block, since if there _is_ nondeleted data,
                // we need to sync it (via closeAndOpen) first, so there is no period during which
                // a crash could cause data loss.
                cfs.markCompacted(toCompact);
                return 0;
            }

            SSTableWriter writer = cfs.createCompactionWriter(keysPerSSTable, compactionFileLocation, toCompact);
            writers.add(writer);
            while (nni.hasNext())
            {
                AbstractCompactedRow row = nni.next();
                if (row.isEmpty())
                    continue;

                long position = writer.append(row);
                totalkeysWritten++;

                if (DatabaseDescriptor.getPreheatKeyCache())
                {
                    for (SSTableReader sstable : toCompact)
                    {
                        if (sstable.getCachedPosition(row.key, false) != null)
                        {
                            cachedKeys.put(row.key, position);
                            break;
                        }
                    }
                }
                if (!nni.hasNext() || newSSTableSegmentThresholdReached(writer, position))
                {
                    SSTableReader toIndex = writer.closeAndOpenReader(getMaxDataAge(toCompact));
                    cachedKeyMap.put(toIndex, cachedKeys);
                    sstables.add(toIndex);
                    writer = cfs.createCompactionWriter(keysPerSSTable, compactionFileLocation, toCompact);
                    writers.add(writer);
                    cachedKeys = new HashMap<DecoratedKey, Long>();
                }
            }
        }
        catch (Exception e)
        {
            for (SSTableWriter writer : writers)
                writer.abort();
            throw FBUtilities.unchecked(e);
        }
        finally
        {
            iter.close();
            if (collector != null)
                collector.finishCompaction(ci);
        }

        cfs.replaceCompactedSSTables(toCompact, sstables);
        // TODO: this doesn't belong here, it should be part of the reader to load when the tracker is wired up
        for (Entry<SSTableReader, Map<DecoratedKey, Long>> ssTableReaderMapEntry : cachedKeyMap.entrySet())
        {
            SSTableReader key = ssTableReaderMapEntry.getKey();
            for (Entry<DecoratedKey, Long> entry : ssTableReaderMapEntry.getValue().entrySet())
               key.cacheKey(entry.getKey(), entry.getValue());
        }

        long dTime = System.currentTimeMillis() - startTime;
        long startsize = SSTable.getTotalBytes(toCompact);
        long endsize = SSTable.getTotalBytes(sstables);
        double ratio = (double)endsize / (double)startsize;

        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (SSTableReader reader : sstables)
            builder.append(reader.getFilename()).append(",");
        builder.append("]");

        double mbps = dTime > 0 ? (double)endsize/(1024*1024)/((double)dTime/1000) : 0;
        logger.info(String.format("Compacted to %s.  %,d to %,d (~%d%% of original) bytes for %,d keys at %fMB/s.  Time: %,dms.",
                                  builder.toString(), startsize, endsize, (int) (ratio * 100), totalkeysWritten, mbps, dTime));
        logger.debug(String.format("CF Total Bytes Compacted: %,d", CompactionTask.addToTotalBytesCompacted(endsize)));
        return toCompact.size();
    }

    protected boolean partialCompactionsAcceptable()
    {
        return !isUserDefined;
    }

    //extensibility point for other strategies that may want to limit the upper bounds of the sstable segment size
    protected boolean newSSTableSegmentThresholdReached(SSTableWriter writer, long position)
    {
        return false;
    }

    /**
     * @return true if the proposed compaction is worth proceeding with.  We allow leveled compaction to
     * override this to allow "promoting" sstables from one level to another w/o rewriting them, if there is no overlapping.
     */
    protected boolean isCompactionInteresting(Set<SSTableReader> toCompact)
    {
        if (isUserDefined || toCompact.size() >= 2)
            return true;
        logger.info(String.format("Nothing to compact in %s.  Use forceUserDefinedCompaction if you wish to force compaction of single sstables (e.g. for tombstone collection)",
                                   cfs.getColumnFamilyName()));
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

    public CompactionTask compactionFileLocation(String compactionFileLocation)
    {
        this.compactionFileLocation = compactionFileLocation;
        return this;
    }

    public CompactionTask isUserDefined(boolean isUserDefined)
    {
        this.isUserDefined = isUserDefined;
        return this;
    }
}
