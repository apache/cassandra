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
package org.apache.cassandra.io.sstable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Lists;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataTracker;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * Wraps one or more writers as output for rewriting one or more readers: every sstable_preemptive_open_interval_in_mb
 * we look in the summary we're collecting for the latest writer for the penultimate key that we know to have been fully
 * flushed to the index file, and then double check that the key is fully present in the flushed data file.
 * Then we move the starts of each reader forwards to that point, replace them in the datatracker, and attach a runnable
 * for on-close (i.e. when all references expire) that drops the page cache prior to that key position
 *
 * hard-links are created for each partially written sstable so that readers opened against them continue to work past
 * the rename of the temporary file, which is deleted once all readers against the hard-link have been closed.
 * If for any reason the writer is rolled over, we immediately rename and fully expose the completed file in the DataTracker.
 *
 * On abort we restore the original lower bounds to the existing readers and delete any temporary files we had in progress,
 * but leave any hard-links in place for the readers we opened to cleanup when they're finished as we would had we finished
 * successfully.
 */
public class SSTableRewriter
{
    private static long preemptiveOpenInterval;
    static
    {
        long interval = DatabaseDescriptor.getSSTablePreempiveOpenIntervalInMB() * (1L << 20);
        if (interval < 0)
            interval = Long.MAX_VALUE;
        preemptiveOpenInterval = interval;
    }

    @VisibleForTesting
    static void overrideOpenInterval(long size)
    {
        preemptiveOpenInterval = size;
    }

    private final DataTracker dataTracker;
    private final ColumnFamilyStore cfs;

    private final long maxAge;
    private final List<SSTableReader> finished = new ArrayList<>();
    private final Set<SSTableReader> rewriting; // the readers we are rewriting (updated as they are replaced)
    private final Map<Descriptor, DecoratedKey> originalStarts = new HashMap<>(); // the start key for each reader we are rewriting
    private final Map<Descriptor, Integer> fileDescriptors = new HashMap<>(); // the file descriptors for each reader descriptor we are rewriting

    private SSTableReader currentlyOpenedEarly; // the reader for the most recent (re)opening of the target file
    private long currentlyOpenedEarlyAt; // the position (in MB) in the target file we last (re)opened at

    private final List<SSTableReader> finishedOpenedEarly = new ArrayList<>(); // the 'finished' tmplink sstables
    private final List<Pair<SSTableWriter, SSTableReader>> finishedWriters = new ArrayList<>();
    private final boolean isOffline; // true for operations that are performed without Cassandra running (prevents updates of DataTracker)

    private SSTableWriter writer;
    private Map<DecoratedKey, RowIndexEntry> cachedKeys = new HashMap<>();

    public SSTableRewriter(ColumnFamilyStore cfs, Set<SSTableReader> rewriting, long maxAge, boolean isOffline)
    {
        this.rewriting = rewriting;
        for (SSTableReader sstable : rewriting)
        {
            originalStarts.put(sstable.descriptor, sstable.first);
            fileDescriptors.put(sstable.descriptor, CLibrary.getfd(sstable.getFilename()));
        }
        this.dataTracker = cfs.getDataTracker();
        this.cfs = cfs;
        this.maxAge = maxAge;
        this.isOffline = isOffline;
    }

    public SSTableWriter currentWriter()
    {
        return writer;
    }

    public RowIndexEntry append(AbstractCompactedRow row)
    {
        // we do this before appending to ensure we can resetAndTruncate() safely if the append fails
        maybeReopenEarly(row.key);
        RowIndexEntry index = writer.append(row);
        if (!isOffline)
        {
            if (index == null)
            {
                cfs.invalidateCachedRow(row.key);
            }
            else
            {
                boolean save = false;
                for (SSTableReader reader : rewriting)
                {
                    if (reader.getCachedPosition(row.key, false) != null)
                    {
                        save = true;
                        break;
                    }
                }
                if (save)
                    cachedKeys.put(row.key, index);
            }
        }
        return index;
    }

    // attempts to append the row, if fails resets the writer position
    public RowIndexEntry tryAppend(AbstractCompactedRow row)
    {
        writer.mark();
        try
        {
            return append(row);
        }
        catch (Throwable t)
        {
            writer.resetAndTruncate();
            throw t;
        }
    }

    private void maybeReopenEarly(DecoratedKey key)
    {
        if (FBUtilities.isUnix() && writer.getFilePointer() - currentlyOpenedEarlyAt > preemptiveOpenInterval)
        {
            if (isOffline)
            {
                for (SSTableReader reader : rewriting)
                {
                    RowIndexEntry index = reader.getPosition(key, SSTableReader.Operator.GE);
                    CLibrary.trySkipCache(fileDescriptors.get(reader.descriptor), 0, index == null ? 0 : index.position);
                }
            }
            else
            {
                SSTableReader reader = writer.openEarly(maxAge);
                if (reader != null)
                {
                    replaceEarlyOpenedFile(currentlyOpenedEarly, reader);
                    currentlyOpenedEarly = reader;
                    currentlyOpenedEarlyAt = writer.getFilePointer();
                    moveStarts(reader, Functions.constant(reader.last), false);
                }
            }
        }
    }

    public void abort()
    {
        switchWriter(null);

        moveStarts(null, Functions.forMap(originalStarts), true);

        List<SSTableReader> close = Lists.newArrayList(finishedOpenedEarly);

        for (Pair<SSTableWriter, SSTableReader> w : finishedWriters)
        {
        // we should close the bloom filter if we have not opened an sstable reader from this
        // writer (it will get closed when we release the sstable reference below):
            w.left.abort(w.right == null);
        }

        // also remove already completed SSTables
        for (SSTableReader sstable : close)
            sstable.markObsolete();

        for (SSTableReader sstable : finished)
        {
            sstable.markObsolete();
            sstable.releaseReference();
        }

        // releases reference in replaceReaders
        if (!isOffline)
        {
            dataTracker.replaceEarlyOpenedFiles(close, Collections.<SSTableReader>emptyList());
            dataTracker.unmarkCompacting(close);
        }
    }


    /**
     * Replace the readers we are rewriting with cloneWithNewStart, reclaiming any page cache that is no longer
     * needed, and transferring any key cache entries over to the new reader, expiring them from the old. if reset
     * is true, we are instead restoring the starts of the readers from before the rewriting began
     *
     * note that we replace an existing sstable with a new *instance* of the same sstable, the replacement
     * sstable .equals() the old one, BUT, it is a new instance, so, for example, since we releaseReference() on the old
     * one, the old *instance* will have reference count == 0 and if we were to start a new compaction with that old
     * instance, we would get exceptions.
     *
     * @param newReader the rewritten reader that replaces them for this region
     * @param newStarts a function mapping a reader's descriptor to their new start value
     * @param reset true iff we are restoring earlier starts (increasing the range over which they are valid)
     */
    private void moveStarts(SSTableReader newReader, Function<? super Descriptor, DecoratedKey> newStarts, boolean reset)
    {
        if (isOffline)
            return;
        List<SSTableReader> toReplace = new ArrayList<>();
        List<SSTableReader> replaceWith = new ArrayList<>();
        final List<DecoratedKey> invalidateKeys = new ArrayList<>();
        if (!reset)
        {
            invalidateKeys.addAll(cachedKeys.keySet());
            for (Map.Entry<DecoratedKey, RowIndexEntry> cacheKey : cachedKeys.entrySet())
                newReader.cacheKey(cacheKey.getKey(), cacheKey.getValue());
        }
        cachedKeys = new HashMap<>();
        for (final SSTableReader sstable : rewriting)
        {
            DecoratedKey newStart = newStarts.apply(sstable.descriptor);
            assert newStart != null;
            if (sstable.first.compareTo(newStart) < 0 || (reset && newStart != sstable.first))
            {
                toReplace.add(sstable);
                // we call getCurrentReplacement() to support multiple rewriters operating over the same source readers at once.
                // note: only one such writer should be written to at any moment
                replaceWith.add(sstable.getCurrentReplacement().cloneWithNewStart(newStart, new Runnable()
                {
                    public void run()
                    {
                        // this is somewhat racey, in that we could theoretically be closing this old reader
                        // when an even older reader is still in use, but it's not likely to have any major impact
                        for (DecoratedKey key : invalidateKeys)
                            sstable.invalidateCacheKey(key);
                    }
                }));
            }
        }
        cfs.getDataTracker().replaceWithNewInstances(toReplace, replaceWith);
        rewriting.removeAll(toReplace);
        rewriting.addAll(replaceWith);
    }



    private void replaceEarlyOpenedFile(SSTableReader toReplace, SSTableReader replaceWith)
    {
        if (isOffline)
            return;
        Set<SSTableReader> toReplaceSet;
        if (toReplace != null)
        {
            toReplace.setReplacedBy(replaceWith);
            toReplaceSet = Collections.singleton(toReplace);
        }
        else
        {
            dataTracker.markCompacting(Collections.singleton(replaceWith));
            toReplaceSet = Collections.emptySet();
        }
        dataTracker.replaceEarlyOpenedFiles(toReplaceSet, Collections.singleton(replaceWith));
    }

    public void switchWriter(SSTableWriter newWriter)
    {
        if (writer == null)
        {
            writer = newWriter;
            return;
        }
        // we leave it as a tmp file, but we open it early and add it to the dataTracker
        SSTableReader reader = writer.openEarly(maxAge);
        if (reader != null)
        {
            finishedOpenedEarly.add(reader);
            replaceEarlyOpenedFile(currentlyOpenedEarly, reader);
            moveStarts(reader, Functions.constant(reader.last), false);
        }
        finishedWriters.add(Pair.create(writer, reader));
        currentlyOpenedEarly = null;
        currentlyOpenedEarlyAt = 0;
        writer = newWriter;
    }

    public List<SSTableReader> finish()
    {
        return finish(-1);
    }

    /**
     * Finishes the new file(s)
     *
     * Creates final files, adds the new files to the dataTracker (via replaceReader).
     *
     * We add them to the tracker to be able to get rid of the tmpfiles
     *
     * It is up to the caller to do the compacted sstables replacement
     * gymnastics (ie, call DataTracker#markCompactedSSTablesReplaced(..))
     *
     *
     * @param repairedAt the repair time, -1 if we should use the time we supplied when we created
     *                   the SSTableWriter (and called rewriter.switchWriter(..)), actual time if we want to override the
     *                   repair time.
     */
    public List<SSTableReader> finish(long repairedAt)
    {
        List<Pair<SSTableReader, SSTableReader>> toReplace = new ArrayList<>();
        switchWriter(null);
        // make real sstables of the written ones:
        Iterator<Pair<SSTableWriter, SSTableReader>> it = finishedWriters.iterator();
        while(it.hasNext())
        {
            Pair<SSTableWriter, SSTableReader> w = it.next();
            if (w.left.getFilePointer() > 0)
            {
                SSTableReader newReader = repairedAt < 0 ? w.left.closeAndOpenReader(maxAge) : w.left.closeAndOpenReader(maxAge, repairedAt);
                finished.add(newReader);

                if (w.right != null)
                    w.right.sharesBfWith(newReader);
                // w.right is the tmplink-reader we added when switching writer, replace with the real sstable.
                toReplace.add(Pair.create(w.right, newReader));
            }
            else
            {
                assert w.right == null;
                w.left.abort(true);
            }
            it.remove();
        }

        for (Pair<SSTableReader, SSTableReader> replace : toReplace)
            replaceEarlyOpenedFile(replace.left, replace.right);

        if (!isOffline)
        {
            dataTracker.unmarkCompacting(finished);
        }
        return finished;
    }

    @VisibleForTesting
    void finishAndThrow(boolean early)
    {
        List<Pair<SSTableReader, SSTableReader>> toReplace = new ArrayList<>();
        switchWriter(null);
        if (early)
            throw new RuntimeException("exception thrown early in finish");
        // make real sstables of the written ones:
        Iterator<Pair<SSTableWriter, SSTableReader>> it = finishedWriters.iterator();
        while(it.hasNext())
        {
            Pair<SSTableWriter, SSTableReader> w = it.next();
            if (w.left.getFilePointer() > 0)
            {
                SSTableReader newReader = w.left.closeAndOpenReader(maxAge);
                finished.add(newReader);
                if (w.right != null)
                    w.right.sharesBfWith(newReader);
                // w.right is the tmplink-reader we added when switching writer, replace with the real sstable.
                toReplace.add(Pair.create(w.right, newReader));
            }
            else
            {
                assert w.right == null;
                w.left.abort(true);
            }
            it.remove();
        }

        throw new RuntimeException("exception thrown after all sstables finished");
    }
}
