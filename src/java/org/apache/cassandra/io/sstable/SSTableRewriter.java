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

import java.lang.ref.WeakReference;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cache.InstrumentingCache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.concurrent.Transactional;

/**
 * Wraps one or more writers as output for rewriting one or more readers: every sstable_preemptive_open_interval_in_mb
 * we look in the summary we're collecting for the latest writer for the penultimate key that we know to have been fully
 * flushed to the index file, and then double check that the key is fully present in the flushed data file.
 * Then we move the starts of each reader forwards to that point, replace them in the Tracker, and attach a runnable
 * for on-close (i.e. when all references expire) that drops the page cache prior to that key position
 *
 * hard-links are created for each partially written sstable so that readers opened against them continue to work past
 * the rename of the temporary file, which is deleted once all readers against the hard-link have been closed.
 * If for any reason the writer is rolled over, we immediately rename and fully expose the completed file in the Tracker.
 *
 * On abort we restore the original lower bounds to the existing readers and delete any temporary files we had in progress,
 * but leave any hard-links in place for the readers we opened to cleanup when they're finished as we would had we finished
 * successfully.
 */
public class SSTableRewriter extends Transactional.AbstractTransactional implements Transactional
{
    @VisibleForTesting
    public static boolean disableEarlyOpeningForTests = false;

    private final long preemptiveOpenInterval;
    private final long maxAge;
    private long repairedAt = -1;
    // the set of final readers we will expose on commit
    private final ILifecycleTransaction transaction; // the readers we are rewriting (updated as they are replaced)
    private final List<SSTableReader> preparedForCommit = new ArrayList<>();

    private long currentlyOpenedEarlyAt; // the position (in MB) in the target file we last (re)opened at

    private final List<SSTableWriter> writers = new ArrayList<>();
    private final boolean keepOriginals; // true if we do not want to obsolete the originals

    private SSTableWriter writer;
    private Map<DecoratedKey, RowIndexEntry> cachedKeys = new HashMap<>();

    // for testing (TODO: remove when have byteman setup)
    private boolean throwEarly, throwLate;

    @Deprecated
    public SSTableRewriter(ILifecycleTransaction transaction, long maxAge, boolean isOffline)
    {
        this(transaction, maxAge, isOffline, true);
    }
    @Deprecated
    public SSTableRewriter(ILifecycleTransaction transaction, long maxAge, boolean isOffline, boolean shouldOpenEarly)
    {
        this(transaction, maxAge, calculateOpenInterval(shouldOpenEarly), false);
    }

    @VisibleForTesting
    public SSTableRewriter(ILifecycleTransaction transaction, long maxAge, long preemptiveOpenInterval, boolean keepOriginals)
    {
        this.transaction = transaction;
        this.maxAge = maxAge;
        this.keepOriginals = keepOriginals;
        this.preemptiveOpenInterval = preemptiveOpenInterval;
    }

    @Deprecated
    public static SSTableRewriter constructKeepingOriginals(ILifecycleTransaction transaction, boolean keepOriginals, long maxAge, boolean isOffline)
    {
        return constructKeepingOriginals(transaction, keepOriginals, maxAge);
    }

    public static SSTableRewriter constructKeepingOriginals(ILifecycleTransaction transaction, boolean keepOriginals, long maxAge)
    {
        return new SSTableRewriter(transaction, maxAge, calculateOpenInterval(true), keepOriginals);
    }

    public static SSTableRewriter constructWithoutEarlyOpening(ILifecycleTransaction transaction, boolean keepOriginals, long maxAge)
    {
        return new SSTableRewriter(transaction, maxAge, calculateOpenInterval(false), keepOriginals);
    }

    public static SSTableRewriter construct(ColumnFamilyStore cfs, ILifecycleTransaction transaction, boolean keepOriginals, long maxAge)
    {
        return new SSTableRewriter(transaction, maxAge, calculateOpenInterval(cfs.supportsEarlyOpen()), keepOriginals);
    }

    private static long calculateOpenInterval(boolean shouldOpenEarly)
    {
        long interval = DatabaseDescriptor.getSSTablePreempiveOpenIntervalInMB() * (1L << 20);
        if (disableEarlyOpeningForTests || !shouldOpenEarly || interval < 0)
            interval = Long.MAX_VALUE;
        return interval;
    }

    public SSTableWriter currentWriter()
    {
        return writer;
    }

    public RowIndexEntry append(UnfilteredRowIterator partition)
    {
        // we do this before appending to ensure we can resetAndTruncate() safely if the append fails
        DecoratedKey key = partition.partitionKey();
        maybeReopenEarly(key);
        RowIndexEntry index = writer.append(partition);
        if (!transaction.isOffline() && index != null)
        {
            for (SSTableReader reader : transaction.originals())
            {
                if (reader.getCachedPosition(key, false) != null)
                {
                    cachedKeys.put(key, index);
                    break;
                }
            }
        }
        return index;
    }

    // attempts to append the row, if fails resets the writer position
    public RowIndexEntry tryAppend(UnfilteredRowIterator partition)
    {
        writer.mark();
        try
        {
            return append(partition);
        }
        catch (Throwable t)
        {
            writer.resetAndTruncate();
            throw t;
        }
    }

    private void maybeReopenEarly(DecoratedKey key)
    {
        if (writer.getFilePointer() - currentlyOpenedEarlyAt > preemptiveOpenInterval)
        {
            if (transaction.isOffline())
            {
                for (SSTableReader reader : transaction.originals())
                {
                    RowIndexEntry index = reader.getPosition(key, SSTableReader.Operator.GE);
                    NativeLibrary.trySkipCache(reader.getFilename(), 0, index == null ? 0 : index.position);
                }
            }
            else
            {
                SSTableReader reader = writer.setMaxDataAge(maxAge).openEarly();
                if (reader != null)
                {
                    transaction.update(reader, false);
                    currentlyOpenedEarlyAt = writer.getFilePointer();
                    moveStarts(reader, reader.last);
                    transaction.checkpoint();
                }
            }
        }
    }

    protected Throwable doAbort(Throwable accumulate)
    {
        // abort the writers
        for (SSTableWriter writer : writers)
            accumulate = writer.abort(accumulate);
        // abort the lifecycle transaction
        accumulate = transaction.abort(accumulate);
        return accumulate;
    }

    protected Throwable doCommit(Throwable accumulate)
    {
        for (SSTableWriter writer : writers)
            accumulate = writer.commit(accumulate);

        accumulate = transaction.commit(accumulate);
        return accumulate;
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
     * @param lowerbound if !reset, must be non-null, and marks the exclusive lowerbound of the start for each sstable
     */
    private void moveStarts(SSTableReader newReader, DecoratedKey lowerbound)
    {
        if (transaction.isOffline())
            return;
        if (preemptiveOpenInterval == Long.MAX_VALUE)
            return;

        newReader.setupOnline();
        List<DecoratedKey> invalidateKeys = null;
        if (!cachedKeys.isEmpty())
        {
            invalidateKeys = new ArrayList<>(cachedKeys.size());
            for (Map.Entry<DecoratedKey, RowIndexEntry> cacheKey : cachedKeys.entrySet())
            {
                invalidateKeys.add(cacheKey.getKey());
                newReader.cacheKey(cacheKey.getKey(), cacheKey.getValue());
            }
        }

        cachedKeys.clear();
        for (SSTableReader sstable : transaction.originals())
        {
            // we call getCurrentReplacement() to support multiple rewriters operating over the same source readers at once.
            // note: only one such writer should be written to at any moment
            final SSTableReader latest = transaction.current(sstable);

            // skip any sstables that we know to already be shadowed
            if (latest.first.compareTo(lowerbound) > 0)
                continue;

            Runnable runOnClose = invalidateKeys != null ? new InvalidateKeys(latest, invalidateKeys) : null;
            if (lowerbound.compareTo(latest.last) >= 0)
            {
                if (!transaction.isObsolete(latest))
                {
                    if (runOnClose != null)
                    {
                        latest.runOnClose(runOnClose);
                    }
                    transaction.obsolete(latest);
                }
                continue;
            }

            DecoratedKey newStart = latest.firstKeyBeyond(lowerbound);
            assert newStart != null;
            SSTableReader replacement = latest.cloneWithNewStart(newStart, runOnClose);
            transaction.update(replacement, true);
        }
    }

    private static final class InvalidateKeys implements Runnable
    {
        final List<KeyCacheKey> cacheKeys = new ArrayList<>();
        final WeakReference<InstrumentingCache<KeyCacheKey, ?>> cacheRef;

        private InvalidateKeys(SSTableReader reader, Collection<DecoratedKey> invalidate)
        {
            this.cacheRef = new WeakReference<>(reader.getKeyCache());
            if (cacheRef.get() != null)
            {
                for (DecoratedKey key : invalidate)
                    cacheKeys.add(reader.getCacheKey(key));
            }
        }

        public void run()
        {
            for (KeyCacheKey key : cacheKeys)
            {
                InstrumentingCache<KeyCacheKey, ?> cache = cacheRef.get();
                if (cache != null)
                    cache.remove(key);
            }
        }
    }

    public void switchWriter(SSTableWriter newWriter)
    {
        if (newWriter != null)
            writers.add(newWriter.setMaxDataAge(maxAge));

        if (writer == null || writer.getFilePointer() == 0)
        {
            if (writer != null)
            {
                writer.abort();

                transaction.untrackNew(writer);
                writers.remove(writer);
            }
            writer = newWriter;

            return;
        }

        if (preemptiveOpenInterval != Long.MAX_VALUE)
        {
            // we leave it as a tmp file, but we open it and add it to the Tracker
            SSTableReader reader = writer.setMaxDataAge(maxAge).openFinalEarly();
            transaction.update(reader, false);
            moveStarts(reader, reader.last);
            transaction.checkpoint();
        }

        currentlyOpenedEarlyAt = 0;
        writer = newWriter;
    }

    /**
     * @param repairedAt the repair time, -1 if we should use the time we supplied when we created
     *                   the SSTableWriter (and called rewriter.switchWriter(..)), actual time if we want to override the
     *                   repair time.
     */
    public SSTableRewriter setRepairedAt(long repairedAt)
    {
        this.repairedAt = repairedAt;
        return this;
    }

    /**
     * Finishes the new file(s)
     *
     * Creates final files, adds the new files to the Tracker (via replaceReader).
     *
     * We add them to the tracker to be able to get rid of the tmpfiles
     *
     * It is up to the caller to do the compacted sstables replacement
     * gymnastics (ie, call Tracker#markCompactedSSTablesReplaced(..))
     *
     *
     */
    public List<SSTableReader> finish()
    {
        super.finish();
        return finished();
    }

    // returns, in list form, the
    public List<SSTableReader> finished()
    {
        assert state() == State.COMMITTED || state() == State.READY_TO_COMMIT;
        return preparedForCommit;
    }

    protected void doPrepare()
    {
        switchWriter(null);

        if (throwEarly)
            throw new RuntimeException("exception thrown early in finish, for testing");

        // No early open to finalize and replace
        for (SSTableWriter writer : writers)
        {
            assert writer.getFilePointer() > 0;
            writer.setRepairedAt(repairedAt).setOpenResult(true).prepareToCommit();
            SSTableReader reader = writer.finished();
            transaction.update(reader, false);
            preparedForCommit.add(reader);
        }
        transaction.checkpoint();

        if (throwLate)
            throw new RuntimeException("exception thrown after all sstables finished, for testing");

        if (!keepOriginals)
            transaction.obsoleteOriginals();

        transaction.prepareToCommit();
    }

    public void throwDuringPrepare(boolean earlyException)
    {
        if (earlyException)
            throwEarly = true;
        else
            throwLate = true;
    }
}
