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
import java.util.List;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.utils.concurrent.Transactional;

/**
 * Wraps one or more writers as output for rewriting one or more readers: every sstable_preemptive_open_interval
 * we look in the summary we're collecting for the latest writer for the penultimate key that we know to have been fully
 * flushed to the index file, and then double check that the key is fully present in the flushed data file.
 * Then we move the starts of each reader forwards to that point, replace them in the Tracker, and attach a runnable
 * for on-close (i.e. when all references expire) that drops the page cache prior to that key position
 *
 * hard-links are created for each partially written sstable so that readers opened against them continue to work past
 * renaming of the temporary file, which is deleted once all readers against the hard-link have been closed.
 * If for any reason the writer is rolled over, we immediately rename and fully expose the completed file in the Tracker.
 *
 * On abort, we restore the original lower bounds to the existing readers and delete any temporary files we had in progress,
 * but leave any hard-links in place for the readers we opened, and clean-up when the readers finish as we would do
 * if we had finished successfully.
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

    private long currentlyOpenedEarlyAt; // the position (in MiB) in the target file we last (re)opened at
    private long bytesWritten; // the bytes written by previous writers, or zero if the current writer is the first writer

    private final List<SSTableWriter> writers = new ArrayList<>();
    private final boolean keepOriginals; // true if we do not want to obsolete the originals
    private final boolean eagerWriterMetaRelease; // true if the writer metadata should be released when switch is called

    private SSTableWriter writer;

    // for testing (TODO: remove when have byteman setup)
    private boolean throwEarly, throwLate;

    /** @deprecated See CASSANDRA-11148 */
    @Deprecated(since = "3.4")
    public SSTableRewriter(ILifecycleTransaction transaction, long maxAge, long preemptiveOpenInterval, boolean keepOriginals)
    {
        this(transaction, maxAge, preemptiveOpenInterval, keepOriginals, false);
    }

    SSTableRewriter(ILifecycleTransaction transaction, long maxAge, long preemptiveOpenInterval, boolean keepOriginals, boolean eagerWriterMetaRelease)
    {
        this.transaction = transaction;
        this.maxAge = maxAge;
        this.preemptiveOpenInterval = preemptiveOpenInterval;
        this.keepOriginals = keepOriginals;
        this.eagerWriterMetaRelease = eagerWriterMetaRelease;
    }

    public static SSTableRewriter constructKeepingOriginals(ILifecycleTransaction transaction, boolean keepOriginals, long maxAge)
    {
        return new SSTableRewriter(transaction, maxAge, calculateOpenInterval(true), keepOriginals, true);
    }

    public static SSTableRewriter constructWithoutEarlyOpening(ILifecycleTransaction transaction, boolean keepOriginals, long maxAge)
    {
        return new SSTableRewriter(transaction, maxAge, calculateOpenInterval(false), keepOriginals, true);
    }

    public static SSTableRewriter construct(ColumnFamilyStore cfs, ILifecycleTransaction transaction, boolean keepOriginals, long maxAge)
    {
        return new SSTableRewriter(transaction, maxAge, calculateOpenInterval(cfs.supportsEarlyOpen()), keepOriginals, true);
    }

    private static long calculateOpenInterval(boolean shouldOpenEarly)
    {
        long interval = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB() * (1L << 20);
        if (disableEarlyOpeningForTests || !shouldOpenEarly || interval < 0)
            interval = Long.MAX_VALUE;
        return interval;
    }

    public SSTableWriter currentWriter()
    {
        return writer;
    }

    public long bytesWritten()
    {
        return bytesWritten + (writer == null ? 0 : writer.getFilePointer());
    }

    public void forEachWriter(Consumer<SSTableWriter> op)
    {
        for (SSTableWriter writer : writers)
            op.accept(writer);
        if (writer != null)
            op.accept(writer);
    }

    public AbstractRowIndexEntry append(UnfilteredRowIterator partition)
    {
        // we do this before appending to ensure we can resetAndTruncate() safely if appending fails
        DecoratedKey key = partition.partitionKey();
        maybeReopenEarly(key);
        return writer.append(partition);
    }

    // attempts to append the row, if fails resets the writer position
    public AbstractRowIndexEntry tryAppend(UnfilteredRowIterator partition)
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
                    reader.trySkipFileCacheBefore(key);
                }
            }
            else
            {
                writer.setMaxDataAge(maxAge);
                writer.openEarly(reader -> {
                    transaction.update(reader, false);
                    currentlyOpenedEarlyAt = writer.getFilePointer();
                    moveStarts(reader.getLast());
                    transaction.checkpoint();
                });
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
     * @param lowerbound if !reset, must be non-null, and marks the exclusive lowerbound of the start for each sstable
     */
    private void moveStarts(DecoratedKey lowerbound)
    {
        if (transaction.isOffline() || preemptiveOpenInterval == Long.MAX_VALUE)
            return;

        for (SSTableReader sstable : transaction.originals())
        {
            // we call getCurrentReplacement() to support multiple rewriters operating over the same source readers at once.
            // note: only one such writer should be written to at any moment
            final SSTableReader latest = transaction.current(sstable);

            // skip any sstables that we know to already be shadowed
            if (latest.getFirst().compareTo(lowerbound) > 0)
                continue;

            if (lowerbound.compareTo(latest.getLast()) >= 0)
            {
                if (!transaction.isObsolete(latest))
                    transaction.obsolete(latest);
                continue;
            }

            if (!transaction.isObsolete(latest))
            {
                DecoratedKey newStart = latest.firstKeyBeyond(lowerbound);
                assert newStart != null;
                SSTableReader replacement = latest.cloneWithNewStart(newStart);
                transaction.update(replacement, true);
            }
        }
    }

    public void switchWriter(SSTableWriter newWriter)
    {
        if (newWriter != null)
        {
            newWriter.setMaxDataAge(maxAge);
            writers.add(newWriter);
        }

        if (eagerWriterMetaRelease && writer != null)
            writer.releaseMetadataOverhead();

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

        // Open fully completed sstables early. This is also required for the final sstable in a set (where newWriter
        // is null) to permit the compilation of a canonical set of sstables (see View.select).
        if (preemptiveOpenInterval != Long.MAX_VALUE)
        {
            // we leave it as a tmp file, but we open it and add it to the Tracker
            writer.setMaxDataAge(maxAge);
            SSTableReader reader = writer.openFinalEarly();
            transaction.update(reader, false);
            moveStarts(reader.getLast());
            transaction.checkpoint();
        }

        currentlyOpenedEarlyAt = 0;
        bytesWritten += writer.getFilePointer();
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
            writer.setRepairedAt(repairedAt);
            writer.setOpenResult(true);
            writer.prepareToCommit();
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
