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
package org.apache.cassandra.db.lifecycle;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.UniqueIdentifier;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Transactional;

import static com.google.common.base.Functions.compose;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.singleton;
import static org.apache.cassandra.db.lifecycle.Helpers.abortObsoletion;
import static org.apache.cassandra.db.lifecycle.Helpers.checkNotReplaced;
import static org.apache.cassandra.db.lifecycle.Helpers.concatUniq;
import static org.apache.cassandra.db.lifecycle.Helpers.emptySet;
import static org.apache.cassandra.db.lifecycle.Helpers.filterIn;
import static org.apache.cassandra.db.lifecycle.Helpers.filterOut;
import static org.apache.cassandra.db.lifecycle.Helpers.markObsolete;
import static org.apache.cassandra.db.lifecycle.Helpers.orIn;
import static org.apache.cassandra.db.lifecycle.Helpers.prepareForObsoletion;
import static org.apache.cassandra.db.lifecycle.Helpers.select;
import static org.apache.cassandra.db.lifecycle.Helpers.selectFirst;
import static org.apache.cassandra.db.lifecycle.Helpers.setReplaced;
import static org.apache.cassandra.db.lifecycle.View.updateCompacting;
import static org.apache.cassandra.db.lifecycle.View.updateLiveSet;
import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.concurrent.Refs.release;
import static org.apache.cassandra.utils.concurrent.Refs.selfRefs;

/**
 * IMPORTANT: When this object is involved in a transactional graph, for correct behaviour its commit MUST occur before
 * any others, since it may legitimately fail. This is consistent with the Transactional API, which permits one failing
 * action to occur at the beginning of the commit phase, but also *requires* that the prepareToCommit() phase only take
 * actions that can be rolled back.
 */
public class LifecycleTransaction extends Transactional.AbstractTransactional implements ILifecycleTransaction
{
    private static final Logger logger = LoggerFactory.getLogger(LifecycleTransaction.class);

    /**
     * A class that represents accumulated modifications to the Tracker.
     * has two instances, one containing modifications that are "staged" (i.e. invisible)
     * and one containing those "logged" that have been made visible through a call to checkpoint()
     */
    private static class State
    {
        // readers that are either brand new, update a previous new reader, or update one of the original readers
        final Set<SSTableReader> update = new HashSet<>();
        // disjoint from update, represents a subset of originals that is no longer needed
        final Set<SSTableReader> obsolete = new HashSet<>();

        void log(State staged)
        {
            update.removeAll(staged.obsolete);
            update.removeAll(staged.update);
            update.addAll(staged.update);
            obsolete.addAll(staged.obsolete);
        }

        boolean contains(SSTableReader reader)
        {
            return update.contains(reader) || obsolete.contains(reader);
        }

        boolean isEmpty()
        {
            return update.isEmpty() && obsolete.isEmpty();
        }

        void clear()
        {
            update.clear();
            obsolete.clear();
        }

        @Override
        public String toString()
        {
            return String.format("[obsolete: %s, update: %s]", obsolete, update);
        }
    }

    public final Tracker tracker;
    // The transaction logs keep track of new and old sstable files
    private final LogTransaction log;
    // the original readers this transaction was opened over, and that it guards
    // (no other transactions may operate over these readers concurrently)
    private final Set<SSTableReader> originals = new HashSet<>();
    // the set of readers we've marked as compacting (only updated on creation and in checkpoint())
    private final Set<SSTableReader> marked = new HashSet<>();
    // the identity set of readers we've ever encountered; used to ensure we don't accidentally revisit the
    // same version of a reader. potentially a dangerous property if there are reference counting bugs
    // as they won't be caught until the transaction's lifespan is over.
    private final Set<UniqueIdentifier> identities = Collections.newSetFromMap(new IdentityHashMap<>());

    // changes that have been made visible
    private final State logged = new State();
    // changes that are pending
    private final State staged = new State();

    // the tidier and their readers, to be used for marking readers obsoleted during a commit
    private List<LogTransaction.Obsoletion> obsoletions;

    // commit/rollback hooks
    private List<Runnable> commitHooks = new ArrayList<>();
    private List<Runnable> abortHooks = new ArrayList<>();

    /**
     * construct a Transaction for use in an offline operation
     */
    public static LifecycleTransaction offline(OperationType operationType, SSTableReader reader)
    {
        return offline(operationType, singleton(reader));
    }

    /**
     * construct a Transaction for use in an offline operation
     */
    public static LifecycleTransaction offline(OperationType operationType, Iterable<SSTableReader> readers)
    {
        // if offline, for simplicity we just use a dummy tracker
        Tracker dummy = Tracker.newDummyTracker();
        dummy.addInitialSSTables(readers);
        dummy.apply(updateCompacting(emptySet(), readers));
        return new LifecycleTransaction(dummy, operationType, readers);
    }

    /**
     * construct an empty Transaction with no existing readers
     */
    public static LifecycleTransaction offline(OperationType operationType)
    {
        Tracker dummy = Tracker.newDummyTracker();
        return new LifecycleTransaction(dummy, new LogTransaction(operationType, dummy), Collections.emptyList());
    }

    LifecycleTransaction(Tracker tracker, OperationType operationType, Iterable<? extends SSTableReader> readers)
    {
        this(tracker, new LogTransaction(operationType, tracker), readers);
    }

    LifecycleTransaction(Tracker tracker, LogTransaction log, Iterable<? extends SSTableReader> readers)
    {
        this.tracker = tracker;
        this.log = log;
        for (SSTableReader reader : readers)
        {
            originals.add(reader);
            marked.add(reader);
            identities.add(reader.instanceId);
        }
    }

    public LogTransaction log()
    {
        return log;
    }

    @Override //LifecycleNewTracker
    public OperationType opType()
    {
        return log.type();
    }

    public TimeUUID opId()
    {
        return log.id();
    }

    public void doPrepare()
    {
        // note for future: in anticompaction two different operations use the same Transaction, and both prepareToCommit()
        // separately: the second prepareToCommit is ignored as a "redundant" transition. since it is only a checkpoint
        // (and these happen anyway) this is fine but if more logic gets inserted here than is performed in a checkpoint,
        // it may break this use case, and care is needed
        checkpoint();

        // prepare for compaction obsolete readers as long as they were part of the original set
        // since those that are not original are early readers that share the same desc with the finals
        maybeFail(prepareForObsoletion(filterIn(logged.obsolete, originals), log, obsoletions = new ArrayList<>(), null));
        log.prepareToCommit();
    }

    /**
     * point of no return: commit all changes, but leave all readers marked as compacting
     */
    public Throwable doCommit(Throwable accumulate)
    {
        assert staged.isEmpty() : "must be no actions introduced between prepareToCommit and a commit";

        if (logger.isTraceEnabled())
            logger.trace("Committing transaction over {} staged: {}, logged: {}", originals, staged, logged);

        // accumulate must be null if we have been used correctly, so fail immediately if it is not
        maybeFail(accumulate);

        // transaction log commit failure means we must abort; safe commit is not possible
        maybeFail(log.commit(null));

        // this is now the point of no return; we cannot safely rollback, so we ignore exceptions until we're done
        // we restore state by obsoleting our obsolete files, releasing our references to them, and updating our size
        // and notification status for the obsolete and new files

        accumulate = markObsolete(obsoletions, accumulate);
        accumulate = tracker.updateSizeTracking(logged.obsolete, logged.update, accumulate);
        accumulate = runOnCommitHooks(accumulate);
        accumulate = release(selfRefs(logged.obsolete), accumulate);
        accumulate = tracker.notifySSTablesChanged(originals, logged.update, log.type(), accumulate);

        return accumulate;
    }


    /**
     * undo all of the changes made by this transaction, resetting the state to its original form
     */
    public Throwable doAbort(Throwable accumulate)
    {
        if (logger.isTraceEnabled())
            logger.trace("Aborting transaction over {} staged: {}, logged: {}", originals, staged, logged);

        accumulate = abortObsoletion(obsoletions, accumulate);

        if (logged.isEmpty() && staged.isEmpty())
            return log.abort(accumulate);

        // mark obsolete all readers that are not versions of those present in the original set
        Iterable<SSTableReader> obsolete = filterOut(concatUniq(staged.update, logged.update), originals);
        logger.trace("Obsoleting {}", obsolete);

        accumulate = prepareForObsoletion(obsolete, log, obsoletions = new ArrayList<>(), accumulate);
        // it's safe to abort even if committed, see maybeFail in doCommit() above, in this case it will just report
        // a failure to abort, which is useful information to have for debug
        accumulate = log.abort(accumulate);
        accumulate = markObsolete(obsoletions, accumulate);

        // replace all updated readers with a version restored to its original state
        List<SSTableReader> restored = restoreUpdatedOriginals();
        List<SSTableReader> invalid = Lists.newArrayList(Iterables.concat(logged.update, logged.obsolete));
        accumulate = tracker.apply(updateLiveSet(logged.update, restored), accumulate);
        accumulate = tracker.notifySSTablesChanged(invalid, restored, OperationType.COMPACTION, accumulate);
        // setReplaced immediately preceding versions that have not been obsoleted
        accumulate = setReplaced(logged.update, accumulate);
        accumulate = runOnAbortooks(accumulate);
        // we have replaced all of logged.update and never made visible staged.update,
        // and the files we have logged as obsolete we clone fresh versions of, so they are no longer needed either
        // any _staged_ obsoletes should either be in staged.update already, and dealt with there,
        // or is still in its original form (so left as is); in either case no extra action is needed
        accumulate = release(selfRefs(concat(staged.update, logged.update, logged.obsolete)), accumulate);

        logged.clear();
        staged.clear();
        return accumulate;
    }

    private Throwable runOnCommitHooks(Throwable accumulate)
    {
        return runHooks(commitHooks, accumulate);
    }

    private Throwable runOnAbortooks(Throwable accumulate)
    {
        return runHooks(abortHooks, accumulate);
    }

    private static Throwable runHooks(Iterable<Runnable> hooks, Throwable accumulate)
    {
        for (Runnable hook : hooks)
        {
            try
            {
                hook.run();
            }
            catch (Exception e)
            {
                accumulate = Throwables.merge(accumulate, e);
            }
        }
        return accumulate;
    }

    @Override
    protected Throwable doPostCleanup(Throwable accumulate)
    {
        log.close();
        return unmarkCompacting(marked, accumulate);
    }

    public boolean isOffline()
    {
        return tracker.isDummy();
    }

    /**
     * call when a consistent batch of changes is ready to be made atomically visible
     * these will be exposed in the Tracker atomically, or an exception will be thrown; in this case
     * the transaction should be rolled back
     */
    public void checkpoint()
    {
        maybeFail(checkpoint(null));
    }
    private Throwable checkpoint(Throwable accumulate)
    {
        if (logger.isTraceEnabled())
            logger.trace("Checkpointing staged {}", staged);

        if (staged.isEmpty())
            return accumulate;

        Set<SSTableReader> toUpdate = toUpdate();
        Set<SSTableReader> fresh = copyOf(fresh());

        // check the current versions of the readers we're replacing haven't somehow been replaced by someone else
        checkNotReplaced(filterIn(toUpdate, staged.update));

        // ensure any new readers are in the compacting set, since we aren't done with them yet
        // and don't want anyone else messing with them
        // apply atomically along with updating the live set of readers
        tracker.apply(compose(updateCompacting(emptySet(), fresh),
                              updateLiveSet(toUpdate, staged.update)));

        // log the staged changes and our newly marked readers
        marked.addAll(fresh);
        logged.log(staged);

        // setup our tracker, and mark our prior versions replaced, also releasing our references to them
        // we do not replace/release obsoleted readers, since we may need to restore them on rollback
        accumulate = setReplaced(filterOut(toUpdate, staged.obsolete), accumulate);
        accumulate = release(selfRefs(filterOut(toUpdate, staged.obsolete)), accumulate);

        staged.clear();
        return accumulate;
    }


    /**
     * update a reader: if !original, this is a reader that is being introduced by this transaction;
     * otherwise it must be in the originals() set, i.e. a reader guarded by this transaction
     */
    public void update(SSTableReader reader, boolean original)
    {
        assert !staged.update.contains(reader) : "each reader may only be updated once per checkpoint: " + reader;
        assert !identities.contains(reader.instanceId) : "each reader instance may only be provided as an update once: " + reader;
        // check it isn't obsolete, and that it matches the original flag
        assert !(logged.obsolete.contains(reader) || staged.obsolete.contains(reader)) : "may not update a reader that has been obsoleted";
        assert original == originals.contains(reader) : String.format("the 'original' indicator was incorrect (%s provided): %s", original, reader);
        staged.update.add(reader);
        identities.add(reader.instanceId);
        if (!isOffline())
            reader.setupOnline();
    }

    public void update(Collection<SSTableReader> readers, boolean original)
    {
        for(SSTableReader reader: readers)
        {
            update(reader, original);
        }
    }

    /**
     * mark this reader as for obsoletion : on checkpoint() the reader will be removed from the live set
     */
    public void obsolete(SSTableReader reader)
    {
        logger.trace("Staging for obsolescence {}", reader);
        // check this is: a reader guarded by the transaction, an instance we have already worked with
        // and that we haven't already obsoleted it, nor do we have other changes staged for it
        assert identities.contains(reader.instanceId) : "only reader instances that have previously been provided may be obsoleted: " + reader;
        assert originals.contains(reader) : "only readers in the 'original' set may be obsoleted: " + reader + " vs " + originals;
        assert !(logged.obsolete.contains(reader) || staged.obsolete.contains(reader)) : "may not obsolete a reader that has already been obsoleted: " + reader;
        assert !staged.update.contains(reader) : "may not obsolete a reader that has a staged update (must checkpoint first): " + reader;
        assert current(reader) == reader : "may only obsolete the latest version of the reader: " + reader;
        staged.obsolete.add(reader);
    }

    public void runOnCommit(Runnable fn)
    {
        commitHooks.add(fn);
    }

    public void runOnAbort(Runnable fn)
    {
        abortHooks.add(fn);
    }

    /**
     * obsolete every file in the original transaction
     */
    public void obsoleteOriginals()
    {
        logger.trace("Staging for obsolescence {}", originals);
        // if we're obsoleting, we should have no staged updates for the original files
        assert Iterables.isEmpty(filterIn(staged.update, originals)) : staged.update;

        // stage obsoletes for any currently visible versions of any original readers
        Iterables.addAll(staged.obsolete, filterIn(current(), originals));
    }

    /**
     * return the readers we're replacing in checkpoint(), i.e. the currently visible version of those in staged
     */
    private Set<SSTableReader> toUpdate()
    {
        return copyOf(filterIn(current(), staged.obsolete, staged.update));
    }

    /**
     * new readers that haven't appeared previously (either in the original set or the logged updates)
     */
    private Iterable<SSTableReader> fresh()
    {
        return filterOut(staged.update, originals, logged.update);
    }

    /**
     * returns the currently visible readers managed by this transaction
     */
    public Iterable<SSTableReader> current()
    {
        // i.e., those that are updates that have been logged (made visible),
        // and any original readers that have neither been obsoleted nor updated
        return concat(logged.update, filterOut(originals, logged.update, logged.obsolete));
    }

    /**
     * update the current replacement of any original reader back to its original start
     */
    private List<SSTableReader> restoreUpdatedOriginals()
    {
        Iterable<SSTableReader> torestore = filterIn(originals, logged.update, logged.obsolete);
        return ImmutableList.copyOf(transform(torestore, (reader) -> current(reader).cloneWithRestoredStart(reader.getFirst())));
    }

    /**
     * the set of readers guarded by this transaction _in their original instance/state_
     * call current(SSTableReader) on any reader in this set to get the latest instance
     */
    public Set<SSTableReader> originals()
    {
        return Collections.unmodifiableSet(originals);
    }

    /**
     * indicates if the reader has been marked for obsoletion
     */
    public boolean isObsolete(SSTableReader reader)
    {
        return logged.obsolete.contains(reader) || staged.obsolete.contains(reader);
    }

    /**
     * return the current version of the provided reader, whether or not it is visible or staged;
     * i.e. returns the first version present by testing staged, logged and originals in order.
     */
    public SSTableReader current(SSTableReader reader)
    {
        Set<SSTableReader> container;
        if (staged.contains(reader))
            container = staged.update.contains(reader) ? staged.update : staged.obsolete;
        else if (logged.contains(reader))
            container = logged.update.contains(reader) ? logged.update : logged.obsolete;
        else if (originals.contains(reader))
            container = originals;
        else throw new AssertionError();
        return select(reader, container);
    }

    /**
     * remove the reader from the set we're modifying
     */
    public void cancel(SSTableReader cancel)
    {
        logger.trace("Cancelling {} from transaction", cancel);
        assert originals.contains(cancel) : "may only cancel a reader in the 'original' set: " + cancel + " vs " + originals;
        assert !(staged.contains(cancel) || logged.contains(cancel)) : "may only cancel a reader that has not been updated or obsoleted in this transaction: " + cancel;
        originals.remove(cancel);
        marked.remove(cancel);
        identities.remove(cancel.instanceId);
        maybeFail(unmarkCompacting(singleton(cancel), null));
    }

    /**
     * remove the readers from the set we're modifying
     */
    public void cancel(Iterable<SSTableReader> cancels)
    {
        for (SSTableReader cancel : cancels)
            cancel(cancel);
    }

    /**
     * remove the provided readers from this Transaction, and return a new Transaction to manage them
     * only permitted to be called if the current Transaction has never been used
     */
    public LifecycleTransaction split(Collection<SSTableReader> readers)
    {
        logger.trace("Splitting {} into new transaction", readers);
        checkUnused();
        for (SSTableReader reader : readers)
            assert identities.contains(reader.instanceId) : "may only split the same reader instance the transaction was opened with: " + reader;

        for (SSTableReader reader : readers)
        {
            identities.remove(reader.instanceId);
            originals.remove(reader);
            marked.remove(reader);
        }
        return new LifecycleTransaction(tracker, log.type(), readers);
    }

    /**
     * check this transaction has never been used
     */
    private void checkUnused()
    {
        assert logged.isEmpty();
        assert staged.isEmpty();
        assert identities.size() == originals.size();
        assert originals.size() == marked.size();
    }

    private Throwable unmarkCompacting(Set<SSTableReader> unmark, Throwable accumulate)
    {
        accumulate = tracker.apply(updateCompacting(unmark, emptySet()), accumulate);
        // when the CFS is invalidated, it will call unreferenceSSTables().  However, unreferenceSSTables only deals
        // with sstables that aren't currently being compacted.  If there are ongoing compactions that finish or are
        // interrupted after the CFS is invalidated, those sstables need to be unreferenced as well, so we do that here.
        accumulate = tracker.dropSSTablesIfInvalid(accumulate);
        return accumulate;
    }

    // convenience method for callers that know only one sstable is involved in the transaction
    public SSTableReader onlyOne()
    {
        assert originals.size() == 1;
        return getFirst(originals, null);
    }

    // LifecycleNewTracker

    @Override
    public void trackNew(SSTable table)
    {
        log.trackNew(table);
    }

    @Override
    public void untrackNew(SSTable table)
    {
        log.untrackNew(table);
    }

    public static boolean removeUnfinishedLeftovers(ColumnFamilyStore cfs)
    {
        return LogTransaction.removeUnfinishedLeftovers(cfs.getDirectories().getCFDirectories());
    }

    public static boolean removeUnfinishedLeftovers(TableMetadata metadata)
    {
        return LogTransaction.removeUnfinishedLeftovers(metadata);
    }

    /**
     * Get the files in the folder specified, provided that the filter returns true.
     * A filter is given each file and its type, and decides which files should be returned
     * and which should be discarded. To classify files into their type, we read transaction
     * log files. Should we fail to read these log files after a few times, we look at onTxnErr
     * to determine what to do.
     *
     * @param folder - the folder to scan
     * @param onTxnErr - how to handle a failure to read a txn log file
     * @param filter - A function that receives each file and its type, it should return true to have the file returned
     * @return - the list of files that were scanned and for which the filter returned true
     */
    public static List<File> getFiles(Path folder, BiPredicate<File, Directories.FileType> filter, Directories.OnTxnErr onTxnErr)
    {
        return new LogAwareFileLister(folder, filter, onTxnErr).list();
    }

    /**
     * Retry all deletions that failed the first time around (presumably b/c the sstable was still mmap'd.)
     * Useful because there are times when we know GC has been invoked; also exposed as an mbean.
     */
    public static void rescheduleFailedDeletions()
    {
        LogTransaction.rescheduleFailedDeletions();
    }

    /**
     * Deletions run on the nonPeriodicTasks executor, (both failedDeletions or global tidiers in SSTableReader)
     * so by scheduling a new empty task and waiting for it we ensure any prior deletion has completed.
     */
    public static void waitForDeletions()
    {
        LogTransaction.waitForDeletions();
    }

    // a class representing the current state of the reader within this transaction, encoding the actions both logged
    // and pending, and the reader instances that are visible now, and will be after the next checkpoint (with null
    // indicating either obsolescence, or that the reader does not occur in the transaction; which is defined
    // by the corresponding Action)
    @VisibleForTesting
    public static class ReaderState
    {
        public enum Action
        {
            UPDATED, OBSOLETED, NONE;
            public static Action get(boolean updated, boolean obsoleted)
            {
                assert !(updated && obsoleted);
                return updated ? UPDATED : obsoleted ? OBSOLETED : NONE;
            }
        }

        final Action staged;
        final Action logged;
        final SSTableReader nextVisible;
        final SSTableReader currentlyVisible;
        final boolean original;

        public ReaderState(Action logged, Action staged, SSTableReader currentlyVisible, SSTableReader nextVisible, boolean original)
        {
            this.staged = staged;
            this.logged = logged;
            this.currentlyVisible = currentlyVisible;
            this.nextVisible = nextVisible;
            this.original = original;
        }

        public boolean equals(Object that)
        {
            return that instanceof ReaderState && equals((ReaderState) that);
        }

        public boolean equals(ReaderState that)
        {
            return this.staged == that.staged && this.logged == that.logged && this.original == that.original
                && this.currentlyVisible == that.currentlyVisible && this.nextVisible == that.nextVisible;
        }

        public String toString()
        {
            return String.format("[logged=%s staged=%s original=%s]", logged, staged, original);
        }

        public static SSTableReader visible(SSTableReader reader, Predicate<SSTableReader> obsolete, Collection<SSTableReader> ... selectFrom)
        {
            return obsolete.apply(reader) ? null : selectFirst(reader, selectFrom);
        }
    }

    @VisibleForTesting
    public ReaderState state(SSTableReader reader)
    {
        SSTableReader currentlyVisible = ReaderState.visible(reader, in(logged.obsolete), logged.update, originals);
        SSTableReader nextVisible = ReaderState.visible(reader, orIn(staged.obsolete, logged.obsolete), staged.update, logged.update, originals);
        return new ReaderState(ReaderState.Action.get(logged.update.contains(reader), logged.obsolete.contains(reader)),
                               ReaderState.Action.get(staged.update.contains(reader), staged.obsolete.contains(reader)),
                               currentlyVisible, nextVisible, originals.contains(reader)
        );
    }

    public String toString()
    {
        return originals.toString();
    }
}
