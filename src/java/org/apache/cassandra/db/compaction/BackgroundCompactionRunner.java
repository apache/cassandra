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

import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture; // checkstyle: permit this import
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.WrappedExecutorPlus;
import org.apache.cassandra.utils.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.BloomFilterTooLargeException;
import org.apache.cassandra.io.FSDiskFullWriteError;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.CorruptFileException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class BackgroundCompactionRunner implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(BackgroundCompactionRunner.class);

    public static final String NO_SPACE_LEFT_MESSAGE = "No space left on device";

    public enum RequestResult
    {
        /**
         * when the compaction check was done and there were no compaction tasks for the CFS
         */
        NOT_NEEDED,

        /**
         * when the compaction was aborted for the CFS because the CF got dropped in the meantime
         */
        ABORTED,

        /**
         * compaction tasks were completed for the CFS
         */
        COMPLETED
    }

    private final ScheduledExecutorPlus checkExecutor;

    /**
     * CFSs for which a compaction was requested mapped to the promise returned to the requesting code
     */
    private final ConcurrentMap<ColumnFamilyStore, FutureRequestResult> compactionRequests = new ConcurrentHashMap<>();

    /**
     * CFSs whose last background compaction failed in a way that is certain to repeat (see
     * {@link #isDeterministicFailure}). {@link #run()} skips them until the flag is cleared, either by a request from
     * outside the runner or by another task of the same CFS that completes successfully. The runner's own requests
     * do not clear it, see {@link #markForCompactionCheckInternal}.
     * <p>
     * The flag is per CFS, not per task, so while it is set the runner starts no compaction at all for the CFS.
     */
    private final Set<ColumnFamilyStore> deterministicFailures = ConcurrentHashMap.newKeySet();

    private final AtomicInteger ongoingUpgrades = new AtomicInteger(0);

    /**
     * Tracks the number of currently requested compactions. Used to delay checking for new compactions until there's
     * room in the executing threads.
     */
    private final AtomicInteger ongoingCompactions = new AtomicInteger(0);

    private final Random random = new Random();

    private final WrappedExecutorPlus compactionExecutor;

    private final ActiveOperations activeOperations;


    BackgroundCompactionRunner(WrappedExecutorPlus compactionExecutor, ActiveOperations activeOperations)
    {
        this(compactionExecutor, executorFactory().scheduled("BackgroundTaskExecutor"), activeOperations);
    }

    @VisibleForTesting
    BackgroundCompactionRunner(WrappedExecutorPlus compactionExecutor, ScheduledExecutorPlus checkExecutor, ActiveOperations activeOperations)
    {
        this.compactionExecutor = compactionExecutor;
        this.checkExecutor = checkExecutor;
        this.activeOperations = activeOperations;
    }

    /**
     * This extends and behave like a {@link CompletableFuture}, with the exception that one cannot call
     * {@link #cancel}, {@link #setSuccess} and {@link #setFailure} (they throw {@link UnsupportedOperationException}).
     */
    public static class FutureRequestResult extends AsyncPromise<RequestResult>
    {
        @Override
        public Promise<RequestResult> setSuccess(RequestResult t)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean cancel(boolean interruptIfRunning)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Promise<RequestResult> setFailure(Throwable throwable)
        {
            throw new UnsupportedOperationException();
        }

        private void completeInternal(RequestResult t)
        {
            super.trySuccess(t);
        }

        private void completeExceptionallyInternal(Throwable throwable)
        {
            super.tryFailure(throwable);
        }
    }

    /**
     * Marks each CFS in a set for compaction. See {@link #markForCompactionCheck(ColumnFamilyStore)} for details.
     */
    void markForCompactionCheck(Set<ColumnFamilyStore> cfss)
    {
        deterministicFailures.removeAll(cfss);
        List<FutureRequestResult> results = cfss.stream()
                                                .map(this::requestCompactionInternal)
                                                .filter(Objects::nonNull)
                                                .collect(Collectors.toList());

        if (!results.isEmpty() && !maybeScheduleNextCheck())
        {
            logger.info("Executor has been shut down, background compactions check will not be scheduled");
            results.forEach(r -> r.completeInternal(RequestResult.ABORTED));
        }
    }

    /**
     * Marks a CFS for compaction. Since marked, it will become a possible candidate for compaction. The mark will be
     * cleared when we actually run the compaction for CFS.
     * <p>
     * This is for requests from outside the runner.
     *
     * @return a promise which will be completed when the mark is cleared. The returned future should not be cancelled or
     * completed by the caller.
     */
    Promise<RequestResult> markForCompactionCheck(ColumnFamilyStore cfs)
    {
        deterministicFailures.remove(cfs);
        return markForCompactionCheckInternal(cfs);
    }

    /** Like {@link #markForCompactionCheck(ColumnFamilyStore)}, but for the runner's own requests: it does not clear
     * {@link #deterministicFailures}. Such a request must not make the CFS eligible again after a deterministic failure.
     */
    Promise<RequestResult> markForCompactionCheckInternal(ColumnFamilyStore cfs)
    {
        FutureRequestResult p = requestCompactionInternal(cfs);
        if (p == null)
            return new AsyncPromise<RequestResult>().setSuccess(RequestResult.ABORTED);

        if (!maybeScheduleNextCheck())
        {
            logger.info("Executor has been shut down, background compactions check will not be scheduled");
            p.completeInternal(RequestResult.ABORTED);
        }
        return p;
    }

    private FutureRequestResult requestCompactionInternal(ColumnFamilyStore cfs)
    {
        logger.trace("Requested background compaction for {}", cfs);
        if (!cfs.isValid())
        {
            logger.trace("Aborting compaction for dropped CF {}", cfs);
            return null;
        }
        if (cfs.isAutoCompactionDisabled())
        {
            logger.trace("Autocompaction is disabled");
            return null;
        }

        return compactionRequests.computeIfAbsent(cfs, ignored -> new FutureRequestResult());
    }

    void shutdown()
    {
        checkExecutor.shutdown();
        compactionRequests.values().forEach(promise -> promise.completeInternal(RequestResult.ABORTED));
        // it's okay to complete a CompletableFuture more than one on race between request, run and shutdown
    }

    @VisibleForTesting
    int getOngoingCompactionsCount()
    {
        return ongoingCompactions.get();
    }

    @VisibleForTesting
    int getOngoingUpgradesCount()
    {
        return ongoingUpgrades.get();
    }

    @VisibleForTesting
    Set<ColumnFamilyStore> getMarkedCFSs()
    {
        return ImmutableSet.copyOf(compactionRequests.keySet());
    }

    @Override
    public void run()
    {
        logger.trace("Running background compactions check");

        // drop flags of invalid CFSs; with no pending request, the invalid-CFS check below never clears them
        deterministicFailures.removeIf(cfs -> !cfs.isValid());

        // When the executor is fully occupied, we delay acting on this request until a thread is available. This
        // helps make a better decision what exactly to compact (e.g. if we issue the request now we may select n
        // sstables, while by the time this request actually has a thread to execute on more may have accumulated,
        // and it may be better to compact all).
        // Note that we make a request whenever a task completes and thus this method is guaranteed to run again
        // when threads free up.
        if (ongoingCompactions.get() >= compactionExecutor.getMaximumPoolSize())
        {
            logger.trace("Background compaction threads are busy; delaying new compactions check until there are free threads");
            return;
        }

        // We shuffle the CFSs for which the compaction was requested so that with each run we traverse those CFSs
        // in different order and make each CFS have equal chance to be selected
        ArrayList<ColumnFamilyStore> compactionRequestsList = new ArrayList<>(compactionRequests.keySet());
        Collections.shuffle(compactionRequestsList, random);

        for (ColumnFamilyStore cfs : compactionRequestsList)
        {
            if (ongoingCompactions.get() >= compactionExecutor.getMaximumPoolSize())
            {
                logger.trace("Background compaction threads are busy; delaying new compactions check until there are free threads");
                return;
            }

            FutureRequestResult promise = compactionRequests.remove(cfs);
            assert promise != null : "Background compaction checker must be single-threaded";

            if (promise.isDone())
            {
                // A shutdown request may abort processing while we are still processing
                try
                {
                    assert promise.get() == RequestResult.ABORTED : "Background compaction checker must be single-threaded";
                }
                catch (InterruptedException e)
                {
                    throw new UncheckedInterruptedException(e);
                }
                catch (ExecutionException e)
                {
                    throw new RuntimeException(e);
                }

                logger.trace("The request for {} was aborted due to shutdown", cfs);
                continue;
            }

            if (!cfs.isValid())
            {
                logger.trace("Aborting compaction for dropped CF {}", cfs);
                deterministicFailures.remove(cfs);
                promise.completeInternal(RequestResult.ABORTED);
                continue;
            }

            if (deterministicFailures.contains(cfs))
            {
                logger.trace("Skipping {}: its last background compaction failed in a way that is certain to repeat", cfs);
                promise.completeInternal(RequestResult.NOT_NEEDED);
                continue;
            }

            logger.trace("Running a background task check for {} with {}", cfs, cfs.getCompactionStrategy().getName());

            CompletableFuture<Void> compactionTasks = startCompactionTasks(cfs);
            if (compactionTasks == null)
                compactionTasks = startUpgradeTasks(cfs);

            if (compactionTasks != null)
            {
                compactionTasks.handle((ignored, throwable) -> {
                    if (throwable != null)
                    {
                        handleCompactionError(throwable, cfs);
                        promise.completeExceptionallyInternal(throwable);
                    }
                    else
                    {
                        logger.trace("Finished compaction for {}", cfs);
                        promise.completeInternal(RequestResult.COMPLETED);
                    }
                    return null;
                });

                // The compaction strategy may return more subsequent tasks if we ask for them. Therefore, we request
                // the compaction again on that CFS early (without waiting for the currently scheduled/started
                // compaction tasks to finish). We can start them in the next check round if we have free slots
                // in the compaction executor. Must not clear deterministicFailures, if this task fails deterministically,
                // the request is still pending, and the check that finds it skips the CFS.
                markForCompactionCheckInternal(cfs);
            }
            else
            {
                promise.completeInternal(RequestResult.NOT_NEEDED);
            }
        }
    }

    private boolean maybeScheduleNextCheck()
    {
        if (checkExecutor.getPendingTaskCount() == 0)
        {
            try
            {
                checkExecutor.execute(this);
            }
            catch (RejectedExecutionException ex)
            {
                if (checkExecutor.isShutdown())
                    logger.info("Executor has been shut down, background compactions check will not be scheduled");
                else
                    logger.error("Failed to submit background compactions check", ex);

                return false;
            }
        }

        return true;
    }

    private CompletableFuture<Void> startCompactionTasks(ColumnFamilyStore cfs)
    {
        // Check if we are in a paused state. If so, we shouldn't modify sstable lists until the pause is done.
        if (!cfs.isCompactionActive())
            return null;

        Collection<AbstractCompactionTask> compactionTasks = cfs.getCompactionStrategy()
                                                                .getNextBackgroundTasks(CompactionManager.getDefaultGcBefore(cfs, FBUtilities.nowInSeconds()));

        // Re-check if we are in a paused state (this status may have changed between the time the next tasks call
        // was initiated and now). If so, we shouldn't modify sstable lists until the pause is done.
        if (cfs.isCompactionActive())
        {

            CompletableFuture<?>[] compactionTaskFutures = startCompactionTasks(cfs, compactionTasks);
            return compactionTaskFutures != null ? CompletableFuture.allOf(compactionTaskFutures) : null;
        }
        else
        {
            logger.debug("Background compactions not issued because compaction is not active.");
            Throwable t = null;
            for (var c : compactionTasks)
                t = c.rejected(t);
            Throwables.maybeFail(t);
            // Note that there is still a race between the compaction pause in `runWithCompactionsDisabled` and task
            // collection creating the transactions for the tasks above that can cause `runWithCompactionsDisabled` to
            // fail if we have prepared a compaction task but not reached this point to reject it yet.
            // This situation, however, should occur very rarely and will resolve itself quickly.
            return null;
        }
    }
    
    CompletableFuture<?>[] startCompactionTasks(ColumnFamilyStore cfs, Collection<AbstractCompactionTask> compactionTasks)
    {
        if (!compactionTasks.isEmpty())
        {
            logger.debug("Running compaction tasks: {}", compactionTasks);
            CompletableFuture<Void>[] arr = new CompletableFuture[compactionTasks.size()];
            int index = 0;
            for (AbstractCompactionTask task : compactionTasks)
                arr[index++] = startTask(cfs, task);

            return arr;
        }
        else
        {
            logger.trace("No compaction tasks for {}", cfs);
            return null;
        }
    }

    private CompletableFuture<Void> startTask(ColumnFamilyStore cfs, AbstractCompactionTask task)
    {
        ongoingCompactions.incrementAndGet();
        try
        {
            Executor executor = task.getCustomExecutor() == null ? compactionExecutor : task.getCustomExecutor();
            return CompletableFuture.runAsync(
            () -> {
                boolean deterministicFailure = false;
                boolean succeeded = false;
                try
                {
                    task.execute(activeOperations);
                    succeeded = true;
                }
                catch (Throwable t)
                {
                    deterministicFailure = isDeterministicFailure(t);
                    // the flag must be set before the finally block schedules the next check
                    if (deterministicFailure)
                        deterministicFailures.add(cfs);
                    throw t;
                }
                finally
                {
                    ongoingCompactions.decrementAndGet();

                    if (deterministicFailure)
                    {
                        // The failed task has released its sstables, so the next check would select the same
                        // task and fail in the same way, in a loop. Retries are left to the external triggers
                        // (flush, the periodic check, changes of the compaction thresholds, etc.). A thread has
                        // still freed up, so request a check without re-requesting this CFS.
                        maybeScheduleNextCheck();
                    }
                    else
                    {
                        // Request a new round of checking for compactions. We do this for two reasons:
                        //  - a task has completed and there may now be new compaction possibilities in this CFS,
                        //  - a thread has freed up, and a new compaction task (from any CFS) can be scheduled on it
                        //
                        // A task that succeeded has changed the sstables of the CFS, so an earlier deterministic
                        // failure of another task of this CFS is no longer certain to repeat, and the other
                        // compactions of the CFS must not wait for an external trigger. If the failing task is
                        // selected again it fails once more and sets the flag again. This cannot loop: each such
                        // retry needs another task of the CFS to complete successfully.
                        if (succeeded)
                            deterministicFailures.remove(cfs);
                        markForCompactionCheckInternal(cfs);
                    }
                }
            }, executor);
        }
        catch (RejectedExecutionException ex)
        {
            ongoingCompactions.decrementAndGet();
            logger.debug("Background compaction task for {} was rejected", cfs);
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Whether a failure of a background compaction task is certain to repeat when the task is retried on the same
     * sstables. To treat another failure like this, add it here.
     */
    private static boolean isDeterministicFailure(Throwable t)
    {
        return Throwables.anyCauseMatches(t, err -> err instanceof BloomFilterTooLargeException);
    }

    private CompletableFuture<Void> startUpgradeTasks(ColumnFamilyStore cfs)
    {
        AbstractCompactionTask upgradeTask = getUpgradeSSTableTask(cfs);

        if (upgradeTask != null)
        {
            logger.debug("Running upgrade task: {}", upgradeTask);
            return startTask(cfs, upgradeTask).handle((ignored1, ignored2) -> {
                ongoingUpgrades.decrementAndGet();
                return null;
            });
        }
        else
        {
            logger.trace("No upgrade tasks for {}", cfs);
            return null;
        }
    }

    /**
     * Finds the oldest (by modification date) non-latest-version sstable on disk and creates an upgrade task for it
     */
    @VisibleForTesting
    public AbstractCompactionTask getUpgradeSSTableTask(ColumnFamilyStore cfs)
    {
        logger.trace("Checking for upgrade tasks {}", cfs);

        if (!DatabaseDescriptor.automaticSSTableUpgrade())
        {
            logger.trace("Automatic sstable upgrade is disabled - will not try to upgrade sstables of {}", cfs);
            return null;
        }

        if (ongoingUpgrades.incrementAndGet() <= DatabaseDescriptor.maxConcurrentAutoUpgradeTasks())
        {
            List<SSTableReader> potentialUpgrade = cfs.getCandidatesForUpgrade();
            for (SSTableReader sstable : potentialUpgrade)
            {
                LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.UPGRADE_SSTABLES);
                if (txn != null)
                {
                    logger.debug("Found tasks for automatic sstable upgrade of {}", sstable);
                    return cfs.getCompactionStrategy().createCompactionTask(txn, Integer.MIN_VALUE, Long.MAX_VALUE);
                }
            }
        }
        else
        {
            logger.trace("Skipped upgrade task for {} because the limit {} of concurrent upgrade tasks has been reached",
                         cfs, DatabaseDescriptor.maxConcurrentAutoUpgradeTasks());
        }

        ongoingUpgrades.decrementAndGet();
        return null;
    }

    public static void handleCompactionError(Throwable t, ColumnFamilyStore cfs)
    {
        t = Throwables.unwrapped(t);

        // FSDiskFullWriteErrors is thrown when checking disk space before starting flush or compaction task. They
        // are expected to be recoverable because we haven't actually hit No-Space-Left error yet, so we don't explicitly
        // trigger the disk failure policy because of them (see CASSANDRA-12385).
        if (t instanceof IOError && !(t instanceof FSDiskFullWriteError))
        {
            logger.error("Potentially unrecoverable error during background compaction of table {}", cfs, t);
            // Strictly speaking it's also possible to hit a read-related IOError during compaction, although the
            // chances for that are much lower than the chances for write-related IOError. If we want to handle that,
            // we might have to rely on error message parsing...
            t = t instanceof FSError ? t : new FSWriteError(t);
            JVMStabilityInspector.inspectThrowable(t);
            CompactionManager.instance.incrementFailed();
        }
        // No-Space-Left IO exception is thrown by JDK when disk has reached its capacity. The key difference between this
        // and the earlier case with `FSDiskFullWriteError` is that here we have definitively run out of disk space, and
        // no further writes can be performed until disk space is freed, potentially leading to data corruption or
        // system instability if not handled properly. We must trigger the disk failure policy.
        else if (Throwables.isCausedBy(t, IOException.class) && t.toString().contains(NO_SPACE_LEFT_MESSAGE))
        {
            logger.error("Encountered no space left error on {}", cfs, t);
            // wrap it with FSWriteError so that JVMStabilityInspector can properly stop or die
            t = t instanceof FSError ? t : new FSWriteError(t);
            JVMStabilityInspector.inspectThrowable(t);
            CompactionManager.instance.incrementFailed();
        }
        else if (Throwables.isCausedBy(t, OutOfMemoryError.class))
        {
            logger.error("Encountered out of memory error on {}", cfs, t);
            JVMStabilityInspector.inspectThrowable(t);
            CompactionManager.instance.incrementFailed();
        }
        else if (Throwables.anyCauseMatches(t, err -> err instanceof CorruptBlockException
                                                      || err instanceof CorruptFileException
                                                      || err instanceof CorruptSSTableException))
        {
            logger.error("Encountered corruption exception on {}", cfs, t);
            JVMStabilityInspector.inspectThrowable(t);
            CompactionManager.instance.incrementFailed();
        }
        else if (t instanceof CompactionInterruptedException)
        {
            logger.warn(String.format("Aborting background compaction of %s due to interruption", cfs), Throwables.unwrapped(t));
            CompactionManager.instance.incrementAborted();
        }
        else if (Throwables.isCausedBy(t, BloomFilterTooLargeException.class))
        {
            // Deterministic, and attempted again by every external trigger, so don't log it as an ERROR with a
            // stack trace each time.
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, cfs.getKeyspaceName() + '.' + cfs.getTableName(),
                             1, TimeUnit.MINUTES,
                             "Background compaction of table {} failed: {}. It will be attempted again on the next " +
                             "flush, sstable addition, change of compaction thresholds or periodic background check, " +
                             "and will keep failing until bloom_filter_fp_chance is increased.",
                             cfs, Throwables.getCauseOfType(t, BloomFilterTooLargeException.class).get().getMessage());
            CompactionManager.instance.incrementFailed();
        }
        else
        {
            logger.error("Exception during background compaction of table {}", cfs, t);
            CompactionManager.instance.incrementFailed();
        }
    }
}
