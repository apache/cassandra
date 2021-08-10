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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.FSDiskFullWriteError;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;

public class BackgroundCompactionRunner implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(BackgroundCompactionRunner.class);

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

    private final DebuggableScheduledThreadPoolExecutor checkExecutor;

    /**
     * CFSs for which a compaction was requested mapped to the promise returned to the requesting code
     */
    private final ConcurrentMap<ColumnFamilyStore, FutureRequestResult> compactionRequests = new ConcurrentHashMap<>();

    private final AtomicInteger ongoingUpgrades = new AtomicInteger(0);

    /**
     * Tracks the number of currently requested compactions. Used to delay checking for new compactions until there's
     * room in the executing threads.
     */
    private final AtomicInteger ongoingCompactions = new AtomicInteger(0);

    private final Random random = new Random();

    private final DebuggableThreadPoolExecutor compactionExecutor;

    private final ActiveOperations activeOperations;


    BackgroundCompactionRunner(DebuggableThreadPoolExecutor compactionExecutor, ActiveOperations activeOperations)
    {
        this(compactionExecutor, new DebuggableScheduledThreadPoolExecutor("BackgroundTaskExecutor"), activeOperations);
    }

    @VisibleForTesting
    BackgroundCompactionRunner(DebuggableThreadPoolExecutor compactionExecutor, DebuggableScheduledThreadPoolExecutor checkExecutor, ActiveOperations activeOperations)
    {
        this.compactionExecutor = compactionExecutor;
        this.checkExecutor = checkExecutor;
        this.activeOperations = activeOperations;
    }

    /**
     * This extends and behave like a {@link CompletableFuture}, with the exception that one cannot call
     * {@link #cancel}, {@link #complete} and {@link #completeExceptionally} (they throw {@link UnsupportedOperationException}).
     */
    public static class FutureRequestResult extends CompletableFuture<RequestResult>
    {
        @Override
        public boolean complete(RequestResult t)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean cancel(boolean interruptIfRunning)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean completeExceptionally(Throwable throwable)
        {
            throw new UnsupportedOperationException();
        }

        private void completeInternal(RequestResult t)
        {
            super.complete(t);
        }

        private void completeExceptionallyInternal(Throwable throwable)
        {
            super.completeExceptionally(throwable);
        }
    }

    /**
     * Marks each CFS in a set for compaction. See {@link #markForCompactionCheck(ColumnFamilyStore)} for details.
     */
    void markForCompactionCheck(Set<ColumnFamilyStore> cfss)
    {
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
     *
     * @return a promise which will be completed when the mark is cleared. The returned future should not be cancelled or
     * completed by the caller.
     */
    CompletableFuture<RequestResult> markForCompactionCheck(ColumnFamilyStore cfs)
    {
        FutureRequestResult p = requestCompactionInternal(cfs);
        if (p == null)
            return CompletableFuture.completedFuture(RequestResult.ABORTED);

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
                assert promise.join() == RequestResult.ABORTED : "Background compaction checker must be single-threaded";

                logger.trace("The request for {} was aborted due to shutdown", cfs);
                continue;
            }

            if (!cfs.isValid())
            {
                logger.trace("Aborting compaction for dropped CF {}", cfs);
                promise.completeInternal(RequestResult.ABORTED);
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
                        logger.warn(String.format("Aborting compaction of %s due to error", cfs), throwable);
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
                // in the compaction executor.
                markForCompactionCheck(cfs);
            }
            else
            {
                promise.completeInternal(RequestResult.NOT_NEEDED);
            }
        }
    }

    private boolean maybeScheduleNextCheck()
    {
        if (checkExecutor.getQueue().isEmpty())
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
        Collection<AbstractCompactionTask> compactionTasks = cfs.getCompactionStrategy()
                                                                .getNextBackgroundTasks(CompactionManager.getDefaultGcBefore(cfs, FBUtilities.nowInSeconds()));

        if (!compactionTasks.isEmpty())
        {
            logger.debug("Running compaction tasks: {}", compactionTasks);
            return CompletableFuture.allOf(
            compactionTasks.stream()
                           .map(task -> startTask(cfs, task))
                           .toArray(CompletableFuture<?>[]::new));
        }
        else
        {
            logger.debug("No compaction tasks for {}", cfs);
            return null;
        }
    }

    private CompletableFuture<Void> startTask(ColumnFamilyStore cfs, AbstractCompactionTask task)
    {
        ongoingCompactions.incrementAndGet();
        try
        {
            return CompletableFuture.runAsync(
            () -> {
                try
                {
                    task.execute(activeOperations);
                }
                finally
                {
                    ongoingCompactions.decrementAndGet();

                    // Request a new round of checking for compactions. We do this for two reasons:
                    //  - a task has completed and there may now be new compaction possibilities in this CFS,
                    //  - a thread has freed up, and a new compaction task (from any CFS) can be scheduled on it
                    markForCompactionCheck(cfs);
                }
            }, compactionExecutor);
        }
        catch (RejectedExecutionException ex)
        {
            ongoingCompactions.decrementAndGet();
            logger.debug("Background compaction task for {} was rejected", cfs);
            return CompletableFuture.completedFuture(null);
        }
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
            logger.debug("No upgrade tasks for {}", cfs);
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

    private static void handleCompactionError(Throwable t, ColumnFamilyStore cfs)
    {
        t = Throwables.unwrapped(t);
        // FSDiskFullWriteErrors caught during compaction are expected to be recoverable, so we don't explicitly
        // trigger the disk failure policy because of them (see CASSANDRA-12385).
        if (t instanceof IOError && !(t instanceof FSDiskFullWriteError))
        {
            logger.error("Potentially unrecoverable error during background compaction of table {}", cfs, t);
            // Strictly speaking it's also possible to hit a read-related IOError during compaction, although the
            // chances for that are much lower than the chances for write-related IOError. If we want to handle that,
            // we might have to rely on error message parsing...
            t = t instanceof FSError ? t : new FSWriteError(t);
            JVMStabilityInspector.inspectThrowable(t);
        }
        else
        {
            logger.error("Exception during background compaction of table {}", cfs, t);
        }
    }
}
