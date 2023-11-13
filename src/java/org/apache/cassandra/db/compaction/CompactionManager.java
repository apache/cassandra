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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.core.util.ThrowingSupplier;
import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.WrappedExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionInfo.Holder;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.lifecycle.WrappedLifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.view.ViewBuilderTask;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.repair.NoSuchRepairSessionException;
import org.apache.cassandra.schema.CompactionParams.TombstoneOption;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.Refs;

import static java.util.Collections.singleton;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.FutureTask.callable;
import static org.apache.cassandra.config.DatabaseDescriptor.getConcurrentCompactors;
import static org.apache.cassandra.db.compaction.CompactionManager.CompactionExecutor.compactionThreadGroup;
import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * <p>
 * A singleton which manages a private executor of ongoing compactions.
 * </p>
 * Scheduling for compaction is accomplished by swapping sstables to be compacted into
 * a set via Tracker. New scheduling attempts will ignore currently compacting
 * sstables.
 */
public class CompactionManager implements CompactionManagerMBean, ICompactionManager
{
    public static final String MBEAN_OBJECT_NAME = "org.apache.cassandra.db:type=CompactionManager";
    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);
    public static final CompactionManager instance;

    @VisibleForTesting
    public final AtomicInteger currentlyBackgroundUpgrading = new AtomicInteger(0);

    public static final int NO_GC = Integer.MIN_VALUE;
    public static final int GC_ALL = Integer.MAX_VALUE;

    static
    {
        instance = new CompactionManager();

        MBeanWrapper.instance.registerMBean(instance, MBEAN_OBJECT_NAME);
    }

    private final CompactionExecutor executor = new CompactionExecutor();
    private final ValidationExecutor validationExecutor = new ValidationExecutor();
    private final CompactionExecutor cacheCleanupExecutor = new CacheCleanupExecutor();
    private final CompactionExecutor viewBuildExecutor = new ViewBuildExecutor();

    // We can't house 2i builds in SecondaryIndexManagement because it could cause deadlocks with itself, and can cause
    // massive to indefinite pauses if prioritized either before or after normal compactions so we instead put it in its
    // own pool to prevent either scenario.
    private final SecondaryIndexExecutor secondaryIndexExecutor = new SecondaryIndexExecutor();

    private final CompactionMetrics metrics = new CompactionMetrics(executor, validationExecutor, viewBuildExecutor, secondaryIndexExecutor);

    @VisibleForTesting
    final Multiset<ColumnFamilyStore> compactingCF = ConcurrentHashMultiset.create();

    public final ActiveCompactions active = new ActiveCompactions();

    // used to temporarily pause non-strategy managed compactions (like index summary redistribution)
    private final AtomicInteger globalCompactionPauseCount = new AtomicInteger(0);

    private final RateLimiter compactionRateLimiter = RateLimiter.create(Double.MAX_VALUE);

    public CompactionMetrics getMetrics()
    {
        return metrics;
    }

    /**
     * Gets compaction rate limiter.
     * Rate unit is bytes per sec.
     *
     * @return RateLimiter with rate limit set
     */
    public RateLimiter getRateLimiter()
    {
        setRateInBytes(DatabaseDescriptor.getCompactionThroughputBytesPerSec());
        return compactionRateLimiter;
    }

    /**
     * Sets the rate for the rate limiter. When compaction_throughput is 0 or node is bootstrapping,
     * this sets the rate to Double.MAX_VALUE bytes per second.
     * @param throughputMbPerSec throughput to set in MiB/s
     * @deprecated Use setRateInBytes instead See CASSANDRA-17225
     */
    @Deprecated(since = "4.1")
    public void setRate(final double throughputMbPerSec)
    {
        setRateInBytes(throughputMbPerSec * 1024.0 * 1024);
    }

    /**
     * Sets the rate for the rate limiter. When compaction_throughput is 0 or node is bootstrapping,
     * this sets the rate to Double.MAX_VALUE bytes per second.
     * @param throughputBytesPerSec throughput to set in B/s
     */
    public void setRateInBytes(final double throughputBytesPerSec)
    {
        double throughput = throughputBytesPerSec;
        // if throughput is set to 0, throttling is disabled
        if (throughput == 0 || StorageService.instance.isBootstrapMode())
            throughput = Double.MAX_VALUE;
        if (compactionRateLimiter.getRate() != throughput)
            compactionRateLimiter.setRate(throughput);
    }

    /**
     * Call this whenever a compaction might be needed on the given columnfamily.
     * It's okay to over-call (within reason) if a call is unnecessary, it will
     * turn into a no-op in the bucketing/candidate-scan phase.
     */
    public List<Future<?>> submitBackground(final ColumnFamilyStore cfs)
    {
        if (cfs.isAutoCompactionDisabled())
        {
            logger.trace("Autocompaction is disabled");
            return Collections.emptyList();
        }

        /**
         * If a CF is currently being compacted, and there are no idle threads, submitBackground should be a no-op;
         * we can wait for the current compaction to finish and re-submit when more information is available.
         * Otherwise, we should submit at least one task to prevent starvation by busier CFs, and more if there
         * are idle threads stil. (CASSANDRA-4310)
         */
        int count = compactingCF.count(cfs);
        if (count > 0 && executor.getActiveTaskCount() >= executor.getMaximumPoolSize())
        {
            logger.trace("Background compaction is still running for {}.{} ({} remaining). Skipping",
                         cfs.getKeyspaceName(), cfs.name, count);
            return Collections.emptyList();
        }

        logger.trace("Scheduling a background task check for {}.{} with {}",
                     cfs.getKeyspaceName(),
                     cfs.name,
                     cfs.getCompactionStrategyManager().getName());

        List<Future<?>> futures = new ArrayList<>(1);
        Future<?> fut = executor.submitIfRunning(new BackgroundCompactionCandidate(cfs), "background task");
        if (!fut.isCancelled())
            futures.add(fut);
        else
            compactingCF.remove(cfs);
        return futures;
    }

    public boolean isCompacting(Iterable<ColumnFamilyStore> cfses, Predicate<SSTableReader> sstablePredicate)
    {
        for (ColumnFamilyStore cfs : cfses)
            if (cfs.getTracker().getCompacting().stream().anyMatch(sstablePredicate))
                return true;
        return false;
    }

    @VisibleForTesting
    public boolean hasOngoingOrPendingTasks()
    {
        if (!active.getCompactions().isEmpty() || !compactingCF.isEmpty())
            return true;

        int pendingTasks = executor.getPendingTaskCount() +
                           validationExecutor.getPendingTaskCount() +
                           viewBuildExecutor.getPendingTaskCount() +
                           cacheCleanupExecutor.getPendingTaskCount() +
                           secondaryIndexExecutor.getPendingTaskCount();
        if (pendingTasks > 0)
            return true;

        int activeTasks = executor.getActiveTaskCount() +
                          validationExecutor.getActiveTaskCount() +
                          viewBuildExecutor.getActiveTaskCount() +
                          cacheCleanupExecutor.getActiveTaskCount() +
                          secondaryIndexExecutor.getActiveTaskCount();

        return activeTasks > 0;
    }

    /**
     * Shutdowns both compaction and validation executors, cancels running compaction / validation,
     * and waits for tasks to complete if tasks were not cancelable.
     */
    public void forceShutdown()
    {
        // shutdown executors to prevent further submission
        executor.shutdown();
        validationExecutor.shutdown();
        viewBuildExecutor.shutdown();
        cacheCleanupExecutor.shutdown();
        secondaryIndexExecutor.shutdown();

        // interrupt compactions and validations
        for (Holder compactionHolder : active.getCompactions())
        {
            compactionHolder.stop();
        }

        // wait for tasks to terminate
        // compaction tasks are interrupted above, so it shuold be fairy quick
        // until not interrupted tasks to complete.
        for (ExecutorService exec : Arrays.asList(executor, validationExecutor, viewBuildExecutor,
                                                  cacheCleanupExecutor, secondaryIndexExecutor))
        {
            try
            {
                if (!exec.awaitTermination(1, TimeUnit.MINUTES))
                    logger.warn("Failed to wait for compaction executors shutdown");
            }
            catch (InterruptedException e)
            {
                logger.error("Interrupted while waiting for tasks to be terminated", e);
            }
        }
    }

    public void finishCompactionsAndShutdown(long timeout, TimeUnit unit) throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination(timeout, unit);
    }

    // the actual sstables to compact are not determined until we run the BCT; that way, if new sstables
    // are created between task submission and execution, we execute against the most up-to-date information
    @VisibleForTesting
    class BackgroundCompactionCandidate implements Runnable
    {
        private final ColumnFamilyStore cfs;

        BackgroundCompactionCandidate(ColumnFamilyStore cfs)
        {
            compactingCF.add(cfs);
            this.cfs = cfs;
        }

        public void run()
        {
            boolean ranCompaction = false;
            try
            {
                logger.trace("Checking {}.{}", cfs.getKeyspaceName(), cfs.name);
                if (!cfs.isValid())
                {
                    logger.trace("Aborting compaction for dropped CF");
                    return;
                }

                CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
                AbstractCompactionTask task = strategy.getNextBackgroundTask(getDefaultGcBefore(cfs, FBUtilities.nowInSeconds()));
                if (task == null)
                {
                    if (DatabaseDescriptor.automaticSSTableUpgrade())
                        ranCompaction = maybeRunUpgradeTask(strategy);
                }
                else
                {
                    task.execute(active);
                    ranCompaction = true;
                }
            }
            finally
            {
                compactingCF.remove(cfs);
            }
            if (ranCompaction) // only submit background if we actually ran a compaction - otherwise we end up in an infinite loop submitting noop background tasks
                submitBackground(cfs);
        }

        boolean maybeRunUpgradeTask(CompactionStrategyManager strategy)
        {
            logger.debug("Checking for upgrade tasks {}.{}", cfs.getKeyspaceName(), cfs.getTableName());
            try
            {
                if (currentlyBackgroundUpgrading.incrementAndGet() <= DatabaseDescriptor.maxConcurrentAutoUpgradeTasks())
                {
                    AbstractCompactionTask upgradeTask = strategy.findUpgradeSSTableTask();
                    if (upgradeTask != null)
                    {
                        upgradeTask.execute(active);
                        return true;
                    }
                }
            }
            finally
            {
                currentlyBackgroundUpgrading.decrementAndGet();
            }
            logger.trace("No tasks available");
            return false;
        }
    }

    @VisibleForTesting
    public BackgroundCompactionCandidate getBackgroundCompactionCandidate(ColumnFamilyStore cfs)
    {
        return new BackgroundCompactionCandidate(cfs);
    }

    /**
     * Run an operation over all sstables using jobs threads
     *
     * @param cfs the column family store to run the operation on
     * @param operation the operation to run
     * @param jobs the number of threads to use - 0 means use all available. It never uses more than concurrent_compactors threads
     * @return status of the operation
     */
    private AllSSTableOpStatus parallelAllSSTableOperation(final ColumnFamilyStore cfs,
                                                           final OneSSTableOperation operation,
                                                           int jobs,
                                                           OperationType operationType)
    {
        String operationName = operationType.name();
        String keyspace = cfs.getKeyspaceName();
        String table = cfs.getTableName();
        return cfs.withAllSSTables(operationType, (compacting) -> {
            logger.info("Starting {} for {}.{}", operationType, cfs.getKeyspaceName(), cfs.getTableName());
            List<LifecycleTransaction> transactions = new ArrayList<>();
            List<Future<?>> futures = new ArrayList<>();
            try
            {
                if (compacting == null)
                    return AllSSTableOpStatus.UNABLE_TO_CANCEL;

                Iterable<SSTableReader> sstables = Lists.newArrayList(operation.filterSSTables(compacting));
                if (Iterables.isEmpty(sstables))
                {
                    logger.info("No sstables to {} for {}.{}", operationName, keyspace, table);
                    return AllSSTableOpStatus.SUCCESSFUL;
                }

                for (final SSTableReader sstable : sstables)
                {
                    final LifecycleTransaction txn = compacting.split(singleton(sstable));
                    transactions.add(txn);
                    Callable<Object> callable = new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            operation.execute(txn);
                            return this;
                        }
                    };
                    Future<?> fut = executor.submitIfRunning(callable, "parallel SSTable operation");
                    if (!fut.isCancelled())
                        futures.add(fut);
                    else
                        return AllSSTableOpStatus.ABORTED;

                    if (jobs > 0 && futures.size() == jobs)
                    {
                        Future<?> f = FBUtilities.waitOnFirstFuture(futures);
                        futures.remove(f);
                    }
                }
                FBUtilities.waitOnFutures(futures);
                assert compacting.originals().isEmpty();
                logger.info("Finished {} for {}.{} successfully", operationType, keyspace, table);
                return AllSSTableOpStatus.SUCCESSFUL;
            }
            finally
            {
                // wait on any unfinished futures to make sure we don't close an ongoing transaction
                try
                {
                    FBUtilities.waitOnFutures(futures);
                }
                catch (Throwable t)
                {
                    // these are handled/logged in CompactionExecutor#afterExecute
                }
                Throwable fail = Throwables.close(null, transactions);
                if (fail != null)
                    logger.error("Failed to cleanup lifecycle transactions ({} for {}.{})", operationType, keyspace, table, fail);
            }
        });
    }

    private static interface OneSSTableOperation
    {
        Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction);
        void execute(LifecycleTransaction input) throws IOException;
    }

    public enum AllSSTableOpStatus
    {
        SUCCESSFUL(0),
        ABORTED(1),
        UNABLE_TO_CANCEL(2);

        public final int statusCode;

        AllSSTableOpStatus(int statusCode)
        {
            this.statusCode = statusCode;
        }
    }

    public AllSSTableOpStatus performScrub(ColumnFamilyStore cfs, IScrubber.Options options, int jobs)
    {
        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction input)
            {
                return input.originals();
            }

            @Override
            public void execute(LifecycleTransaction input)
            {
                scrubOne(cfs, input, options, active);
            }
        }, jobs, OperationType.SCRUB);
    }

    public AllSSTableOpStatus performVerify(ColumnFamilyStore cfs, IVerifier.Options options) throws InterruptedException, ExecutionException
    {
        assert !cfs.isIndex();
        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction input)
            {
                return input.originals();
            }

            @Override
            public void execute(LifecycleTransaction input)
            {
                verifyOne(cfs, input.onlyOne(), options, active);
            }
        }, 0, OperationType.VERIFY);
    }

    public AllSSTableOpStatus performSSTableRewrite(final ColumnFamilyStore cfs,
                                                    final boolean skipIfCurrentVersion,
                                                    final long skipIfOlderThanTimestamp,
                                                    final boolean skipIfCompressionMatches,
                                                    int jobs) throws InterruptedException, ExecutionException
    {
        return performSSTableRewrite(cfs, (sstable) -> {
            // Skip if descriptor version matches current version
            if (skipIfCurrentVersion && sstable.descriptor.version.equals(sstable.descriptor.getFormat().getLatestVersion()))
                return false;

            // Skip if SSTable creation time is past given timestamp
            if (sstable.getDataCreationTime() > skipIfOlderThanTimestamp)
                return false;

            TableMetadata metadata = cfs.metadata.get();
            // Skip if SSTable compression parameters match current ones
            if (skipIfCompressionMatches &&
                ((!sstable.compression && !metadata.params.compression.isEnabled()) ||
                 (sstable.compression && metadata.params.compression.equals(sstable.getCompressionMetadata().parameters))))
                return false;

            return true;
        }, jobs);
    }

    /**
     * Perform SSTable rewrite

     * @param sstableFilter sstables for which predicate returns {@link false} will be excluded
     */
    public AllSSTableOpStatus performSSTableRewrite(final ColumnFamilyStore cfs, Predicate<SSTableReader> sstableFilter, int jobs) throws InterruptedException, ExecutionException
    {
        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                List<SSTableReader> sortedSSTables = Lists.newArrayList(transaction.originals());
                Collections.sort(sortedSSTables, SSTableReader.sizeComparator.reversed());
                Iterator<SSTableReader> iter = sortedSSTables.iterator();
                while (iter.hasNext())
                {
                    SSTableReader sstable = iter.next();
                    if (!sstableFilter.test(sstable))
                    {
                        transaction.cancel(sstable);
                        iter.remove();
                    }
                }
                return sortedSSTables;
            }

            @Override
            public void execute(LifecycleTransaction txn)
            {
                AbstractCompactionTask task = cfs.getCompactionStrategyManager().getCompactionTask(txn, NO_GC, Long.MAX_VALUE);
                task.setUserDefined(true);
                task.setCompactionType(OperationType.UPGRADE_SSTABLES);
                task.execute(active);
            }
        }, jobs, OperationType.UPGRADE_SSTABLES);
    }

    public AllSSTableOpStatus performCleanup(final ColumnFamilyStore cfStore, int jobs) throws InterruptedException, ExecutionException
    {
        assert !cfStore.isIndex();
        Keyspace keyspace = cfStore.keyspace;

        // if local ranges is empty, it means no data should remain
        final RangesAtEndpoint replicas = StorageService.instance.getLocalReplicas(keyspace.getName());
        final Set<Range<Token>> allRanges = replicas.ranges();
        final Set<Range<Token>> transientRanges = replicas.onlyTransient().ranges();
        final Set<Range<Token>> fullRanges = replicas.onlyFull().ranges();
        final boolean hasIndexes = cfStore.indexManager.hasIndexes();

        return parallelAllSSTableOperation(cfStore, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                List<SSTableReader> sortedSSTables = Lists.newArrayList(transaction.originals());
                Iterator<SSTableReader> sstableIter = sortedSSTables.iterator();
                int totalSSTables = 0;
                int skippedSStables = 0;
                while (sstableIter.hasNext())
                {
                    SSTableReader sstable = sstableIter.next();
                    boolean needsCleanupFull = needsCleanup(sstable, fullRanges);
                    boolean needsCleanupTransient = !transientRanges.isEmpty() && sstable.isRepaired() && needsCleanup(sstable, transientRanges);
                    //If there are no ranges for which the table needs cleanup either due to lack of intersection or lack
                    //of the table being repaired.
                    totalSSTables++;
                    if (!needsCleanupFull && !needsCleanupTransient)
                    {
                        logger.debug("Skipping {} ([{}, {}]) for cleanup; all rows should be kept. Needs cleanup full ranges: {} Needs cleanup transient ranges: {} Repaired: {}",
                                    sstable,
                                    sstable.getFirst().getToken(),
                                    sstable.getLast().getToken(),
                                    needsCleanupFull,
                                    needsCleanupTransient,
                                    sstable.isRepaired());
                        sstableIter.remove();
                        transaction.cancel(sstable);
                        skippedSStables++;
                    }
                }
                logger.info("Skipping cleanup for {}/{} sstables for {}.{} since they are fully contained in owned ranges (full ranges: {}, transient ranges: {})",
                            skippedSStables, totalSSTables, cfStore.getKeyspaceName(), cfStore.getTableName(), fullRanges, transientRanges);
                sortedSSTables.sort(SSTableReader.sizeComparator);
                return sortedSSTables;
            }

            @Override
            public void execute(LifecycleTransaction txn) throws IOException
            {
                CleanupStrategy cleanupStrategy = CleanupStrategy.get(cfStore, allRanges, transientRanges, txn.onlyOne().isRepaired(), FBUtilities.nowInSeconds());
                doCleanupOne(cfStore, txn, cleanupStrategy, replicas.ranges(), hasIndexes);
            }
        }, jobs, OperationType.CLEANUP);
    }

    public AllSSTableOpStatus performGarbageCollection(final ColumnFamilyStore cfStore, TombstoneOption tombstoneOption, int jobs) throws InterruptedException, ExecutionException
    {
        assert !cfStore.isIndex();

        return parallelAllSSTableOperation(cfStore, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                List<SSTableReader> filteredSSTables = new ArrayList<>();
                if (cfStore.getCompactionStrategyManager().onlyPurgeRepairedTombstones())
                {
                    for (SSTableReader sstable : transaction.originals())
                    {
                        if (!sstable.isRepaired())
                        {
                            try
                            {
                                transaction.cancel(sstable);
                            }
                            catch (Throwable t)
                            {
                                logger.warn(String.format("Unable to cancel %s from transaction %s", sstable, transaction.opId()), t);
                            }
                        }
                        else
                        {
                            filteredSSTables.add(sstable);
                        }
                    }
                }
                else
                {
                    filteredSSTables.addAll(transaction.originals());
                }

                filteredSSTables.sort(SSTableReader.maxTimestampAscending);
                return filteredSSTables;
            }

            @Override
            public void execute(LifecycleTransaction txn) throws IOException
            {
                logger.debug("Garbage collecting {}", txn.originals());
                CompactionTask task = new CompactionTask(cfStore, txn, getDefaultGcBefore(cfStore, FBUtilities.nowInSeconds()))
                {
                    @Override
                    protected CompactionController getCompactionController(Set<SSTableReader> toCompact)
                    {
                        return new CompactionController(cfStore, toCompact, gcBefore, null, tombstoneOption);
                    }

                    @Override
                    protected int getLevel()
                    {
                        return txn.onlyOne().getSSTableLevel();
                    }
                };
                task.setUserDefined(true);
                task.setCompactionType(OperationType.GARBAGE_COLLECT);
                task.execute(active);
            }
        }, jobs, OperationType.GARBAGE_COLLECT);
    }

    public AllSSTableOpStatus relocateSSTables(final ColumnFamilyStore cfs, int jobs) throws ExecutionException, InterruptedException
    {
        if (!cfs.getPartitioner().splitter().isPresent())
        {
            logger.info("Partitioner does not support splitting");
            return AllSSTableOpStatus.ABORTED;
        }

        if (StorageService.instance.getLocalReplicas(cfs.getKeyspaceName()).isEmpty())
        {
            logger.info("Relocate cannot run before a node has joined the ring");
            return AllSSTableOpStatus.ABORTED;
        }

        final DiskBoundaries diskBoundaries = cfs.getDiskBoundaries();

        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                Set<SSTableReader> originals = Sets.newHashSet(transaction.originals());
                Set<SSTableReader> needsRelocation = originals.stream().filter(s -> !inCorrectLocation(s)).collect(Collectors.toSet());
                transaction.cancel(Sets.difference(originals, needsRelocation));

                Map<Integer, List<SSTableReader>> groupedByDisk = groupByDiskIndex(needsRelocation);

                int maxSize = 0;
                for (List<SSTableReader> diskSSTables : groupedByDisk.values())
                    maxSize = Math.max(maxSize, diskSSTables.size());

                List<SSTableReader> mixedSSTables = new ArrayList<>();

                for (int i = 0; i < maxSize; i++)
                    for (List<SSTableReader> diskSSTables : groupedByDisk.values())
                        if (i < diskSSTables.size())
                            mixedSSTables.add(diskSSTables.get(i));

                return mixedSSTables;
            }

            public Map<Integer, List<SSTableReader>> groupByDiskIndex(Set<SSTableReader> needsRelocation)
            {
                return needsRelocation.stream().collect(Collectors.groupingBy((s) -> diskBoundaries.getDiskIndex(s)));
            }

            private boolean inCorrectLocation(SSTableReader sstable)
            {
                if (!cfs.getPartitioner().splitter().isPresent())
                    return true;

                // Compare the expected data directory for the sstable with its current data directory
                Directories.DataDirectory currentDirectory = cfs.getDirectories().getDataDirectoryForFile(sstable.descriptor);
                return diskBoundaries.isInCorrectLocation(sstable, currentDirectory);
            }

            @Override
            public void execute(LifecycleTransaction txn)
            {
                logger.debug("Relocating {}", txn.originals());
                AbstractCompactionTask task = cfs.getCompactionStrategyManager().getCompactionTask(txn, NO_GC, Long.MAX_VALUE);
                task.setUserDefined(true);
                task.setCompactionType(OperationType.RELOCATE);
                task.execute(active);
            }
        }, jobs, OperationType.RELOCATE);
    }

    /**
     * Splits the given token ranges of the given sstables into a pending repair silo
     */
    public Future<Void> submitPendingAntiCompaction(ColumnFamilyStore cfs,
                                                    RangesAtEndpoint tokenRanges,
                                                    Refs<SSTableReader> sstables,
                                                    LifecycleTransaction txn,
                                                    TimeUUID sessionId,
                                                    BooleanSupplier isCancelled)
    {
        Runnable runnable = new WrappedRunnable()
        {
            protected void runMayThrow() throws Exception
            {
                try (TableMetrics.TableTimer.Context ctx = cfs.metric.anticompactionTime.time())
                {
                    performAnticompaction(cfs, tokenRanges, sstables, txn, sessionId, isCancelled);
                }
            }
        };

        Future<Void> task = null;
        try
        {
            task = executor.submitIfRunning(runnable, "pending anticompaction");
            return task;
        }
        finally
        {
            if (task == null || task.isCancelled())
            {
                sstables.release();
                txn.abort();
            }
        }
    }

    /**
     * for sstables that are fully contained in the given ranges, just rewrite their metadata with
     * the pending repair id and remove them from the transaction
     */
    private static void mutateFullyContainedSSTables(ColumnFamilyStore cfs,
                                                     Refs<SSTableReader> refs,
                                                     Iterator<SSTableReader> sstableIterator,
                                                     Collection<Range<Token>> ranges,
                                                     LifecycleTransaction txn,
                                                     TimeUUID sessionID,
                                                     boolean isTransient) throws IOException
    {
        if (ranges.isEmpty())
            return;

        List<Range<Token>> normalizedRanges = Range.normalize(ranges);

        Set<SSTableReader> fullyContainedSSTables = findSSTablesToAnticompact(sstableIterator, normalizedRanges, sessionID);

        cfs.metric.bytesMutatedAnticompaction.inc(SSTableReader.getTotalBytes(fullyContainedSSTables));
        cfs.getCompactionStrategyManager().mutateRepaired(fullyContainedSSTables, UNREPAIRED_SSTABLE, sessionID, isTransient);
        // since we're just re-writing the sstable metdata for the fully contained sstables, we don't want
        // them obsoleted when the anti-compaction is complete. So they're removed from the transaction here
        txn.cancel(fullyContainedSSTables);
        refs.release(fullyContainedSSTables);
    }

    /**
     * Make sure the {validatedForRepair} are marked for compaction before calling this.
     *
     * Caller must reference the validatedForRepair sstables (via ParentRepairSession.getActiveRepairedSSTableRefs(..)).
     *
     * @param cfs
     * @param replicas token ranges to be repaired
     * @param validatedForRepair SSTables containing the repaired ranges. Should be referenced before passing them.
     * @param sessionID the repair session we're anti-compacting for
     * @param isCancelled function that indicates if active anti-compaction should be canceled
     * @throws InterruptedException
     * @throws IOException
     */
    public void performAnticompaction(ColumnFamilyStore cfs,
                                      RangesAtEndpoint replicas,
                                      Refs<SSTableReader> validatedForRepair,
                                      LifecycleTransaction txn,
                                      TimeUUID sessionID,
                                      BooleanSupplier isCancelled) throws IOException
    {
        try
        {
            ActiveRepairService.ParentRepairSession prs;
            try
            {
                prs = ActiveRepairService.instance().getParentRepairSession(sessionID);
            }
            catch (NoSuchRepairSessionException e)
            {
                throw new CompactionInterruptedException(e.getMessage());
            }
            Preconditions.checkArgument(!prs.isPreview(), "Cannot anticompact for previews");
            Preconditions.checkArgument(!replicas.isEmpty(), "No ranges to anti-compact");

            if (logger.isInfoEnabled())
                logger.info("{} Starting anticompaction for {}.{} on {}/{} sstables", PreviewKind.NONE.logPrefix(sessionID), cfs.getKeyspaceName(), cfs.getTableName(), validatedForRepair.size(), cfs.getLiveSSTables().size());
            if (logger.isTraceEnabled())
                logger.trace("{} Starting anticompaction for ranges {}", PreviewKind.NONE.logPrefix(sessionID), replicas);

            Set<SSTableReader> sstables = new HashSet<>(validatedForRepair);
            validateSSTableBoundsForAnticompaction(sessionID, sstables, replicas);
            mutateFullyContainedSSTables(cfs, validatedForRepair, sstables.iterator(), replicas.onlyFull().ranges(), txn, sessionID, false);
            mutateFullyContainedSSTables(cfs, validatedForRepair, sstables.iterator(), replicas.onlyTransient().ranges(), txn, sessionID, true);

            assert txn.originals().equals(sstables);
            if (!sstables.isEmpty())
                doAntiCompaction(cfs, replicas, txn, sessionID, isCancelled);
            txn.finish();
        }
        finally
        {
            validatedForRepair.release();
            txn.close();
        }

        logger.info("{} Completed anticompaction successfully", PreviewKind.NONE.logPrefix(sessionID));
    }

    static void validateSSTableBoundsForAnticompaction(TimeUUID sessionID,
                                                       Collection<SSTableReader> sstables,
                                                       RangesAtEndpoint ranges)
    {
        List<Range<Token>> normalizedRanges = Range.normalize(ranges.ranges());
        for (SSTableReader sstable : sstables)
        {
            AbstractBounds<Token> bounds = sstable.getBounds();

            if (!Iterables.any(normalizedRanges, r -> (r.contains(bounds.left) && r.contains(bounds.right)) || r.intersects(bounds)))
            {
                // this should never happen - in PendingAntiCompaction#getSSTables we select all sstables that intersect the repaired ranges, that can't have changed here
                String message = String.format("%s SSTable %s (%s) does not intersect repaired ranges %s, this sstable should not have been included.",
                                               PreviewKind.NONE.logPrefix(sessionID), sstable, bounds, normalizedRanges);
                logger.error(message);
                throw new IllegalStateException(message);
            }
        }

    }

    @VisibleForTesting
    static Set<SSTableReader> findSSTablesToAnticompact(Iterator<SSTableReader> sstableIterator, List<Range<Token>> normalizedRanges, TimeUUID parentRepairSession)
    {
        Set<SSTableReader> fullyContainedSSTables = new HashSet<>();
        while (sstableIterator.hasNext())
        {
            SSTableReader sstable = sstableIterator.next();

            AbstractBounds<Token> sstableBounds = sstable.getBounds();

            for (Range<Token> r : normalizedRanges)
            {
                // ranges are normalized - no wrap around - if first and last are contained we know that all tokens are contained in the range
                if (r.contains(sstable.getFirst().getToken()) && r.contains(sstable.getLast().getToken()))
                {
                    logger.info("{} SSTable {} fully contained in range {}, mutating repairedAt instead of anticompacting", PreviewKind.NONE.logPrefix(parentRepairSession), sstable, r);
                    fullyContainedSSTables.add(sstable);
                    sstableIterator.remove();
                    break;
                }
                else if (r.intersects(sstableBounds))
                {
                    logger.info("{} SSTable {} ({}) will be anticompacted on range {}", PreviewKind.NONE.logPrefix(parentRepairSession), sstable, sstableBounds, r);
                }
            }
        }
        return fullyContainedSSTables;
    }

    public void performMaximal(final ColumnFamilyStore cfStore, boolean splitOutput)
    {
        FBUtilities.waitOnFutures(submitMaximal(cfStore, getDefaultGcBefore(cfStore, FBUtilities.nowInSeconds()), splitOutput));
    }

    public List<Future<?>> submitMaximal(final ColumnFamilyStore cfStore, final long gcBefore, boolean splitOutput)
    {
            return submitMaximal(cfStore, gcBefore, splitOutput, OperationType.MAJOR_COMPACTION);
    }

    public List<Future<?>> submitMaximal(final ColumnFamilyStore cfStore, final long gcBefore, boolean splitOutput, OperationType operationType)
    {
        // here we compute the task off the compaction executor, so having that present doesn't
        // confuse runWithCompactionsDisabled -- i.e., we don't want to deadlock ourselves, waiting
        // for ourselves to finish/acknowledge cancellation before continuing.
        CompactionTasks tasks = cfStore.getCompactionStrategyManager().getMaximalTasks(gcBefore, splitOutput, operationType);

        if (tasks.isEmpty())
            return Collections.emptyList();

        List<Future<?>> futures = new ArrayList<>();

        int nonEmptyTasks = 0;
        for (final AbstractCompactionTask task : tasks)
        {
            if (task.transaction.originals().size() > 0)
                nonEmptyTasks++;

            Runnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow()
                {
                    task.execute(active);
                }
            };

            Future<?> fut = executor.submitIfRunning(runnable, "maximal task");
            if (!fut.isCancelled())
                futures.add(fut);
        }
        if (nonEmptyTasks > 1)
            logger.info("Major compaction will not result in a single sstable - repaired and unrepaired data is kept separate and compaction runs per data_file_directory.");

        return futures;
    }

    public void forceCompaction(ColumnFamilyStore cfStore, Supplier<Collection<SSTableReader>> sstablesFn, com.google.common.base.Predicate<SSTableReader> sstablesPredicate)
    {
        Callable<CompactionTasks> taskCreator = () -> {
            Collection<SSTableReader> sstables = sstablesFn.get();
            if (sstables == null || sstables.isEmpty())
            {
                logger.debug("No sstables found for the provided token range");
                return CompactionTasks.empty();
            }
            return cfStore.getCompactionStrategyManager().getUserDefinedTasks(sstables, getDefaultGcBefore(cfStore, FBUtilities.nowInSeconds()));
        };

        try (CompactionTasks tasks = cfStore.runWithCompactionsDisabled(taskCreator,
                                                                        sstablesPredicate,
                                                                        OperationType.MAJOR_COMPACTION,
                                                                        false,
                                                                        false,
                                                                        false))
        {
            if (tasks.isEmpty())
                return;

            Runnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow()
                {
                    for (AbstractCompactionTask task : tasks)
                        if (task != null)
                        {
                            task.setCompactionType(OperationType.MAJOR_COMPACTION);
                            task.execute(active);
                        }
                }
            };

            FBUtilities.waitOnFuture(executor.submitIfRunning(runnable, "force compaction for token range"));
        }
    }

    /**
     * Forces a major compaction of specified token ranges of the specified column family.
     * <p>
     * The token ranges will be interpreted as closed intervals to match the closed interval defined by the first and
     * last keys of a sstable, even though the {@link Range} class is suppossed to be half-open by definition.
     *
     * @param cfStore The column family store to be compacted.
     * @param ranges The token ranges to be compacted, interpreted as closed intervals.
     */
    public void forceCompactionForTokenRange(ColumnFamilyStore cfStore, Collection<Range<Token>> ranges)
    {
        forceCompaction(cfStore,
                        () -> sstablesInBounds(cfStore, ranges),
                        sstable -> sstable.getBounds().intersects(ranges));
    }

    /**
     * Returns the sstables of the specified column family store that intersect with the specified token ranges.
     * <p>
     * The token ranges will be interpreted as closed intervals to match the closed interval defined by the first and
     * last keys of a sstable, even though the {@link Range} class is suppossed to be half-open by definition.
     */
    private static Collection<SSTableReader> sstablesInBounds(ColumnFamilyStore cfs, Collection<Range<Token>> tokenRangeCollection)
    {
        final Set<SSTableReader> sstables = new HashSet<>();
        Iterable<SSTableReader> liveTables = cfs.getTracker().getView().select(SSTableSet.LIVE);
        SSTableIntervalTree tree = SSTableIntervalTree.build(liveTables);

        for (Range<Token> tokenRange : tokenRangeCollection)
        {
            if (!AbstractBounds.strictlyWrapsAround(tokenRange.left, tokenRange.right))
            {
                Iterable<SSTableReader> ssTableReaders = View.sstablesInBounds(tokenRange.left.minKeyBound(), tokenRange.right.maxKeyBound(), tree);
                Iterables.addAll(sstables, ssTableReaders);
            }
            else
            {
                // Searching an interval tree will not return the correct results for a wrapping range
                // so we have to unwrap it first
                for (Range<Token> unwrappedRange : tokenRange.unwrap())
                {
                    Iterable<SSTableReader> ssTableReaders = View.sstablesInBounds(unwrappedRange.left.minKeyBound(), unwrappedRange.right.maxKeyBound(), tree);
                    Iterables.addAll(sstables, ssTableReaders);
                }
            }
        }
        return sstables;
    }

    public void forceCompactionForKey(ColumnFamilyStore cfStore, DecoratedKey key)
    {
        forceCompaction(cfStore, () -> sstablesWithKey(cfStore, key), Predicates.alwaysTrue());
    }

    public void forceCompactionForKeys(ColumnFamilyStore cfStore, Collection<DecoratedKey> keys)
    {
        forceCompaction(cfStore, () -> sstablesWithKeys(cfStore, keys), Predicates.alwaysTrue());
    }

    private static Collection<SSTableReader> sstablesWithKey(ColumnFamilyStore cfs, DecoratedKey key)
    {
        final Set<SSTableReader> sstables = new HashSet<>();
        Iterable<SSTableReader> liveTables = cfs.getTracker().getView().liveSSTablesInBounds(key.getToken().minKeyBound(),
                                                                                             key.getToken().maxKeyBound());
        for (SSTableReader sstable : liveTables)
        {
            if (sstable.mayContainAssumingKeyIsInRange(key))
                sstables.add(sstable);
        }
        return sstables.isEmpty() ? Collections.emptyList() : sstables;
    }

    private static Collection<SSTableReader> sstablesWithKeys(ColumnFamilyStore cfs, Collection<DecoratedKey> decoratedKeys)
    {
        final Set<SSTableReader> sstables = new HashSet<>();

        for (DecoratedKey decoratedKey : decoratedKeys)
        {
            sstables.addAll(sstablesWithKey(cfs, decoratedKey));
        }

        return sstables;
    }

    public void forceUserDefinedCompaction(String dataFiles)
    {
        String[] filenames = dataFiles.split(",");
        Multimap<ColumnFamilyStore, Descriptor> descriptors = ArrayListMultimap.create();

        for (String filename : filenames)
        {
            // extract keyspace and columnfamily name from filename
            Descriptor desc = Descriptor.fromFileWithComponent(new File(filename.trim()), false).left;
            if (Schema.instance.getTableMetadataRef(desc) == null)
            {
                logger.warn("Schema does not exist for file {}. Skipping.", filename);
                continue;
            }
            // group by keyspace/columnfamily
            ColumnFamilyStore cfs = Keyspace.open(desc.ksname).getColumnFamilyStore(desc.cfname);
            descriptors.put(cfs, cfs.getDirectories().find(new File(filename.trim()).name()));
        }

        List<Future<?>> futures = new ArrayList<>(descriptors.size());
        long nowInSec = FBUtilities.nowInSeconds();
        for (ColumnFamilyStore cfs : descriptors.keySet())
            futures.add(submitUserDefined(cfs, descriptors.get(cfs), getDefaultGcBefore(cfs, nowInSec)));
        FBUtilities.waitOnFutures(futures);
    }

    public void forceUserDefinedCleanup(String dataFiles)
    {
        String[] filenames = dataFiles.split(",");
        HashMap<ColumnFamilyStore, Descriptor> descriptors = Maps.newHashMap();

        for (String filename : filenames)
        {
            // extract keyspace and columnfamily name from filename
            Descriptor desc = Descriptor.fromFileWithComponent(new File(filename.trim()), false).left;
            if (Schema.instance.getTableMetadataRef(desc) == null)
            {
                logger.warn("Schema does not exist for file {}. Skipping.", filename);
                continue;
            }
            // group by keyspace/columnfamily
            ColumnFamilyStore cfs = Keyspace.open(desc.ksname).getColumnFamilyStore(desc.cfname);
            desc = cfs.getDirectories().find(new File(filename.trim()).name());
            if (desc != null)
                descriptors.put(cfs, desc);
        }

        if (!StorageService.instance.isJoined())
        {
            logger.error("Cleanup cannot run before a node has joined the ring");
            return;
        }

        for (Map.Entry<ColumnFamilyStore,Descriptor> entry : descriptors.entrySet())
        {
            ColumnFamilyStore cfs = entry.getKey();
            Keyspace keyspace = cfs.keyspace;
            final RangesAtEndpoint replicas = StorageService.instance.getLocalReplicas(keyspace.getName());
            final Set<Range<Token>> allRanges = replicas.ranges();
            final Set<Range<Token>> transientRanges = replicas.onlyTransient().ranges();
            boolean hasIndexes = cfs.indexManager.hasIndexes();
            SSTableReader sstable = lookupSSTable(cfs, entry.getValue());

            if (sstable == null)
            {
                logger.warn("Will not clean {}, it is not an active sstable", entry.getValue());
            }
            else
            {
                CleanupStrategy cleanupStrategy = CleanupStrategy.get(cfs, allRanges, transientRanges, sstable.isRepaired(), FBUtilities.nowInSeconds());
                try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.CLEANUP))
                {
                    doCleanupOne(cfs, txn, cleanupStrategy, allRanges, hasIndexes);
                }
                catch (IOException e)
                {
                    logger.error("forceUserDefinedCleanup failed: {}", e.getLocalizedMessage());
                }
            }
        }
    }


    public Future<?> submitUserDefined(final ColumnFamilyStore cfs, final Collection<Descriptor> dataFiles, final long gcBefore)
    {
        Runnable runnable = new WrappedRunnable()
        {
            protected void runMayThrow() throws Exception
            {
                // look up the sstables now that we're on the compaction executor, so we don't try to re-compact
                // something that was already being compacted earlier.
                Collection<SSTableReader> sstables = new ArrayList<>(dataFiles.size());
                for (Descriptor desc : dataFiles)
                {
                    // inefficient but not in a performance sensitive path
                    SSTableReader sstable = lookupSSTable(cfs, desc);
                    if (sstable == null)
                    {
                        logger.info("Will not compact {}: it is not an active sstable", desc);
                    }
                    else
                    {
                        sstables.add(sstable);
                    }
                }

                if (sstables.isEmpty())
                {
                    logger.info("No files to compact for user defined compaction");
                }
                else
                {
                    try (CompactionTasks tasks = cfs.getCompactionStrategyManager().getUserDefinedTasks(sstables, gcBefore))
                    {
                        for (AbstractCompactionTask task : tasks)
                        {
                            if (task != null)
                                task.execute(active);
                        }
                    }
                }
            }
        };

        return executor.submitIfRunning(runnable, "user defined task");
    }

    // This acquire a reference on the sstable
    // This is not efficient, do not use in any critical path
    private SSTableReader lookupSSTable(final ColumnFamilyStore cfs, Descriptor descriptor)
    {
        for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
        {
            if (sstable.descriptor.equals(descriptor))
                return sstable;
        }
        return null;
    }

    public Future<?> submitValidation(Callable<Object> validation)
    {
        return validationExecutor.submitIfRunning(validation, "validation");
    }

    /* Used in tests. */
    public void disableAutoCompaction()
    {
        for (String ksname : Schema.instance.distributedKeyspaces().names())
        {
            for (ColumnFamilyStore cfs : Keyspace.open(ksname).getColumnFamilyStores())
                cfs.disableAutoCompaction();
        }
    }

    @VisibleForTesting
    void scrubOne(ColumnFamilyStore cfs, LifecycleTransaction modifier, IScrubber.Options options, ActiveCompactionsTracker activeCompactions)
    {
        CompactionInfo.Holder scrubInfo = null;
        SSTableFormat format = modifier.onlyOne().descriptor.getFormat();
        try (IScrubber scrubber = format.getScrubber(cfs, modifier, new OutputHandler.LogOutput(), options))
        {
            scrubInfo = scrubber.getScrubInfo();
            activeCompactions.beginCompaction(scrubInfo);
            scrubber.scrub();
        }
        finally
        {
            if (scrubInfo != null)
                activeCompactions.finishCompaction(scrubInfo);
        }
    }

    @VisibleForTesting
    void verifyOne(ColumnFamilyStore cfs, SSTableReader sstable, IVerifier.Options options, ActiveCompactionsTracker activeCompactions)
    {
        CompactionInfo.Holder verifyInfo = null;
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, options))
        {
            verifyInfo = verifier.getVerifyInfo();
            activeCompactions.beginCompaction(verifyInfo);
            verifier.verify();
        }
        finally
        {
            if (verifyInfo != null)
                activeCompactions.finishCompaction(verifyInfo);
        }
    }

    /**
     * Determines if a cleanup would actually remove any data in this SSTable based
     * on a set of owned ranges.
     */
    @VisibleForTesting
    public static boolean needsCleanup(SSTableReader sstable, Collection<Range<Token>> ownedRanges)
    {
        if (ownedRanges.isEmpty())
        {
            return true; // all data will be cleaned
        }

        // unwrap and sort the ranges by LHS token
        List<Range<Token>> sortedRanges = Range.normalize(ownedRanges);

        // see if there are any keys LTE the token for the start of the first range
        // (token range ownership is exclusive on the LHS.)
        Range<Token> firstRange = sortedRanges.get(0);
        if (sstable.getFirst().getToken().compareTo(firstRange.left) <= 0)
            return true;

        // then, iterate over all owned ranges and see if the next key beyond the end of the owned
        // range falls before the start of the next range
        for (int i = 0; i < sortedRanges.size(); i++)
        {
            Range<Token> range = sortedRanges.get(i);
            if (range.right.isMinimum())
            {
                // we split a wrapping range and this is the second half.
                // there can't be any keys beyond this (and this is the last range)
                return false;
            }

            DecoratedKey firstBeyondRange = sstable.firstKeyBeyond(range.right.maxKeyBound());
            if (firstBeyondRange == null)
            {
                // we ran off the end of the sstable looking for the next key; we don't need to check any more ranges
                return false;
            }

            if (i == (sortedRanges.size() - 1))
            {
                // we're at the last range and we found a key beyond the end of the range
                return true;
            }

            Range<Token> nextRange = sortedRanges.get(i + 1);
            if (firstBeyondRange.getToken().compareTo(nextRange.left) <= 0)
            {
                // we found a key in between the owned ranges
                return true;
            }
        }

        return false;
    }

    /**
     * This function goes over a file and removes the keys that the node is not responsible for
     * and only keeps keys that this node is responsible for.
     *
     * @throws IOException
     */
    private void doCleanupOne(final ColumnFamilyStore cfs,
                              LifecycleTransaction txn,
                              CleanupStrategy cleanupStrategy,
                              Collection<Range<Token>> allRanges,
                              boolean hasIndexes) throws IOException
    {
        assert !cfs.isIndex();

        SSTableReader sstable = txn.onlyOne();

        // if ranges is empty and no index, entire sstable is discarded
        if (!hasIndexes && !sstable.getBounds().intersects(allRanges))
        {
            txn.obsoleteOriginals();
            txn.finish();
            logger.info("SSTable {} ([{}, {}]) does not intersect the owned ranges ({}), dropping it", sstable, sstable.getFirst().getToken(), sstable.getLast().getToken(), allRanges);
            return;
        }

        long start = nanoTime();

        long totalkeysWritten = 0;

        long expectedBloomFilterSize = Math.max(cfs.metadata().params.minIndexInterval,
                                               SSTableReader.getApproximateKeyCount(txn.originals()));
        if (logger.isTraceEnabled())
            logger.trace("Expected bloom filter size : {}", expectedBloomFilterSize);

        logger.info("Cleaning up {}", sstable);

        File compactionFileLocation = sstable.descriptor.directory;
        RateLimiter limiter = getRateLimiter();
        double compressionRatio = sstable.getCompressionRatio();
        if (compressionRatio == MetadataCollector.NO_COMPRESSION_RATIO)
            compressionRatio = 1.0;

        List<SSTableReader> finished;

        long nowInSec = FBUtilities.nowInSeconds();
        try (SSTableRewriter writer = SSTableRewriter.construct(cfs, txn, false, sstable.maxDataAge);
             ISSTableScanner scanner = cleanupStrategy.getScanner(sstable);
             CompactionController controller = new CompactionController(cfs, txn.originals(), getDefaultGcBefore(cfs, nowInSec));
             Refs<SSTableReader> refs = Refs.ref(Collections.singleton(sstable));
             CompactionIterator ci = new CompactionIterator(OperationType.CLEANUP, Collections.singletonList(scanner), controller, nowInSec, nextTimeUUID(), active, null))
        {
            StatsMetadata metadata = sstable.getSSTableMetadata();
            writer.switchWriter(createWriter(cfs, compactionFileLocation, expectedBloomFilterSize, metadata.repairedAt, metadata.pendingRepair, metadata.isTransient, sstable, txn));
            long lastBytesScanned = 0;

            while (ci.hasNext())
            {
                ci.setTargetDirectory(writer.currentWriter().getFilename());
                try (UnfilteredRowIterator partition = ci.next();
                     UnfilteredRowIterator notCleaned = cleanupStrategy.cleanup(partition))
                {
                    if (notCleaned == null)
                        continue;

                    if (writer.append(notCleaned) != null)
                        totalkeysWritten++;

                    long bytesScanned = scanner.getBytesScanned();

                    compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned, compressionRatio);

                    lastBytesScanned = bytesScanned;
                }
            }

            // flush to ensure we don't lose the tombstones on a restart, since they are not commitlog'd
            cfs.indexManager.flushAllIndexesBlocking();

            finished = writer.finish();
        }

        if (!finished.isEmpty())
        {
            String format = "Cleaned up to %s.  %s to %s (~%d%% of original) for %,d keys.  Time: %,dms.";
            long dTime = TimeUnit.NANOSECONDS.toMillis(nanoTime() - start);
            long startsize = sstable.onDiskLength();
            long endsize = 0;
            for (SSTableReader newSstable : finished)
                endsize += newSstable.onDiskLength();
            double ratio = (double) endsize / (double) startsize;
            logger.info(String.format(format, finished.get(0).getFilename(), FBUtilities.prettyPrintMemory(startsize),
                                      FBUtilities.prettyPrintMemory(endsize), (int) (ratio * 100), totalkeysWritten, dTime));
        }

    }

    static void compactionRateLimiterAcquire(RateLimiter limiter, long bytesScanned, long lastBytesScanned, double compressionRatio)
    {
        long lengthRead = (long) ((bytesScanned - lastBytesScanned) * compressionRatio) + 1;
        while (lengthRead >= Integer.MAX_VALUE)
        {
            limiter.acquire(Integer.MAX_VALUE);
            lengthRead -= Integer.MAX_VALUE;
        }
        if (lengthRead > 0)
        {
            limiter.acquire((int) lengthRead);
        }
    }

    private static abstract class CleanupStrategy
    {
        protected final Collection<Range<Token>> ranges;
        protected final long nowInSec;

        protected CleanupStrategy(Collection<Range<Token>> ranges, long nowInSec)
        {
            this.ranges = ranges;
            this.nowInSec = nowInSec;
        }

        public static CleanupStrategy get(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, Collection<Range<Token>> transientRanges, boolean isRepaired, long nowInSec)
        {
            if (cfs.indexManager.hasIndexes())
            {
                if (!transientRanges.isEmpty())
                {
                    //Shouldn't have been possible to create this situation
                    throw new AssertionError("Can't have indexes and transient ranges");
                }
                return new Full(cfs, ranges, nowInSec);
            }
            return new Bounded(cfs, ranges, transientRanges, isRepaired, nowInSec);
        }

        public abstract ISSTableScanner getScanner(SSTableReader sstable);
        public abstract UnfilteredRowIterator cleanup(UnfilteredRowIterator partition);

        private static final class Bounded extends CleanupStrategy
        {
            private final Collection<Range<Token>> transientRanges;
            private final boolean isRepaired;

            public Bounded(final ColumnFamilyStore cfs, Collection<Range<Token>> ranges, Collection<Range<Token>> transientRanges, boolean isRepaired, long nowInSec)
            {
                super(ranges, nowInSec);
                instance.cacheCleanupExecutor.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        cfs.cleanupCache();
                    }
                });
                this.transientRanges = transientRanges;
                this.isRepaired = isRepaired;
            }

            @Override
            public ISSTableScanner getScanner(SSTableReader sstable)
            {
                //If transient replication is enabled and there are transient ranges
                //then cleanup should remove any partitions that are repaired and in the transient range
                //as they should already be synchronized at other full replicas.
                //So just don't scan the portion of the table containing the repaired transient ranges
                Collection<Range<Token>> rangesToScan = ranges;
                if (isRepaired)
                {
                    rangesToScan = Collections2.filter(ranges, range -> !transientRanges.contains(range));
                }
                return sstable.getScanner(rangesToScan);
            }

            @Override
            public UnfilteredRowIterator cleanup(UnfilteredRowIterator partition)
            {
                return partition;
            }
        }

        private static final class Full extends CleanupStrategy
        {
            private final ColumnFamilyStore cfs;

            public Full(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, long nowInSec)
            {
                super(ranges, nowInSec);
                this.cfs = cfs;
            }

            @Override
            public ISSTableScanner getScanner(SSTableReader sstable)
            {
                return sstable.getScanner();
            }

            @Override
            public UnfilteredRowIterator cleanup(UnfilteredRowIterator partition)
            {
                if (Range.isInRanges(partition.partitionKey().getToken(), ranges))
                    return partition;

                cfs.invalidateCachedPartition(partition.partitionKey());

                cfs.indexManager.deletePartition(partition, nowInSec);
                return null;
            }
        }
    }

    public static SSTableWriter createWriter(ColumnFamilyStore cfs,
                                             File compactionFileLocation,
                                             long expectedBloomFilterSize,
                                             long repairedAt,
                                             TimeUUID pendingRepair,
                                             boolean isTransient,
                                             SSTableReader sstable,
                                             LifecycleTransaction txn)
    {
        FileUtils.createDirectory(compactionFileLocation);

        Descriptor descriptor = cfs.newSSTableDescriptor(compactionFileLocation);
        return descriptor.getFormat().getWriterFactory().builder(descriptor)
                         .setKeyCount(expectedBloomFilterSize)
                         .setRepairedAt(repairedAt)
                         .setPendingRepair(pendingRepair)
                         .setTransientSSTable(isTransient)
                         .setTableMetadataRef(cfs.metadata)
                         .setMetadataCollector(new MetadataCollector(cfs.metadata().comparator).sstableLevel(sstable.getSSTableLevel()))
                         .setSerializationHeader(sstable.header)
                         .addDefaultComponents(cfs.indexManager.listIndexGroups())
                         .setSecondaryIndexGroups(cfs.indexManager.listIndexGroups())
                         .build(txn, cfs);
    }

    public static SSTableWriter createWriterForAntiCompaction(ColumnFamilyStore cfs,
                                                              File compactionFileLocation,
                                                              int expectedBloomFilterSize,
                                                              long repairedAt,
                                                              TimeUUID pendingRepair,
                                                              boolean isTransient,
                                                              Collection<SSTableReader> sstables,
                                                              ILifecycleTransaction txn)
    {
        FileUtils.createDirectory(compactionFileLocation);
        int minLevel = Integer.MAX_VALUE;
        // if all sstables have the same level, we can compact them together without creating overlap during anticompaction
        // note that we only anticompact from unrepaired sstables, which is not leveled, but we still keep original level
        // after first migration to be able to drop the sstables back in their original place in the repaired sstable manifest
        for (SSTableReader sstable : sstables)
        {
            if (minLevel == Integer.MAX_VALUE)
                minLevel = sstable.getSSTableLevel();

            if (minLevel != sstable.getSSTableLevel())
            {
                minLevel = 0;
                break;
            }
        }

        Descriptor descriptor = cfs.newSSTableDescriptor(compactionFileLocation);
        return descriptor.getFormat().getWriterFactory().builder(descriptor)
                         .setKeyCount(expectedBloomFilterSize)
                         .setRepairedAt(repairedAt)
                         .setPendingRepair(pendingRepair)
                         .setTransientSSTable(isTransient)
                         .setTableMetadataRef(cfs.metadata)
                         .setMetadataCollector(new MetadataCollector(sstables, cfs.metadata().comparator).sstableLevel(minLevel))
                         .setSerializationHeader(SerializationHeader.make(cfs.metadata(), sstables))
                         .addDefaultComponents(cfs.indexManager.listIndexGroups())
                         .setSecondaryIndexGroups(cfs.indexManager.listIndexGroups())
                         .build(txn, cfs);
    }

    /**
     * Splits up an sstable into two new sstables. The first of the new tables will store repaired ranges, the second
     * will store the non-repaired ranges. Once anticompation is completed, the original sstable is marked as compacted
     * and subsequently deleted.
     * @param cfs
     * @param txn a transaction over the repaired sstables to anticompact
     * @param ranges full and transient ranges to be placed into one of the new sstables. The repaired table will be tracked via
     *   the {@link org.apache.cassandra.io.sstable.metadata.StatsMetadata#pendingRepair} field.
     * @param pendingRepair the repair session we're anti-compacting for
     * @param isCancelled function that indicates if active anti-compaction should be canceled
     */
    private void doAntiCompaction(ColumnFamilyStore cfs,
                                  RangesAtEndpoint ranges,
                                  LifecycleTransaction txn,
                                  TimeUUID pendingRepair,
                                  BooleanSupplier isCancelled)
    {
        int originalCount = txn.originals().size();
        logger.info("Performing anticompaction on {} sstables for {}", originalCount, pendingRepair);

        //Group SSTables
        Set<SSTableReader> sstables = txn.originals();

        // Repairs can take place on both unrepaired (incremental + full) and repaired (full) data.
        // Although anti-compaction could work on repaired sstables as well and would result in having more accurate
        // repairedAt values for these, we still avoid anti-compacting already repaired sstables, as we currently don't
        // make use of any actual repairedAt value and splitting up sstables just for that is not worth it at this point.
        Set<SSTableReader> unrepairedSSTables = sstables.stream().filter((s) -> !s.isRepaired()).collect(Collectors.toSet());
        cfs.metric.bytesAnticompacted.inc(SSTableReader.getTotalBytes(unrepairedSSTables));
        Collection<Collection<SSTableReader>> groupedSSTables = cfs.getCompactionStrategyManager().groupSSTablesForAntiCompaction(unrepairedSSTables);

        // iterate over sstables to check if the full / transient / unrepaired ranges intersect them.
        int antiCompactedSSTableCount = 0;
        for (Collection<SSTableReader> sstableGroup : groupedSSTables)
        {
            try (LifecycleTransaction groupTxn = txn.split(sstableGroup))
            {
                int antiCompacted = antiCompactGroup(cfs, ranges, groupTxn, pendingRepair, isCancelled);
                antiCompactedSSTableCount += antiCompacted;
            }
        }
        String format = "Anticompaction completed successfully, anticompacted from {} to {} sstable(s) for {}.";
        logger.info(format, originalCount, antiCompactedSSTableCount, pendingRepair);
    }

    @VisibleForTesting
    int antiCompactGroup(ColumnFamilyStore cfs,
                         RangesAtEndpoint ranges,
                         LifecycleTransaction txn,
                         TimeUUID pendingRepair,
                         BooleanSupplier isCancelled)
    {
        Preconditions.checkArgument(!ranges.isEmpty(), "need at least one full or transient range");
        long groupMaxDataAge = -1;

        for (Iterator<SSTableReader> i = txn.originals().iterator(); i.hasNext();)
        {
            SSTableReader sstable = i.next();
            if (groupMaxDataAge < sstable.maxDataAge)
                groupMaxDataAge = sstable.maxDataAge;
        }

        if (txn.originals().size() == 0)
        {
            logger.info("No valid anticompactions for this group, All sstables were compacted and are no longer available");
            return 0;
        }

        logger.info("Anticompacting {} in {}.{} for {}", txn.originals(), cfs.getKeyspaceName(), cfs.getTableName(), pendingRepair);
        Set<SSTableReader> sstableAsSet = txn.originals();

        File destination = cfs.getDirectories().getWriteableLocationAsFile(cfs.getExpectedCompactedFileSize(sstableAsSet, OperationType.ANTICOMPACTION));
        long nowInSec = FBUtilities.nowInSeconds();
        RateLimiter limiter = getRateLimiter();

        /**
         * HACK WARNING
         *
         * We have multiple writers operating over the same Transaction, producing different sets of sstables that all
         * logically replace the transaction's originals.  The SSTableRewriter assumes it has exclusive control over
         * the transaction state, and this will lead to temporarily inconsistent sstable/tracker state if we do not
         * take special measures to avoid it.
         *
         * Specifically, if a number of rewriter have prepareToCommit() invoked in sequence, then two problematic things happen:
         *   1. The obsoleteOriginals() call of the first rewriter immediately remove the originals from the tracker, despite
         *      their having been only partially replaced.  To avoid this, we must either avoid obsoleteOriginals() or checkpoint()
         *   2. The LifecycleTransaction may only have prepareToCommit() invoked once, and this will checkpoint() also.
         *
         * Similarly commit() would finalise partially complete on-disk state.
         *
         * To avoid these problems, we introduce a SharedTxn that proxies all calls onto the underlying transaction
         * except prepareToCommit(), checkpoint(), obsoleteOriginals(), and commit().
         * We then invoke these methods directly once each of the rewriter has updated the transaction
         * with their share of replacements.
         *
         * Note that for the same essential reason we also explicitly disable early open.
         * By noop-ing checkpoint we avoid any of the problems with early open, but by continuing to explicitly
         * disable it we also prevent any of the extra associated work from being performed.
         */
        class SharedTxn extends WrappedLifecycleTransaction
        {
            public SharedTxn(ILifecycleTransaction delegate) { super(delegate); }
            public Throwable commit(Throwable accumulate) { return accumulate; }
            public void prepareToCommit() {}
            public void checkpoint() {}
            public void obsoleteOriginals() {}
            public void close() {}
        }

        CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
        try (SharedTxn sharedTxn = new SharedTxn(txn);
             SSTableRewriter fullWriter = SSTableRewriter.constructWithoutEarlyOpening(sharedTxn, false, groupMaxDataAge);
             SSTableRewriter transWriter = SSTableRewriter.constructWithoutEarlyOpening(sharedTxn, false, groupMaxDataAge);
             SSTableRewriter unrepairedWriter = SSTableRewriter.constructWithoutEarlyOpening(sharedTxn, false, groupMaxDataAge);

             AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(txn.originals());
             CompactionController controller = new CompactionController(cfs, sstableAsSet, getDefaultGcBefore(cfs, nowInSec));
             CompactionIterator ci = getAntiCompactionIterator(scanners.scanners, controller, nowInSec, nextTimeUUID(), active, isCancelled))
        {
            int expectedBloomFilterSize = Math.max(cfs.metadata().params.minIndexInterval, (int)(SSTableReader.getApproximateKeyCount(sstableAsSet)));

            fullWriter.switchWriter(CompactionManager.createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, UNREPAIRED_SSTABLE, pendingRepair, false, sstableAsSet, txn));
            transWriter.switchWriter(CompactionManager.createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, UNREPAIRED_SSTABLE, pendingRepair, true, sstableAsSet, txn));
            unrepairedWriter.switchWriter(CompactionManager.createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, UNREPAIRED_SSTABLE, NO_PENDING_REPAIR, false, sstableAsSet, txn));

            Predicate<Token> fullChecker = !ranges.onlyFull().isEmpty() ? new Range.OrderedRangeContainmentChecker(ranges.onlyFull().ranges()) : t -> false;
            Predicate<Token> transChecker = !ranges.onlyTransient().isEmpty() ? new Range.OrderedRangeContainmentChecker(ranges.onlyTransient().ranges()) : t -> false;
            double compressionRatio = scanners.getCompressionRatio();
            if (compressionRatio == MetadataCollector.NO_COMPRESSION_RATIO)
                compressionRatio = 1.0;

            long lastBytesScanned = 0;

            while (ci.hasNext())
            {
                try (UnfilteredRowIterator partition = ci.next())
                {
                    Token token = partition.partitionKey().getToken();
                    // if this row is contained in the full or transient ranges, append it to the appropriate sstable
                    if (fullChecker.test(token))
                    {
                        fullWriter.append(partition);
                        ci.setTargetDirectory(fullWriter.currentWriter().getFilename());
                    }
                    else if (transChecker.test(token))
                    {
                        transWriter.append(partition);
                        ci.setTargetDirectory(transWriter.currentWriter().getFilename());
                    }
                    else
                    {
                        // otherwise, append it to the unrepaired sstable
                        unrepairedWriter.append(partition);
                        ci.setTargetDirectory(unrepairedWriter.currentWriter().getFilename());
                    }
                    long bytesScanned = scanners.getTotalBytesScanned();
                    compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned, compressionRatio);
                    lastBytesScanned = bytesScanned;
                }
            }

            fullWriter.prepareToCommit();
            transWriter.prepareToCommit();
            unrepairedWriter.prepareToCommit();
            txn.checkpoint();
            txn.obsoleteOriginals();
            txn.prepareToCommit();

            List<SSTableReader> fullSSTables = new ArrayList<>(fullWriter.finished());
            List<SSTableReader> transSSTables = new ArrayList<>(transWriter.finished());
            List<SSTableReader> unrepairedSSTables = new ArrayList<>(unrepairedWriter.finished());

            fullWriter.commit();
            transWriter.commit();
            unrepairedWriter.commit();
            txn.commit();
            logger.info("Anticompacted {} in {}.{} to full = {}, transient = {}, unrepaired = {} for {}",
                        sstableAsSet,
                        cfs.getKeyspaceName(),
                        cfs.getTableName(),
                        fullSSTables,
                        transSSTables,
                        unrepairedSSTables,
                        pendingRepair);
            return fullSSTables.size() + transSSTables.size() + unrepairedSSTables.size();
        }
        catch (Throwable e)
        {
            if (e instanceof CompactionInterruptedException)
            {
                if (isCancelled.getAsBoolean())
                {
                    logger.info("Anticompaction has been canceled for session {}", pendingRepair);
                    logger.trace(e.getMessage(), e);
                }
                else
                {
                    logger.info("Anticompaction for session {} has been stopped by request.", pendingRepair);
                }
            }
            else
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.error("Error anticompacting " + txn + " for " + pendingRepair, e);
            }
            throw e;
        }
    }

    @VisibleForTesting
    public static CompactionIterator getAntiCompactionIterator(List<ISSTableScanner> scanners, CompactionController controller, long nowInSec, TimeUUID timeUUID, ActiveCompactionsTracker activeCompactions, BooleanSupplier isCancelled)
    {
        return new CompactionIterator(OperationType.ANTICOMPACTION, scanners, controller, nowInSec, timeUUID, activeCompactions, null)
        {
            public boolean isStopRequested()
            {
                return super.isStopRequested() || isCancelled.getAsBoolean();
            }
        };
    }

    @VisibleForTesting
    Future<?> submitIndexBuild(final SecondaryIndexBuilder builder, ActiveCompactionsTracker activeCompactions)
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                activeCompactions.beginCompaction(builder);
                try
                {
                    builder.build();
                }
                finally
                {
                    activeCompactions.finishCompaction(builder);
                }
            }
        };

        return secondaryIndexExecutor.submitIfRunning(runnable, "index build");
    }

    /**
     * Is not scheduled, because it is performing disjoint work from sstable compaction.
     */
    public Future<?> submitIndexBuild(final SecondaryIndexBuilder builder)
    {
        return submitIndexBuild(builder, active);
    }

    public Future<?> submitCacheWrite(final AutoSavingCache.Writer writer)
    {
        return submitCacheWrite(writer, active);
    }

    Future<?> submitCacheWrite(final AutoSavingCache.Writer writer, ActiveCompactionsTracker activeCompactions)
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                if (!AutoSavingCache.flushInProgress.add(writer.cacheType()))
                {
                    logger.trace("Cache flushing was already in progress: skipping {}", writer.getCompactionInfo());
                    return;
                }
                try
                {
                    activeCompactions.beginCompaction(writer);
                    try
                    {
                        writer.saveCache();
                    }
                    finally
                    {
                        activeCompactions.finishCompaction(writer);
                    }
                }
                finally
                {
                    AutoSavingCache.flushInProgress.remove(writer.cacheType());
                }
            }
        };

        return executor.submitIfRunning(runnable, "cache write");
    }

    public <T, E extends Throwable> T runAsActiveCompaction(Holder activeCompactionInfo, ThrowingSupplier<T, E> callable) throws E
    {
        active.beginCompaction(activeCompactionInfo);
        try
        {
            return callable.get();
        }
        finally
        {
            active.finishCompaction(activeCompactionInfo);
        }
    }

    public static long getDefaultGcBefore(ColumnFamilyStore cfs, long nowInSec)
    {
        // 2ndary indexes have ExpiringColumns too, so we need to purge tombstones deleted before now. We do not need to
        // add any GcGrace however since 2ndary indexes are local to a node.
        return cfs.isIndex() ? nowInSec : cfs.gcBefore(nowInSec);
    }

    public Future<Long> submitViewBuilder(final ViewBuilderTask task)
    {
        return submitViewBuilder(task, active);
    }

    @VisibleForTesting
    Future<Long> submitViewBuilder(final ViewBuilderTask task, ActiveCompactionsTracker activeCompactions)
    {
        return viewBuildExecutor.submitIfRunning(() -> {
            activeCompactions.beginCompaction(task);
            try
            {
                return task.call();
            }
            finally
            {
                activeCompactions.finishCompaction(task);
            }
        }, "view build");
    }

    public int getActiveCompactions()
    {
        return active.getCompactions().size();
    }

    public static boolean isCompactor(Thread thread)
    {
        return thread.getThreadGroup().getParent() == compactionThreadGroup;
    }

    // TODO: this is a bit ugly, but no uglier than it was
    static class CompactionExecutor extends WrappedExecutorPlus
    {
        static final ThreadGroup compactionThreadGroup = executorFactory().newThreadGroup("compaction");

        public CompactionExecutor()
        {
            this(executorFactory(), getConcurrentCompactors(), "CompactionExecutor", Integer.MAX_VALUE);
        }

        public CompactionExecutor(int threads, String name, int queueSize)
        {
            this(executorFactory(), threads, name, queueSize);
        }

        protected CompactionExecutor(ExecutorFactory executorFactory, int threads, String name, int queueSize)
        {
            super(executorFactory
                    .withJmxInternal()
                    .configurePooled(name, threads)
                    .withThreadGroup(compactionThreadGroup)
                    .withQueueLimit(queueSize).build());
        }

        public Future<Void> submitIfRunning(Runnable task, String name)
        {
            return submitIfRunning(callable(name, task), name);
        }

        /**
         * Submit the task but only if the executor has not been shutdown.If the executor has
         * been shutdown, or in case of a rejected execution exception return a cancelled future.
         *
         * @param task - the task to submit
         * @param name - the task name to use in log messages
         *
         * @return the future that will deliver the task result, or a future that has already been
         *         cancelled if the task could not be submitted.
         */
        public <T> Future<T> submitIfRunning(Callable<T> task, String name)
        {
            try
            {
                return submit(task);
            }
            catch (RejectedExecutionException ex)
            {
                if (isShutdown())
                    logger.info("Executor has shut down, could not submit {}", name);
                else
                    logger.error("Failed to submit {}", name, ex);

                return ImmediateFuture.cancelled();
            }
        }

        public void execute(Runnable command)
        {
            executor.execute(command);
        }

        public <T> Future<T> submit(Callable<T> task)
        {
            return executor.submit(task);
        }

        public <T> Future<T> submit(Runnable task, T result)
        {
            return submit(callable(task, result));
        }

        public Future<?> submit(Runnable task)
        {
            return submit(task, null);
        }
    }

    // TODO: pull out relevant parts of CompactionExecutor and move to ValidationManager
    public static class ValidationExecutor extends CompactionExecutor
    {
        // CompactionExecutor, and by extension ValidationExecutor, use ExecutorPlus's
        // default RejectedExecutionHandler which blocks the submitting thread when the work queue is
        // full. The calling thread in this case is AntiEntropyStage, so in most cases we don't actually
        // want to block when the ValidationExecutor is saturated as this prevents progress on all
        // repair tasks and may cause repair sessions to time out. Also, it can lead to references to
        // heavyweight validation responses containing merkle trees being held for extended periods which
        // increases GC pressure. Using LinkedBlockingQueue instead of the default SynchronousQueue allows
        // tasks to be submitted without blocking the caller, but will always prefer queueing to creating
        // new threads if the pool already has at least `corePoolSize` threads already running. For this
        // reason we set corePoolSize to the maximum desired concurrency, but allow idle core threads to
        // be terminated.

        public ValidationExecutor()
        {
            super(DatabaseDescriptor.getConcurrentValidations(),
                  "ValidationExecutor",
                  Integer.MAX_VALUE);
        }

        public void adjustPoolSize()
        {
            setMaximumPoolSize(DatabaseDescriptor.getConcurrentValidations());
            setCorePoolSize(DatabaseDescriptor.getConcurrentValidations());
        }
    }

    private static class ViewBuildExecutor extends CompactionExecutor
    {
        public ViewBuildExecutor()
        {
            super(DatabaseDescriptor.getConcurrentViewBuilders(), "ViewBuildExecutor", Integer.MAX_VALUE);
        }
    }

    private static class CacheCleanupExecutor extends CompactionExecutor
    {
        public CacheCleanupExecutor()
        {
            super(1, "CacheCleanupExecutor", Integer.MAX_VALUE);
        }
    }

    public void incrementAborted()
    {
        metrics.compactionsAborted.inc();
    }

    public void incrementCompactionsReduced()
    {
        metrics.compactionsReduced.inc();
    }

    public void incrementSstablesDropppedFromCompactions(long num)
    {
        metrics.sstablesDropppedFromCompactions.inc(num);
    }

    private static class SecondaryIndexExecutor extends CompactionExecutor
    {
        public SecondaryIndexExecutor()
        {
            super(DatabaseDescriptor.getConcurrentIndexBuilders(), "SecondaryIndexExecutor", Integer.MAX_VALUE);
        }
    }

    public List<Map<String, String>> getCompactions()
    {
        List<Holder> compactionHolders = active.getCompactions();
        List<Map<String, String>> out = new ArrayList<Map<String, String>>(compactionHolders.size());
        for (CompactionInfo.Holder ci : compactionHolders)
            out.add(ci.getCompactionInfo().asMap());
        return out;
    }

    public List<String> getCompactionSummary()
    {
        List<Holder> compactionHolders = active.getCompactions();
        List<String> out = new ArrayList<String>(compactionHolders.size());
        for (CompactionInfo.Holder ci : compactionHolders)
            out.add(ci.getCompactionInfo().toString());
        return out;
    }

    public TabularData getCompactionHistory()
    {
        try
        {
            return SystemKeyspace.getCompactionHistory();
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public long getTotalBytesCompacted()
    {
        return metrics.bytesCompacted.getCount();
    }

    public long getTotalCompactionsCompleted()
    {
        return metrics.totalCompactionsCompleted.getCount();
    }

    @Override
    public int getPendingTasks()
    {
        return metrics.pendingTasks.getValue();
    }

    public long getCompletedTasks()
    {
        return metrics.completedTasks.getValue();
    }

    public void stopCompaction(String type)
    {
        OperationType operation = OperationType.valueOf(type);
        for (Holder holder : active.getCompactions())
        {
            if (holder.getCompactionInfo().getTaskType() == operation)
                holder.stop();
        }
    }

    public void stopCompactionById(String compactionId)
    {
        for (Holder holder : active.getCompactions())
        {
            TimeUUID holderId = holder.getCompactionInfo().getTaskId();
            if (holderId != null && holderId.equals(TimeUUID.fromString(compactionId)))
                holder.stop();
        }
    }

    public void setConcurrentCompactors(int value)
    {
        if (value > executor.getCorePoolSize())
        {
            // we are increasing the value
            executor.setMaximumPoolSize(value);
            executor.setCorePoolSize(value);
        }
        else if (value < executor.getCorePoolSize())
        {
            // we are reducing the value
            executor.setCorePoolSize(value);
            executor.setMaximumPoolSize(value);
        }
    }

    public void setConcurrentValidations()
    {
        validationExecutor.adjustPoolSize();
    }

    public void setConcurrentViewBuilders(int value)
    {
        if (value > viewBuildExecutor.getCorePoolSize())
        {
            // we are increasing the value
            viewBuildExecutor.setMaximumPoolSize(value);
            viewBuildExecutor.setCorePoolSize(value);
        }
        else if (value < viewBuildExecutor.getCorePoolSize())
        {
            // we are reducing the value
            viewBuildExecutor.setCorePoolSize(value);
            viewBuildExecutor.setMaximumPoolSize(value);
        }
    }

    public int getCoreCompactorThreads()
    {
        return executor.getCorePoolSize();
    }

    public void setCoreCompactorThreads(int number)
    {
        executor.setCorePoolSize(number);
    }

    public int getMaximumCompactorThreads()
    {
        return executor.getMaximumPoolSize();
    }

    public void setMaximumCompactorThreads(int number)
    {
        executor.setMaximumPoolSize(number);
    }

    public int getCoreValidationThreads()
    {
        return validationExecutor.getCorePoolSize();
    }

    public void setCoreValidationThreads(int number)
    {
        validationExecutor.setCorePoolSize(number);
    }

    public int getMaximumValidatorThreads()
    {
        return validationExecutor.getMaximumPoolSize();
    }

    public void setMaximumValidatorThreads(int number)
    {
        validationExecutor.setMaximumPoolSize(number);
    }

    public boolean getDisableSTCSInL0()
    {
        return DatabaseDescriptor.getDisableSTCSInL0();
    }

    public void setDisableSTCSInL0(boolean disabled)
    {
        if (disabled != DatabaseDescriptor.getDisableSTCSInL0())
            logger.info("Changing STCS in L0 disabled from {} to {}", DatabaseDescriptor.getDisableSTCSInL0(), disabled);
        DatabaseDescriptor.setDisableSTCSInL0(disabled);
    }

    public int getCoreViewBuildThreads()
    {
        return viewBuildExecutor.getCorePoolSize();
    }

    public void setCoreViewBuildThreads(int number)
    {
        viewBuildExecutor.setCorePoolSize(number);
    }

    public int getMaximumViewBuildThreads()
    {
        return viewBuildExecutor.getMaximumPoolSize();
    }

    public void setMaximumViewBuildThreads(int number)
    {
        viewBuildExecutor.setMaximumPoolSize(number);
    }

    public boolean getAutomaticSSTableUpgradeEnabled()
    {
        return DatabaseDescriptor.automaticSSTableUpgrade();
    }

    public void setAutomaticSSTableUpgradeEnabled(boolean enabled)
    {
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(enabled);
    }

    public int getMaxConcurrentAutoUpgradeTasks()
    {
        return DatabaseDescriptor.maxConcurrentAutoUpgradeTasks();
    }

    public void setMaxConcurrentAutoUpgradeTasks(int value)
    {
        try
        {
            DatabaseDescriptor.setMaxConcurrentAutoUpgradeTasks(value);
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    public List<Holder> getCompactionsMatching(Iterable<TableMetadata> columnFamilies, Predicate<CompactionInfo> predicate)
    {
        Preconditions.checkArgument(columnFamilies != null, "Attempted to getCompactionsMatching in CompactionManager with no columnFamilies specified.");

        List<Holder> matched = new ArrayList<>();
        // consider all in-progress compactions
        for (Holder holder : active.getCompactions())
        {
            CompactionInfo info = holder.getCompactionInfo();
            if (info.getTableMetadata() == null || Iterables.contains(columnFamilies, info.getTableMetadata()))
            {
                if (predicate.test(info))
                    matched.add(holder);
            }
        }
        return matched;
    }

    /**
     * Try to stop all of the compactions for given ColumnFamilies.
     *
     * Note that this method does not wait for all compactions to finish; you'll need to loop against
     * isCompacting if you want that behavior.
     *
     * @param columnFamilies The ColumnFamilies to try to stop compaction upon.
     * @param sstablePredicate the sstable predicate to match on
     * @param interruptValidation true if validation operations for repair should also be interrupted
     */
    public void interruptCompactionFor(Iterable<TableMetadata> columnFamilies, Predicate<SSTableReader> sstablePredicate, boolean interruptValidation)
    {
        assert columnFamilies != null;

        // interrupt in-progress compactions
        for (Holder compactionHolder : active.getCompactions())
        {
            CompactionInfo info = compactionHolder.getCompactionInfo();
            if ((info.getTaskType() == OperationType.VALIDATION) && !interruptValidation)
                continue;

            if (info.getTableMetadata() == null || Iterables.contains(columnFamilies, info.getTableMetadata()))
            {
                if (info.shouldStop(sstablePredicate))
                    compactionHolder.stop();
            }
        }
    }

    public void interruptCompactionForCFs(Iterable<ColumnFamilyStore> cfss, Predicate<SSTableReader> sstablePredicate, boolean interruptValidation)
    {
        List<TableMetadata> metadata = new ArrayList<>();
        for (ColumnFamilyStore cfs : cfss)
            metadata.add(cfs.metadata());

        interruptCompactionFor(metadata, sstablePredicate, interruptValidation);
    }

    public void waitForCessation(Iterable<ColumnFamilyStore> cfss, Predicate<SSTableReader> sstablePredicate)
    {
        long start = nanoTime();
        long delay = TimeUnit.MINUTES.toNanos(1);

        while (nanoTime() - start < delay)
        {
            if (CompactionManager.instance.isCompacting(cfss, sstablePredicate))
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
            else
                break;
        }
    }


    public List<CompactionInfo> getSSTableTasks()
    {
        return active.getCompactions()
                     .stream()
                     .map(CompactionInfo.Holder::getCompactionInfo)
                     .filter(task -> task.getTaskType() != OperationType.COUNTER_CACHE_SAVE
                                     && task.getTaskType() != OperationType.KEY_CACHE_SAVE
                                     && task.getTaskType() != OperationType.ROW_CACHE_SAVE)
                     .collect(Collectors.toList());
    }

    /**
     * Return whether "global" compactions should be paused, used by ColumnFamilyStore#runWithCompactionsDisabled
     *
     * a global compaction is one that includes several/all tables, currently only IndexSummaryBuilder
     */
    public boolean isGlobalCompactionPaused()
    {
        return globalCompactionPauseCount.get() > 0;
    }

    public CompactionPauser pauseGlobalCompaction()
    {
        CompactionPauser pauser = globalCompactionPauseCount::decrementAndGet;
        globalCompactionPauseCount.incrementAndGet();
        return pauser;
    }

    public interface CompactionPauser extends AutoCloseable
    {
        public void close();
    }
}
