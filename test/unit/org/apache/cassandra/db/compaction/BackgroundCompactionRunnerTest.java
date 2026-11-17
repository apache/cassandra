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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.WrappedExecutorPlus;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.exceptions.BloomFilterTooLargeException;
import org.apache.cassandra.io.FSDiskFullWriteError;
import org.apache.cassandra.io.FSNoDiskAvailableForWriteError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.CorruptFileException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.JVMKiller;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;
import org.apache.cassandra.utils.concurrent.Promise;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.BackgroundCompactionRunner.RequestResult;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.assertj.core.util.Lists;
import org.assertj.core.util.Preconditions;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.openjdk.jmh.util.FileUtils;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackgroundCompactionRunnerTest
{
    private final WrappedExecutorPlus compactionExecutor = Mockito.mock(WrappedExecutorPlus.class);
    private final ScheduledExecutorPlus checkExecutor = Mockito.mock(ScheduledExecutorPlus.class);
    private final ActiveOperations activeOperations = Mockito.mock(ActiveOperations.class);
    private final ColumnFamilyStore cfs = Mockito.mock(ColumnFamilyStore.class);
    private final CompactionStrategy compactionStrategy = Mockito.mock(CompactionStrategy.class);

    private BackgroundCompactionRunner runner;
    public int pendingTaskCount;
    private List<AbstractCompactionTask> compactionTasks;
    private ArgumentCaptor<Runnable> capturedCompactionRunnables, capturedCheckRunnables;

    private static boolean savedAutomaticSSTableUpgrade;
    private static int savedMaxConcurrentAuotUpgradeTasks;

    @BeforeClass
    public static void initClass()
    {
        DatabaseDescriptor.daemonInitialization();
        savedAutomaticSSTableUpgrade = DatabaseDescriptor.automaticSSTableUpgrade();
        savedMaxConcurrentAuotUpgradeTasks = DatabaseDescriptor.maxConcurrentAutoUpgradeTasks();
    }

    @AfterClass
    public static void tearDownClass()
    {
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(savedAutomaticSSTableUpgrade);
        DatabaseDescriptor.setMaxConcurrentAutoUpgradeTasks(savedMaxConcurrentAuotUpgradeTasks);
    }

    @Before
    public void initTest()
    {
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(true);
        DatabaseDescriptor.setMaxConcurrentAutoUpgradeTasks(2);

        pendingTaskCount = 0;
        runner = new BackgroundCompactionRunner(compactionExecutor, checkExecutor, activeOperations);
        compactionTasks = new ArrayList<>();
        capturedCompactionRunnables = ArgumentCaptor.forClass(Runnable.class);
        capturedCheckRunnables = ArgumentCaptor.forClass(Runnable.class);

        assertThat(runner.getOngoingCompactionsCount()).isZero();
        assertThat(runner.getOngoingUpgradesCount()).isZero();

        when(compactionExecutor.getMaximumPoolSize()).thenReturn(2);
        when(cfs.isAutoCompactionDisabled()).thenReturn(false);
        when(cfs.isCompactionActive()).thenReturn(true);
        when(cfs.isValid()).thenReturn(true);
        when(checkExecutor.getPendingTaskCount()).thenAnswer(i-> pendingTaskCount);
        when(cfs.getCompactionStrategy()).thenReturn(compactionStrategy);
        when(compactionStrategy.getNextBackgroundTasks(ArgumentMatchers.anyLong())).thenReturn(compactionTasks);
        doNothing().when(checkExecutor).execute(capturedCheckRunnables.capture());
        doNothing().when(compactionExecutor).execute(capturedCompactionRunnables.capture());
    }

    @After
    public void tearDownTest()
    {
        reset(compactionExecutor, checkExecutor, activeOperations, cfs, compactionStrategy);
    }


    // when cfs is invalid we should immediately return ABORTED
    @Test
    public void invalidCFS() throws Exception
    {
        when(cfs.isAutoCompactionDisabled()).thenReturn(false);
        when(cfs.isValid()).thenReturn(false);

        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.ABORTED);
        assertThat(runner.getMarkedCFSs()).isEmpty();
        verify(checkExecutor, never()).execute(notNull());
    }


    // when automatic compactions are disabled for cfs, we should immediately return ABORTED
    @Test
    public void automaticCompactionsDisabled() throws Exception
    {
        when(cfs.isAutoCompactionDisabled()).thenReturn(true);
        when(cfs.isValid()).thenReturn(true);

        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.ABORTED);
        assertThat(runner.getMarkedCFSs()).isEmpty();
        verify(checkExecutor, never()).execute(notNull());
    }


    // we should mark cfs for compaction and schedule a check
    @Test
    public void markCFSForCompactionAndScheduleCheck() throws Exception
    {
        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result).isNotDone();

        verify(checkExecutor).execute(notNull());
        assertThat(runner.getMarkedCFSs()).contains(cfs);
    }


    // when cfs is invalid we should immediately return ABORTED
    @Test
    public void invalidCFSs() throws Exception
    {
        when(cfs.isAutoCompactionDisabled()).thenReturn(false);
        when(cfs.isValid()).thenReturn(false);

        runner.markForCompactionCheck(ImmutableSet.of(cfs));

        assertThat(runner.getMarkedCFSs()).isEmpty();
        verify(checkExecutor, never()).execute(notNull());
    }


    // when automatic compactions are disabled for cfs, we should immediately return ABORTED
    @Test
    public void automaticCompactionsDisabledForCFSs() throws Exception
    {
        when(cfs.isAutoCompactionDisabled()).thenReturn(true);
        when(cfs.isValid()).thenReturn(true);

        runner.markForCompactionCheck(ImmutableSet.of(cfs));

        assertThat(runner.getMarkedCFSs()).isEmpty();
        verify(checkExecutor, never()).execute(notNull());
    }


    // we should mark cfs for compaction and schedule a check
    @Test
    public void markCFSsForCompactionAndScheduleCheck() throws Exception
    {
        runner.markForCompactionCheck(ImmutableSet.of(cfs));

        verify(checkExecutor).execute(notNull());
        assertThat(runner.getMarkedCFSs()).contains(cfs);
    }


    // we should mark cfs for compaction but not schedule new check if there is one already scheduled
    @Test
    public void markCFSForCompactionAndNotScheduleCheck() throws Exception
    {
        pendingTaskCount = 100;
        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result).isNotDone();

        verify(checkExecutor, never()).execute(notNull());
        assertThat(runner.getMarkedCFSs()).contains(cfs);
    }


    // we should immeditatlly return ABORTED if the executor is shutdown
    @Test
    public void immediatelyReturnIfExecutorIsDown() throws Exception
    {
        when(checkExecutor.isShutdown()).thenReturn(true);
        doThrow(new RejectedExecutionException("rejected")).when(checkExecutor).execute(ArgumentMatchers.notNull());

        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.ABORTED);

        verify(checkExecutor).execute(notNull());
    }


    // shutdown should shut down the check executor and should not shut down the compaction executor
    @Test
    public void shutdown() throws Exception
    {
        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result.isDone()).isFalse();

        runner.shutdown();

        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.ABORTED);

        verify(checkExecutor).shutdown();
        verify(compactionExecutor, never()).shutdown();
    }


    // a check should make a task finish with NOT_NEEDED if there are no compaction tasks and upgrades are disabled
    @Test
    public void finishWithNotNeededWhenNoCompactionTasksAndUpgradesDisabled() throws Exception
    {
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(false);

        when(cfs.getCandidatesForUpgrade()).thenReturn(ImmutableList.of(mock(SSTableReader.class)));

        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);
        verifyCFSWasMarkedForCompaction();
        capturedCheckRunnables.getValue().run();

        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.NOT_NEEDED);
        verify(checkExecutor, never()).execute(notNull());
        assertThat(runner.getMarkedCFSs()).isEmpty();
    }

    // a check should make a task finish with NOT_NEEDED if there are no compaction tasks and no upgrade tasks
    @Test
    public void finishWithNotNeededWhenNoCompactionTasksAndNoUpgradeTasks() throws Exception
    {
        when(cfs.getCandidatesForUpgrade()).thenReturn(Lists.emptyList());

        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);
        verifyCFSWasMarkedForCompaction();
        capturedCheckRunnables.getValue().run();

        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.NOT_NEEDED);
        verify(checkExecutor, never()).execute(notNull());
        assertThat(runner.getMarkedCFSs()).isEmpty();
    }


    // a check should start a compaction task if there is some
    @Test
    public void startCompactionTask() throws Exception
    {
        // although it is possible to run upgrade tasks, we make sure that compaction tasks are selected
        Promise<RequestResult> result = markCFSAndRunCheck();

        // check the task was scheduled on compaction executor
        verifyTaskScheduled(compactionExecutor);
        verifyState(1, 0);
        assertThat(result).isNotDone();

        // ... we immediatelly marked that CFS for compaction again
        verifyCFSWasMarkedForCompaction();

        // now we will execute the task
        capturedCompactionRunnables.getValue().run();

        // so we expect that:
        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.COMPLETED);
        verifyState(0, 0);

        // another check should be schedued upon task completion
        verifyCFSWasMarkedForCompaction();

        // make sure we haven't even attempted to check for upgrade tasks (because there were compaction tasks to run)
        verify(cfs, Mockito.never()).getCandidatesForUpgrade();
    }


    // a check should start an upgrade task if there is some and there is no compaction task
    @Test
    public void startUpgradeTask() throws Exception
    {
        AbstractCompactionTask compactionTask = mock(AbstractCompactionTask.class);
        // although it is possible to run upgrade tasks, we make sure that compaction tasks are selected
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(true);
        SSTableReader sstable = mock(SSTableReader.class);
        Tracker tracker = mock(Tracker.class);
        LifecycleTransaction txn = mock(LifecycleTransaction.class);
        when(cfs.getCandidatesForUpgrade()).thenReturn(Collections.singletonList(sstable));
        when(cfs.getTracker()).thenReturn(tracker);
        when(tracker.tryModify(sstable, OperationType.UPGRADE_SSTABLES)).thenReturn(txn);
        when(compactionStrategy.createCompactionTask(same(txn), anyLong(), anyLong())).thenReturn(compactionTask);

        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);
        verifyCFSWasMarkedForCompaction();

        capturedCheckRunnables.getValue().run();

        // make sure we did check for the upgrade candidates
        verify(cfs).getCandidatesForUpgrade();

        // check the task was scheduled on compaction executor
        verifyTaskScheduled(compactionExecutor);
        verifyState(1, 1);
        assertThat(result).isNotDone();

        // ... we immediatelly marked that CFS for compaction again
        verifyCFSWasMarkedForCompaction();

        // now we will execute the task
        capturedCompactionRunnables.getValue().run();

        // so we expect that:
        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.COMPLETED);
        verifyState(0, 0);

        // another check should be schedued upon task completion
        verifyCFSWasMarkedForCompaction();
    }


    // we should run multiple compactions for a CFS in parallel if possible
    @Test
    public void startMultipleCompactionTasksInParallel() throws Exception
    {
        // first task
        Promise<RequestResult> result1 = markCFSAndRunCheck();
        verifyTaskScheduled(compactionExecutor);
        verifyState(1, 0);
        verifyCFSWasMarkedForCompaction();

        // second task
        Promise<RequestResult> result2 = markCFSAndRunCheck();
        verifyTaskScheduled(compactionExecutor);
        verifyState(2, 0);
        verifyCFSWasMarkedForCompaction();

        assertThat(result2).isNotSameAs(result1);

        // now we will execute the first task
        assertThat(result1).isNotDone();
        capturedCompactionRunnables.getAllValues().get(0).run();
        assertThat(result1).isDone();
        assertThat(result1.get()).isEqualTo(RequestResult.COMPLETED);

        // so we expect that:
        verifyState(1, 0);

        // execute the second task
        assertThat(result2).isNotDone();
        capturedCompactionRunnables.getAllValues().get(1).run();
        assertThat(result2).isDone();
        assertThat(result2.get()).isEqualTo(RequestResult.COMPLETED);

        // so we expect that:
        verifyState(0, 0);

        verifyState(0, 0);
    }


    // postpone execution if the thread pool is busy
    @Test
    public void postponeCompactionTasksIfPoolIsBusy() throws Exception
    {
        // first task
        Promise<RequestResult> result1 = markCFSAndRunCheck();
        verifyTaskScheduled(compactionExecutor);
        verifyState(1, 0);
        verifyCFSWasMarkedForCompaction();

        // second task
        Promise<RequestResult> result2 = markCFSAndRunCheck();
        verifyTaskScheduled(compactionExecutor);
        verifyState(2, 0);
        verifyCFSWasMarkedForCompaction();

        // third task, but now the task should not be scheduled for execution because of the pool size (2)
        clearInvocations(compactionStrategy);
        Promise<RequestResult> result3 = markCFSAndRunCheck();
        verifyState(2, 0);
        // we should not execute a new task, actually not even attempt to get a new compaction task
        verify(compactionStrategy, never()).getNextBackgroundTasks(anyInt());
        verify(compactionExecutor, never()).execute(notNull());
        // we should also not schedule a new check or remove mark for the CFS
        verify(checkExecutor, never()).execute(notNull());
        assertThat(runner.getMarkedCFSs()).contains(cfs);

        assertThat(result3).isNotSameAs(result1);
        assertThat(result3).isNotSameAs(result2);

        // now we will execute the task 1
        assertThat(result1).isNotDone();
        capturedCompactionRunnables.getAllValues().get(0).run();
        // so we expect that:
        assertThat(result1).isDone();
        assertThat(result1.get()).isEqualTo(RequestResult.COMPLETED);
        verifyState(1, 0);

        // execute the check, so that the thrid task is submitted
        clearInvocations(checkExecutor);
        capturedCheckRunnables.getValue().run();
        verifyTaskScheduled(compactionExecutor);
        verifyState(2, 0);
        verifyCFSWasMarkedForCompaction();

        // execute the rest of the tasks
        assertThat(result2).isNotDone();
        capturedCompactionRunnables.getAllValues().get(1).run();
        assertThat(result2).isDone();
        assertThat(result2.get()).isEqualTo(RequestResult.COMPLETED);

        assertThat(result3).isNotDone();
        capturedCompactionRunnables.getAllValues().get(2).run();
        assertThat(result3).isDone();
        assertThat(result3.get()).isEqualTo(RequestResult.COMPLETED);

        verifyState(0, 0);
    }


    // returned future should not support complete or cancel
    @Test
    public void futureRequestResultNotSupportForTermination()
    {
        Promise<RequestResult> result = markCFSAndRunCheck();

        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> result.setSuccess(RequestResult.COMPLETED));
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> result.setFailure(new RuntimeException()));
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> result.cancel(false));

        runner.shutdown();
    }


    // handling submission failure
    @Test
    public void handleTaskSubmissionFailure() throws Exception
    {
        doThrow(new RejectedExecutionException()).when(compactionExecutor).execute(notNull());

        Promise<RequestResult> result = markCFSAndRunCheck();
        clearInvocations(checkExecutor);

        // so we expect that:
        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.COMPLETED);
        verifyState(0, 0);

        verify(checkExecutor, never()).execute(notNull());
    }


    // handling task failure
    @Test
    public void handleTaskFailure() throws Exception
    {
        Promise<RequestResult> result = markCFSAndRunCheck();
        clearInvocations(checkExecutor);

        doThrow(new IOError(new RuntimeException())).when(compactionTasks.get(0)).execute(activeOperations);
        capturedCompactionRunnables.getValue().run();

        // so we expect that:
        assertThatThrownBy(() -> result.get()).isInstanceOf(ExecutionException.class);
        verifyState(0, 0);

        // another check should be schedued upon task completion
        verifyCFSWasMarkedForCompaction();
    }

    @Test
    public void handleOOMError()
    {
        JVMKiller originalKiller = JVMStabilityInspector.replaceKiller(new KillerForTests());
        BiConsumer<Throwable,Boolean> originalGlobalErrorHandler = JVMStabilityInspector.getGlobalErrorHandler();
        try
        {
            AtomicReference<Throwable> globalErrorEncountered = new AtomicReference<>();
            JVMStabilityInspector.setGlobalErrorHandler((err,bool) -> {
                Preconditions.checkArgument(globalErrorEncountered.get() == null, "Expect only one exception");
                globalErrorEncountered.set(err);
            });

            long before = CompactionManager.instance.getMetrics().totalCompactionsFailed.getCount();

            OutOfMemoryError oomError = new OutOfMemoryError("oom");
            BackgroundCompactionRunner.handleCompactionError(oomError, cfs);

            assertThat(globalErrorEncountered.get()).isSameAs(oomError);
            assertThat(CompactionManager.instance.getMetrics().totalCompactionsFailed.getCount()).isEqualTo(before + 1);
        }
        finally
        {
            JVMStabilityInspector.replaceKiller(originalKiller);
            JVMStabilityInspector.setGlobalErrorHandler(originalGlobalErrorHandler);
        }
    }

    @Test
    public void handleCorruptionSSTableException()
    {
        handleCorruptionException(() -> new CorruptSSTableException(null, "corrupted"));
    }

    @Test
    public void handleCorruptionBlockException()
    {
        handleCorruptionException(() -> {
            try
            {
                CompressionMetadata.Chunk chunk = new CompressionMetadata.Chunk(1L, 1);
                File file = new File(FileUtils.tempFile("temp"));
                CorruptBlockException corruptBlockException = new CorruptBlockException(file, chunk);
                assertThat(corruptBlockException.getFile()).isEqualTo(file);
                return corruptBlockException;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void handleCorruptionFileException()
    {
        handleCorruptionException(() -> {
            try
            {
                File file = new File(FileUtils.tempFile("temp"));
                CorruptFileException corruptFileException = new CorruptFileException(null, file);
                assertThat(corruptFileException.getFile()).isEqualTo(file);
                return corruptFileException;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
    }


    private void handleCorruptionException(Supplier<Exception> provider)
    {
        JVMKiller originalKiller = JVMStabilityInspector.replaceKiller(new KillerForTests());
        Config.DiskFailurePolicy originalPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        Consumer<Throwable> originalDiskErrorHandler = JVMStabilityInspector.getDiskErrorHandler();
        try
        {
            AtomicReference<Throwable> diskErrorEncountered = new AtomicReference<>();
            JVMStabilityInspector.setDiskErrorHandler(err -> {
                Preconditions.checkArgument(diskErrorEncountered.get() == null, "Expect only one exception");
                diskErrorEncountered.set(err);
            });

            long before = CompactionManager.instance.getMetrics().totalCompactionsFailed.getCount();

            Exception exception = provider.get();
            BackgroundCompactionRunner.handleCompactionError(exception, cfs);

            assertThat(diskErrorEncountered.get()).isSameAs(exception);
            assertThat(CompactionManager.instance.getMetrics().totalCompactionsFailed.getCount()).isEqualTo(before + 1);
        }
        finally
        {
            DatabaseDescriptor.setDiskFailurePolicy(originalPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
            JVMStabilityInspector.setDiskErrorHandler(originalDiskErrorHandler);
        }
    }


    @Test
    public void handleFSWriteError()
    {
        JVMKiller originalKiller = JVMStabilityInspector.replaceKiller(new KillerForTests());
        Config.DiskFailurePolicy originalPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        Consumer<Throwable> originalDiskErrorHandler = JVMStabilityInspector.getDiskErrorHandler();
        try
        {
            AtomicReference<Throwable> diskErrorEncountered = new AtomicReference<>();
            JVMStabilityInspector.setDiskErrorHandler(err -> {
                Preconditions.checkArgument(diskErrorEncountered.get() == null, "Expect only one exception");
                diskErrorEncountered.set(err);
            });

            long before = CompactionManager.instance.getMetrics().totalCompactionsFailed.getCount();

            FSWriteError fsWriteError = new FSWriteError(null, "file");
            BackgroundCompactionRunner.handleCompactionError(fsWriteError, cfs);

            assertThat(diskErrorEncountered.get()).isSameAs(fsWriteError);
            assertThat(CompactionManager.instance.getMetrics().totalCompactionsFailed.getCount()).isEqualTo(before + 1);
        }
        finally
        {
            DatabaseDescriptor.setDiskFailurePolicy(originalPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
            JVMStabilityInspector.setDiskErrorHandler(originalDiskErrorHandler);
        }
    }

    @Test
    public void handleNoSpaceLeftException()
    {
        JVMKiller originalKiller = JVMStabilityInspector.replaceKiller(new KillerForTests());
        Config.DiskFailurePolicy originalPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        Consumer<Throwable> originalDiskErrorHandler = JVMStabilityInspector.getDiskErrorHandler();
        try
        {
            // it's wrapped in FSWriteError
            AtomicReference<Throwable> diskErrorEncountered = new AtomicReference<>();
            JVMStabilityInspector.setDiskErrorHandler(err -> diskErrorEncountered.set(err.getCause() != null ? err.getCause() : err));

            long before = CompactionManager.instance.getMetrics().totalCompactionsFailed.getCount();

            IOException noSpaceLeftException = new IOException("No space left on device");
            BackgroundCompactionRunner.handleCompactionError(noSpaceLeftException, cfs);

            assertThat(diskErrorEncountered.get()).isSameAs(noSpaceLeftException);
            assertThat(CompactionManager.instance.getMetrics().totalCompactionsFailed.getCount()).isEqualTo(before + 1);
        }
        finally
        {
            DatabaseDescriptor.setDiskFailurePolicy(originalPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
            JVMStabilityInspector.setDiskErrorHandler(originalDiskErrorHandler);
        }
    }

    @Test
    public void handDiskFullErrors()
    {
        JVMKiller originalKiller = JVMStabilityInspector.replaceKiller(new KillerForTests());
        BiConsumer<Throwable,Boolean> originalGlobalErrorHandler = JVMStabilityInspector.getGlobalErrorHandler();
        try
        {
            long before = CompactionManager.instance.getMetrics().totalCompactionsFailed.getCount();

            FSNoDiskAvailableForWriteError noDiskAvailableForWriteError = new FSNoDiskAvailableForWriteError("ks");
            assertThat(noDiskAvailableForWriteError.getKeyspace()).isEqualTo("ks");
            BackgroundCompactionRunner.handleCompactionError(noDiskAvailableForWriteError, cfs);
            assertThat(CompactionManager.instance.getMetrics().totalCompactionsFailed.getCount()).isEqualTo(before + 1);


            FSDiskFullWriteError diskFullWriteError = new FSDiskFullWriteError("ks2", 100);
            assertThat(diskFullWriteError.getKeyspace()).isEqualTo("ks2");
            BackgroundCompactionRunner.handleCompactionError(diskFullWriteError, cfs);
            assertThat(CompactionManager.instance.getMetrics().totalCompactionsFailed.getCount()).isEqualTo(before + 2);
        }
        finally
        {
            JVMStabilityInspector.replaceKiller(originalKiller);
            JVMStabilityInspector.setGlobalErrorHandler(originalGlobalErrorHandler);
        }
    }

    @Test
    public void handleBloomFilterTooLargeException()
    {
        Logger runnerLogger = (Logger) LoggerFactory.getLogger(BackgroundCompactionRunner.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        runnerLogger.addAppender(appender);
        try
        {
            // the WARN is rate limited per table, and the other tests in this class log it for the same mocked table
            when(cfs.getKeyspaceName()).thenReturn("ks");
            when(cfs.getTableName()).thenReturn("table_" + System.nanoTime());
            long before = CompactionManager.instance.getMetrics().totalCompactionsFailed.getCount();

            // wrapped, as it arrives from the CompletableFuture
            BackgroundCompactionRunner.handleCompactionError(new CompletionException(new BloomFilterTooLargeException(20L << 30)), cfs);

            assertThat(CompactionManager.instance.getMetrics().totalCompactionsFailed.getCount()).isEqualTo(before + 1);
            // a WARN without a stack trace, not the generic ERROR
            assertThat(appender.list).extracting(ILoggingEvent::getLevel, ILoggingEvent::getThrowableProxy)
                                     .containsExactly(tuple(Level.WARN, null));
        }
        finally
        {
            runnerLogger.detachAppender(appender);
        }
    }

    // A task that fails with BloomFilterTooLargeException must not be retried through an already-pending request.
    // run() re-requests the CFS after submitting the task. The next check finds the pool full and leaves that request
    // pending, so it must be consumed without retrying the CFS after the task fails.
    @Test
    public void noRetryAfterBloomFilterTooLargeWhenPoolIsSaturated() throws Exception
    {
        // a long-running task takes the first of the 2 slots
        markCFSAndRunCheck();
        verifyTaskScheduled(compactionExecutor);
        verifyCFSWasMarkedForCompaction();
        verifyState(1, 0);

        // the doomed task takes the last slot
        AbstractCompactionTask doomedTask = bloomFilterTooLargeTask();
        capturedCheckRunnables.getValue().run();
        verifyTaskScheduled(compactionExecutor);
        verifyCFSWasMarkedForCompaction();
        // 2 of 2 slots in use: the pool is full
        verifyState(2, 0);

        // the pool is full, so this check returns without consuming the request
        clearInvocations(compactionStrategy);
        capturedCheckRunnables.getValue().run();
        verify(compactionStrategy, never()).getNextBackgroundTasks(anyLong());
        assertThat(runner.getMarkedCFSs()).contains(cfs);

        // the doomed task fails and releases its sstables, which would let the strategy select it again
        clearInvocations(checkExecutor);
        capturedCompactionRunnables.getAllValues().get(1).run();

        // the runnable at index 1 is the doomed task; index 0 is the long-running one
        verify(doomedTask).execute(activeOperations);
        verifyState(1, 0);

        // a thread has freed up, so a check is requested; it will find the request left over from the check that
        // started the doomed task
        verifyTaskScheduled(checkExecutor);

        clearInvocations(compactionStrategy, compactionExecutor);
        capturedCheckRunnables.getValue().run();
        verify(compactionStrategy, never()).getNextBackgroundTasks(anyLong());
        verify(compactionExecutor, never()).execute(notNull());
        assertThat(runner.getMarkedCFSs()).isEmpty();
    }

    // Only an external request, such as a flush, retries a CFS whose task failed with BloomFilterTooLargeException.
    // Once that attempt succeeds, the CFS is checked as usual.
    @Test
    public void externalRequestRetriesAfterBloomFilterTooLarge() throws Exception
    {
        // phase 1: a BloomFilterTooLargeException suppresses the leftover request
        AbstractCompactionTask doomedTask = bloomFilterTooLargeTask();
        runner.markForCompactionCheck(cfs);
        verifyCFSWasMarkedForCompaction();
        capturedCheckRunnables.getValue().run();
        verifyTaskScheduled(compactionExecutor);
        verifyCFSWasMarkedForCompaction();
        verifyState(1, 0);

        capturedCompactionRunnables.getValue().run();
        verify(doomedTask).execute(activeOperations);
        verifyState(0, 0);
        // a thread has freed up, so a check is requested
        verifyTaskScheduled(checkExecutor);

        // the flagged table is skipped and its request consumed
        clearInvocations(compactionStrategy);
        capturedCheckRunnables.getValue().run();
        verify(compactionStrategy, never()).getNextBackgroundTasks(anyLong());
        assertThat(runner.getMarkedCFSs()).isEmpty();

        // phase 2: an external request clears the failure flag and permits a successful retry
        AbstractCompactionTask retriedTask = mock(AbstractCompactionTask.class);
        compactionTasks.clear();
        compactionTasks.add(retriedTask);
        clearInvocations(compactionStrategy);
        runner.markForCompactionCheck(cfs);
        verifyCFSWasMarkedForCompaction();
        capturedCheckRunnables.getValue().run();
        // exactly once: the invocations were cleared right before the external request
        verify(compactionStrategy).getNextBackgroundTasks(anyLong());
        verifyTaskScheduled(compactionExecutor);
        verifyCFSWasMarkedForCompaction();
        verifyState(1, 0);

        // getValue() is the latest captured runnable, i.e. the task started by this attempt
        capturedCompactionRunnables.getValue().run();
        verify(retriedTask).execute(activeOperations);
        verifyState(0, 0);
        // the completion of the task requests a check
        verifyCFSWasMarkedForCompaction();

        // successful completion allows normal background checking to continue
        clearInvocations(compactionStrategy);
        capturedCheckRunnables.getValue().run();
        verify(compactionStrategy).getNextBackgroundTasks(anyLong());
    }

    // An external request made while the failing task runs coalesces with the early re-request.
    // Its promise must complete rather than remain pending.
    @Test
    public void requestMadeWhileBloomFilterTooLargeTaskRunsIsCompleted() throws Exception
    {
        bloomFilterTooLargeTask();
        runner.markForCompactionCheck(cfs);
        verifyCFSWasMarkedForCompaction();
        capturedCheckRunnables.getValue().run();
        verifyTaskScheduled(compactionExecutor);
        verifyCFSWasMarkedForCompaction();

        // The request joins the pending check and must wait for it to complete.
        Promise<RequestResult> duringTask = runner.markForCompactionCheck(cfs);
        verifyTaskScheduled(checkExecutor);
        assertThat(duringTask).isNotDone();

        capturedCompactionRunnables.getValue().run();
        verifyState(0, 0);

        // The failed CFS is skipped, but the coalesced request must still be completed.
        clearInvocations(compactionStrategy);
        capturedCheckRunnables.getValue().run();
        assertThat(duringTask).isDone();
        assertThat(duringTask.get()).isEqualTo(RequestResult.NOT_NEEDED);
        verify(compactionStrategy, never()).getNextBackgroundTasks(anyLong());
        assertThat(runner.getMarkedCFSs()).isEmpty();
    }

    // The failure flag is per CFS. Another task of the same CFS that completes successfully clears it, so that the
    // follow-up compactions of the CFS do not wait for an external request.
    @Test
    public void successfulTaskOfSameCFSClearsBloomFilterTooLargeFlag() throws Exception
    {
        // a healthy task takes the first of the 2 slots
        markCFSAndRunCheck();
        verifyTaskScheduled(compactionExecutor);
        verifyCFSWasMarkedForCompaction();
        verifyState(1, 0);

        // the doomed task takes the last slot
        bloomFilterTooLargeTask();
        capturedCheckRunnables.getValue().run();
        verifyTaskScheduled(compactionExecutor);
        verifyCFSWasMarkedForCompaction();
        verifyState(2, 0);

        // the doomed task fails: the CFS is flagged, and the check it requests skips the CFS
        capturedCompactionRunnables.getAllValues().get(1).run();
        verifyState(1, 0);
        verifyTaskScheduled(checkExecutor);
        clearInvocations(compactionStrategy);
        capturedCheckRunnables.getValue().run();
        verify(compactionStrategy, never()).getNextBackgroundTasks(anyLong());
        assertThat(runner.getMarkedCFSs()).isEmpty();

        // the healthy task completes: it re-requests the CFS and clears the flag
        capturedCompactionRunnables.getAllValues().get(0).run();
        verifyState(0, 0);
        verifyCFSWasMarkedForCompaction();

        // so this check is not skipped
        capturedCheckRunnables.getValue().run();
        verify(compactionStrategy).getNextBackgroundTasks(anyLong());
    }

    // A task that fails in another way does not clear the flag set by a BloomFilterTooLargeException.
    @Test
    public void failedTaskOfSameCFSKeepsBloomFilterTooLargeFlag() throws Exception
    {
        // a task that fails with an unrelated error takes the first of the 2 slots
        AbstractCompactionTask failingTask = mock(AbstractCompactionTask.class);
        doThrow(new RuntimeException("unrelated failure")).when(failingTask).execute(activeOperations);
        compactionTasks.clear();
        compactionTasks.add(failingTask);
        runner.markForCompactionCheck(cfs);
        verifyCFSWasMarkedForCompaction();
        capturedCheckRunnables.getValue().run();
        verifyTaskScheduled(compactionExecutor);
        verifyCFSWasMarkedForCompaction();
        verifyState(1, 0);

        // the doomed task takes the last slot
        bloomFilterTooLargeTask();
        capturedCheckRunnables.getValue().run();
        verifyTaskScheduled(compactionExecutor);
        verifyCFSWasMarkedForCompaction();
        verifyState(2, 0);

        // the doomed task fails and flags the CFS
        capturedCompactionRunnables.getAllValues().get(1).run();
        verifyState(1, 0);
        verifyTaskScheduled(checkExecutor);

        // the other task fails too: it re-requests the CFS, but the flag stays
        capturedCompactionRunnables.getAllValues().get(0).run();
        verify(failingTask).execute(activeOperations);
        verifyState(0, 0);
        assertThat(runner.getMarkedCFSs()).contains(cfs);

        clearInvocations(compactionStrategy);
        capturedCheckRunnables.getValue().run();
        verify(compactionStrategy, never()).getNextBackgroundTasks(anyLong());
        assertThat(runner.getMarkedCFSs()).isEmpty();
    }

    /** Makes the strategy return a task that fails with {@link BloomFilterTooLargeException}. */
    private AbstractCompactionTask bloomFilterTooLargeTask()
    {
        AbstractCompactionTask task = mock(AbstractCompactionTask.class);
        doThrow(new BloomFilterTooLargeException(20L << 30)).when(task).execute(activeOperations);
        compactionTasks.clear();
        compactionTasks.add(task);
        return task;
    }

    private void verifyTaskScheduled(Executor executor)
    {
        verify(executor).execute(notNull());
        clearInvocations(executor);
    }

    private void verifyState(int ongoingCompactions, int ongoingUpgrades)
    {
        assertThat(runner.getOngoingCompactionsCount()).isEqualTo(ongoingCompactions);
        assertThat(runner.getOngoingUpgradesCount()).isEqualTo(ongoingUpgrades);
    }

    private void verifyCFSWasMarkedForCompaction()
    {
        verifyTaskScheduled(checkExecutor);
        assertThat(runner.getMarkedCFSs()).contains(cfs);
    }

    private Promise<RequestResult> markCFSAndRunCheck()
    {
        AbstractCompactionTask compactionTask = mock(AbstractCompactionTask.class);

        compactionTasks.clear();
        compactionTasks.add(compactionTask);

        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);
        verifyCFSWasMarkedForCompaction();

        capturedCheckRunnables.getValue().run();
        return result;
    }
}