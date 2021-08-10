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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.BackgroundCompactionRunner.RequestResult;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.assertj.core.util.Lists;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
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
    private final DebuggableThreadPoolExecutor compactionExecutor = Mockito.mock(DebuggableThreadPoolExecutor.class);
    private final DebuggableScheduledThreadPoolExecutor checkExecutor = Mockito.mock(DebuggableScheduledThreadPoolExecutor.class);
    private final ActiveOperations activeOperations = Mockito.mock(ActiveOperations.class);
    private final ColumnFamilyStore cfs = Mockito.mock(ColumnFamilyStore.class);
    private final CompactionStrategy compactionStrategy = Mockito.mock(CompactionStrategy.class);

    private BackgroundCompactionRunner runner;
    private BlockingQueue<Runnable> queue;
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

        queue = new ArrayBlockingQueue<>(100);
        runner = new BackgroundCompactionRunner(compactionExecutor, checkExecutor, activeOperations);
        compactionTasks = new ArrayList<>();
        capturedCompactionRunnables = ArgumentCaptor.forClass(Runnable.class);
        capturedCheckRunnables = ArgumentCaptor.forClass(Runnable.class);

        assertThat(runner.getOngoingCompactionsCount()).isZero();
        assertThat(runner.getOngoingUpgradesCount()).isZero();

        when(compactionExecutor.getMaximumPoolSize()).thenReturn(2);
        when(cfs.isAutoCompactionDisabled()).thenReturn(false);
        when(cfs.isValid()).thenReturn(true);
        when(checkExecutor.getQueue()).thenReturn(queue);
        when(cfs.getCompactionStrategy()).thenReturn(compactionStrategy);
        when(compactionStrategy.getNextBackgroundTasks(ArgumentMatchers.anyInt())).thenReturn(compactionTasks);
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

        CompletableFuture<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result).isCompletedWithValue(RequestResult.ABORTED);
        assertThat(runner.getMarkedCFSs()).isEmpty();
        verify(checkExecutor, never()).execute(notNull());
    }


    // when automatic compactions are disabled for cfs, we should immediately return ABORTED
    @Test
    public void automaticCompactionsDisabled() throws Exception
    {
        when(cfs.isAutoCompactionDisabled()).thenReturn(true);
        when(cfs.isValid()).thenReturn(true);

        CompletableFuture<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result).isCompletedWithValue(RequestResult.ABORTED);
        assertThat(runner.getMarkedCFSs()).isEmpty();
        verify(checkExecutor, never()).execute(notNull());
    }


    // we should mark cfs for compaction and schedule a check
    @Test
    public void markCFSForCompactionAndScheduleCheck() throws Exception
    {
        CompletableFuture<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result).isNotCompleted();

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
        queue.add(mock(Runnable.class));
        CompletableFuture<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result).isNotCompleted();

        verify(checkExecutor, never()).execute(notNull());
        assertThat(runner.getMarkedCFSs()).contains(cfs);
    }


    // we should immeditatlly return ABORTED if the executor is shutdown
    @Test
    public void immediatelyReturnIfExecutorIsDown() throws Exception
    {
        when(checkExecutor.isShutdown()).thenReturn(true);
        doThrow(new RejectedExecutionException("rejected")).when(checkExecutor).execute(ArgumentMatchers.notNull());

        CompletableFuture<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result).isCompletedWithValue(RequestResult.ABORTED);

        verify(checkExecutor).execute(notNull());
    }


    // shutdown should shut down the check executor and should not shut down the compaction executor
    @Test
    public void shutdown() throws Exception
    {
        CompletableFuture<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result.isDone()).isFalse();

        runner.shutdown();

        assertThat(result).isCompletedWithValue(RequestResult.ABORTED);

        verify(checkExecutor).shutdown();
        verify(compactionExecutor, never()).shutdown();
    }


    // a check should make a task finish with NOT_NEEDED if there are no compaction tasks and upgrades are disabled
    @Test
    public void finishWithNotNeededWhenNoCompactionTasksAndUpgradesDisabled() throws Exception
    {
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(false);

        when(cfs.getCandidatesForUpgrade()).thenReturn(ImmutableList.of(mock(SSTableReader.class)));

        CompletableFuture<RequestResult> result = runner.markForCompactionCheck(cfs);
        verifyCFSWasMarkedForCompaction();
        capturedCheckRunnables.getValue().run();

        assertThat(result).isCompletedWithValue(RequestResult.NOT_NEEDED);
        verify(checkExecutor, never()).execute(notNull());
        assertThat(runner.getMarkedCFSs()).isEmpty();
    }

    // a check should make a task finish with NOT_NEEDED if there are no compaction tasks and no upgrade tasks
    @Test
    public void finishWithNotNeededWhenNoCompactionTasksAndNoUpgradeTasks() throws Exception
    {
        when(cfs.getCandidatesForUpgrade()).thenReturn(Lists.emptyList());

        CompletableFuture<RequestResult> result = runner.markForCompactionCheck(cfs);
        verifyCFSWasMarkedForCompaction();
        capturedCheckRunnables.getValue().run();

        assertThat(result).isCompletedWithValue(RequestResult.NOT_NEEDED);
        verify(checkExecutor, never()).execute(notNull());
        assertThat(runner.getMarkedCFSs()).isEmpty();
    }


    // a check should start a compaction task if there is some
    @Test
    public void startCompactionTask() throws Exception
    {
        // although it is possible to run upgrade tasks, we make sure that compaction tasks are selected
        CompletableFuture<RequestResult> result = markCFSAndRunCheck();

        // check the task was scheduled on compaction executor
        verifyTaskScheduled(compactionExecutor);
        verifyState(1, 0);
        assertThat(result).isNotCompleted();

        // ... we immediatelly marked that CFS for compaction again
        verifyCFSWasMarkedForCompaction();

        // now we will execute the task
        capturedCompactionRunnables.getValue().run();

        // so we expect that:
        assertThat(result).isCompletedWithValue(RequestResult.COMPLETED);
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
        when(compactionStrategy.createCompactionTask(same(txn), anyInt(), anyLong())).thenReturn(compactionTask);

        CompletableFuture<RequestResult> result = runner.markForCompactionCheck(cfs);
        verifyCFSWasMarkedForCompaction();

        capturedCheckRunnables.getValue().run();

        // make sure we did check for the upgrade candidates
        verify(cfs).getCandidatesForUpgrade();

        // check the task was scheduled on compaction executor
        verifyTaskScheduled(compactionExecutor);
        verifyState(1, 1);
        assertThat(result).isNotCompleted();

        // ... we immediatelly marked that CFS for compaction again
        verifyCFSWasMarkedForCompaction();

        // now we will execute the task
        capturedCompactionRunnables.getValue().run();

        // so we expect that:
        assertThat(result).isCompletedWithValue(RequestResult.COMPLETED);
        verifyState(0, 0);

        // another check should be schedued upon task completion
        verifyCFSWasMarkedForCompaction();
    }


    // we should run multiple compactions for a CFS in parallel if possible
    @Test
    public void startMultipleCompactionTasksInParallel() throws Exception
    {
        // first task
        CompletableFuture<RequestResult> result1 = markCFSAndRunCheck();
        verifyTaskScheduled(compactionExecutor);
        verifyState(1, 0);
        verifyCFSWasMarkedForCompaction();

        // second task
        CompletableFuture<RequestResult> result2 = markCFSAndRunCheck();
        verifyTaskScheduled(compactionExecutor);
        verifyState(2, 0);
        verifyCFSWasMarkedForCompaction();

        assertThat(result2).isNotSameAs(result1);

        // now we will execute the first task
        assertThat(result1).isNotCompleted();
        capturedCompactionRunnables.getAllValues().get(0).run();
        assertThat(result1).isCompletedWithValue(RequestResult.COMPLETED);

        // so we expect that:
        verifyState(1, 0);

        // execute the second task
        assertThat(result2).isNotCompleted();
        capturedCompactionRunnables.getAllValues().get(1).run();
        assertThat(result2).isCompletedWithValue(RequestResult.COMPLETED);

        // so we expect that:
        verifyState(0, 0);

        verifyState(0, 0);
    }


    // postpone execution if the thread pool is busy
    @Test
    public void postponeCompactionTasksIfPoolIsBusy() throws Exception
    {
        // first task
        CompletableFuture<RequestResult> result1 = markCFSAndRunCheck();
        verifyTaskScheduled(compactionExecutor);
        verifyState(1, 0);
        verifyCFSWasMarkedForCompaction();

        // second task
        CompletableFuture<RequestResult> result2 = markCFSAndRunCheck();
        verifyTaskScheduled(compactionExecutor);
        verifyState(2, 0);
        verifyCFSWasMarkedForCompaction();

        // third task, but now the task should not be scheduled for execution because of the pool size (2)
        clearInvocations(compactionStrategy);
        CompletableFuture<RequestResult> result3 = markCFSAndRunCheck();
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
        assertThat(result1).isNotCompleted();
        capturedCompactionRunnables.getAllValues().get(0).run();
        // so we expect that:
        assertThat(result1).isCompletedWithValue(RequestResult.COMPLETED);
        verifyState(1, 0);

        // execute the check, so that the thrid task is submitted
        clearInvocations(checkExecutor);
        capturedCheckRunnables.getValue().run();
        verifyTaskScheduled(compactionExecutor);
        verifyState(2, 0);
        verifyCFSWasMarkedForCompaction();

        // execute the rest of the tasks
        assertThat(result2).isNotCompleted();
        capturedCompactionRunnables.getAllValues().get(1).run();
        assertThat(result2).isCompletedWithValue(RequestResult.COMPLETED);

        assertThat(result3).isNotCompleted();
        capturedCompactionRunnables.getAllValues().get(2).run();
        assertThat(result3).isCompletedWithValue(RequestResult.COMPLETED);

        verifyState(0, 0);
    }


    // returned future should not support complete or cancel
    @Test
    public void futureRequestResultNotSupportForTermination()
    {
        CompletableFuture<RequestResult> result = markCFSAndRunCheck();

        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> result.complete(RequestResult.COMPLETED));
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> result.completeExceptionally(new RuntimeException()));
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> result.cancel(false));

        runner.shutdown();
    }


    // handling submission failure
    @Test
    public void handleTaskSubmissionFailure() throws Exception
    {
        doThrow(new RejectedExecutionException()).when(compactionExecutor).execute(notNull());

        CompletableFuture<RequestResult> result = markCFSAndRunCheck();
        clearInvocations(checkExecutor);

        // so we expect that:
        assertThat(result).isCompletedWithValue(RequestResult.COMPLETED);
        verifyState(0, 0);

        verify(checkExecutor, never()).execute(notNull());
    }


    // handling task failure
    @Test
    public void handleTaskFailure() throws Exception
    {
        CompletableFuture<RequestResult> result = markCFSAndRunCheck();
        clearInvocations(checkExecutor);

        doThrow(new IOError(new RuntimeException())).when(compactionTasks.get(0)).execute(activeOperations);
        capturedCompactionRunnables.getValue().run();

        // so we expect that:
        assertThat(result).isCompletedExceptionally();
        verifyState(0, 0);

        // another check should be schedued upon task completion
        verifyCFSWasMarkedForCompaction();
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

    private CompletableFuture<RequestResult> markCFSAndRunCheck()
    {
        AbstractCompactionTask compactionTask = mock(AbstractCompactionTask.class);

        compactionTasks.clear();
        compactionTasks.add(compactionTask);

        CompletableFuture<RequestResult> result = runner.markForCompactionCheck(cfs);
        verifyCFSWasMarkedForCompaction();

        capturedCheckRunnables.getValue().run();
        return result;
    }
}