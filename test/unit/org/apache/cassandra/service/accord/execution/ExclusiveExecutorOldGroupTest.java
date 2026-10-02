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

package org.apache.cassandra.service.accord.execution;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Node.Id;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.execution.Task.ExclusiveGroup;
import org.apache.cassandra.service.accord.execution.Task.GlobalGroup;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITH_LOCK;
import static org.apache.cassandra.service.accord.execution.TaskPositions.AGE_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test was authored by Claude (Anthropic).
 *
 * A command store's {@link ExclusiveExecutor} is queued at the executor level in {@link GlobalGroup#OLD} while its next
 * task is OLD, and in {@link GlobalGroup#COMMAND_STORE} otherwise, moving between the two as its next task changes.
 */
public class ExclusiveExecutorOldGroupTest
{
    static final long HLC = 1_790_000_000_000_000L;

    final List<Throwable> agentExceptions = new CopyOnWriteArrayList<>();
    final List<String> ran = new CopyOnWriteArrayList<>();
    AccordExecutor executor;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void after()
    {
        if (executor != null)
            executor.shutdown();
    }

    private AccordExecutor newExecutor()
    {
        AccordAgent agent = new AccordAgent()
        {
            @Override public void onException(Throwable t) { agentExceptions.add(t); }
            @Override public void onException(Throwable t, String context) { agentExceptions.add(t); }
        };
        agent.setup(Id.NONE);
        return executor = new AccordExecutorSyncSubmit(0, RUN_WITH_LOCK, "ExclusiveExecutorOldGroupTest", agent);
    }

    private class Probe extends Plain
    {
        final ExclusiveExecutor exclusiveExecutor;
        final String name;
        final CountDownLatch done;

        Probe(ExclusiveExecutor exclusiveExecutor, ExclusiveGroup group, long position, String name, CountDownLatch done)
        {
            super(exclusiveExecutor.selfTask.executor(), group);
            this.exclusiveExecutor = exclusiveExecutor;
            this.position = position;
            this.name = name;
            this.done = done;
        }

        @Override ExclusiveExecutor exclusiveExecutor() { return exclusiveExecutor; }
        @Override boolean runMayThrow() { ran.add(name); done.decrement(); return true; }
        @Override void reportFailureMayThrow(Throwable fail) { done.decrement(); }
        @Override public String description() { return name; }
        @Override String briefDescription() { return name; }
    }

    private void awaitQuiescent(CountDownLatch done) throws InterruptedException
    {
        assertThat(done.await(30, TimeUnit.SECONDS)).describedAs("all tasks completed").isTrue();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (executor.hasTasks() && System.nanoTime() < deadline)
            Thread.yield();
        assertThat(executor.hasTasks()).describedAs("executor still has registered tasks").isFalse();
        assertThat(agentExceptions).isEmpty();
    }

    @Test
    public void storeMovesBetweenOldAndCommandStoreGroups() throws Throwable
    {
        AccordExecutor executor = newExecutor();
        ExclusiveExecutor store = executor.newExclusiveExecutor(0);
        CountDownLatch done = CountDownLatch.newCountDownLatch(5);
        Probe anchor = new Probe(store, ExclusiveGroup.DECIDE, HLC, "anchor", done);
        Probe old1 = new Probe(store, ExclusiveGroup.RECOVER, HLC - 10 * AGE_LIMIT, "old1", done);
        Probe old2 = new Probe(store, ExclusiveGroup.RECOVER, HLC - 20 * AGE_LIMIT, "old2", done);
        Probe young = new Probe(store, ExclusiveGroup.RECOVER, HLC - 1, "young", done);
        Probe live = new Probe(store, ExclusiveGroup.DECIDE, HLC - 10 * AGE_LIMIT, "live", done);

        executor.executeDirectlyWithLock(() -> {
            anchor.submitExclusiveNoExcept();
            old1.submitExclusiveNoExcept();
            old2.submitExclusiveNoExcept();
            young.submitExclusiveNoExcept();
            live.submitExclusiveNoExcept();

            // classification on registration
            assertThat(old1.isOld()).isTrue();
            assertThat(old1.position).isEqualTo(HLC - 10 * AGE_LIMIT);
            assertThat(old2.isOld()).isTrue();
            assertThat(young.isOld()).isFalse();
            assertThat(young.position).isEqualTo(HLC - 1);
            assertThat(live.isOld()).isFalse();
            assertThat(live.position).isGreaterThan(HLC); // queued FIFO

            // the store's next task is the first submitted, which is not OLD
            assertThat(store.task).isSameAs(anchor);
            assertThat(store.selfTask.is(GlobalGroup.COMMAND_STORE)).isTrue();
            assertThat(executor.runnable.isWaiting(store.selfTask)).isTrue();

            // cancelling it while waiting makes the oldest work our next task (the first poll is always by priority),
            // which is OLD, so we move to OLD
            anchor.tryCancelExclusive(new CancellationException());
            Task next = store.task;
            assertThat(next).isSameAs(old2);
            assertThat(store.selfTask.is(GlobalGroup.OLD)).isTrue();
            assertThat(store.selfTask.position).isEqualTo(next.position);
            assertThat(executor.runnable.isWaiting(store.selfTask)).isTrue();

            // and cancelling that moves us again, as appropriate
            ((Probe) next).tryCancelExclusive(new CancellationException());
            next = store.task;
            assertThat(next.isOld()).isEqualTo(store.selfTask.is(GlobalGroup.OLD));
            assertThat(executor.runnable.isWaiting(store.selfTask)).isTrue();
        });

        awaitQuiescent(done);
        assertThat(ran).hasSize(3);
    }

    @Test
    public void consequencesOfOldWorkAreOld() throws Throwable
    {
        AccordExecutor executor = newExecutor();
        ExclusiveExecutor store = executor.newExclusiveExecutor(0);
        CountDownLatch done = CountDownLatch.newCountDownLatch(3);
        long oldPosition = HLC - 10 * AGE_LIMIT;
        // as observed by the consequence when it runs: whether it is OLD, its position, and whether its store was queued as OLD
        boolean[] childIsOld = new boolean[1], storeIsOld = new boolean[1];
        long[] childPosition = new long[1];
        Probe child = new Probe(store, ExclusiveGroup.OTHER, 0, "child", done)
        {
            @Override
            boolean runMayThrow()
            {
                childIsOld[0] = isOld();
                childPosition[0] = position;
                storeIsOld[0] = store.selfTask.is(GlobalGroup.OLD);
                return super.runMayThrow();
            }
        };
        Probe anchor = new Probe(store, ExclusiveGroup.DECIDE, HLC, "anchor", done);
        Probe old = new Probe(store, ExclusiveGroup.RECOVER, oldPosition, "old", done)
        {
            @Override
            boolean runMayThrow()
            {
                addConsequence(child);
                return super.runMayThrow();
            }
        };

        executor.executeDirectlyWithLock(() -> {
            anchor.submitExclusiveNoExcept();
            old.submitExclusiveNoExcept();
        });

        awaitQuiescent(done);
        assertThat(ran).containsExactlyInAnyOrder("anchor", "old", "child");
        assertThat(childIsOld[0]).describedAs("a consequence of OLD work is OLD").isTrue();
        assertThat(childPosition[0]).describedAs("and retains its parent's position").isEqualTo(oldPosition);
        assertThat(storeIsOld[0]).describedAs("and its store is queued as OLD while it is next to run").isTrue();
    }
}
