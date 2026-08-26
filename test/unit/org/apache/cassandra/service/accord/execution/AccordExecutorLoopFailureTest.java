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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import javax.annotation.Nullable;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Node.Id;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.SignalLock;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test has been authored entirely by Claude.
 *
 * Whatever a task does, {@link AccordExecutorSignalLoop} must not be wedged by it: its loop threads must release the
 * executor lock, stay alive, and go on running work.
 *
 * <p>The failure modes covered here all present identically - as an {@code Invariants.require(owner != self)} failure in
 * {@link SignalLock#awaitAsyncOrLock}, raised one loop iteration <em>after</em> the actual fault:
 * <ul>
 *   <li>an acquisition of the lock nested inside exclusive work that is never released (the shape of
 *       {@code ExclusiveCaches.close()} skipping its unlock because {@code tryShrinkOrEvict} threw): the loop's unwind
 *       used to pop only one level of the reentrant hold, silently leaving the lock held;</li>
 *   <li>a task that returns while still holding the lock: nothing failed, so nothing was reported;</li>
 *   <li>an {@link accord.api.Agent} whose {@code onException} itself throws - the shape of an in-JVM dtest instance
 *       kill, where {@code JVMStabilityInspector.uncaughtException} discards the original throwable and throws
 *       {@code InstanceShutdown}: the throw escaped the loop's catch clause and silently killed the loop thread.</li>
 * </ul>
 *
 * Nothing here needs a command store, a schema, or a kill: the tasks are minimal {@link Plain} tasks that leak the
 * lock and/or fail in a chosen phase, and the assertions are only that the executor is still usable afterwards.
 */
public class AccordExecutorLoopFailureTest
{
    private static final long TIMEOUT_SECONDS = 10;
    private static final int THREADS = 2;

    /** a phase of a task's lifecycle; all but {@link #RUN} are performed holding the executor lock */
    private enum Phase { PREPARE, COMPLETE, RUN }

    private final List<AccordExecutor> executors = new CopyOnWriteArrayList<>();

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void after() throws InterruptedException
    {
        for (AccordExecutor executor : executors)
        {
            // an executor whose lock has been leaked cannot be shut down, as shutdown() must take the lock; do not
            // block the run on it, so that the assertions that detected the leak are reported
            Thread thread = new Thread(executor::shutdown, "shutdown");
            thread.setDaemon(true);
            thread.start();
            thread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        }
        executors.clear();
    }

    /**
     * A loop thread registers the executor with its {@link TaskRunner} when it takes the lock, so that the reentrancy
     * guards see the acquisition, and must deregister it when it releases - the release path is
     * {@code lock.unlockAndAcquireAsyncWork()}, not {@code AccordExecutor.unlock}, so the balance is easy to lose.
     * If it is lost the thread claims to hold this executor for ever, which is not a leaked lock (so
     * {@link Env#assertHealthy} does not see it) but silently disables {@code tryLock} - completions are never done
     * inline - makes {@code ensureLockNotHeld} report a bogus "Invalid lock state" on every subsequent exception, and
     * makes locking any second executor from that thread throw for ever. Tasks here run without the lock, so a loop
     * thread must hold nothing at all while running one.
     */
    @Test
    public void loopThreadHoldsNoExecutorWhileRunningTask() throws Throwable
    {
        Env env = new Env(false);

        // twice: the first acquisition is balanced even when the accounting is not, so a single run proves nothing
        for (int i = 0 ; i < 2 ; ++i)
        {
            List<AccordExecutor> heldWhileRunning = new CopyOnWriteArrayList<>();
            CountDownLatch ran = CountDownLatch.newCountDownLatch(1);
            env.executor.execute(() -> {
                AccordExecutor locked = TaskRunner.get().accordLockedExecutor();
                if (locked != null)
                    heldWhileRunning.add(locked);
                ran.decrement();
            });
            assertThat(ran.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).describedAs("the task never ran").isTrue();
            assertThat(heldWhileRunning).describedAs("run %s: the loop thread was still registered as holding an executor while running a task, "
                                                     + "so its lock acquisition was not balanced on release", i)
                                        .isEmpty();
        }

        // and nothing was reported to the agent: a stale registration makes ensureLockNotHeld cry wolf
        assertThat(env.reported).describedAs("the executor reported a problem while running healthy work").isEmpty();
        env.assertHealthy();
    }

    @Test
    public void failureWhilePreparing() throws Throwable
    {
        assertRecovers(null, Phase.PREPARE, false);
    }

    @Test
    public void failureWhileCompleting() throws Throwable
    {
        assertRecovers(null, Phase.COMPLETE, false);
    }

    @Test
    public void failureWhileRunning() throws Throwable
    {
        assertRecovers(null, Phase.RUN, false);
    }

    /** exclusive work that leaks a nested acquisition of the lock must not leave the executor locked */
    @Test
    public void leakedLockWhilePreparing() throws Throwable
    {
        assertRecovers(Phase.PREPARE, Phase.PREPARE, false);
    }

    @Test
    public void leakedLockWhileCompleting() throws Throwable
    {
        assertRecovers(Phase.COMPLETE, Phase.COMPLETE, false);
    }

    @Test
    public void leakedLockWhileRunning() throws Throwable
    {
        assertRecovers(Phase.RUN, Phase.RUN, false);
    }

    /** the same, but nothing fails: the task simply returns holding the lock, so nothing is reported anywhere */
    @Test
    public void silentlyLeakedLockWhileRunning() throws Throwable
    {
        assertRecovers(Phase.RUN, null, false);
    }

    /** an agent that throws instead of reporting must not kill the loop thread */
    @Test
    public void throwingAgentWithLeakedLockWhilePreparing() throws Throwable
    {
        assertRecovers(Phase.PREPARE, Phase.PREPARE, true);
    }

    @Test
    public void throwingAgentWhileRunning() throws Throwable
    {
        assertRecovers(null, Phase.RUN, true);
    }

    /**
     * Submit a task that acquires the lock without releasing it in {@code leakIn} and/or fails in {@code failIn}, then
     * assert the executor is still healthy: it runs new work, no loop thread holds the lock, and none has been lost.
     *
     * @param agentThrows report failures to an agent whose {@code onException} throws, as an instance kill does
     */
    private void assertRecovers(@Nullable Phase leakIn, @Nullable Phase failIn, boolean agentThrows) throws Throwable
    {
        Env env = new Env(agentThrows);
        RuntimeException failure = new RuntimeException("deliberate " + failIn + " failure");
        FailingTask task = new FailingTask(env.executor, leakIn, failIn, failure);

        env.executor.submitTask(task);

        if (failIn == null)
        {
            await(task.ran, "the task ran");
        }
        else if (failIn != Phase.COMPLETE)
        {
            // a task that fails while completing has already been notified of its success
            await(task.notified, "the failing task was notified");
            assertThat(task.failure).isSameAs(failure);
        }

        env.assertHealthy();

        // What the agent was told. Until now `reported` was only ever interpolated into failure messages, so an executor
        // that swallowed a failure entirely - reporting it neither to the task nor to the agent - passed these tests as
        // long as it stayed alive. The loop thread reports asynchronously, so every expectation here is polled: reading
        // `reported` once can sample it between the two reports a leak produces.
        if (leakIn != null)
        {
            // a leaked acquisition is caught twice: by the reentrant acquire, and by the lock-state audit the loop
            // performs when the task returns
            assertThat(await(() -> !env.reported.isEmpty())).describedAs("the leaked lock was not reported to the agent").isTrue();
            assertThat(await(() -> env.reported.stream().anyMatch(t -> String.valueOf(t.getMessage()).contains("Invalid lock state"))))
                      .describedAs("no lock-state audit was reported: %s", env.reported).isTrue();
        }
        if (failIn == Phase.COMPLETE)
        {
            // the task has already been told it succeeded, so a completion failure has nowhere to go but the agent
            assertThat(await(() -> env.reported.contains(failure)))
                      .describedAs("the completion failure was not reported to the agent: %s", env.reported).isTrue();
        }
        else if (failIn != null && leakIn == null)
        {
            // a prepare or run failure belongs to the task, and reached it via reportFailureMayThrow (asserted above);
            // telling the agent as well would report it twice
            assertThat(env.reported).describedAs("a %s failure must go to the task, not the agent", failIn).isEmpty();
        }
    }

    private class Env
    {
        final List<Throwable> reported = new CopyOnWriteArrayList<>();
        final AccordExecutor executor;

        Env(boolean agentThrows)
        {
            AccordAgent agent = new AccordAgent()
            {
                @Override
                public void onException(Throwable t)
                {
                    reported.add(t);
                    if (agentThrows)
                        throw new SimulatedKill();
                }

                @Override
                public void onException(Throwable t, String context)
                {
                    onException(t);
                }
            };
            agent.setup(Id.NONE);
            this.executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, THREADS, -1, -1, MICROSECONDS,
                                                         i -> "AccordExecutorLoopFailureTest[" + i + ']', agent);
            executors.add(executor);
        }

        void assertHealthy() throws Throwable
        {
            SignalLock lock = (SignalLock) executor.unsafeLock();

            // the executor must still run new work: a leaked lock stalls every other thread
            CountDownLatch ran = CountDownLatch.newCountDownLatch(1);
            executor.execute(ran::decrement);
            boolean progressed = ran.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // and no loop thread may still hold the lock; other threads (e.g. a metrics snapshot) may legitimately
            // hold it briefly, and so may a loop thread - it takes the lock to pick up and complete the work we just
            // submitted - so a leak is only a hold that never goes away, i.e. one that every poll below observes
            long[] state = new long[1];
            Thread[] holder = new Thread[1];
            boolean leaked = !await(() -> !isHeldByLoopThread(lock, state, holder));

            if (!progressed || leaked)
            {
                dumpThreads();
                // the leak first: when the lock has been leaked it is also why nothing progressed, and only this
                // message can name the thread that holds it
                assertThat(leaked).describedAs("a loop thread (%s) leaked the executor lock (%s, reported: %s)",
                                               holder[0], SignalLock.toString(state[0]), reported).isFalse();
                assertThat(progressed).describedAs("executor ran no new work after the failure (lock %s, reported: %s)",
                                                   SignalLock.toString(state[0]), reported).isTrue();
            }

            // every loop thread must still be running
            assertThat(((AbstractLoop) executor).loops().runningCount()).describedAs("loop threads were lost (reported: %s)", reported).isEqualTo(THREADS);
            assertThat(executor.isTerminated()).describedAs("executor terminated itself").isFalse();

            // finally, the work must be accounted for; this takes the lock, so it is only safe once we know the lock
            // has not been leaked, as otherwise it would block forever
            assertThat(await(() -> !executor.hasTasks())).describedAs("executor did not return to quiescence").isTrue();
        }

        /**
         * Whether a loop thread holds the lock, deciding it from {@code state} - which is volatile, so it is the only
         * safely published answer - and reporting the state it decided from in {@code stateOut} and the thread it
         * attributed the hold to in {@code ownerOut}. {@code unsafeOwner()} is a plain field with no visibility
         * guarantees, so it can only be consulted to <em>attribute</em> an ownership the state has already established,
         * never to establish one.
         */
        private boolean isHeldByLoopThread(SignalLock lock, long[] stateOut, Thread[] ownerOut)
        {
            long state = stateOut[0] = lock.state();
            ownerOut[0] = null;
            if (!SignalLock.isLockOwned(state))
                return false; // nobody holds it, whatever a stale read of `owner` might say

            int thread = SignalLock.lockThread(state);
            if (thread >= 0)
            {
                ownerOut[0] = lock.registeredThread(thread); // the state names the registered (i.e. loop) thread
                return true;
            }

            // an anonymous acquisition (e.g. one a task made while running without the lock): only `owner` says who,
            // and the volatile read above orders this read after the acquisition that set it
            Thread owner = lock.unsafeOwner();
            for (int i = 0 ; owner != null && i < THREADS ; ++i)
            {
                if (lock.registeredThread(i) == owner)
                {
                    ownerOut[0] = owner;
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Leaks an acquisition of the executor lock in one phase of its lifecycle, and/or fails in one, so that the
     * executor's handling of both can be verified.
     */
    private static class FailingTask extends Plain
    {
        final @Nullable Phase leakIn;
        final @Nullable Phase failIn;
        final RuntimeException failWith;
        final CountDownLatch notified = CountDownLatch.newCountDownLatch(1);
        final CountDownLatch ran = CountDownLatch.newCountDownLatch(1);
        volatile Throwable failure;

        FailingTask(AccordExecutor executor, @Nullable Phase leakIn, @Nullable Phase failIn, RuntimeException failWith)
        {
            super(executor, GlobalGroup.OTHER);
            this.leakIn = leakIn;
            this.failIn = failIn;
            this.failWith = failWith;
        }

        private void maybeLeakOrFail(Phase phase)
        {
            if (leakIn == phase)
                executor.lockCaches(); // deliberately neither closed nor unlocked

            if (failIn == phase)
                throw failWith;
        }

        @Override
        ExclusiveExecutor exclusiveExecutor()
        {
            return null;
        }

        @Override
        boolean prepareExclusiveMayThrow()
        {
            maybeLeakOrFail(Phase.PREPARE);
            return true;
        }

        @Override
        boolean runMayThrow()
        {
            maybeLeakOrFail(Phase.RUN);
            ran.decrement();
            return true;
        }

        @Override
        void completeExclusiveMayThrow()
        {
            maybeLeakOrFail(Phase.COMPLETE);
            super.completeExclusiveMayThrow();
        }

        @Override
        void reportFailureMayThrow(Throwable fail)
        {
            failure = fail;
            notified.decrement();
        }

        @Override
        public String description()
        {
            return "FailingTask[leakIn=" + leakIn + ",failIn=" + failIn + ']';
        }

        @Override
        String briefDescription()
        {
            return description();
        }
    }

    /** thrown by an agent that kills its instance rather than reporting, as in-JVM dtests do */
    private static class SimulatedKill extends RuntimeException {}

    /** print the loop threads' stacks, so that a leaked lock or lost thread can be diagnosed from a CI log */
    private static void dumpThreads()
    {
        Thread.getAllStackTraces().forEach((thread, stack) -> {
            if (!thread.getName().startsWith(AccordExecutorLoopFailureTest.class.getSimpleName()))
                return;

            StringBuilder out = new StringBuilder(thread.getName()).append(' ').append(thread.getState());
            for (StackTraceElement element : stack)
                out.append("\n\tat ").append(element);
            System.err.println(out);
        });
    }

    private static void await(CountDownLatch latch, String what) throws InterruptedException
    {
        assertThat(latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).describedAs(what).isTrue();
    }

    /**
     * Poll {@code condition} for up to {@link #TIMEOUT_SECONDS}, returning whether it was ever met. Returns the value
     * that ended the poll: re-evaluating the condition after the loop would report a transiently true condition as
     * false, which for a lock the loop threads legitimately take and release is a coin toss.
     */
    private static boolean await(BooleanSupplier condition)
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (true)
        {
            if (condition.getAsBoolean())
                return true;
            if (System.nanoTime() >= deadline)
                return false;
            Thread.yield();
        }
    }
}
