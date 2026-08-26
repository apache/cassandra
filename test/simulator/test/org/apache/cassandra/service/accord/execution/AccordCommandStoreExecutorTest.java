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

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

import org.junit.Test;

import accord.api.AsyncExecutor;
import accord.api.ExclusiveAsyncExecutor;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.Coordinations;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultLocalListeners.NotifySink;
import accord.impl.DefaultRemoteListeners;
import accord.impl.TestAgent;
import accord.impl.basic.InMemoryJournal;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.ExecutionContext;
import accord.local.ExecutionContext.ExecutionKind;
import accord.local.ExecutionContext.ExecutionSequence;
import accord.local.LoadKeys;
import accord.local.LoadKeysFor;
import accord.local.Node.Id;
import accord.local.NodeCommandStoreService;
import accord.local.SafeCommandStore;
import accord.local.SafeState;
import accord.local.TimeService;
import accord.local.cfk.CommandsForKey;
import accord.local.durability.DurabilityService;
import accord.primitives.Ballot;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.TopologyManager;
import accord.utils.DefaultRandom;
import accord.utils.Invariants;
import accord.utils.QuadFunction;
import accord.utils.async.Cancellable;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.AccordConfig;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableFunction;
import org.apache.cassandra.metrics.AccordSystemMetrics;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.ControllableRangeIndex;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.simulator.test.SimulationTestBase;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_SEED;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.apache.cassandra.service.accord.execution.SaferState.global;

/**
 * Simulator driven test of {@link SafeTask} execution on real {@link AccordCommandStore}s, i.e. of the parts of the
 * executor {@link org.apache.cassandra.simulator.test.AccordExecutorTest} cannot reach: cache references, the
 * per-key/per-txnId queues and the ordering guarantees they provide, and the boundaries between command stores and
 * between executors.
 *
 * <p>The command stores are real, but everything below them is synthetic: an in-memory journal, and cache load
 * functions that return {@code null} (an uninitialised value) rather than reading from {@code system_accord}. So no
 * schema, no cluster metadata and no commit log - only the executor and task machinery is under test.
 *
 * <h2>Scope</h2>
 * {@value #STORES} command stores spread over {@value #EXECUTORS} executors (so some stores share an executor and some
 * do not), all sharing one key space; {@code SYNC} key-domain contexts; each submission optionally submits one nested
 * task, targeting the same store, another store on the same executor, or a store on another executor. No cancellation
 * and no failures yet. Verifies:
 * <ul>
 *   <li><b>liveness</b>: every submission is notified exactly once, and successfully;</li>
 *   <li><b>declared access</b>: while running, a task holds a reference for every key and txnId it declared, and
 *       every reference it holds belongs to its own command store;</li>
 *   <li><b>mutual exclusion</b>: two tasks that declare the same key or txnId <i>of the same store</i> never execute
 *       concurrently - the central guarantee of the cache-entry queues;</li>
 *   <li><b>consequences are executor scoped</b>: work submitted from a running task is attached to that task (and so
 *       runs only once it has finished) if and only if it belongs to the same executor; work for another executor is
 *       submitted to that executor independently;</li>
 *   <li><b>one executor per thread</b>: a task never observes another executor's lock held by its thread, and
 *       {@link ExclusiveExecutor#tryExecuteImmediately} refuses work for another executor;</li>
 *   <li><b>sequencing is universal</b>: every task is assigned its context's {@code ExecutionSequence}, whether it was
 *       pre-set-up on its parent's command store or submitted at top level or for another store. The declared sequence is
 *       a lower bound on the ordering imposed, not an exact one: {@code BY_PRIORITY} permits an INCR task's batches to
 *       interleave rather than requiring it, so the fifo upgrade that prevents interleaving still meets it;</li>
 *   <li><b>sequenced ordering</b>: sibling tasks that share a cache entry execute in the order the cache entry queues
 *       promise - {@code BY_PRIORITY_ATOMIC} ahead of {@code BY_PRIORITY} regardless of submission order, atomic
 *       siblings in submission (fifo) order, and prioritised siblings in {@code position}/kind order;</li>
 *   <li><b>no leaks</b>: once the executors have finished, no cache entry is still referenced and no task retains
 *       its references;</li>
 *   <li><b>batching</b>: an {@code ASYNC} task runs once over a batch of the keys that were ready, an {@code INCR}
 *       task runs over successive disjoint batches until it has processed every key it declared, each run holds
 *       references for exactly the keys it has not yet processed, and an {@code INCR} task that holds a txnId keeps it
 *       for the whole of its execution, not just one batch;</li>
 *   <li><b>load failures</b>: a task whose cache entry fails to load fails with that failure (and its consequences are
 *       cancelled), releases everything it had acquired, does not disturb any other task, and does not wedge the entry -
 *       every key and txnId is usable again once the round is over;</li>
 *   <li><b>capacity pressure</b>: with a cache far smaller than its working set, eviction and the loading pause it
 *       drives must not lose or stall work.</li>
 * </ul>
 *
 * <h2>Notes</h2>
 * <ul>
 *   <li>only the {@code SIGNAL} and {@code ASYNC} submission models can be simulated; {@code SYNC}/{@code SEMI_SYNC}
 *       (the production default) need the simulator to intercept {@link java.util.concurrent.locks.ReentrantLock};</li>
 *   <li>each submission model is run over all three configurations the two off-switches permit: queues and batching on,
 *       queues on and batching off ({@code queue_nonsync_enabled=false}), and both off
 *       ({@code queue_key_ordering_enabled=false}, which disables batching regardless). The expectations that only hold
 *       for one of them are derived from {@code AccordExecutor.CACHE_QUEUES_ENABLED}/{@code NONSYNC_ENABLED} rather than
 *       from the parameter, so the test cannot disagree with the flags it is exercising.</li>
 * </ul>
 */
public class AccordCommandStoreExecutorTest extends SimulationTestBase
{
    private static final int EXECUTORS = 2;
    private static final int STORES = 4;
    private static final int KEYS = 8;
    private static final int TXN_IDS = 8;
    private static final int SUBMIT_THREADS = 8;
    private static final int OUTER_LOOP = 10;
    private static final int INNER_LOOP = 100;
    private static final int MAX_KEYS_PER_TASK = 3;
    /** batched (non-SYNC) tasks declare more keys, so that they need more than one batch */
    private static final int MAX_KEYS_PER_BATCHED_TASK = 6;
    private static final int MAX_TXN_IDS_PER_TASK = 2;
    /** keys ready before a batched task will run, and the most it will process in one run; see {@link #test} */
    private static final int MIN_BATCH = 2, MAX_BATCH = 3;
    private static final float ASYNC_CHANCE = 0.15f, INCR_CHANCE = 0.15f;
    private static final long AMPLE_CAPACITY = 8 << 20, AMPLE_WORKING_SET = 4 << 20;
    /** far smaller than the working set of a round, so that we are always over capacity */
    private static final long TINY_CAPACITY = 8 << 10, TINY_WORKING_SET = 8 << 10;
    private static final float LOAD_FAILURE_CHANCE = 0.1f;
    /**
     * How often a top-level task declares {@code LoadKeysFor.RECOVERY}, which sends it through {@code RangeTxnScanner}:
     * SCANNING_RANGES, then a re-entry into presetup once the scan completes.
     *
     * <p>This axis is <em>on</em>, and what it exposes under {@code -Daccord.paranoid=true
     * -Daccord.paranoia.cpu=SUPERLINEAR -Daccord.paranoia.memory=LINEAR} (which is what the ant test targets now set;
     * see {@code accord.test.jvmargs} in build.xml) is:
     *
     * <ul>
     *   <li><b>fixed</b>: the failure-path leak. A task whose scan failed used to keep every queue position it held
     *       while FAILED, so anything queued behind it waited for ever.
     *       {@code Task.completeExclusiveNoExcept} now calls {@code releaseResourcesOnFailureExclusive}, and R6 is
     *       asserted there. Not reproduced since; and in the runs where this suite did stall, {@link QueueCycleDetector}
     *       reported no cycle and no mis-counted waiter, i.e. no FAILED task was holding a position.</li>
     *   <li><b>still failing, intermittently</b>: the membership/wait-count disagreement the re-entry into presetup
     *       leaves behind. {@code SafeTask.completeSetupOfLoading}'s
     *       {@code paranoid(entry.waitingCount() == entry.references())} fires - observed in 1 of 3 runs of the six
     *       tests, class seed {@code -5978575408629660273} ({@code -Dcassandra.test.seed}), in {@code asyncSubmitTest}.
     *       Each test method draws its own simulation seed, so pinning the class seed does not reliably replay it.</li>
     *   <li><b>failing, reliably, only with {@code -Daccord.testing=true}</b>: {@code setInheritedRangeScan}'s
     *       {@code expect(..)} - "inherits a suppressed range scan whose summaries it needs" - is reported ~100 times
     *       per run, and with {@code expect} failing rather than logging it fails all six tests. That path was believed
     *       unreachable; this axis reaches it.</li>
     * </ul>
     *
     * Both remaining items are in {@code SafeTask}, not in this suite.
     */
    private static final float RECOVERY_CHANCE = 0.15f;
    /** of those scans, how many fail, and how many return summaries that were never really found */
    private static final float SCAN_FAILURE_CHANCE = 0.15f, SCAN_ARBITRARY_CHANCE = 0.25f;
    private static final float MUTATE_CHANCE = 0.25f;
    /**
     * How often a task declares {@code Ranges} rather than {@code RoutingKeys}, which is a materially different
     * lifecycle: {@code setupRangeLoadsExclusive} builds a {@code RangeTxnAndKeyScanner}, whose keys are
     * <em>discovered</em> rather than declared, and which registers a {@code KeyWatcher} on the commands-for-key cache
     * for the duration of the scan. That watcher may <b>adopt</b> a reference to any relevant key inside the ranges the
     * cache tells it about - the only place a task's reference set grows after setup, and so the only place the at-once
     * acquisition premise the cache queues rest on can be broken.
     *
     * <p>Adoption needs the scan to <em>not</em> already know about the key, so only every other key is discoverable (see
     * {@code Harness.isDiscoverable}); whatever a scan misses can only reach the task through the watcher.
     */
    private static final float RANGE_CHANCE = 0.15f;
    /** a range covers a contiguous window of this many keys; key 0 is the exclusive lower bound a TokenRange needs */
    private static final int MAX_KEYS_PER_RANGE = 3;

    /** thrown by the load function to exercise the load failure paths; nothing else may fail a task */
    static class InjectedLoadFailure extends RuntimeException
    {
        InjectedLoadFailure(Object key)
        {
            super("injected load failure for " + key);
        }
    }

    private static boolean isExpectedFailure(Throwable t)
    {
        for (Throwable cur = t ; cur != null ; cur = cur.getCause())
        {
            if (cur instanceof InjectedLoadFailure || cur instanceof ControllableRangeIndex.InjectedScanFailure)
                return true;
            // a task whose parent failed is cancelled, which is expected once we inject failures
            if (cur instanceof CancellationException)
                return true;
        }
        return false;
    }
    private static final float NESTED_CHANCE = 0.5f;
    private static final float BATCH_CHANCE = 0.3f;
    /**
     * children per sequenced batch; must not exceed the number of {@link ExecutionKind}s, as a batch gives each of its
     * children a distinct kind so that {@link AccordCacheEntryQueue#compare} totally orders them. Four children on one
     * key also outgrows the queue's initial capacity, exercising its growth and compaction.
     */
    private static final int MAX_BATCH_CHILDREN = 4;
    /** submissions per round, including the nested ones */
    private static final int MAX_TASKS = SUBMIT_THREADS * OUTER_LOOP * INNER_LOOP * (1 + MAX_BATCH_CHILDREN);
    private static final int NO_TASK = -1;
    private static final int[] NO_ORDINALS = new int[0];

    @Test
    public void signalLoopTest()
    {
        test(id -> new AccordExecutorSignalLoop(id, RUN_WITHOUT_LOCK, 4, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + id + '.' + i, new RecordingAgent()), true, true);
    }

    @Test
    public void asyncSubmitTest()
    {
        test(id -> new AccordExecutorAsyncSubmit(id, RUN_WITHOUT_LOCK, 4, i -> "Loop" + id + '.' + i, new RecordingAgent()), true, true);
    }

    /**
     * The same workload with {@code queue_key_ordering_enabled = false}, i.e. {@link AccordExecutor#CACHE_QUEUES_ENABLED}
     * off. Nothing about key-level ordering is expected to hold, as no task ever queues on an entry; what is validated is
     * that the off-switch is <em>whole</em> - every task still runs, completes, saves and releases.
     *
     * <p>Two things follow from the switch, and the workload's expectations are derived from them rather than hard-coded:
     * batching is defined over the positions the cache entries hand out, so disabling the queues disables non-sync
     * execution too and every task runs as {@code SYNC} (see {@link #effectiveLoadKeys}); and a
     * {@code BY_PRIORITY_ATOMIC} task takes no fifo position, so it is sequenced but provides no atomicity (see
     * {@link Harness#verifySequenced}).
     *
     * <p>Each simulation gets a fresh {@code InstanceClassLoader}, so the {@code static final} switch is re-derived per
     * test method; these can share a JVM with the enabled variants.
     */
    @Test
    public void signalLoopWithoutCacheQueuesTest()
    {
        test(id -> new AccordExecutorSignalLoop(id, RUN_WITHOUT_LOCK, 4, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + id + '.' + i, new RecordingAgent()), false, true);
    }

    @Test
    public void asyncSubmitWithoutCacheQueuesTest()
    {
        test(id -> new AccordExecutorAsyncSubmit(id, RUN_WITHOUT_LOCK, 4, i -> "Loop" + id + '.' + i, new RecordingAgent()), false, true);
    }

    /**
     * The same workload with {@code queue_nonsync_enabled = false} but the cache queues left <em>on</em>: every task runs
     * as {@code SYNC}, and the entry queues order them exactly as they order any other sync work. This is the one
     * combination the two switches allow that neither of the others covers - key ordering without batching - and it only
     * became reachable once {@code NONSYNC_ENABLED} was made to depend on {@code CACHE_QUEUES_ENABLED} rather than the
     * reverse.
     *
     * <p>Everything the enabled variants require still holds, ordering and atomicity included; only the batched-run
     * coverage is dropped, as there is nothing to batch (see {@link Harness#verifyRoundComplete}).
     */
    @Test
    public void signalLoopWithoutNonSyncTest()
    {
        test(id -> new AccordExecutorSignalLoop(id, RUN_WITHOUT_LOCK, 4, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + id + '.' + i, new RecordingAgent()), true, false);
    }

    @Test
    public void asyncSubmitWithoutNonSyncTest()
    {
        test(id -> new AccordExecutorAsyncSubmit(id, RUN_WITHOUT_LOCK, 4, i -> "Loop" + id + '.' + i, new RecordingAgent()), true, false);
    }

    /**
     * @param cacheQueues {@code queue_key_ordering_enabled}
     * @param nonSync     {@code queue_nonsync_enabled}, which the former disables regardless
     */
    private void test(SerializableFunction<Integer, AccordExecutor> executorFactory, boolean cacheQueues, boolean nonSync)
    {
        long seed = TEST_SEED.getLong(new SecureRandom().nextLong());
        System.out.println("AccordCommandStoreExecutorTest seed " + seed + " (override with -D" + TEST_SEED.getKey() + ')');
        simulate(arr(() -> {
                     try
                     {
                         // one JVM runs several of these methods, and each simulation re-initialises DatabaseDescriptor in a
                         // fresh InstanceClassLoader - but the platform MBean server is JVM-wide, so the second
                         // daemonInitialization would fail registering DynamicEndpointSnitch. We have no use for MBeans
                         ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(true);
                         DatabaseDescriptor.daemonInitialization();
                         AccordService.unsafeSetNoop();
                         // Initialise AccordSystemMetrics here, while we are still single-threaded. It is otherwise
                         // initialised lazily by whichever executor thread first pauses loading, and its <clinit>
                         // registers ThreadLocalCounters, which take a monitor the simulator intercepts and defers to its
                         // scheduler - all while holding the JVM's class-initialisation lock, which the simulator cannot
                         // see. A second thread arriving meanwhile blocks on that invisible lock, and the scheduler then
                         // waits forever for a thread it cannot know is blocked, wedging the whole simulation.
                         Invariants.require(AccordSystemMetrics.metrics != null);
                         // ASYNC and INCR tasks default to batches of 16..64 keys, far more than a test wants to
                         // declare; shrink them (before AccordExecutor initialises, as it reads them once) so that a
                         // handful of keys still needs several batches
                         AccordConfig config = DatabaseDescriptor.getAccord();
                         config.queue_nonsync_min_batch_size = MIN_BATCH;
                         config.queue_nonsync_max_batch_size = MAX_BATCH;
                         config.queue_nonsync_blocked_limit = MIN_BATCH + 1;
                         // the off-switches under test; like the sizes above they are read once, when AccordExecutor
                         // initialises. Non-sync execution is defined over the queue positions the cache entries hand out,
                         // so disabling those disables it too, whatever was asked for
                         config.queue_key_ordering_enabled = cacheQueues;
                         config.queue_nonsync_enabled = nonSync;
                         Invariants.require(AccordExecutor.CACHE_QUEUES_ENABLED == cacheQueues
                                            && AccordExecutor.NONSYNC_ENABLED == (cacheQueues && nonSync),
                                            "expected CACHE_QUEUES_ENABLED=%s and NONSYNC_ENABLED=%s, found %s and %s: read before they were set",
                                            cacheQueues, cacheQueues && nonSync,
                                            AccordExecutor.CACHE_QUEUES_ENABLED, AccordExecutor.NONSYNC_ENABLED);
                         Harness harness = new Harness(executorFactory);
                         harness.startStallWatchdog();

                         for (float loadDelayChance : new float[]{ 0f, 0.1f })
                         {
                             for (float sleepChance : new float[]{ 0f, 0.1f })
                             {
                                 System.out.printf("loadDelayChance %.2f, sleepChance %.2f%n", loadDelayChance, sleepChance);
                                 harness.loadDelayChance = loadDelayChance;
                                 harness.round(sleepChance);
                                 harness.modifyCachedEntries();
                             }
                         }

                         // and one round under a cache far smaller than its working set, with loads that fail
                         System.out.printf("loadDelayChance %.2f, sleepChance %.2f, loadFailureChance %.2f, tiny cache%n", 0.1f, 0.1f, LOAD_FAILURE_CHANCE);
                         harness.loadDelayChance = 0.1f;
                         harness.loadFailureChance = LOAD_FAILURE_CHANCE;
                         harness.setCapacity(TINY_CAPACITY, TINY_WORKING_SET);
                         harness.round(0.1f);
                         Invariants.require(harness.failedTaskCount.get() > 0, "no load failures were injected");

                         // finally verify that nothing was wedged by a failed load or an eviction
                         harness.loadFailureChance = 0f;
                         harness.scanFailureChance = 0f;
                         harness.setCapacity(AMPLE_CAPACITY, AMPLE_WORKING_SET);
                         harness.verifyRecovered();
                         harness.verifySaved();
                     }
                     catch (Throwable t)
                     {
                         throw new RuntimeException(t);
                     }
                 }),
                 () -> {}, seed);
    }

    /**
     * Records everything reported to the agent: on these paths any report indicates a broken internal invariant,
     * not a failed operation. We do not use {@code AccordAgent}, as reporting an exception there touches
     * {@code AccordSystemMetrics}, which requires a started {@code AccordService}.
     */
    public static class RecordingAgent extends TestAgent
    {
        static final List<Throwable> exceptions = new CopyOnWriteArrayList<>();

        @Override
        public void onException(Throwable t)
        {
            if (isExpectedFailure(t))
                return;

            exceptions.add(t);
            System.out.println("### agent.onException: " + t);
            t.printStackTrace(System.out);
        }

        @Override
        public void onException(Throwable t, String context)
        {
            onException(t);
        }
    }

    /** how a nested submission relates to the task that submits it */
    enum Nested
    {
        /** the same command store, so the same executor */
        SAME_STORE,
        /** a different command store on the same executor */
        SAME_EXECUTOR,
        /** a command store on another executor */
        OTHER_EXECUTOR
    }

    /**
     * One child of a {@link Batch}: a nested submission on its parent's own command store, declaring a subset of its
     * parent's keys and a prefix of its parent's txnIds, so that it inherits every reference it needs (and so is
     * legal for {@link ExecutionSequence#ATOMIC}, which requires a txnId subset, and a key subset if it is
     * not to fall back to running as soon as any one key is ready).
     */
    static class Child
    {
        final ExecutionSequence sequence;
        /** distinct within a batch, so that equal (inherited) positions are still totally ordered */
        final ExecutionKind kind;
        /** what we ask for, and what the executor will really run us as (see {@link #effectiveLoadKeys}) */
        final LoadKeys loadKeys, runAs;
        final int[] keyOrdinals, txnIdOrdinals;
        /** the order in which this child was submitted by its parent */
        final int submitIndex;
        int taskId = NO_TASK;
        /** the order in which this child ran, relative to its siblings */
        volatile int runIndex = NO_TASK;

        Child(ExecutionSequence sequence, ExecutionKind kind, LoadKeys loadKeys, LoadKeys runAs, int[] keyOrdinals, int[] txnIdOrdinals, int submitIndex)
        {
            this.sequence = sequence;
            this.kind = kind;
            this.loadKeys = loadKeys;
            this.runAs = runAs;
            this.keyOrdinals = keyOrdinals;
            this.txnIdOrdinals = txnIdOrdinals;
            this.submitIndex = submitIndex;
        }

        boolean isAtomic()
        {
            return sequence == ExecutionSequence.ATOMIC;
        }

        /**
         * Only tasks that load their keys up front are ordered against their siblings by the cache entry queues alone:
         * a batched task also has to wait for a batch of its keys to be ready, so no order is promised.
         */
        boolean isSequenced()
        {
            return sequence != ExecutionSequence.UNSEQUENCED && runAs == LoadKeys.SYNC;
        }

        boolean sharesAndGatedAlike(Child that)
        {   // txnId must be the same else queueing will be different
            return intersects(keyOrdinals, that.keyOrdinals) && Arrays.equals(txnIdOrdinals, that.txnIdOrdinals);
        }

        public String toString()
        {
            return "task " + taskId + " (" + sequence + ' ' + kind + ' ' + loadKeys + (runAs == loadKeys ? "" : "->" + runAs)
                   + " keys=" + Arrays.toString(keyOrdinals) + " txnIds=" + Arrays.toString(txnIdOrdinals) + " submitted " + submitIndex + " ran " + runIndex + ')';
        }
    }

    /** a task and the sequenced children it submits while running */
    static class Batch
    {
        final int parentTaskId;
        final Child[] children;
        /** assigns each child its {@link Child#runIndex}; safe as all of a batch run on one command store */
        final AtomicInteger runOrder = new AtomicInteger();
        volatile long parentPosition;

        Batch(int parentTaskId, Child[] children)
        {
            this.parentTaskId = parentTaskId;
            this.children = children;
        }
    }

    /** the only way to control the sequencing of a task, as {@link ExecutionContext#contextFor} does not */
    static class SeqContext implements ExecutionContext.Wrapped
    {
        final ExecutionContext wrapped;
        final ExecutionSequence sequence;
        final ExecutionKind kind;

        SeqContext(ExecutionContext wrapped, ExecutionSequence sequence, ExecutionKind kind)
        {
            this.wrapped = wrapped;
            this.sequence = sequence;
            this.kind = kind;
        }

        @Override public ExecutionContext wrapped() { return wrapped; }
        @Override public ExecutionSequence executionSequence() { return sequence; }
        @Override public ExecutionKind executionKind() { return kind; }
        /**
         * {@code SafeTask.initNonSync} requires every INCR task to declare itself idempotent, as an INCR task may
         * partially succeed and be retried. Our task bodies only verify state, so they are safe to re-run, and
         * {@link ExecutionContext#contextFor} does not offer a way to say so.
         */
        @Override public boolean isIdempotent() { return loadKeys() == LoadKeys.INCR; }
    }

    static class Harness
    {
        final AccordExecutor[] executors = new AccordExecutor[EXECUTORS];
        final Store[] stores = new Store[STORES];
        /** every store shares one key space, so a key is a distinct cache entry per store */
        final RoutingKey[] keys = new RoutingKey[KEYS];
        final TxnId[] txnIds = new TxnId[TXN_IDS];

        // per round
        AtomicInteger nextTaskId = new AtomicInteger();
        AtomicInteger outstanding = new AtomicInteger();
        AtomicIntegerArray notifications = new AtomicIntegerArray(MAX_TASKS);
        AtomicIntegerArray hasReturned = new AtomicIntegerArray(MAX_TASKS);
        /** for batched tasks: the keys declared, the keys processed so far, and the number of runs */
        AtomicIntegerArray declaredKeys = new AtomicIntegerArray(MAX_TASKS);
        AtomicIntegerArray processedKeys = new AtomicIntegerArray(MAX_TASKS);
        AtomicIntegerArray runs = new AtomicIntegerArray(MAX_TASKS);
        AtomicIntegerArray loadKeysOf = new AtomicIntegerArray(MAX_TASKS);
        /** the ExecutionSequence each task's context declared, so its body can verify what was assigned */
        AtomicIntegerArray sequenceOf = new AtomicIntegerArray(MAX_TASKS);
        /** which tasks declared Ranges rather than RoutingKeys, and every key ordinal each has ever referenced */
        AtomicIntegerArray isRangeTask = new AtomicIntegerArray(MAX_TASKS);
        AtomicIntegerArray heldKeys = new AtomicIntegerArray(MAX_TASKS);
        SafeTask<?>[] tasks = new SafeTask<?>[MAX_TASKS];
        final List<Throwable> failures = new CopyOnWriteArrayList<>();
        /** coverage: how many nested submissions of each kind we have verified */
        final AtomicIntegerArray nestedCount = new AtomicIntegerArray(Nested.values().length);
        /** coverage: how many ordering constraints of each kind we have verified */
        final AtomicIntegerArray orderingCount = new AtomicIntegerArray(Ordering.values().length);
        /**
         * An atomic child submitted <em>after</em> a prioritised sibling has to overtake it: its {@code addFifo} must
         * revoke a sibling that may already lead everything it declared and be sitting in the run queue. Nothing
         * sequences its placement against that sibling's prepare, so this is recorded rather than required.
         */
        final AtomicInteger overtookPriority = new AtomicInteger(), failedToOvertakePriority = new AtomicInteger();
        /** tasks that declared RECOVERY and so went through a range scan, and how those scans were made to behave */
        final AtomicInteger rangeScans = new AtomicInteger(), scanFailures = new AtomicInteger(), scanArbitrary = new AtomicInteger();
        /**
         * Tasks that declared Ranges, and runs that touched a key the scan could not have discovered - i.e. one the
         * {@code KeyWatcher} must have adopted. The second is the point of the axis, so it is gated, not merely printed.
         */
        final AtomicInteger rangeTasks = new AtomicInteger(), adoptedCount = new AtomicInteger();
        /** zeroed alongside {@code loadFailureChance} before {@code verifyRecovered}, which asserts nothing fails */
        float scanFailureChance = SCAN_FAILURE_CHANCE;
        /** coverage: how many batched tasks of each shape we have verified */
        final AtomicIntegerArray batchedCount = new AtomicIntegerArray(Batched.values().length);
        List<Batch> batches = new CopyOnWriteArrayList<>();

        volatile float loadDelayChance;
        volatile float loadFailureChance;
        volatile boolean submitNested = true;
        volatile boolean submitBatched = true;
        /** how many loads we have failed, and how many tasks failed or were cancelled as a result */
        final AtomicInteger loadFailures = new AtomicInteger();
        final AtomicInteger failedTaskCount = new AtomicInteger();
        /** entries we have modified, and entries we have been asked to save */
        final Set<String> modified = ConcurrentHashMap.newKeySet();
        final Set<String> saved = ConcurrentHashMap.newKeySet();
        final AtomicInteger saveCount = new AtomicInteger();
        AtomicIntegerArray failedTasks = new AtomicIntegerArray(MAX_TASKS);

        /**
         * Install a {@link ControllableRangeIndex}, so range scans run their real loader lifecycle and only their
         * outcome is ours to choose. Keyed off the primary txnId, so a task behaves the same way on every attempt.
         */
        private void installRangeScanner()
        {
            AccordCommandStore.unsafeRangeIndexFactory = store -> newRangeIndex(store);
        }

        /**
         * A {@link ControllableRangeIndex} per store, seeded with the keys its scans will <em>discover</em> - what a real
         * scan would find already persisted. Deliberately not all of them: a key a scan misses can still reach the task
         * through the {@code KeyWatcher}, which is the path this axis exists to exercise.
         */
        private ControllableRangeIndex newRangeIndex(AccordCommandStore store)
        {
            ControllableRangeIndex index = new ControllableRangeIndex(store, primaryTxnId -> {
                if (primaryTxnId == null)
                    return ControllableRangeIndex.Outcome.NOTHING;
                int choice = Math.abs(primaryTxnId.hashCode() % 100);
                if (choice < 100 * scanFailureChance)
                {
                    scanFailures.incrementAndGet();
                    return ControllableRangeIndex.Outcome.FAIL;
                }
                if (choice < 100 * (scanFailureChance + SCAN_ARBITRARY_CHANCE))
                {
                    scanArbitrary.incrementAndGet();
                    return ControllableRangeIndex.Outcome.SUMMARIES;
                }
                return ControllableRangeIndex.Outcome.NOTHING;
            });
            for (int i = 0 ; i < KEYS ; ++i)
            {
                if (isDiscoverable(i))
                    index.discover((TokenKey) keys[i]);
            }
            return index;
        }

        /**
         * Whether a scan finds key {@code ordinal} for itself: every other key, so half the key space is reachable only by
         * adoption. A function of the ordinal, so a scan behaves the same way on every attempt.
         */
        private static boolean isDiscoverable(int ordinal)
        {
            return (ordinal & 1) == 0;
        }

        Harness(SerializableFunction<Integer, AccordExecutor> executorFactory)
        {
            installRangeScanner();
            TableId tableId = TableId.fromUUID(new java.util.UUID(0, 1));
            IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
            // in *token* order, so that keys[lo]..keys[hi] is a contiguous window a TokenRange can express
            TokenKey[] tokenKeys = new TokenKey[KEYS];
            for (int i = 0 ; i < KEYS ; ++i)
                tokenKeys[i] = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(i)));
            Arrays.sort(tokenKeys);
            System.arraycopy(tokenKeys, 0, keys, 0, KEYS);
            for (int i = 0 ; i < TXN_IDS ; ++i)
                txnIds[i] = TxnId.fromValues(1, 1 + i, 0, new Id(1));

            for (int e = 0 ; e < EXECUTORS ; ++e)
            {
                executors[e] = executorFactory.apply(e);
                for (AccordCache.Type<?, ?, ?> type : executors[e].cacheUnsafe().types())
                    setSyntheticFunctions(type);
            }
            for (int s = 0 ; s < STORES ; ++s)
                stores[s] = new Store(s, s % EXECUTORS, executors[s % EXECUTORS], tableId, partitioner);
        }

        /**
         * Loads are synthetic: an empty {@link CommandsForKey} for a key (a non-null value, so that the entry is worth
         * caching and can be modified), and an uninitialised value for a txnId, as a {@code Command} is not something a
         * test can cheaply invent. Saves are synthetic too, so that we exercise the save path without the schema and
         * commit log the real one needs; they only record what they were asked to persist.
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private void setSyntheticFunctions(AccordCache.Type type)
        {
            type.unsafeSetLoadFunction((BiFunction<AccordCommandStore, Object, Object>) (ignoreStore, key) -> {
                maybePark(loadDelayChance);
                float failureChance = loadFailureChance;
                if (failureChance > 0 && ThreadLocalRandom.current().nextFloat() < failureChance)
                {
                    loadFailures.incrementAndGet();
                    throw new InjectedLoadFailure(key);
                }
                return key instanceof RoutingKey ? new CommandsForKey((RoutingKey) key) : null;
            });
            type.unsafeSetSaveFunction((QuadFunction<AccordCommandStore, Object, Object, Object, Runnable>) (store, key, value, shrunk) -> {
                Invariants.require(value != null || shrunk != null, "asked to save nothing for %s", key);
                saved.add(store.id() + ":" + key);
                saveCount.incrementAndGet();
                return null;
            });
        }

        void round(float sleepChance) throws ExecutionException, InterruptedException
        {
            nextTaskId = new AtomicInteger();
            outstanding = new AtomicInteger();
            notifications = new AtomicIntegerArray(MAX_TASKS);
            hasReturned = new AtomicIntegerArray(MAX_TASKS);
            declaredKeys = new AtomicIntegerArray(MAX_TASKS);
            processedKeys = new AtomicIntegerArray(MAX_TASKS);
            runs = new AtomicIntegerArray(MAX_TASKS);
            loadKeysOf = new AtomicIntegerArray(MAX_TASKS);
            sequenceOf = new AtomicIntegerArray(MAX_TASKS);
            isRangeTask = new AtomicIntegerArray(MAX_TASKS);
            heldKeys = new AtomicIntegerArray(MAX_TASKS);
            failedTasks = new AtomicIntegerArray(MAX_TASKS);
            tasks = new SafeTask<?>[MAX_TASKS];
            batches = new CopyOnWriteArrayList<>();

            ExecutorPlus submit = executorFactory().pooled("submit", SUBMIT_THREADS);
            try
            {
                List<Future<?>> submitting = new ArrayList<>();
                for (int i = 0 ; i < SUBMIT_THREADS ; ++i)
                {
                    int id = i;
                    submitting.add(submit.submit(() -> {
                        for (int outer = 0 ; outer < OUTER_LOOP ; ++outer)
                        {
                            CountDownLatch inner = CountDownLatch.newCountDownLatch(INNER_LOOP);
                            for (int j = 0 ; j < INNER_LOOP ; ++j)
                            {
                                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                                Store store = stores[rnd.nextInt(STORES)];
                                // a submission either nests one task somewhere in the topology, or a batch of
                                // sequenced tasks on its own store, or nothing at all
                                Nested nested = null;
                                boolean batch = false;
                                float choose = rnd.nextFloat();
                                if (!submitNested) {}
                                else if (choose < BATCH_CHANCE) batch = true;
                                else if (choose < BATCH_CHANCE + NESTED_CHANCE) nested = Nested.values()[rnd.nextInt(Nested.values().length)];
                                // a task either loads its keys before it runs, or is run over batches of them
                                float loadChoose = submitBatched ? rnd.nextFloat() : 1f;
                                LoadKeys loadKeys = loadChoose < ASYNC_CHANCE ? LoadKeys.ASYNC
                                                    : loadChoose < ASYNC_CHANCE + INCR_CHANCE ? LoadKeys.INCR
                                                    : LoadKeys.SYNC;
                                submitOne(store, null, null, nested, batch, loadKeys, null, null, sleepChance, inner);
                            }
                            inner.awaitUninterruptibly();
                            System.out.println("Loop " + id + '.' + outer);
                        }
                    }));
                }
                for (Future<?> f : submitting)
                    f.get();
            }
            finally
            {
                submit.shutdown();
            }

            awaitOutstanding();
            verifyRoundComplete();
        }

        /**
         * @param parent       the task submitting this one, or null; note that we are handed the parent's
         *                     {@link SafeTask} rather than looking it up, as a task may run (and submit) before its
         *                     own submission has returned to the thread that submitted it
         * @param relationship how this submission relates to {@code parent}, or null if there is no parent
         * @param nested       if not null, the relationship of a further task this one should submit while running
         * @param batch        if true, this task should submit a batch of sequenced children while running
         * @param loadKeys     whether this task loads its keys up front, or is run over batches of them
         * @param declareKeys  the keys this task must declare, or null to choose them at random
         * @param declareTxnIds the txnIds this task must declare, or null to choose them at random
         * @param done         if not null, decremented when this submission is notified
         */
        private void submitOne(Store store, SafeTask<?> parent, Nested relationship, Nested nested, boolean batch, LoadKeys loadKeys, int[] declareKeys, int[] declareTxnIds, float sleepChance, CountDownLatch done)
        {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            int taskId = nextTaskId.getAndIncrement();
            outstanding.incrementAndGet();
            int maxKeys = loadKeys == LoadKeys.SYNC ? MAX_KEYS_PER_TASK : MAX_KEYS_PER_BATCHED_TASK;
            // a range task's keys are a contiguous window in token order, as that is what a range can express; key 0 is
            // never in it, as a TokenRange is (start, end] and the window's lower neighbour is its exclusive start
            boolean isRange = declareKeys == null && rnd.nextFloat() < RANGE_CHANCE;
            int[] keyOrdinals;
            if (isRange)
            {
                int width = 1 + rnd.nextInt(MAX_KEYS_PER_RANGE);
                int lo = 1 + rnd.nextInt(KEYS - width);
                keyOrdinals = new int[width];
                for (int i = 0 ; i < width ; ++i)
                    keyOrdinals[i] = lo + i;
            }
            else
            {
                keyOrdinals = declareKeys != null ? declareKeys : distinct(rnd, KEYS, 1 + rnd.nextInt(maxKeys));
            }
            int[] chooseTxnIds = declareTxnIds != null ? declareTxnIds
                                 : distinct(rnd, TXN_IDS, rnd.nextInt(1 + MAX_TXN_IDS_PER_TASK));
            // a range scan is always on behalf of some transaction, so a range task declares at least one txnId
            if (isRange && chooseTxnIds.length == 0)
                chooseTxnIds = new int[]{ rnd.nextInt(TXN_IDS) };
            final int[] txnIdOrdinals = chooseTxnIds;
            // what the task will really run as; the context still asks for what we chose, so that the downgrade is exercised
            LoadKeys runAs = effectiveLoadKeys(loadKeys);
            // for a range task this is the *upper bound* on what it may touch: which of those keys it really references is
            // discovered by the scan, or adopted from the cache while the scan runs
            declaredKeys.set(taskId, mask(keyOrdinals));
            loadKeysOf.set(taskId, runAs.ordinal());
            if (isRange)
            {
                isRangeTask.set(taskId, 1);
                rangeTasks.incrementAndGet();
            }

            // RECOVERY only where there is a txnId to recover for, and only for top-level work (see RECOVERY_CHANCE)
            LoadKeysFor loadKeysFor = parent == null && txnIdOrdinals.length > 0 && rnd.nextFloat() < RECOVERY_CHANCE
                                      ? LoadKeysFor.RECOVERY : LoadKeysFor.READ_WRITE;
            // a range task scans whatever its loadKeysFor, as long as it is not WRITE; a key task scans only for RECOVERY
            if (loadKeysFor == LoadKeysFor.RECOVERY || isRange)
                rangeScans.incrementAndGet();
            // Sequencing is imposed on every task, not only a pre-set-up child: a task submitted at top level or for
            // another store takes its context's declared sequence. An INCR task that declares a txnId may not be
            // UNSEQUENCED; ATOMIC is meaningful with no parent only for an INCR task, whose runs it holds together.
            ExecutionSequence sequence;
            if (runAs == LoadKeys.INCR)
                sequence = rnd.nextBoolean() ? ExecutionSequence.BY_PRIORITY : ExecutionSequence.ATOMIC;
            else
                sequence = rnd.nextBoolean() ? ExecutionSequence.BY_PRIORITY : ExecutionSequence.UNSEQUENCED;
            sequenceOf.set(taskId, sequence.ordinal());

            ExecutionContext context = isRange ? rangeContextFor(taskId, keyOrdinals, txnIdOrdinals, loadKeys, loadKeysFor)
                                               : contextFor(taskId, keyOrdinals, txnIdOrdinals, loadKeys, loadKeysFor);
            context = new SeqContext(context, sequence, context.executionKind());
            Cancellable submitted =
                store.store.execute(context, (Consumer<? super SafeCommandStore>) safeStore ->
                                             body(store, taskId, keyOrdinals, txnIdOrdinals, safeStore, sleepChance, (task, runKeys) -> {
                                                 if (nested != null)
                                                     submitOne(target(store, nested), task, nested, null, false, LoadKeys.SYNC, null, null, sleepChance, null);
                                                 if (batch && runs.get(taskId) == 1)
                                                     submitBatch(store, task, taskId, runKeys, keyOrdinals, txnIdOrdinals, sleepChance);
                                             }),
                                    (success, fail) -> notify(taskId, fail, done));
            tasks[taskId] = (SafeTask<?>) submitted;
            if (parent != null)
                verifyNested(store, parent, (SafeTask<?>) submitted, relationship);
        }

        /**
         * Submit a batch of children, in order, from within the parent's execution. A child that loads its keys up
         * front declares a subset of the keys the parent is running with, so that it inherits every reference it needs
         * and is queued on entries the parent holds - which is what makes its execution order predictable, and what an
         * atomic child requires if it is not to fall back to running as soon as any one of its keys is ready. A batched
         * child may declare any of the parent's keys, including those outside the parent's current batch.
         *
         * @param runKeys  the keys the parent is running with, i.e. those a child can inherit
         * @param declared every key the parent declared
         */
        private void submitBatch(Store store, SafeTask<?> parent, int parentTaskId, int[] runKeys, int[] declared, int[] parentTxnIds, float sleepChance)
        {
            // A range parent may be running with no keys at all, because its scan discovered none. A child that loads up
            // front has to declare a non-empty subset of the keys the parent is running with, so there is no batch to
            // submit for such a parent; every other parent covers the axis. Without this newBatch reaches
            // rnd.nextInt(0) and the parent fails with "bound must be positive" - an intermittent harness failure that
            // has nothing to do with the executor.
            if (runKeys.length == 0)
                return;
            Batch batch = newBatch(ThreadLocalRandom.current(), parentTaskId, runKeys, declared, parentTxnIds);
            batch.parentPosition = parent.position;
            for (Child child : batch.children)
            {
                int taskId = nextTaskId.getAndIncrement();
                outstanding.incrementAndGet();
                child.taskId = taskId;
                declaredKeys.set(taskId, mask(child.keyOrdinals));
                loadKeysOf.set(taskId, child.runAs.ordinal());
                sequenceOf.set(taskId, child.sequence.ordinal());

                ExecutionContext context = new SeqContext(contextFor(taskId, child.keyOrdinals, child.txnIdOrdinals, child.loadKeys), child.sequence, child.kind);
                Cancellable submitted =
                    store.store.execute(context, (Consumer<? super SafeCommandStore>) safeStore ->
                                                 body(store, taskId, child.keyOrdinals, child.txnIdOrdinals, safeStore, sleepChance, (task, runKeysIgnored) -> {
                                                     // a batched child runs more than once; its order is its first run
                                                     if (child.runIndex == NO_TASK)
                                                         child.runIndex = batch.runOrder.getAndIncrement();
                                                     Invariants.require(hasReturned.get(batch.parentTaskId) == 1,
                                                                        "%s ran before its parent task %d returned", child, batch.parentTaskId);
                                                     Invariants.require(task.position == batch.parentPosition,
                                                                        "%s did not inherit its parent's position %d", child, batch.parentPosition);
                                                 }),
                                        (success, fail) -> notify(taskId, fail, null));
                tasks[taskId] = (SafeTask<?>) submitted;
                verifyNested(store, parent, (SafeTask<?>) submitted, Nested.SAME_STORE);
                verifySequenced((SafeTask<?>) submitted, child.sequence, child);
            }
        }

        private void notify(int taskId, Throwable fail, CountDownLatch done)
        {
            notifications.incrementAndGet(taskId);
            if (fail != null)
            {
                // only an injected load failure, or the cancellation of a task whose parent failed, may fail a task
                if (isExpectedFailure(fail))
                {
                    failedTasks.set(taskId, 1);
                    failedTaskCount.incrementAndGet();
                }
                else
                {
                    if (failures.isEmpty())
                    {
                        System.out.println("### task " + taskId + " failed: " + fail);
                        fail.printStackTrace(System.out);
                    }
                    failures.add(fail);
                }
            }
            if (done != null)
                done.decrement();
            outstanding.decrementAndGet();
        }

        void setCapacity(long capacity, long workingSet)
        {
            for (AccordExecutor executor : executors)
            {
                executor.executeDirectlyWithLock(() -> {
                    executor.setCapacity(capacity);
                    executor.setWorkingSetSize(workingSet);
                });
            }
        }

        /**
         * Modify some of the entries the round left cached, so that they have to be persisted before they can be evicted.
         * Done to the entry rather than through {@code SafeCommandsForKey.set}, as two empty {@link CommandsForKey} are
         * not a change as far as {@code hasChanges} is concerned. Nothing references these entries and we hold the cache
         * lock, so this is the same transition {@code AccordCache.release} makes for a task that modified its state.
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        void modifyCachedEntries()
        {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            for (Store store : stores)
            {
                try (AccordCommandStore.ExclusiveCaches caches = store.store.lockCaches())
                {
                    for (AccordCacheEntry<?, ?, ?> entry : caches.commandsForKeys())
                    {
                        if (entry.references() != 0 || entry.status() != AccordCacheEntry.Status.LOADED || rnd.nextFloat() >= MUTATE_CHANCE)
                            continue;

                        ((AccordCacheEntry) entry).setExclusive(new CommandsForKey((RoutingKey) entry.key()));
                        modified.add(store.store.id() + ":" + entry.key());
                    }
                }
            }
        }

        /**
         * Nothing we modified may be lost: shrink the cache to nothing so that every entry has to be evicted, and
         * require that each one we modified was persisted first.
         */
        void verifySaved() throws ExecutionException, InterruptedException
        {
            Invariants.require(!modified.isEmpty(), "no entries were modified");
            // shrink to nothing, so that every unreferenced entry has to go. Note that a working capacity of zero cannot
            // be satisfied by any task, so AccordExecutor treats it as unlimited - without that, loading pauses on every
            // check and never resumes, and this phase wedges (see refreshCapacity)
            setCapacity(0, 0);
            // a completing task is what drives eviction, so give each store some
            verifyRecovered();

            for (int attempt = 0 ; ; ++attempt)
            {
                String cached = cachedExclusive();
                if (cached == null)
                    break;

                if (attempt > 0 && (attempt % 1000) == 0)
                    System.out.println("draining after " + attempt + " attempts: " + cached);
                Invariants.require(attempt < 10000, "%s", cached);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }

            for (String entry : modified)
            {
                if (saved.contains(entry))
                    continue;

                StringBuilder detail = new StringBuilder(entry).append(" was modified but never saved");
                detail.append("; saveCount=").append(saveCount.get()).append(" saved=").append(saved.size()).append(" modified=").append(modified.size());
                for (Store store : stores)
                {
                    try (AccordCommandStore.ExclusiveCaches caches = store.store.lockCaches())
                    {
                        for (AccordCacheEntry<?, ?, ?> cached : caches.commandsForKeys())
                        {
                            if (entry.equals(store.store.id() + ":" + cached.key()))
                                detail.append("; still cached as ").append(cached);
                        }
                    }
                }
                Invariants.require(false, "%s", detail);
            }
            System.out.println("saved " + saveCount.get() + " times for " + modified.size() + " modified entries");
        }

        /** any entry still cached once we have shrunk the cache to nothing and drained it */
        private String cachedExclusive()
        {
            for (Store store : stores)
            {
                try (AccordCommandStore.ExclusiveCaches caches = store.store.lockCaches())
                {
                    for (AccordCacheEntry<?, ?, ?> entry : caches.commandsForKeys())
                        return "store " + store.index + " still caches " + entry;
                    for (AccordCacheEntry<?, ?, ?> entry : caches.commands())
                        return "store " + store.index + " still caches " + entry;
                }
            }
            return null;
        }

        /**
         * A failed load must not wedge its cache entry, and neither must eviction: with failures disabled, one task per
         * key and one per txnId of every store must now succeed.
         */
        void verifyRecovered() throws ExecutionException, InterruptedException
        {
            nextTaskId = new AtomicInteger();
            outstanding = new AtomicInteger();
            notifications = new AtomicIntegerArray(MAX_TASKS);
            hasReturned = new AtomicIntegerArray(MAX_TASKS);
            declaredKeys = new AtomicIntegerArray(MAX_TASKS);
            processedKeys = new AtomicIntegerArray(MAX_TASKS);
            runs = new AtomicIntegerArray(MAX_TASKS);
            loadKeysOf = new AtomicIntegerArray(MAX_TASKS);
            sequenceOf = new AtomicIntegerArray(MAX_TASKS);
            isRangeTask = new AtomicIntegerArray(MAX_TASKS);
            heldKeys = new AtomicIntegerArray(MAX_TASKS);
            failedTasks = new AtomicIntegerArray(MAX_TASKS);
            tasks = new SafeTask<?>[MAX_TASKS];
            batches = new CopyOnWriteArrayList<>();

            for (Store store : stores)
            {
                for (int k = 0 ; k < KEYS ; ++k)
                    submitOne(store, null, null, null, false, LoadKeys.SYNC, new int[]{ k }, NO_ORDINALS, 0f, null);
                for (int t = 0 ; t < TXN_IDS ; ++t)
                    submitOne(store, null, null, null, false, LoadKeys.SYNC, new int[]{ 0 }, new int[]{ t }, 0f, null);
            }

            awaitOutstanding();
            Invariants.require(failures.isEmpty(), "%s", failures);
            Invariants.require(RecordingAgent.exceptions.isEmpty(), "%s", RecordingAgent.exceptions);
            int lastTaskId = nextTaskId.get();
            for (int taskId = 0 ; taskId < lastTaskId ; ++taskId)
            {
                Invariants.require(notifications.get(taskId) == 1, "task %d was notified %d times", taskId, notifications.get(taskId));
                Invariants.require(failedTasks.get(taskId) == 0, "task %d failed after we stopped failing loads", taskId);
                Invariants.require(hasReturned.get(taskId) == 1, "task %d did not run", taskId);
            }
            awaitReleased();
            System.out.println("recovered: " + lastTaskId + " tasks ran after " + loadFailures.get() + " failed loads");
        }

        /**
         * As {@link #contextFor}, but declaring {@code Ranges}: a contiguous window {@code (keys[lo-1], keys[hi]]}, which is
         * the domain that sends a task through {@code setupRangeLoadsExclusive} and its {@code RangeTxnAndKeyScanner}.
         */
        private ExecutionContext rangeContextFor(int taskId, int[] keyOrdinals, int[] txnIdOrdinals, LoadKeys loadKeys, LoadKeysFor loadKeysFor)
        {
            TxnId primary = txnIdOrdinals.length > 0 ? txnIds[txnIdOrdinals[0]] : null;
            TxnId additional = txnIdOrdinals.length > 1 ? txnIds[txnIdOrdinals[1]] : null;
            int lo = keyOrdinals[0], hi = keyOrdinals[keyOrdinals.length - 1];
            Invariants.require(lo > 0, "a range's exclusive start is the key below its first member, so key 0 cannot be in it");
            Ranges ranges = Ranges.of(TokenRange.create((TokenKey) keys[lo - 1], (TokenKey) keys[hi]));
            return ExecutionContext.contextFor(primary, additional, ranges, loadKeys, loadKeysFor, "task" + taskId);
        }

        private ExecutionContext contextFor(int taskId, int[] keyOrdinals, int[] txnIdOrdinals, LoadKeys loadKeys)
        {
            return contextFor(taskId, keyOrdinals, txnIdOrdinals, loadKeys, LoadKeysFor.READ_WRITE);
        }

        /**
         * @param loadKeysFor RECOVERY is what drives a task through {@code RangeTxnScanner}: setupKeyLoadsExclusive
         *                    starts a scan, the task passes through SCANNING_RANGES, and setup re-enters presetup when
         *                    the scan completes - a second pass over refs that are already queued.
         */
        private ExecutionContext contextFor(int taskId, int[] keyOrdinals, int[] txnIdOrdinals, LoadKeys loadKeys, LoadKeysFor loadKeysFor)
        {
            TxnId primary = txnIdOrdinals.length > 0 ? txnIds[txnIdOrdinals[0]] : null;
            TxnId additional = txnIdOrdinals.length > 1 ? txnIds[txnIdOrdinals[1]] : null;
            return ExecutionContext.contextFor(primary, additional, keys(keyOrdinals), loadKeys, loadKeysFor, "task" + taskId);
        }

        private Batch newBatch(ThreadLocalRandom rnd, int parentTaskId, int[] inheritable, int[] declared, int[] parentTxnIds)
        {
            ExecutionKind[] kinds = ExecutionKind.values().clone();
            for (int i = kinds.length - 1 ; i > 0 ; --i)
            {
                int swap = rnd.nextInt(i + 1);
                ExecutionKind tmp = kinds[i]; kinds[i] = kinds[swap]; kinds[swap] = tmp;
            }

            Child[] children = new Child[2 + rnd.nextInt(MAX_BATCH_CHILDREN - 1)];
            for (int i = 0 ; i < children.length ; ++i)
            {
                float loadChoose = rnd.nextFloat();
                LoadKeys loadKeys = loadChoose < ASYNC_CHANCE ? LoadKeys.ASYNC
                                    : loadChoose < ASYNC_CHANCE + INCR_CHANCE ? LoadKeys.INCR
                                    : LoadKeys.SYNC;
                int[] txnIdOrdinals = Arrays.copyOf(parentTxnIds, rnd.nextInt(parentTxnIds.length + 1));
                // what the child will really run as; see effectiveLoadKeys
                LoadKeys runAs = effectiveLoadKeys(loadKeys);
                // An INCR task is upgraded to a fifo claim on its first run, which revokes the whole bag's permission to
                // run - so UNSEQUENCED blocks more than BY_PRIORITY would, and is forbidden where it declares a txnId.
                // BY_PRIORITY is permitted: the inversion that once forbade it cannot arise, as the fifo upgrade happens
                // in the same pass that takes the lock.
                ExecutionSequence sequence = runAs == LoadKeys.INCR && txnIdOrdinals.length > 0
                                             ? (rnd.nextBoolean() ? ExecutionSequence.ATOMIC : ExecutionSequence.BY_PRIORITY)
                                             : SEQUENCES[rnd.nextInt(SEQUENCES.length)];
                // a batched child may declare keys outside the parent's current batch; one that loads up front may not,
                // as an atomic task that is not a subset of its parent's keys relies on its own batching to make
                // progress - and preSetup sets nonSync.alwaysReady on it, which is null unless it is batched (bug 3)
                int[] pool = runAs == LoadKeys.SYNC ? inheritable : declared;
                Invariants.require(pool.length > 0, "a child must declare at least one key, so its pool cannot be empty");
                int[] keyOrdinals = subset(rnd, pool, 1 + rnd.nextInt(pool.length));
                children[i] = new Child(sequence, kinds[i], loadKeys, runAs, keyOrdinals, txnIdOrdinals, i);
            }
            Batch batch = new Batch(parentTaskId, children);
            batches.add(batch);
            return batch;
        }

        private void body(Store store, int taskId, int[] declared, int[] txnIdOrdinals, SafeCommandStore safeStore, float sleepChance, BiConsumer<SafeTask<?>, int[]> whileRunning)
        {
            SafeTask<?> task = ((SaferCommandStore) safeStore).task;
            // a task may run before its submission has returned, so publish it from here as well
            tasks[taskId] = task;
            int run = runs.incrementAndGet(taskId);

            // Its sequence is assigned by preSetup for a pre-set-up child and by submitExclusiveMayThrow for everything
            // else, so for anything submitted asynchronously the only place it is reliably observable is from inside the
            // task itself - by which point setup, and the fifo upgrade at first prepare, have both happened.
            verifySequenced(task, SEQUENCES[sequenceOf.get(taskId)], "task" + taskId);

            // one executor per thread: we must not be inside any other executor
            int owning = 0;
            for (AccordExecutor executor : executors)
            {
                if (executor.isOwningThread())
                {
                    Invariants.require(executor == store.executor, "task %d of store %d is executing inside executor %s", taskId, store.index, executor.executorId());
                    ++owning;
                }
            }
            Invariants.require(owning <= 1, "task %d is executing inside %d executors", taskId, owning);

            // A batched task is run over a batch of the keys it declared, not all of them. A *range* task declared no keys at
            // all: its set is whatever the scan discovered plus whatever the KeyWatcher adopted, and adoption can grow it
            // after submission - so read it from the task and check it against the range instead.
            boolean isRange = isRangeTask.get(taskId) != 0;
            int[] keyOrdinals = task.isSync() ? (isRange ? referencedOrdinals(taskId, task) : declared)
                                             : verifyBatch(store, taskId, task, declared, run, isRange);
            if (isRange)
            {
                int declaredMask = declaredKeys.get(taskId);
                Invariants.require((mask(keyOrdinals) & ~declaredMask) == 0,
                                   "range task %d ran with keys %s outside the range it declared (%s)",
                                   taskId, Arrays.toString(keyOrdinals), Integer.toBinaryString(declaredMask));
                heldKeys.accumulateAndGet(taskId, mask(keyOrdinals), (a, b) -> a | b);
                if (Integer.bitCount(mask(keyOrdinals)) > Integer.bitCount(discoverableWithin(declaredMask)))
                    adoptedCount.incrementAndGet();
            }

            // declared access: we must hold a reference to an entry of our own store for everything we are running with
            for (int k : keyOrdinals)
                verifyReference(store, taskId, task, keys[k], "key " + k);
            for (int t : txnIdOrdinals)
                verifyReference(store, taskId, task, txnIds[t], "txnId " + t);
            for (int k : keyOrdinals)
                safeStore.ifLoadedAndInitialised(keys[k]);
            for (int t : txnIdOrdinals)
                safeStore.ifInitialised(txnIds[t]);

            // an INCR task that declared a txnId locks it for the whole of its execution, not one batch, so we take it
            // on our first run and hold it until our last; anyone else running with it meanwhile is a violation
            boolean holdsTxnIds = task.isIncremental() && txnIdOrdinals.length > 0;
            boolean takeTxnIds = !holdsTxnIds || run == 1;
            boolean releaseTxnIds = !holdsTxnIds || task.isIncrementalFinishing();

            // mutual exclusion: nobody else may be executing with any key or txnId of this store that we hold
            int keysTaken = 0, txnIdsTaken = 0;
            try
            {
                while (keysTaken < keyOrdinals.length)
                {
                    int k = keyOrdinals[keysTaken];
                    Invariants.require(store.keyOwner.compareAndSet(k, NO_TASK, taskId),
                                       "task %d ran concurrently with task %d, which also declared key %d of store %d",
                                       taskId, store.keyOwner.get(k), k, store.index);
                    ++keysTaken;
                }
                while (takeTxnIds && txnIdsTaken < txnIdOrdinals.length)
                {
                    int t = txnIdOrdinals[txnIdsTaken];
                    Invariants.require(store.txnIdOwner.compareAndSet(t, NO_TASK, taskId),
                                       "task %d ran concurrently with task %d, which also declared txnId %d of store %d",
                                       taskId, store.txnIdOwner.get(t), t, store.index);
                    ++txnIdsTaken;
                }
                if (holdsTxnIds)
                {
                    for (int t : txnIdOrdinals)
                    {
                        Invariants.require(store.txnIdOwner.get(t) == taskId,
                                           "task %d lost txnId %d of store %d to task %d between batches",
                                           taskId, t, store.index, store.txnIdOwner.get(t));
                    }
                }

                if (whileRunning != null)
                    whileRunning.accept(task, keyOrdinals);

                maybePark(sleepChance);
            }
            finally
            {
                while (keysTaken > 0)
                    store.keyOwner.set(keyOrdinals[--keysTaken], NO_TASK);
                if (releaseTxnIds)
                {
                    for (int t : txnIdOrdinals)
                        store.txnIdOwner.set(t, NO_TASK);
                }
                hasReturned.set(taskId, 1);
            }
        }

        /**
         * A task that does not load its keys up front is instead run over batches of them: verify that this run's batch
         * is a non-empty subset of the keys we declared, that we have not already processed any of them, and that we no
         * longer reference any key we have processed, i.e. that each run holds exactly the keys it still needs.
         */
        private int[] verifyBatch(Store store, int taskId, SafeTask<?> task, int[] declared, int run, boolean isRange)
        {
            Invariants.require(task.isNonSync(), "task %d is not batched", taskId);
            Invariants.require(task.nonSync().active != null, "task %d is running without a batch", taskId);
            int[] active = ordinalsOf(taskId, task.nonSync().active);
            int activeMask = mask(active), declaredMask = mask(declared);

            Invariants.require(active.length > 0, "task %d ran with an empty batch", taskId);
            Invariants.require(active.length <= MAX_BATCH, "task %d ran with %d keys, more than the batch size", taskId, active.length);
            Invariants.require((activeMask & ~declaredMask) == 0, "task %d ran with keys %s it did not declare", taskId, Arrays.toString(active));

            int processed = processedKeys.get(taskId);
            Invariants.require((activeMask & processed) == 0, "task %d ran with keys %s it had already processed", taskId, Arrays.toString(active));
            processedKeys.set(taskId, processed | activeMask);

            // each run holds exactly the keys it still needs: a key it has processed is released, one it has not is
            // kept. For a range task the set to check is what it has actually held, as a declared key the scan never
            // found and nobody adopted was never referenced and is not pending either.
            int check = isRange ? (heldKeys.get(taskId) | activeMask | processed) : declaredMask;
            for (int k = 0 ; k < KEYS ; ++k)
            {
                if (0 == (check & (1 << k)))
                    continue;
                boolean isProcessed = 0 != (processed & (1 << k));
                boolean hasRef = task.refs.get(keys[k]) != null;
                Invariants.require(isProcessed != hasRef, isProcessed ? "task %d still references key %d, which it processed"
                                                                      : "task %d does not reference key %d, which it has not processed", taskId, k);
            }

            if (task.isIncremental())
            {
                batchedCount.incrementAndGet(Batched.INCR_RUN.ordinal());
                if (run > 1)
                    batchedCount.incrementAndGet(Batched.INCR_LATER_BATCH.ordinal());
                if (task.isIncrementalFinishing())
                {
                    // an incremental task finishes only once it has processed every key it holds; for a range task that
                    // is task.keys, which the adoption path grows, rather than the mask it declared
                    int expected = isRange ? task.keys : Integer.bitCount(declaredMask);
                    Invariants.require(Integer.bitCount(processed | activeMask) == expected,
                                       "task %d finished having processed %d of %d keys", taskId, Integer.bitCount(processed | activeMask), expected);
                }
            }
            else
            {
                Invariants.require(run == 1, "async task %d ran %d times", taskId, run);
                batchedCount.incrementAndGet(Batched.ASYNC_RUN.ordinal());
                if (activeMask != declaredMask)
                    batchedCount.incrementAndGet(Batched.ASYNC_PARTIAL.ordinal());
            }
            return active;
        }

        /** the ordinals a task currently holds a commands-for-key reference for */
        private int[] referencedOrdinals(int taskId, SafeTask<?> task)
        {
            int found = 0;
            for (int k = 0 ; k < KEYS ; ++k)
            {
                if (task.refs.get(keys[k]) != null)
                    found |= 1 << k;
            }
            int[] result = new int[Integer.bitCount(found)];
            for (int k = 0, i = 0 ; k < KEYS ; ++k)
            {
                if (0 != (found & (1 << k)))
                    result[i++] = k;
            }
            return result;
        }

        /** of the ordinals in {@code declaredMask}, those a scan could have found for itself */
        private static int discoverableWithin(int declaredMask)
        {
            int result = 0;
            for (int k = 0 ; k < KEYS ; ++k)
            {
                if (0 != (declaredMask & (1 << k)) && isDiscoverable(k))
                    result |= 1 << k;
            }
            return result;
        }

        private int[] ordinalsOf(int taskId, RoutingKeys active)
        {
            int[] result = new int[active.size()];
            int count = 0;
            for (RoutingKey key : active)
            {
                int ordinal = NO_TASK;
                for (int k = 0 ; k < KEYS ; ++k)
                {
                    if (keys[k].equals(key))
                    {
                        ordinal = k;
                        break;
                    }
                }
                Invariants.require(ordinal >= 0, "task %d is running with unknown key %s", taskId, key);
                result[count++] = ordinal;
            }
            Arrays.sort(result);
            return result;
        }

        private void verifyReference(Store store, int taskId, SafeTask<?> task, Object declared, String describe)
        {
            SafeState<?> ref = task.refs.get(declared);
            Invariants.require(ref != null, "task %d declared %s but holds no reference for it", taskId, describe);
            AccordCacheEntry<?, ?, ?> entry = global(ref);
            Invariants.require(entry.references() > 0, "task %d holds an unreferenced entry for %s", taskId, describe);
            Invariants.require(entry.owner.commandStore == store.store,
                               "task %d of store %d holds an entry for %s belonging to store %s", taskId, store.index, describe, entry.owner.commandStore);
        }

        /**
         * Work submitted by a running task must be attached to it as a consequence - and so must not run until it has
         * finished - if and only if it belongs to the same executor; otherwise it must be submitted to its own
         * executor independently. Invoked by the submitting task, so we can inspect its state safely.
         */
        private void verifyNested(Store store, SafeTask<?> parent, SafeTask<?> child, Nested relationship)
        {
            boolean isConsequence = isConsequenceOf(parent, child);
            if (store.executor == parent.commandStore.executor())
            {
                Invariants.require(isConsequence, "%s was submitted by %s on the same executor, but is not one of its consequences", child, parent);
                Invariants.require(child.is(Task.State.UNREGISTERED), "consequence %s of %s has already been registered", child, parent);
            }
            else
            {
                Invariants.require(!isConsequence, "%s was submitted by %s on another executor, but is one of its consequences", child, parent);
            }

            // and, on another executor, immediate execution must be refused
            if (store.executor != parent.commandStore.executor())
            {
                AtomicInteger ran = new AtomicInteger();
                boolean accepted = store.store.exclusiveExecutor().tryExecuteImmediately(ran::incrementAndGet);
                Invariants.require(!accepted, "%s executed immediately on another executor", parent);
                Invariants.require(ran.get() == 0, "%s ran work immediately on another executor", parent);
            }

            nestedCount.incrementAndGet(relationship.ordinal());
        }

        /**
         * A task is assigned its context's {@link ExecutionSequence} whether or not it was pre-set-up: preSetup does it for
         * a child on its parent's own store, and submitExclusiveMayThrow for everything else.
         * <p>
         * This asserts the <em>specified</em> guarantee rather than the sequence bits, because a task may legally end up
         * more strongly ordered than it declared: the fifo upgrade at first prepare raises {@code SEQUENCED_MASK} to its
         * ATOMIC_AND_QUEUED form, so an upgraded BY_PRIORITY task stops reporting {@code isSequencedByPriority()}. That is
         * within spec - BY_PRIORITY permits an INCR task's batches to interleave, it does not require it - so what is
         * checked is that the task is ordered at all.
         *
         * Checked at two points, both of which are past the assignment: a pre-set-up child from inside its parent's body,
         * before the parent completes and its consequences are submitted; and every task from inside its own body.
         */
        private void verifySequenced(SafeTask<?> task, ExecutionSequence sequence, Object describe)
        {
            switch (sequence)
            {
                default: throw new AssertionError("Unhandled " + sequence);
                case UNSEQUENCED:
                    // may run as soon as it is ready, with no regard to ordering on other tasks on the same keys. Nothing
                    // upgrades it: that needs ATOMIC or a txnId lock, and an UNSEQUENCED INCR task may not declare one
                    Invariants.require(task.isUnsequenced(), "%s is sequenced", describe);
                    break;
                case BY_PRIORITY:
                    // ordered with respect to other tasks' priorities
                    Invariants.require(!task.isUnsequenced(), "%s is unsequenced", describe);
                    break;
                case ATOMIC:
                    // atomic from the point of view of other sequenced tasks, which requires a fifo position: a pre-set-up
                    // child inherits its parent's at setup, one with no parent to inherit from takes a fresh one on its
                    // first prepare. With the cache queues off there is nowhere to take a position, so the sequence is
                    // recorded but no fifo claim is made and no atomicity is provided
                    Invariants.require(task.isAtomic(), "%s is not sequenced by priority atomic", describe);
                    Invariants.require(task.isCacheQueuedFifo() == AccordExecutor.CACHE_QUEUES_ENABLED,
                                       "%s is %squeued fifo", describe, task.isCacheQueuedFifo() ? "" : "not ");
                    break;
            }
        }

        private Store target(Store store, Nested nested)
        {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            switch (nested)
            {
                default: throw new AssertionError("Unhandled " + nested);
                case SAME_STORE:
                    return store;
                case SAME_EXECUTOR:
                {
                    // stores are assigned to executors round-robin, so the next store sharing our executor is EXECUTORS away
                    int stride = EXECUTORS;
                    int candidates = (STORES - 1 - store.index) / stride;
                    if (candidates <= 0)
                        candidates = store.index / stride;
                    if (candidates <= 0)
                        return store;
                    int index = store.index + stride * (1 + rnd.nextInt(candidates));
                    if (index >= STORES)
                        index = store.index - stride * (1 + rnd.nextInt(candidates));
                    return stores[index];
                }
                case OTHER_EXECUTOR:
                {
                    int index = rnd.nextInt(STORES);
                    while (stores[index].executorIndex == store.executorIndex)
                        index = (index + 1) % STORES;
                    return stores[index];
                }
            }
        }

        private void awaitOutstanding()
        {
            long startedAt = System.nanoTime();
            int lastOutstanding = outstanding.get();
            for (int attempt = 0 ; outstanding.get() > 0 ; ++attempt)
            {
                Invariants.require(attempt < 10000, "%d submissions have not been notified", outstanding.get());
                // Under the simulator a park costs as much wall clock as the schedule needs to reach it, so the attempt
                // limit alone can take longer than the test timeout without ever tripping. Report progress as we go, so
                // that a slow phase is distinguishable from a stalled one in the log rather than only in a thread dump.
                if (attempt > 0 && (attempt % 1000) == 0)
                {
                    int now = outstanding.get();
                    System.out.printf("awaiting %d submissions after %d attempts (%.1fs, %+d since last report)%n",
                                      now, attempt, (System.nanoTime() - startedAt) / 1e9, now - lastOutstanding);
                    if (now == lastOutstanding)
                        explainStall();
                    lastOutstanding = now;
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
        }

        /**
         * The simulator fails a run the instant its schedule has nothing left to run, which is too early for a wait loop
         * to notice and describe the impasse. A periodic action hours away is always in the schedule, so its due time is
         * reached exactly when everything else blocks - which is when we want to look at the queues.
         */
        void startStallWatchdog()
        {
            ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(() -> {
                if (outstanding.get() > 0)
                    explainStall();
            }, 1, 1, TimeUnit.HOURS);
        }

        private void explainStall()
        {
            List<SafeTask<?>> stalled = new ArrayList<>();
            int lastTaskId = nextTaskId.get();
            for (int taskId = 0 ; taskId < lastTaskId ; ++taskId)
            {
                SafeTask<?> task = tasks[taskId];
                if (task != null && notifications.get(taskId) == 0)
                    stalled.add(task);
            }

            // read the queues without the executor lock: if we are stalled nothing is mutating them, and if we are not
            // then acquiring it here would join the stall we are trying to describe
            String explanation;
            try
            {
                explanation = QueueCycleDetector.explainStall(stalled);
            }
            catch (Throwable t)
            {
                // the detector is the only stall oracle this suite has: if it cannot read the queues we must not go on
                // to report "no cycle". Recorded so the round fails with this, not with the simulator's later
                // "nothing left to run".
                System.out.println("### explainStall threw, so the stall is undiagnosed:");
                t.printStackTrace(System.out);
                failures.add(t);
                explanation = "explainStall threw " + t;
            }
            System.out.printf("### stalled with %d outstanding, %d never notified%n%s%n", outstanding.get(), stalled.size(),
                              explanation == null ? "  no cycle and no mis-counted waiter: the stall is elsewhere" : explanation);
            for (SafeTask<?> task : stalled)
                System.out.printf("###   %s is %s: %s%n", task.description(), task.currentState(), QueueCycleDetector.describeReadiness(task));
        }

        private void verifyRoundComplete()
        {
            Invariants.require(failures.isEmpty(), "%s", failures);
            Invariants.require(RecordingAgent.exceptions.isEmpty(), "%s", RecordingAgent.exceptions);

            int lastTaskId = nextTaskId.get();
            for (int taskId = 0 ; taskId < lastTaskId ; ++taskId)
            {
                Invariants.require(notifications.get(taskId) == 1, "task %d was notified %d times", taskId, notifications.get(taskId));
                // a task whose load failed, or whose parent failed, never runs
                if (failedTasks.get(taskId) != 0)
                    continue;

                Invariants.require(hasReturned.get(taskId) == 1, "task %d did not run", taskId);
                LoadKeys loadKeys = LoadKeys.values()[loadKeysOf.get(taskId)];
                boolean isRange = isRangeTask.get(taskId) != 0;
                if (loadKeys == LoadKeys.INCR)
                {
                    // a range task processes what it discovered and adopted, which is a subset of the range it declared; a
                    // key task processes exactly what it declared
                    if (isRange)
                        Invariants.require((processedKeys.get(taskId) & ~declaredKeys.get(taskId)) == 0,
                                           "incremental range task %d processed keys outside its range", taskId);
                    else
                        Invariants.require(processedKeys.get(taskId) == declaredKeys.get(taskId),
                                           "incremental task %d processed keys %s of %s", taskId,
                                           Integer.toBinaryString(processedKeys.get(taskId)), Integer.toBinaryString(declaredKeys.get(taskId)));
                }
                else if (loadKeys == LoadKeys.ASYNC)
                {
                    Invariants.require(runs.get(taskId) == 1, "async task %d ran %d times", taskId, runs.get(taskId));
                    Invariants.require((processedKeys.get(taskId) & ~declaredKeys.get(taskId)) == 0,
                                       "async task %d processed keys it did not declare", taskId);
                }
                else
                {
                    Invariants.require(runs.get(taskId) == 1, "task %d ran %d times", taskId, runs.get(taskId));
                }
            }
            for (Store store : stores)
            {
                for (int i = 0 ; i < KEYS ; ++i)
                    Invariants.require(store.keyOwner.get(i) == NO_TASK, "key %d of store %d is still owned by task %d", i, store.index, store.keyOwner.get(i));
                for (int i = 0 ; i < TXN_IDS ; ++i)
                    Invariants.require(store.txnIdOwner.get(i) == NO_TASK, "txnId %d of store %d is still owned by task %d", i, store.index, store.txnIdOwner.get(i));
            }

            awaitReleased();
            verifyBatches();

            StringBuilder coverage = new StringBuilder("verified " + lastTaskId + " tasks; nested:");
            for (Nested nested : Nested.values())
            {
                coverage.append(' ').append(nested).append('=').append(nestedCount.get(nested.ordinal()));
                Invariants.require(nestedCount.get(nested.ordinal()) > 0, "no %s nested submissions were verified", nested);
            }
            coverage.append("; ordering:");
            for (Ordering ordering : Ordering.values())
            {
                coverage.append(' ').append(ordering).append('=').append(orderingCount.get(ordering.ordinal()));
                // nothing to require with the queues off: no task takes a position, so no ordering is imposed
                Invariants.require(!AccordExecutor.CACHE_QUEUES_ENABLED || orderingCount.get(ordering.ordinal()) > 0,
                                   "no %s ordering constraints were verified", ordering);
            }
            // The unconstrained case: an atomic child overtaking an already-placed prioritised sibling. Sibling order is
            // undefined by the contract, so this is a canary rather than a requirement - but a total failure of the
            // revocation an out-of-order fifo arrival performs would be a real regression, so require it to work at all.
            // The ratio is printed rather than gated, as it is a function of scheduling: it essentially always wins, the
            // loser being the case where the sibling had already locked its keys at prepare.
            if (RECOVERY_CHANCE > 0)
            {
                coverage.append("; rangeScans=").append(rangeScans.get())
                        .append(" (failed=").append(scanFailures.get()).append(", arbitrary=").append(scanArbitrary.get()).append(')');
                Invariants.require(rangeScans.get() > 0, "no task declared RECOVERY, so no range scan was verified");
                Invariants.require(scanArbitrary.get() > 0, "no range scan returned arbitrary summaries");
            }
            if (RANGE_CHANCE > 0)
            {
                coverage.append("; rangeTasks=").append(rangeTasks.get()).append(" adopted=").append(adoptedCount.get());
                Invariants.require(rangeTasks.get() > 0, "no task declared Ranges, so RangeTxnAndKeyScanner was not exercised");
                // half the key space is undiscoverable by construction, so a range task that ran with such a key can
                // only have got it from the KeyWatcher; if that never happened, the axis has not tested adoption at all
                Invariants.require(adoptedCount.get() > 0,
                                   "no range task ever ran with a key its scan could not have discovered, so no adoption was verified");
            }

            int overtook = overtookPriority.get(), failed = failedToOvertakePriority.get();
            coverage.append(" overtakes=").append(overtook).append('/').append(overtook + failed);
            Invariants.require(!AccordExecutor.CACHE_QUEUES_ENABLED || overtook > 0,
                               "no sibling ever overtook one submitted before it (%d tried, none succeeded), so the "
                               + "revocation an out-of-order fifo arrival performs is not working at all", failed);
            coverage.append("; batched:");
            for (Batched batched : Batched.values())
            {
                coverage.append(' ').append(batched).append('=').append(batchedCount.get(batched.ordinal()));
                // nothing to require without the queues: they are what batching is defined over, so every task is SYNC
                Invariants.require(!AccordExecutor.NONSYNC_ENABLED || batchedCount.get(batched.ordinal()) > 0,
                                   "no %s runs were verified", batched);
            }
            if (loadFailures.get() > 0)
                coverage.append("; loadFailures=").append(loadFailures.get()).append(" failedTasks=").append(failedTaskCount.get());
            System.out.println(coverage);
        }

        /**
         * Verify the execution order of each batch's children, noting that the contract guarantees nothing about it:
         * atomicity is with respect to the parent, and siblings have no defined relative order. What is checked is what
         * the queues currently produce, as a canary: two siblings that share a cache entry are ordered by that entry's
         * queue, so an atomic child submitted before a prioritised sibling runs before it; atomic siblings run in fifo
         * (submission) order; and prioritised siblings run in the queue's comparison order, which - as they all inherit
         * their parent's position - is their execution kind. Unsequenced siblings are unconstrained.
         *
         * <p>An atomic child submitted <em>after</em> a prioritised sibling is excluded, as it would have to revoke a
         * sibling that may already have locked its keys; those are counted instead - see {@code overtookPriority}.
         */
        private void verifyBatches()
        {
            for (Batch batch : batches)
            {
                for (Child child : batch.children)
                {
                    // a child that failed, or whose parent failed, never runs
                    Invariants.require(child.runIndex >= 0 || child.taskId == NO_TASK || failedTasks.get(child.taskId) != 0,
                                       "%s did not run", child);
                }

                // with the cache queues switched off nothing arranges siblings on a shared entry at all, so there is no
                // ordering to verify - only that every child ran, which is checked above
                if (!AccordExecutor.CACHE_QUEUES_ENABLED)
                    continue;

                for (int i = 0 ; i < batch.children.length ; ++i)
                {
                    for (int j = i + 1 ; j < batch.children.length ; ++j)
                    {
                        Child a = batch.children[i], b = batch.children[j];
                        if (!a.sharesAndGatedAlike(b) || !a.isSequenced() || !b.isSequenced() || a.runIndex < 0 || b.runIndex < 0)
                            continue;

                        Ordering ordering;
                        Child first;
                        if (a.isAtomic() != b.isAtomic())
                        {
                            ordering = Ordering.ATOMIC_BEFORE_PRIORITY;
                            first = a.isAtomic() ? a : b;
                        }
                        else if (a.isAtomic())
                        {
                            ordering = Ordering.ATOMIC_FIFO;
                            first = a.submitIndex < b.submitIndex ? a : b;
                        }
                        else
                        {
                            ordering = Ordering.PRIORITY_BY_KIND;
                            first = a.kind.compareTo(b.kind) < 0 ? a : b;
                        }

                        Child second = first == a ? b : a;
                        // the queue rule only decides the outcome if both siblings were placed before either ran, which
                        // nothing provides: an earlier-submitted sibling can be placed, become runnable and run before a
                        // later one is placed. So where the rule wants the later-submitted sibling first it has to
                        // overtake - a race, recorded rather than required; where the two agree, the order is asserted.
                        if (first.submitIndex > second.submitIndex)
                        {
                            if (first.runIndex < second.runIndex) overtookPriority.incrementAndGet();
                            else failedToOvertakePriority.incrementAndGet();
                            continue;
                        }
                        Invariants.require(first.runIndex < second.runIndex,
                                           "%s: expected %s to run before %s (children of task %d)",
                                           ordering, first, second, batch.parentTaskId);
                        orderingCount.incrementAndGet(ordering.ordinal());
                    }
                }
            }
        }

        /**
         * A task's callback is invoked from within {@code finish()}, i.e. while it is still running and before it
         * completes and releases its references - so having been notified of every submission does not imply the
         * executors have finished with them, and we must wait. We also inspect exclusively, both to synchronise with
         * the thread that released the references and because these fields may only be read under the lock.
         */
        private void awaitReleased()
        {
            for (int attempt = 0 ; ; ++attempt)
            {
                String outstanding = outstandingExclusive();
                if (outstanding == null)
                    return;

                if (attempt > 0 && (attempt % 1000) == 0)
                    System.out.println("awaiting release after " + attempt + " attempts: " + outstanding);
                Invariants.require(attempt < 10000, "%s", outstanding);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
        }

        private String outstandingExclusive()
        {
            for (int taskId = 0 ; taskId < nextTaskId.get() ; ++taskId)
            {
                SafeTask<?> task = tasks[taskId];
                try (AccordCommandStore.ExclusiveCaches ignore = task.commandStore.lockCaches())
                {
                    if (task.refs != null)
                        return "task " + taskId + " (" + task.currentState() + ") has not released its references";
                }
            }
            for (Store store : stores)
            {
                try (AccordCommandStore.ExclusiveCaches caches = store.store.lockCaches())
                {
                    for (AccordCacheEntry<?, ?, ?> entry : caches.commands())
                    {
                        if (entry.references() != 0)
                            return entry + " of store " + store.index + " is still referenced";
                    }
                    for (AccordCacheEntry<?, ?, ?> entry : caches.commandsForKeys())
                    {
                        if (entry.references() != 0)
                            return entry + " of store " + store.index + " is still referenced";
                    }
                }
            }
            return null;
        }

        private RoutingKeys keys(int[] ordinals)
        {
            RoutingKey[] result = new RoutingKey[ordinals.length];
            for (int i = 0 ; i < ordinals.length ; ++i)
                result[i] = keys[ordinals[i]];
            return RoutingKeys.of(result);
        }
    }

    static class Store
    {
        final int index;
        final int executorIndex;
        final AccordExecutor executor;
        final AccordCommandStore store;
        /** key/txnId ordinal -> id of the task currently executing with it, or {@link #NO_TASK} */
        final AtomicIntegerArray keyOwner = new AtomicIntegerArray(KEYS);
        final AtomicIntegerArray txnIdOwner = new AtomicIntegerArray(TXN_IDS);

        Store(int index, int executorIndex, AccordExecutor executor, TableId tableId, IPartitioner partitioner)
        {
            this.index = index;
            this.executorIndex = executorIndex;
            this.executor = executor;
            this.store = newCommandStore(index, tableId, partitioner, executor);
            for (int i = 0 ; i < KEYS ; ++i)
                keyOwner.set(i, NO_TASK);
            for (int i = 0 ; i < TXN_IDS ; ++i)
                txnIdOwner.set(i, NO_TASK);
        }
    }

    /** a batched (non-SYNC) run shape we want to be sure we exercised */
    enum Batched
    {
        /** a run of an ASYNC task */
        ASYNC_RUN,
        /** an ASYNC task that ran over fewer than all of the keys it declared */
        ASYNC_PARTIAL,
        /** a run of an INCR task */
        INCR_RUN,
        /** a run of an INCR task that was not its first, i.e. a task that really did batch */
        INCR_LATER_BATCH
    }

    /**
     * An ordering the cache entry queues currently produce among the children of one task. None of these is a semantic
     * guarantee: the contract is atomicity with respect to the <em>parent</em> only, and siblings have no defined
     * relative order. They are asserted because they are what the queues do today and a change is worth noticing, so the
     * response to one failing is to decide whether the new behaviour is acceptable and weaken the assertion.
     */
    enum Ordering
    {
        /**
         * An atomic child submitted <em>before</em> a prioritised sibling runs before it: it took its fifo position
         * first, so the sibling's later {@code addPrioritised} sees {@code hasFifo()} and reports NOT_RUNNABLE. The
         * reverse depends on scheduling and is recorded instead; see {@code overtookPriority}.
         */
        ATOMIC_BEFORE_PRIORITY,
        /** atomic siblings run in submission order */
        ATOMIC_FIFO,
        /** prioritised siblings with equal positions run in execution kind order */
        PRIORITY_BY_KIND
    }

    private static final ExecutionSequence[] SEQUENCES = ExecutionSequence.values();

    /**
     * What a submission will really be run as, which is not always what it asked for: {@code SafeTask.loadKeys}
     * downgrades everything to SYNC when non-sync execution is disabled, and
     * {@code queue_key_ordering_enabled = false} disables it - batching is defined in terms of the queue positions the
     * cache entries hand out, so there is nothing to batch over without them.
     */
    private static LoadKeys effectiveLoadKeys(LoadKeys requested)
    {
        return AccordExecutor.NONSYNC_ENABLED ? requested : LoadKeys.SYNC;
    }

    /** {@code count} of {@code pool}'s ordinals, in ascending order */
    private static int[] subset(ThreadLocalRandom rnd, int[] pool, int count)
    {
        int mask = 0;
        for (int i = 0 ; i < count ; ++i)
            mask |= 1 << rnd.nextInt(pool.length);
        int[] result = new int[Integer.bitCount(mask)];
        for (int i = 0, index = 0 ; mask != 0 ; ++index, mask >>>= 1)
        {
            if ((mask & 1) != 0)
                result[i++] = pool[index];
        }
        return result;
    }

    /** ordinals as a bit mask; the key and txnId spaces are small enough to fit in an int */
    private static int mask(int[] ordinals)
    {
        int mask = 0;
        for (int ordinal : ordinals)
            mask |= 1 << ordinal;
        return mask;
    }

    /** both are sorted ascending */
    private static boolean intersects(int[] as, int[] bs)
    {
        for (int ai = 0, bi = 0 ; ai < as.length && bi < bs.length ;)
        {
            int c = Integer.compare(as[ai], bs[bi]);
            if (c == 0) return true;
            else if (c < 0) ++ai;
            else ++bi;
        }
        return false;
    }

    private static boolean isConsequenceOf(Task parent, Task task)
    {
        for (Task cur = parent.next ; cur != null ; cur = cur.next)
        {
            if (cur == task)
                return true;
        }
        return false;
    }

    private static void maybePark(float chance)
    {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        if (chance > 0 && rnd.nextFloat() < chance)
            LockSupport.parkNanos(rnd.nextInt(10000, 100000));
    }

    /** {@code count} distinct ordinals in [0..limit), in ascending order (so a task never declares a key twice) */
    private static int[] distinct(ThreadLocalRandom rnd, int limit, int count)
    {
        int mask = 0;
        for (int i = 0 ; i < count ; ++i)
            mask |= 1 << rnd.nextInt(limit);
        int[] result = new int[Integer.bitCount(mask)];
        for (int i = 0, ordinal = 0 ; mask != 0 ; ++ordinal, mask >>>= 1)
        {
            if ((mask & 1) != 0)
                result[i++] = ordinal;
        }
        return result;
    }

    private static AccordCommandStore newCommandStore(int id, TableId tableId, IPartitioner partitioner, AccordExecutor executor)
    {
        AtomicLong clock = new AtomicLong();
        LongSupplier now = clock::incrementAndGet;
        Id nodeId = new Id(1);
        NodeCommandStoreService node = new NodeCommandStoreService()
        {
            private final ToLongFunction<TimeUnit> elapsed = TimeService.elapsedWrapperFromNonMonotonicSource(TimeUnit.MICROSECONDS, this::now);
            private long stamp = 0;

            @Override public AsyncExecutor someExecutor() { return null; }
            @Override public ExclusiveAsyncExecutor someExclusiveExecutor() { return null; }
            @Override public accord.api.Timeouts timeouts() { return null; }
            @Override public DurableBefore durableBefore() { return DurableBefore.EMPTY; }
            @Override public DurabilityService durability() { return null; }
            @Override public Id id() { return nodeId; }
            @Override public long epoch() { return 1; }
            @Override public long now() { return now.getAsLong(); }
            @Override public long uniqueNow(long atLeast) { return now.getAsLong(); }
            @Override public long elapsed(TimeUnit units) { return elapsed.applyAsLong(units); }
            @Override public TopologyManager topology() { throw new UnsupportedOperationException(); }
            @Override public Coordinations coordinations() { return new Coordinations(); }
            @Override public long currentStamp() { return stamp; }
            @Override public void updateStamp() { ++stamp; }
            @Override public boolean isReplaying() { return false; }
            @Override public void reportLocalExecution(TxnId txnId, Route<?> route, Ballot ballot, Timestamp applyAt, Writes writes, Result result) {}
        };

        Range range = TokenRange.fullRange(tableId, partitioner);
        RangesForEpoch rangesForEpoch = new RangesForEpoch(1, Ranges.of(range));
        AccordCommandStore store = new AccordCommandStore(id, node, new RecordingAgent(), null,
                                                          cs -> new ProgressLog.NoOpProgressLog(),
                                                          cs -> new DefaultLocalListeners(null, new DefaultRemoteListeners.NoOpRemoteListeners(), new NotifySink.NoOpNotifySink()),
                                                          rangesForEpoch,
                                                          new InMemoryJournal(nodeId, new DefaultRandom(1 + id)),
                                                          executor);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(AMPLE_CAPACITY);
            executor.setWorkingSetSize(AMPLE_WORKING_SET);
        });
        return store;
    }
}
