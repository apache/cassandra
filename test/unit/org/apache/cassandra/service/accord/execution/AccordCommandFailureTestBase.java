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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.junit.BeforeClass;

import accord.api.ProgressLog;
import accord.api.ReplicaEventListener;
import accord.api.Result.PersistableResult;
import accord.api.RoutingKey;
import accord.local.CheckedCommands;
import accord.local.Command;
import accord.local.ExecutionContext;
import accord.local.LogFaultException;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.NotifySink;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.async.AsyncChain;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.Pair;

import static accord.local.ExecutionContext.unsequencedReadWrite;
import static accord.primitives.SaveStatus.Stable;
import static org.apache.cassandra.service.accord.AccordService.getBlocking;
import static org.apache.cassandra.service.accord.AccordTestUtils.createWriteTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.fullRange;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;

/**
 * This class was authored by Claude.
 *
 * Support for tests that inject a failure at a chosen point of command processing and then ask what the pipeline was
 * left holding. It drives one command store through the replica side of the protocol - PreAccept, Accept, Commit
 * (Stable), execute, Apply, PostApply - using real transactions against a real table, so that the executor's failure
 * handling ({@code SafeTask.discardUpdatesExclusive}, consequence cancellation, journalling) is what is under test.
 *
 * <h2>Injection points</h2>
 * Faults are injected without touching production code, at the seams a fault can genuinely arrive through:
 * <ul>
 * <li>{@link Point#PROGRESS_LOG_UPDATE} - the first call in {@code SafeCommandStore.update()}, i.e. before the
 *     command's max conflicts / CommandsForKey are updated.</li>
 * <li>the {@link ReplicaEventListener} hooks ({@link Point#ON_PREACCEPTED} ... {@link Point#ON_APPLIED}) - immediately
 *     after {@code SafeCommand.<status>()} has completed its {@code update()}, and (for most) before the command's
 *     listeners are notified.</li>
 * <li>{@link Point#CFK_NOTIFY} - from the {@code NotifySink} a CommandsForKey update uses to notify the transactions
 *     that were waiting on the one being updated, i.e. after those transactions have been notified and have had the
 *     chance to schedule follow-up work of their own. This is the shape of the production fault that motivated these
 *     tests: a {@code logFault} thrown while updating CommandsForKey, after it had notified waiting commands.</li>
 * <li>{@link Point#TASK_BODY} - after the message has been applied but before the task returns, i.e. any fault later in
 *     the same task (a second key's cleanup, a listener, an unrelated bug).</li>
 * </ul>
 *
 * <h2>Observation</h2>
 * The progress log and the replica event listener double as recorders, so a test can compare what each subsystem was
 * told with what the command store ended up holding; the agent records every exception the executor reported.
 *
 * <h2>Where a logFault can arrive from</h2>
 * {@code Cleanup.logFault} is thrown from {@code Cleanup.shouldCleanup}, which every cleanup-performing accessor calls -
 * {@code SafeCommandStore.get}, {@code unsafeGet}, {@code ifInitialised}, {@code ifLoadedAndInitialised}. Cleanup belongs
 * where an operation <em>acquires</em> a command, not once it has begun mutating state: a refusal cannot undo what has
 * been done, and unwinds a partially applied update instead. The sites reachable from inside
 * {@code SafeCommandStore.update()} and its notification cascade therefore acquire their command with
 * {@code unsafeGetNoCleanupOrThrow}:
 * <ul>
 * <li>{@code SafeCommandStore.updateManagedCommandsForKey}, {@code updateUnmanagedCommandsForKey} and
 *     {@code registerTransitive} - the command whose derived state is being updated;</li>
 * <li>{@code Commands.postApply} and {@code Commands.PostFastApply} - the writes have been applied, so refusing the
 *     record would leave nothing to record that;</li>
 * <li>{@code Commands.NotifyWaitingOn} - which {@code maybeExecute} may run inline;</li>
 * <li>{@code NotifySink.DefaultNotifySink.doNotifyAlreadyReady} and {@code cfk.Updating.updateUnmanagedAsync} - the
 *     transaction being notified, and the continuation of that notification.</li>
 * </ul>
 * Cleanup remains (and log faults may still be raised) where an operation begins: message handling
 * ({@code get(txnId, participants)}), {@code Await}, {@code Catchup}, {@code cfk.ExecuteTxnBacklog} (which starts a
 * coordination), {@code DefaultProgressLog}/{@code WaitingState}, {@code DefaultLocalListeners}' notify and
 * {@code clearBefore} tasks, {@code CommandStore.tryExecuteListening}, {@code Commands.eraseEphemeralRead} and
 * {@code AbstractReplayer}. Note that a CommandsForKey acquisition ({@code get(key)}) cannot raise a log fault: its
 * cleanup only advances {@code redundantBefore}.
 *
 * <p>One residual remains by choice: {@code NotifyWaitingOn} acquires a <em>dependency</em> with
 * {@code ifInitialised(loadDepId)}, which cleans up, and may be reached inline from a command update. Cleanup of a
 * dependency there is load-bearing (it is how an invalidated dependency is truncated - see the {@code TODO} in
 * {@code NotifyWaitingOn}), so it is left alone.
 *
 * <p>Note also that {@code AccordAgent.expectedException} counts {@code LogFaultException} as expected, so it is reported
 * through {@code NoSpamLogger.warn}: a rollback of this kind is, in production, preceded only by a rate-limited warning.
 */
public abstract class AccordCommandFailureTestBase
{
    protected static final long TIMEOUT_SECONDS = 30;
    protected static final AtomicLong clock = new AtomicLong(1000);
    private static boolean schemaCreated;

    /**
     * The table these tests transact against: {@code ks.tbl}, as {@code AccordTestUtils.createWriteTxn} writes to it by
     * name. Idempotent, so that more than one of these classes may share a JVM.
     */
    @BeforeClass
    public static void createSchema() throws Throwable
    {
        if (schemaCreated)
            return;
        schemaCreated = true;
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    CreateTableStatement.parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
        StorageService.instance.initServer();
    }

    /** where a fault is injected */
    public enum Point
    {
        /** first call of {@code SafeCommandStore.update()}, before any of the command's derived state is updated */
        PROGRESS_LOG_UPDATE,
        ON_PREACCEPTED,
        ON_ACCEPTED,
        ON_COMMITTED,
        ON_STABLE,
        /** after {@code preapplied()}/{@code applying()}, before {@code applyChain()} is begun */
        ON_PREAPPLIED,
        /** from {@code Commands.postApply}, after {@code applied()} */
        ON_APPLIED,
        /** while a CommandsForKey update notifies the transactions that were waiting on the updated one */
        CFK_NOTIFY,
        /** after the message has been applied, in the same task */
        TASK_BODY,
        NONE
    }

    /** the fault to inject: at {@code point}, optionally only for {@code txnId} and/or when reaching {@code atStatus} */
    public static class Fault
    {
        public final Point point;
        public final @Nullable TxnId txnId;
        public final @Nullable SaveStatus atStatus;
        public final boolean asLogFault;
        public int fired;

        public Fault(Point point, @Nullable TxnId txnId, @Nullable SaveStatus atStatus, boolean asLogFault)
        {
            this.point = point;
            this.txnId = txnId;
            this.atStatus = atStatus;
            this.asLogFault = asLogFault;
        }

        public static Fault none()
        {
            return new Fault(Point.NONE, null, null, false);
        }

        public static Fault at(Point point, TxnId txnId)
        {
            return new Fault(point, txnId, null, false);
        }

        public Fault asLogFault()
        {
            return new Fault(point, txnId, atStatus, true);
        }

        boolean matches(Point point, @Nullable TxnId txnId, @Nullable SaveStatus status)
        {
            return this.point == point
                   && (this.txnId == null || this.txnId.equals(txnId))
                   && (this.atStatus == null || this.atStatus == status);
        }

        RuntimeException create(Point point, TxnId txnId)
        {
            ++fired;
            String message = "injected fault at " + point + " for " + txnId;
            return asLogFault ? new LogFaultException(message) : new InjectedFault(message);
        }

        @Override
        public String toString()
        {
            return point + (txnId == null ? "" : "[" + txnId + ']') + (atStatus == null ? "" : "@" + atStatus);
        }
    }

    /** an injected fault that is not a {@link LogFaultException}: nothing may treat it as expected */
    public static class InjectedFault extends RuntimeException
    {
        public InjectedFault(String message)
        {
            super(message);
        }
    }

    /**
     * One command store, its recorders, and the fault to inject. The store is created per test, so each has its own
     * (real) journal and cache.
     */
    public static class Harness implements AutoCloseable
    {
        public final AccordCommandStore store;
        public final RecordingAgent agent;
        public final RecordingProgressLog progressLog;
        public final ReplicaEventListener replicaEvents = replicaEvents(this);
        public final List<String> events = new CopyOnWriteArrayList<>();
        public Fault fault = Fault.none();

        public Harness(String keyspace, String table)
        {
            RecordingAgent agent = new RecordingAgent(this);
            agent.setup(Node.Id.NONE);
            this.agent = agent;
            this.progressLog = new RecordingProgressLog(this);
            this.store = AccordTestUtils.createAccordCommandStore(clock::incrementAndGet, keyspace, table,
                                                                  agent, ignore -> progressLog);
        }

        void maybeFail(Point point, @Nullable TxnId txnId, @Nullable SaveStatus status)
        {
            Fault fault = this.fault;
            if (fault.matches(point, txnId, status))
                throw fault.create(point, txnId);
        }

        void record(String event)
        {
            events.add(event);
        }

        public List<String> events()
        {
            return new ArrayList<>(events);
        }

        public boolean sawEvent(String event, TxnId txnId)
        {
            return events.contains(event + ':' + txnId);
        }

        @Override
        public void close()
        {
            store.shutdown();
        }
    }

    /** records every exception the executor reports, and keeps {@code AccordAgent} from tripping over metrics */
    public static class RecordingAgent extends AccordAgent
    {
        public final List<Throwable> exceptions = new CopyOnWriteArrayList<>();
        private final Harness harness;

        RecordingAgent(Harness harness)
        {
            this.harness = harness;
        }

        @Override
        public void onException(Throwable t)
        {
            exceptions.add(t);
        }

        @Override
        public void onException(Throwable t, String context)
        {
            exceptions.add(t);
        }

        @Override
        public ReplicaEventListener replicaEvents()
        {
            return harness.replicaEvents;
        }

        /** exceptions that are neither the injected fault nor a cancellation caused by it */
        public List<Throwable> unexpected()
        {
            List<Throwable> result = new ArrayList<>();
            for (Throwable t : exceptions)
            {
                if (!isInjected(t) && !isCancellation(t))
                    result.add(t);
            }
            return result;
        }
    }

    public static boolean isInjected(Throwable t)
    {
        for (Throwable cur = t; cur != null; cur = cur.getCause())
        {
            if (cur instanceof InjectedFault || cur instanceof LogFaultException)
                return true;
            for (Throwable suppressed : cur.getSuppressed())
            {
                if (isInjected(suppressed))
                    return true;
            }
        }
        return false;
    }

    public static boolean isCancellation(Throwable t)
    {
        for (Throwable cur = t; cur != null; cur = cur.getCause())
        {
            if (cur instanceof CancellationException)
                return true;
        }
        return false;
    }

    /**
     * Records the last status the progress log was told each command had reached, so a test can ask whether the log's
     * view survived a rolled back update; also the injection point for a fault at the top of
     * {@code SafeCommandStore.update()}.
     */
    public static class RecordingProgressLog implements ProgressLog
    {
        private final Harness harness;
        private final Map<TxnId, SaveStatus> updated = Collections.synchronizedMap(new LinkedHashMap<>());

        RecordingProgressLog(Harness harness)
        {
            this.harness = harness;
        }

        /** the last status the log was told {@code txnId} reached, or null */
        public @Nullable SaveStatus lastUpdate(TxnId txnId)
        {
            return updated.get(txnId);
        }

        @Override
        public void update(SafeCommandStore safeStore, Command before, Command after, boolean force)
        {
            harness.maybeFail(Point.PROGRESS_LOG_UPDATE, after.txnId(), after.saveStatus());
            updated.put(after.txnId(), after.saveStatus());
        }

        @Override
        public void remoteCallback(SafeCommandStore safeStore, SafeCommand safeCommand, SaveStatus remoteStatus, int callbackId, Node.Id from) {}

        @Override
        public void waiting(BlockedUntil blockedUntil, SafeCommandStore safeStore, SafeCommand blockedBy, Route<?> blockedOnRoute, Participants<?> blockedOnParticipants, StoreParticipants participants) {}

        @Override public void invalidIfUncommitted(TxnId txnId) {}

        /** a cleared transaction is one the log has forgotten, so it no longer believes anything about its status */
        @Override
        public void clear(TxnId txnId)
        {
            updated.remove(txnId);
        }

        @Override public void clearBefore(SafeCommandStore safeStore, TxnId clearWaitingBefore, TxnId clearAnyBefore) {}
        @Override public void start() {}
        @Override public void stop() {}
        @Override public void clear() {}
    }

    /**
     * A {@link NotifySink} that delegates to the default sink - so waiting transactions really are notified, and really
     * do get to schedule the work that follows - and then throws, reproducing the shape of a fault that arrives part
     * way through a CommandsForKey update.
     */
    public static class ThrowingNotifySink implements NotifySink
    {
        private final NotifySink defaultSink = new DefaultNotifySink();
        private final Harness harness;

        ThrowingNotifySink(Harness harness)
        {
            this.harness = harness;
        }

        @Override
        public void notWaiting(SafeCommandStore safeStore, TxnId txnId, RoutingKey key, long uniqueHlc)
        {
            defaultSink.notWaiting(safeStore, txnId, key, uniqueHlc);
            harness.record("notWaiting:" + txnId);
            harness.maybeFail(Point.CFK_NOTIFY, txnId, null);
        }

        @Override
        public void waitingOn(SafeCommandStore safeStore, CommandsForKey.TxnInfo txn, RoutingKey key, SaveStatus waitingOnStatus, ProgressLog.BlockedUntil blockedUntil, boolean notifyCfk)
        {
            defaultSink.waitingOn(safeStore, txn, key, waitingOnStatus, blockedUntil, notifyCfk);
        }
    }

    // ------------------------------------------------------------------ driving the pipeline

    /** everything a test needs to push one transaction through the replica pipeline */
    public static class Fixture
    {
        public final TxnId txnId;
        public final Txn txn;
        public final FullRoute<?> route;
        public final Route<?> partialRoute;
        public final PartialTxn partialTxn;
        public final PartialDeps deps;
        public final PartitionKey key;
        public Writes writes;
        public PersistableResult result;

        Fixture(TxnId txnId, Txn txn, FullRoute<?> route, Route<?> partialRoute, PartialTxn partialTxn, PartialDeps deps, PartitionKey key)
        {
            this.txnId = txnId;
            this.txn = txn;
            this.route = route;
            this.partialRoute = partialRoute;
            this.partialTxn = partialTxn;
            this.deps = deps;
            this.key = key;
        }

        public RoutingKey routingKey()
        {
            return key.toUnseekable();
        }

        public ExecutionContext context()
        {
            return unsequencedReadWrite(txnId, route, "Test");
        }
    }

    /**
     * @param dependsOn transactions this one should be told (at Commit) that it depends on, so that it waits for them
     */
    protected Fixture newTxn(Harness harness, int key, TxnId... dependsOn)
    {
        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);
        Txn txn = createWriteTxn(key);
        PartitionKey partitionKey = (PartitionKey) txn.keys().get(0);
        FullRoute<?> route = txn.keys().toRoute(partitionKey.toUnseekable());
        Ranges ranges = harness.store.unsafeGetRangesForEpoch().currentRanges();
        Route<?> partialRoute = route.overlapping(ranges);
        PartialTxn partialTxn = txn.intersecting(partialRoute, true);
        PartialDeps deps;
        if (dependsOn.length == 0)
        {
            deps = Deps.NONE.intersecting(fullRange(txn));
        }
        else
        {
            try (PartialDeps.Builder builder = PartialDeps.builder(partialRoute, true))
            {
                for (TxnId dep : dependsOn)
                    builder.add(partitionKey.toUnseekable(), dep);
                deps = builder.build();
            }
        }
        return new Fixture(txnId, txn, route, partialRoute, partialTxn, deps, partitionKey);
    }

    // ---- phases; each returns the failure the task reported, or null

    /** the replica-side phases these tests drive, in order */
    public enum Phase
    {
        PREACCEPT(Point.ON_PREACCEPTED),
        ACCEPT(Point.ON_ACCEPTED),
        COMMIT(Point.ON_STABLE),
        APPLY(Point.ON_PREAPPLIED);

        /** the {@link ReplicaEventListener} hook this phase's command update reports through */
        public final Point event;

        Phase(Point event)
        {
            this.event = event;
        }
    }

    /** run {@code phase} of the pipeline, returning the failure reported to the caller, or null */
    protected @Nullable Throwable runPhase(Harness harness, Fixture fixture, Phase phase)
    {
        switch (phase)
        {
            default: throw new AssertionError(phase);
            case PREACCEPT: return preaccept(harness, fixture);
            case ACCEPT: return accept(harness, fixture);
            case COMMIT: return commit(harness, fixture, Stable);
            case APPLY:
                computeWrites(harness, fixture);
                return apply(harness, fixture);
        }
    }

    /** run every phase before {@code phase}, with no fault injected */
    protected void runPhasesBefore(Harness harness, Fixture fixture, Phase phase)
    {
        for (Phase cur : Phase.values())
        {
            if (cur == phase)
                return;
            Throwable failure = runPhase(harness, fixture, cur);
            if (failure != null)
                throw new AssertionError(cur + " was expected to succeed", failure);
        }
    }

    protected @Nullable Throwable preaccept(Harness harness, Fixture fixture)
    {
        return run(harness, fixture, safeStore ->
            CheckedCommands.preaccept(safeStore, fixture.txnId, fixture.partialTxn, fixture.route));
    }

    protected @Nullable Throwable accept(Harness harness, Fixture fixture)
    {
        return run(harness, fixture, safeStore ->
            CheckedCommands.accept(safeStore, fixture.txnId, Ballot.ZERO, fixture.partialRoute, fixture.txnId, fixture.deps));
    }

    protected @Nullable Throwable commit(Harness harness, Fixture fixture, SaveStatus saveStatus)
    {
        return run(harness, fixture, safeStore ->
            CheckedCommands.commit(safeStore, saveStatus, Ballot.ZERO, fixture.txnId, fixture.route,
                                                fixture.partialTxn, fixture.txnId, fixture.deps));
    }

    /** run the transaction's reads and compute its writes, as the replica would when it becomes ready to execute */
    protected void computeWrites(Harness harness, Fixture fixture)
    {
        Function<SafeCommandStore, AsyncChain<Pair<Writes, PersistableResult>>> compute =
            safeStore -> AccordTestUtils.processTxnResultDirect(safeStore, fixture.txnId, fixture.partialTxn, fixture.txnId);
        Pair<Writes, PersistableResult> result = getBlockingUnchecked(harness.store.chain(fixture.context(), compute).flatMap(i -> i));
        fixture.writes = result.left;
        fixture.result = result.right;
    }

    protected @Nullable Throwable apply(Harness harness, Fixture fixture)
    {
        return run(harness, fixture, safeStore ->
            CheckedCommands.apply(safeStore, fixture.txnId, fixture.route, fixture.txnId, fixture.deps,
                                               fixture.partialTxn, fixture.writes, fixture.result));
    }

    /** run {@code body} in a task, injecting a {@link Point#TASK_BODY} fault after it if one is configured */
    protected @Nullable Throwable run(Harness harness, Fixture fixture, Consumer<SafeCommandStore> body)
    {
        return runTask(harness, fixture.context(), safeStore -> {
            body.accept(safeStore);
            harness.maybeFail(Point.TASK_BODY, fixture.txnId, safeStore.unsafeTryGet(fixture.txnId).current().saveStatus());
        });
    }

    protected @Nullable Throwable runTask(Harness harness, ExecutionContext context, Consumer<SafeCommandStore> body)
    {
        Throwable failure = null;
        try
        {
            getBlocking(harness.store.execute(context, body::accept));
        }
        catch (Throwable t)
        {
            failure = t;
        }
        drain(harness);
        return failure;
    }

    /** install a sink on {@code key}'s CommandsForKey that notifies as usual and then throws */
    protected void installThrowingNotifySink(Harness harness, Fixture fixture)
    {
        Consumer<SafeCommandStore> install = safeStore ->
            ((SaferCommandsForKey) safeStore.get(fixture.routingKey())).overrideSink(new ThrowingNotifySink(harness));
        getBlockingUnchecked(harness.store.chain(fixture.context(), install));
    }

    // ---- observation

    /** whether the transaction's write is visible in the table, i.e. whether its mutation was applied */
    protected static boolean rowExists(int key)
    {
        UntypedResultSet result = QueryProcessor.executeInternal("SELECT * FROM ks.tbl WHERE k = ? AND c = 0", key);
        return result != null && !result.isEmpty();
    }

    /** {@code txnId=STATUS} for every transaction CommandsForKey holds for {@code key} */
    protected String commandsForKeySummary(Harness harness, RoutingKey key)
    {
        CommandsForKey cfk = commandsForKey(harness, key);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cfk.size(); ++i)
            sb.append(i == 0 ? "" : ", ").append(cfk.txnId(i)).append('=').append(cfk.get(i).status());
        return sb.toString();
    }

    protected SaveStatus status(Harness harness, TxnId txnId)
    {
        Command command = command(harness, txnId);
        return command == null ? SaveStatus.Uninitialised : command.saveStatus();
    }

    protected @Nullable Command command(Harness harness, TxnId txnId)
    {
        Function<SafeCommandStore, Command> read = safeStore -> {
            SafeCommand safeCommand = safeStore.unsafeTryGetNoCleanup(txnId);
            return safeCommand == null ? null : safeCommand.current();
        };
        return getBlockingUnchecked(harness.store.chain(ExecutionContext.unsequenced(txnId, "Read"), read));
    }

    /** the command as it would be recovered from the journal: the cache entry is evicted first */
    protected SaveStatus persistedStatus(Harness harness, TxnId txnId)
    {
        evict(harness, txnId);
        return status(harness, txnId);
    }

    protected void evict(Harness harness, TxnId txnId)
    {
        try (AccordExecutor.ExclusiveGlobalCaches caches = harness.store.executor().lockCaches())
        {
            AccordCacheEntry<TxnId, Command, ?> entry = harness.store.cachesUnsafe().commands().getUnsafe(txnId);
            if (entry != null)
                caches.global.tryEvict(entry);
        }
    }

    protected CommandsForKey commandsForKey(Harness harness, RoutingKey key)
    {
        Function<SafeCommandStore, CommandsForKey> read = safeStore -> safeStore.get(key).current();
        return getBlockingUnchecked(harness.store.chain(unsequencedReadWrite(RoutingKeys.of(key), "Read"), read));
    }

    protected static void drain(Harness harness)
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (harness.store.executor().hasTasks())
        {
            if (System.nanoTime() > deadline)
                throw new AssertionError("the executor never went idle");
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    protected static <T> T getBlockingUnchecked(AsyncChain<T> chain)
    {
        try
        {
            return getBlocking(chain);
        }
        catch (Throwable t)
        {
            throw new AssertionError(t);
        }
    }

    // ---- the replica event listener, which doubles as a recorder and as an injection point

    protected static ReplicaEventListener replicaEvents(Harness harness)
    {
        return new ReplicaEventListener()
        {
            @Override
            public void onPreAccepted(SafeCommandStore safeStore, Command cmd)
            {
                harness.record("PreAccepted:" + cmd.txnId());
                harness.maybeFail(Point.ON_PREACCEPTED, cmd.txnId(), cmd.saveStatus());
            }

            @Override
            public void onAccepted(SafeCommandStore safeStore, Command cmd)
            {
                harness.record("Accepted:" + cmd.txnId());
                harness.maybeFail(Point.ON_ACCEPTED, cmd.txnId(), cmd.saveStatus());
            }

            @Override
            public void onCommitted(SafeCommandStore safeStore, Command cmd)
            {
                harness.record("Committed:" + cmd.txnId());
                harness.maybeFail(Point.ON_COMMITTED, cmd.txnId(), cmd.saveStatus());
            }

            @Override
            public void onStable(SafeCommandStore safeStore, Command cmd)
            {
                harness.record("Stable:" + cmd.txnId());
                harness.maybeFail(Point.ON_STABLE, cmd.txnId(), cmd.saveStatus());
            }

            @Override
            public void onPreApplied(SafeCommandStore safeStore, Command cmd)
            {
                harness.record("PreApplied:" + cmd.txnId());
                harness.maybeFail(Point.ON_PREAPPLIED, cmd.txnId(), cmd.saveStatus());
            }

            @Override
            public void onApplied(SafeCommandStore safeStore, Command cmd)
            {
                harness.record("Applied:" + cmd.txnId());
                harness.maybeFail(Point.ON_APPLIED, cmd.txnId(), cmd.saveStatus());
            }

                @Override
                public void onReadStarted(SafeCommandStore safeStore, Command cmd)
                {
                    harness.record("ReadStarted:" + cmd.txnId());
                }
        };
    }
}
