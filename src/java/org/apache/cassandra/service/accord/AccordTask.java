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
package org.apache.cassandra.service.accord;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import accord.api.Journal;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey;
import accord.primitives.AbstractRanges;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.collections.ObjectHashSet;
import org.apache.cassandra.service.accord.AccordCacheEntry.Status;
import org.apache.cassandra.service.accord.AccordCommandStore.Caches;
import org.apache.cassandra.service.accord.AccordExecutor.Task;
import org.apache.cassandra.service.accord.AccordExecutor.TaskQueue;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.Condition;

import static accord.local.KeyHistory.TIMESTAMPS;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.utils.Invariants.illegalState;
import static org.apache.cassandra.config.CassandraRelevantProperties.DTEST_ACCORD_JOURNAL_SANITY_CHECK_ENABLED;
import static org.apache.cassandra.service.accord.AccordTask.State.CANCELLED;
import static org.apache.cassandra.service.accord.AccordTask.State.FAILED;
import static org.apache.cassandra.service.accord.AccordTask.State.FAILING;
import static org.apache.cassandra.service.accord.AccordTask.State.FINISHED;
import static org.apache.cassandra.service.accord.AccordTask.State.INITIALIZED;
import static org.apache.cassandra.service.accord.AccordTask.State.LOADING;
import static org.apache.cassandra.service.accord.AccordTask.State.PERSISTING;
import static org.apache.cassandra.service.accord.AccordTask.State.RUNNING;
import static org.apache.cassandra.service.accord.AccordTask.State.SCANNING_RANGES;
import static org.apache.cassandra.service.accord.AccordTask.State.WAITING_TO_LOAD;
import static org.apache.cassandra.service.accord.AccordTask.State.WAITING_TO_RUN;
import static org.apache.cassandra.service.accord.AccordTask.State.WAITING_TO_SCAN_RANGES;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public abstract class AccordTask<R> extends Task implements Runnable, Function<SafeCommandStore, R>, Cancellable
{
    private static final Logger logger = LoggerFactory.getLogger(AccordTask.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);
    private static final boolean SANITY_CHECK = DTEST_ACCORD_JOURNAL_SANITY_CHECK_ENABLED.getBoolean();

    private static class LoggingProps
    {
        private static final String COMMAND_STORE = "command_store";
        private static final String ACCORD_TASK = "accord_task";
    }

    static class ForFunction<R> extends AccordTask<R>
    {
        private final Function<? super SafeCommandStore, R> function;

        public ForFunction(AccordCommandStore commandStore, PreLoadContext loadCtx, Function<? super SafeCommandStore, R> function)
        {
            super(commandStore, loadCtx);
            this.function = function;
        }

        @Override
        public R apply(SafeCommandStore commandStore)
        {
            return function.apply(commandStore);
        }
    }

    // TODO (desired): these anonymous ops are somewhat tricky to debug. We may want to at least give them names.
    static class ForConsumer extends AccordTask<Void>
    {
        private final Consumer<? super SafeCommandStore> consumer;

        private ForConsumer(AccordCommandStore commandStore, PreLoadContext loadCtx, Consumer<? super SafeCommandStore> consumer)
        {
            super(commandStore, loadCtx);
            this.consumer = consumer;
        }

        @Override
        public Void apply(SafeCommandStore commandStore)
        {
            consumer.accept(commandStore);
            return null;
        }
    }

    public static <T> AccordTask<T> create(CommandStore commandStore, PreLoadContext ctx, Function<? super SafeCommandStore, T> function)
    {
        return new ForFunction<>((AccordCommandStore) commandStore, ctx, function);
    }

    public static AccordTask<Void> create(CommandStore commandStore, PreLoadContext ctx, Consumer<? super SafeCommandStore> consumer)
    {
        return new ForConsumer((AccordCommandStore) commandStore, ctx, consumer);
    }

    public enum State
    {
        INITIALIZED(),
        WAITING_TO_SCAN_RANGES(INITIALIZED),
        SCANNING_RANGES(WAITING_TO_SCAN_RANGES),
        WAITING_TO_LOAD(INITIALIZED, SCANNING_RANGES),
        LOADING(INITIALIZED, SCANNING_RANGES, WAITING_TO_LOAD),
        WAITING_TO_RUN(INITIALIZED, SCANNING_RANGES, WAITING_TO_LOAD, LOADING),
        RUNNING(WAITING_TO_RUN),
        PERSISTING(RUNNING),
        FAILING(WAITING_TO_SCAN_RANGES, SCANNING_RANGES, WAITING_TO_LOAD, LOADING, WAITING_TO_RUN, RUNNING, PERSISTING),
        FINISHED(RUNNING, PERSISTING),
        CANCELLED(WAITING_TO_SCAN_RANGES, SCANNING_RANGES, WAITING_TO_LOAD, LOADING, WAITING_TO_RUN),
        FAILED(WAITING_TO_SCAN_RANGES, SCANNING_RANGES, WAITING_TO_LOAD, LOADING, WAITING_TO_RUN, RUNNING, PERSISTING, FAILING);

        private final int permittedFrom;

        State()
        {
            this.permittedFrom = 0;
        }

        State(State ... permittedFroms)
        {
            int permittedFrom = 0;
            for (State state : permittedFroms)
                permittedFrom |= 1 << state.ordinal();
            this.permittedFrom = permittedFrom;
        }

        boolean isPermittedFrom(State prev)
        {
            return (permittedFrom & (1 << prev.ordinal())) != 0;
        }

        boolean isExecuted()
        {
            return this.compareTo(PERSISTING) >= 0;
        }

        boolean isComplete()
        {
            return this.compareTo(FINISHED) >= 0;
        }
    }

    private State state = INITIALIZED;
    private final PreLoadContext preLoadContext;
    private final String loggingId;

    // TODO (expected): merge all of these maps into one
    @Nullable Object2ObjectHashMap<TxnId, AccordSafeCommand> commands;
    @Nullable Object2ObjectHashMap<RoutingKey, AccordSafeTimestampsForKey> timestampsForKey;
    @Nullable Object2ObjectHashMap<RoutingKey, AccordSafeCommandsForKey> commandsForKey;
    @Nullable Object2ObjectHashMap<Object, AccordSafeState<?, ?>> loading;
    // TODO (expected): collection supporting faster deletes but still fast poll (e.g. some ordered collection)
    @Nullable ArrayDeque<AccordCacheEntry<?, ?>> waitingToLoad;
    @Nullable RangeTxnScanner rangeScanner;
    boolean hasRanges;
    @Nullable CommandsForRanges commandsForRanges;
    @Nullable private TaskQueue queued;

    private BiConsumer<? super R, Throwable> callback;
    private List<Command> sanityCheck;
    public long createdAt = nanoTime(), loadedAt, runQueuedAt, runAt, completedAt;

    private void setLoggingIds()
    {
        MDC.put(LoggingProps.COMMAND_STORE, commandStore.loggingId);
        MDC.put(LoggingProps.ACCORD_TASK, loggingId);
    }

    private void clearLoggingIds()
    {
        MDC.remove(LoggingProps.COMMAND_STORE);
        MDC.remove(LoggingProps.ACCORD_TASK);
    }

    public AccordTask(AccordCommandStore commandStore, PreLoadContext preLoadContext)
    {
        super(commandStore);
        this.loggingId = "0x" + Integer.toHexString(System.identityHashCode(this));
        this.preLoadContext = preLoadContext;

        if (logger.isTraceEnabled())
        {
            setLoggingIds();
            logger.trace("Created {} on {}", this, commandStore);
            clearLoggingIds();
        }
    }

    @Override
    public String toString()
    {
        return "AccordTask{" + state + "}-" + loggingId;
    }

    public String toDescription()
    {
        return "AccordTask{" + state + "}-" + loggingId + ": "
               + (queued == null ? "unqueued" : queued.kind)
               + ", primaryTxnId: " + preLoadContext.primaryTxnId()
               + ", waitingToLoad: " + summarise(waitingToLoad)
               + ", loading:" + summarise(loading, AccordSafeState::global)
               + ", cfks:" + summarise(commandsForKey, AccordSafeState::global)
               + ", tfks:" + summarise(timestampsForKey, AccordSafeState::global)
               + ", txns:" + summarise(commands, AccordSafeState::global);

    }

    private static <V> String summarise(Map<?, V> map, Function<V, Object> transform)
    {
        if (map == null)
            return "null";

        return summarise(map.values(), transform);
    }

    private static <V> String summarise(Collection<V> collection)
    {
        return summarise(collection, Function.identity());
    }

    private static <V> String summarise(Collection<V> collection, Function<? super V, Object> transform)
    {
        if (collection == null)
            return "null";

        StringBuilder out = new StringBuilder("[");
        int count = 0;
        for (V v : collection)
        {
            if (count++ > 0)
            {
                out.append(',');
                if (count >= 10)
                {
                    out.append("...(*").append(collection.size() - 10).append(')');
                    break;
                }
            }
            out.append(transform.apply(v));
        }
        out.append(']');
        return out.toString();
    }

    private void state(State state)
    {
        Invariants.checkState(state.isPermittedFrom(this.state), "%s forbidden from %s", state, this, AccordTask::toDescription);
        this.state = state;
        if (state == WAITING_TO_RUN)
        {
            Invariants.checkState(rangeScanner == null || rangeScanner.scanned);
            Invariants.checkState(loading == null && waitingToLoad == null, "WAITING_TO_RUN => no loading or waiting; found %s", this, AccordTask::toDescription);
            loadedAt = nanoTime();
        }
        else if (state == RUNNING)
        {
            runAt = nanoTime();
        }
        else if (state.isExecuted())
        {
            completedAt = nanoTime();
        }
    }

    Unseekables<?> keys()
    {
        return preLoadContext.keys();
    }

    public AsyncChain<R> chain()
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super R, Throwable> callback)
            {
                Invariants.checkState(AccordTask.this.callback == null);
                AccordTask.this.callback = callback;
                commandStore.tryPreSetup(AccordTask.this);
                commandStore.executor().submit(AccordTask.this);
                return AccordTask.this;
            }
        };
    }

    // to be invoked only by the CommandStore owning thread, to take references to objects already in use by the current execution
    public void presetup(AccordTask<?> parent)
    {
        // note we use the caches "unsafely" here deliberately, as we only reference commands we already have references to
        // so we do not mutate anything, except the atomic counter of references
        if (parent.commands != null)
        {
            for (TxnId txnId : preLoadContext.txnIds())
                presetupExclusive(txnId, AccordTask::ensureCommands, parent.commands, commandStore.cachesUnsafe().commands());
        }

        if ((preLoadContext.keyHistory() == TIMESTAMPS ? parent.timestampsForKey : parent.commandsForKey) == null) return;
        if (preLoadContext.keys().domain() != Key) return;
        switch (preLoadContext.keyHistory())
        {
            default: throw new AssertionError("Unhandled KeyHistory: " + preLoadContext.keyHistory());
            case NONE:
                break;

            case TIMESTAMPS:
                for (RoutingKey key : (AbstractUnseekableKeys)preLoadContext.keys())
                    presetupExclusive(key, AccordTask::ensureTimestampsForKey, parent.timestampsForKey, commandStore.cachesUnsafe().timestampsForKeys());
                break;

            case ASYNC:
            case RECOVER:
            case INCR:
            case SYNC:
                for (RoutingKey key : (AbstractUnseekableKeys)preLoadContext.keys())
                    presetupExclusive(key, AccordTask::ensureCommandsForKey, parent.commandsForKey, commandStore.cachesUnsafe().commandsForKeys());
                break;
        }
    }

    public void setupExclusive()
    {
        setupInternal(commandStore.cachesExclusive());
        state(rangeScanner != null ? WAITING_TO_SCAN_RANGES
                                   : waitingToLoad != null ? State.WAITING_TO_LOAD
                                    : loading != null ? LOADING : WAITING_TO_RUN);
    }

    private void setupInternal(Caches caches)
    {
        {
            boolean hasPreSetup = commands != null;
            for (TxnId txnId : preLoadContext.txnIds())
            {
                if (hasPreSetup && completePresetupExclusive(txnId, commands, caches.commands()))
                    continue;
                setupExclusive(txnId, AccordTask::ensureCommands, caches.commands());
            }
        }

        if (preLoadContext.keys().isEmpty())
            return;

        switch (preLoadContext.keys().domain())
        {
            case Key: setupKeyLoadsExclusive(caches, (AbstractUnseekableKeys)preLoadContext.keys(), false); break;
            case Range: setupRangeLoadsExclusive(caches);
        }
    }

    private void setupKeyLoadsExclusive(Caches caches, Iterable<? extends RoutingKey> keys, boolean isToCompleteRangeScan)
    {
        switch (preLoadContext.keyHistory())
        {
            default: throw new AssertionError("Unhandled KeyHistory: " + preLoadContext.keyHistory());
            case NONE:
                break;

            case TIMESTAMPS:
            {
                boolean hasPreSetup = timestampsForKey != null;
                for (RoutingKey key : keys)
                {
                    if (hasPreSetup && completePresetupExclusive(key, timestampsForKey, caches.timestampsForKeys())) continue;
                    setupExclusive(key, AccordTask::ensureTimestampsForKey, caches.timestampsForKeys());
                }
                break;
            }
            case RECOVER:
                if (!isToCompleteRangeScan)
                {
                    Invariants.checkState(rangeScanner == null);
                    rangeScanner = new RangeTxnScanner();
                }

            case ASYNC:
            case INCR:
            case SYNC:
            {
                boolean hasPreSetup = commandsForKey != null;
                for (RoutingKey key : keys)
                {
                    if (hasPreSetup && completePresetupExclusive(key, commandsForKey, caches.commandsForKeys())) continue;
                    setupExclusive(key, AccordTask::ensureCommandsForKey, caches.commandsForKeys());
                }
                break;
            }
        }
    }

    private void setupRangeLoadsExclusive(Caches caches)
    {
        switch (preLoadContext.keyHistory())
        {
            default: throw new AssertionError("Unhandled KeyHistory: " + preLoadContext.keyHistory());
            case NONE:
            case ASYNC:
                break;

            case INCR:
                throw new AssertionError("Incremental mode should only be used with an explicit list of keys");

            case TIMESTAMPS:
                throw new AssertionError("TimestampsForKey unsupported for range transactions");

            case RECOVER:
            case SYNC:
                hasRanges = true;
                rangeScanner = new RangeTxnAndKeyScanner(caches.commandsForKeys());
        }
    }

    // expects mutual exclusivity only on the command store
    private <K, V, S extends AccordSafeState<K, V>> void presetupExclusive(K k, Function<AccordTask<?>, Map<? super K, ? super S>> loaded, Map<? super K, S> parentMap, AccordCache.Type<K, V, S>.Instance cache)
    {
        AccordSafeState<K, V> ref = parentMap.get(k);
        if (ref == null)
            return;

        AccordCacheEntry<K, V> node = ref.global();
        int refs = node.increment();
        Invariants.checkState(refs > 1);
        loaded.apply(this).put(k, cache.parent().adapter().safeRef(node));
    }

    // expects to hold lock
    private <K, V, S extends AccordSafeState<K, V>> boolean completePresetupExclusive(K k, Map<? super K, S> map, AccordCache.Type<K, V, S>.Instance cache)
    {
        AccordSafeState<K, V> preacquired = map.get(k);
        if (preacquired != null)
        {
            cache.recordPreAcquired(preacquired);
            return true;
        }
        return false;
    }

    // expects to hold lock
    private <K, V, S extends AccordSafeState<K, V>> void setupExclusive(K k, Function<AccordTask<?>, Map<? super K, ? super S>> loaded, AccordCache.Type<K, V, S>.Instance cache)
    {
        S safeRef = cache.acquire(k);
        Status entryStatus = safeRef.global().status();
        Map<? super K, ? super S> map;
        switch (entryStatus)
        {
            default: throw new IllegalStateException("Unhandled global state: " + entryStatus);
            case WAITING_TO_LOAD:
            case LOADING:
                map = ensureLoading();
                break;
            case SAVING:
            case LOADED:
            case MODIFIED:
            case FAILED_TO_SAVE:
                map = loaded.apply(this);
        }

        Object prev = map.putIfAbsent(k, safeRef);
        if (prev != null)
        {
            noSpamLogger.warn("PreLoadContext {} contained key {} more than once", map, k);
            cache.release(safeRef, this);
        }
        else if (map == loading)
        {
            if (entryStatus == Status.WAITING_TO_LOAD)
                ensureWaitingToLoad().add(safeRef.global());
            safeRef.global().loadingOrWaiting().add(this);
            Invariants.paranoid(safeRef.global().loadingOrWaiting().waiters().size() == safeRef.global().references());
        }
    }

    // expects to hold lock
    public boolean onLoad(AccordCacheEntry<?, ?> state)
    {
        AccordSafeState<?, ?> safeRef = loading == null ? null : loading.remove(state.key());
        Invariants.checkState(safeRef != null && safeRef.global() == state, "Expected to find %s loading; found %s", state, this, AccordTask::toDescription);
        if (safeRef.getClass() == AccordSafeCommand.class)
        {
            ensureCommands().put((TxnId)state.key(), (AccordSafeCommand) safeRef);
        }
        else if (safeRef.getClass() == AccordSafeCommandsForKey.class)
        {
            ensureCommandsForKey().put((RoutingKey) state.key(), (AccordSafeCommandsForKey) safeRef);
        }
        else
        {
            Invariants.checkState (safeRef.getClass() == AccordSafeTimestampsForKey.class);
            ensureTimestampsForKey().put((RoutingKey) state.key(), (AccordSafeTimestampsForKey) safeRef);
        }

        if (!loading.isEmpty())
            return false;

        loading = null;
        if (this.state.compareTo(State.WAITING_TO_LOAD) < 0)
            return false;

        Invariants.checkState(waitingToLoad == null, "Invalid state: %s", this, AccordTask::toDescription);
        state(WAITING_TO_RUN);
        return true;
    }

    // expects to hold lock
    public boolean onLoading(AccordCacheEntry<?, ?> state)
    {
        boolean removed = waitingToLoad != null && waitingToLoad.remove(state);
        Invariants.checkState(removed, "%s not found in waitingToLoad %s", state, this, AccordTask::toDescription);
        if (!waitingToLoad.isEmpty())
            return false;

        return onEmptyWaitingToLoad();
    }

    private boolean onEmptyWaitingToLoad()
    {
        waitingToLoad = null;
        if (this.state.compareTo(State.WAITING_TO_LOAD) < 0)
            return false;

        state(loading == null ? WAITING_TO_RUN : LOADING);
        return true;
    }

    public PreLoadContext preLoadContext()
    {
        return preLoadContext;
    }

    public Map<TxnId, AccordSafeCommand> commands()
    {
        return commands;
    }

    public Map<TxnId, AccordSafeCommand> ensureCommands()
    {
        if (commands == null)
            commands = new Object2ObjectHashMap<>();
        return commands;
    }

    public Map<RoutingKey, AccordSafeTimestampsForKey> timestampsForKey()
    {
        return timestampsForKey;
    }

    public Map<RoutingKey, AccordSafeTimestampsForKey> ensureTimestampsForKey()
    {
        if (timestampsForKey == null)
            timestampsForKey = new Object2ObjectHashMap<>();
        return timestampsForKey;
    }

    public Map<RoutingKey, AccordSafeCommandsForKey> commandsForKey()
    {
        return commandsForKey;
    }

    public Map<RoutingKey, AccordSafeCommandsForKey> ensureCommandsForKey()
    {
        if (commandsForKey == null)
            commandsForKey = new Object2ObjectHashMap<>();
        return commandsForKey;
    }

    private Map<Object, AccordSafeState<?, ?>> ensureLoading()
    {
        if (loading == null)
            loading = new Object2ObjectHashMap<>();
        return loading;
    }

    private ArrayDeque<AccordCacheEntry<?, ?>> ensureWaitingToLoad()
    {
        Invariants.checkState(state.compareTo(WAITING_TO_LOAD) <= 0, "Expected status to be on or before WAITING_TO_LOAD; found %s", this, AccordTask::toDescription);
        if (waitingToLoad == null)
            waitingToLoad = new ArrayDeque<>();
        return waitingToLoad;
    }

    public AccordCacheEntry<?, ?> pollWaitingToLoad()
    {
        Invariants.checkState(state == State.WAITING_TO_LOAD, "Expected status to be WAITING_TO_LOAD; found %s", this, AccordTask::toDescription);
        if (waitingToLoad == null)
            return null;

        AccordCacheEntry<?, ?> next = waitingToLoad.poll();
        if (waitingToLoad.isEmpty())
            onEmptyWaitingToLoad();
        return next;
    }

    public AccordCacheEntry<?, ?> peekWaitingToLoad()
    {
        return waitingToLoad == null ? null : waitingToLoad.peek();
    }

    private void maybeSanityCheck(AccordSafeCommand safeCommand)
    {
        if (SANITY_CHECK)
        {
            if (sanityCheck == null)
                sanityCheck = new ArrayList<>(commands.size());
            sanityCheck.add(safeCommand.current());
        }
    }

    private void save(List<Journal.CommandUpdate> diffs, Runnable onFlush)
    {
        if (sanityCheck != null)
        {
            Invariants.checkState(SANITY_CHECK);
            Condition condition = Condition.newOneTimeCondition();
            this.commandStore.appendCommands(diffs, condition::signal);
            condition.awaitUninterruptibly();

            for (Command check : sanityCheck)
                this.commandStore.sanityCheckCommand(check);

            if (onFlush != null) onFlush.run();
        }
        else
        {
            this.commandStore.appendCommands(diffs, onFlush);
        }
    }

    @Override
    protected void preRunExclusive()
    {
        state(RUNNING);
        if (rangeScanner != null)
        {
            commandsForRanges = rangeScanner.finish(commandStore.cachesExclusive());
            rangeScanner = null;
        }
        if (commands != null)
            commands.forEach((k, v) -> v.preExecute());
        if (commandsForKey != null)
            commandsForKey.forEach((k, v) -> v.preExecute());
        if (timestampsForKey != null)
            timestampsForKey.forEach((k, v) -> v.preExecute());
    }

    @Override
    public void run()
    {
        setLoggingIds();
        logger.trace("Running {} with state {}", this, state);
        AccordSafeCommandStore safeStore = null;
        try
        {
            if (state != RUNNING)
                throw illegalState("Unexpected state " + toDescription());

            safeStore = commandStore.begin(this, commandsForRanges);
            R result = apply(safeStore);

            // TODO (required): currently, we are not very efficient about ensuring that we persist the absolute minimum amount of state. Improve that.
            List<Journal.CommandUpdate> changes = null;
            if (commands != null)
            {
                for (AccordSafeCommand safeCommand : commands.values())
                {
                    if (safeCommand.txnId().is(EphemeralRead))
                        continue;

                    Journal.CommandUpdate diff = safeCommand.update();
                    if (diff == null)
                        continue;

                    if (changes == null)
                        changes = new ArrayList<>(commands.size());
                    changes.add(diff);

                    maybeSanityCheck(safeCommand);
                }
            }

            boolean flush = changes != null || safeStore.fieldUpdates() != null;
            if (flush)
            {
                state(PERSISTING);
                Runnable onFlush = () -> finish(result, null);
                if (safeStore.fieldUpdates() != null)
                    commandStore.persistFieldUpdates(safeStore.fieldUpdates(), changes == null ? onFlush : null);
                if (changes != null)
                    save(changes, onFlush);
            }

            commandStore.complete(safeStore);
            safeStore = null;
            if (!flush)
                finish(result, null);
        }
        catch (Throwable t)
        {
            if (safeStore != null)
            {
                revert();
                commandStore.abort(safeStore);
            }
            throw t;
        }
        finally
        {
            logger.trace("Exiting {}", this);
            clearLoggingIds();
        }
    }

    public void fail(Throwable throwable)
    {
        commandStore.agent().onUncaughtException(throwable);
        if (state.isComplete())
            return;

        if (commandStore.hasSafeStore())
            commandStore.agent().onUncaughtException(new IllegalStateException(String.format("Failure to cleanup safe store for %s; status=%s", this, state), throwable));

        state(FAILING);
        if (callback != null)
            callback.accept(null, throwable);
    }

    public void failExclusive(Throwable throwable)
    {
        boolean newFailure = state != FAILING;
        try
        {
            if (newFailure)
            {
                commandStore.agent().onUncaughtException(throwable);
                if (state.isComplete())
                    return;

                if (commandStore.hasSafeStore())
                    commandStore.agent().onUncaughtException(new IllegalStateException(String.format("Failure to cleanup safe store for %s; status=%s", this, state), throwable));
            }

            state(FAILED);
        }
        finally
        {
            if (newFailure && callback != null)
                callback.accept(null, throwable);
        }
    }

    protected void cleanupExclusive()
    {
        releaseResources(commandStore.cachesExclusive());
        if (state == FAILING)
            state(FAILED);
    }

    @Nullable
    public RangeTxnScanner rangeScanner()
    {
        return rangeScanner;
    }

    public boolean hasRanges()
    {
        return hasRanges;
    }

    @Override
    public void cancel()
    {
        commandStore.executor().cancel(this);
    }

    public void cancelExclusive()
    {
        releaseResources(commandStore.cachesExclusive());
        state(CANCELLED);
        if (callback != null)
            callback.accept(null, new CancellationException());
    }

    public State state()
    {
        return state;
    }

    private void finish(R result, Throwable failure)
    {
        state(failure == null ? FINISHED : FAILED);
        if (callback != null)
            callback.accept(result, failure);
    }

    void releaseResources(Caches caches)
    {
        try
        {
            // TODO (expected): we should destructively iterate to avoid invoking second time in fail; or else read and set to null
            if (rangeScanner != null)
            {
                rangeScanner.cleanup(caches);
                rangeScanner = null;
            }
            if (commands != null)
            {
                commands.forEach((k, v) -> caches.commands().release(v, this));
                commands.clear();
                commands = null;
            }
            if (timestampsForKey != null)
            {
                timestampsForKey.forEach((k, v) -> caches.timestampsForKeys().release(v, this));
                timestampsForKey.clear();
                timestampsForKey = null;
            }
            if (commandsForKey != null)
            {
                commandsForKey.forEach((k, v) -> caches.commandsForKeys().release(v, this));
                commandsForKey.clear();
                commandsForKey = null;
            }
            if (waitingToLoad != null)
            {
                while (!waitingToLoad.isEmpty())
                    waitingToLoad.poll().loadingOrWaiting().remove(this);
                waitingToLoad = null;
            }
            if (loading != null)
            {
                loading.forEach((k, v) -> caches.global().release(v, this));
                loading.clear();
                loading = null;
            }
        }
        catch (Throwable t)
        {
            releaseResourcesSlow(caches, t);
            throw t;
        }
    }

    private void releaseResourcesSlow(Caches caches, Throwable suppressedBy)
    {
        if (commands != null)
        {
            safeRelease(commands, caches.commands(), suppressedBy);
            commands.clear();
            commands = null;
        }
        if (timestampsForKey != null)
        {
            safeRelease(timestampsForKey, caches.timestampsForKeys(), suppressedBy);
            timestampsForKey.clear();
            timestampsForKey = null;
        }
        if (commandsForKey != null)
        {
            safeRelease(commandsForKey, caches.commandsForKeys(), suppressedBy);
            commandsForKey.clear();
            commandsForKey = null;
        }
        if (waitingToLoad != null)
        {
            while (!waitingToLoad.isEmpty())
            {
                try { waitingToLoad.poll().loadingOrWaiting().remove(this); }
                catch (Throwable t) { suppressedBy.addSuppressed(t); }
            }
            waitingToLoad = null;
        }
        if (loading != null)
        {
            safeRelease(loading, caches.global(), suppressedBy);
            loading.clear();
            loading = null;
        }
    }

    private <K, V> void safeRelease(Map<K, ? extends AccordSafeState<K, V>> map, AccordCache.Type<K, V, ?>.Instance cache, Throwable suppressedBy)
    {
        for (AccordSafeState<K, V> safeState : map.values())
        {
            if (safeState.invalidated()) continue;
            try { cache.release(safeState, this); }
            catch (Throwable t) { suppressedBy.addSuppressed(t); }
        }
    }

    private void safeRelease(Map<?, ? extends AccordSafeState<?, ?>> map, AccordCache cache, Throwable suppressedBy)
    {
        for (AccordSafeState<?, ?> safeState : map.values())
        {
            if (safeState.invalidated()) continue;
            try { cache.release(safeState, this); }
            catch (Throwable t) { suppressedBy.addSuppressed(t); }
        }
    }

    void revert()
    {
        if (commands != null)
            commands.forEach((k, v) -> v.revert());
        if (timestampsForKey != null)
            timestampsForKey.forEach((k, v) -> v.revert());
        if (commandsForKey != null)
            commandsForKey.forEach((k, v) -> v.revert());
    }

    protected void addToQueue(TaskQueue queue)
    {
        Invariants.checkState(queue.kind == state || (queue.kind == State.WAITING_TO_LOAD && state == WAITING_TO_SCAN_RANGES), "Invalid queue type: %s vs %s", queue.kind, this, AccordTask::toDescription);
        Invariants.checkState(this.queued == null, "Already queued with state: %s", this, AccordTask::toDescription);
        queued = queue;
        queue.append(this);
    }

    @Nullable
    TaskQueue<?> queued()
    {
        return queued;
    }

    TaskQueue<?> unqueue()
    {
        TaskQueue<?> wasQueued = queued;
        queued.remove(this);
        queued = null;
        return wasQueued;
    }

    TaskQueue<?> unqueueIfQueued()
    {
        if (queued == null)
            return null;
        return unqueue();
    }

    public class RangeTxnAndKeyScanner extends RangeTxnScanner
    {
        class KeyWatcher implements AccordCache.Listener<RoutingKey, CommandsForKey>
        {
            @Override
            public void onUpdate(AccordCacheEntry<RoutingKey, CommandsForKey> state)
            {
                if (ranges.contains(state.key()))
                    reference(state);
            }
        }

        final Set<AccordRoutingKey.TokenKey> intersectingKeys = new ObjectHashSet<>();
        final KeyWatcher keyWatcher = new KeyWatcher();
        final Ranges ranges = ((AbstractRanges) preLoadContext.keys()).toRanges();
        final AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKeyCache;

        public RangeTxnAndKeyScanner(AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKeyCache)
        {
            this.commandsForKeyCache = commandsForKeyCache;
        }

        boolean scanned;

        void runInternal()
        {
            for (Range range : ranges)
            {
                AccordKeyspace.findAllKeysBetween(commandStore.id(),
                                                  (AccordRoutingKey) range.start(), range.startInclusive(),
                                                  (AccordRoutingKey) range.end(), range.endInclusive(),
                                                  intersectingKeys::add);
            }
            super.runInternal();
        }

        private void reference(AccordCacheEntry<RoutingKey, CommandsForKey> entry)
        {
            if (loading != null && loading.containsKey(entry.key()))
                return;

            switch (entry.status())
            {
                default: throw new AssertionError("Unhandled Status: " + entry.status());
                case WAITING_TO_LOAD:
                case LOADING:
                    if (scanned)
                        // if we've finished scanning and not already taken a reference we shouldn't need to witness (unless modified)
                        return;
                    ensureLoading().put(entry.key(), commandsForKeyCache.acquire(entry));
                    if (entry.status() == Status.WAITING_TO_LOAD)
                        ensureWaitingToLoad().add(entry);
                    entry.loadingOrWaiting().add(AccordTask.this);
                    return;

                case MODIFIED:
                case SAVING:
                case LOADED:
                case FAILED_TO_SAVE:
                    if (commandsForKey != null && commandsForKey.containsKey(entry.key()))
                        return;
                    ensureCommandsForKey().putIfAbsent(entry.key(), commandsForKeyCache.acquire(entry));
            }
        }

        void startInternal(Caches caches)
        {
            for (RoutingKey key : caches.commandsForKeys().keySet())
            {
                if (ranges.contains(key))
                    intersectingKeys.add((AccordRoutingKey.TokenKey) key);
            }
            caches.commandsForKeys().register(keyWatcher);
            super.startInternal(caches);
        }

        void scannedInternal()
        {
            if (commandsForKey != null)
                intersectingKeys.removeAll(commandsForKey.keySet());
            if (loading != null)
                intersectingKeys.removeAll(loading.keySet());
            setupKeyLoadsExclusive(commandStore.cachesExclusive(), intersectingKeys, true);
            super.scannedInternal();
        }

        void cleanup(Caches caches)
        {
            caches.commandsForKeys().tryUnregister(keyWatcher);
            super.cleanup(caches);
        }

        CommandsForRanges finish(Caches caches)
        {
            caches.commandsForKeys().unregister(keyWatcher);
            return super.finish(caches);
        }
    }

    public class RangeTxnScanner implements Runnable
    {
        class CommandWatcher implements AccordCache.Listener<TxnId, Command>
        {
            @Override
            public void onUpdate(AccordCacheEntry<TxnId, Command> state)
            {
                CommandsForRanges.Summary summary = summaryLoader.ifRelevant(state);
                if (summary != null)
                    summaries.put(summary.txnId, summary);
            }
        }

        final ConcurrentHashMap<TxnId, CommandsForRanges.Summary> summaries = new ConcurrentHashMap<>();
        // TODO (expected): produce key summaries to avoid locking all in memory
        final CommandWatcher commandWatcher = new CommandWatcher();
        final Unseekables<?> keysOrRanges = preLoadContext.keys();

        CommandsForRanges.Loader summaryLoader;
        boolean scanned;

        @Override
        public void run()
        {
            try
            {
                runInternal();
            }
            catch (Throwable t)
            {
                commandStore.executor().onScannedRanges(AccordTask.this, t);
                throw t;
            }
            commandStore.executor().onScannedRanges(AccordTask.this, null);
        }

        void runInternal()
        {
            summaryLoader.intersects(txnId -> {
                if (summaries.containsKey(txnId))
                    return;

                CommandsForRanges.Summary summary = summaryLoader.load(txnId);
                if (summary != null)
                    summaries.putIfAbsent(txnId, summary);
            });
        }

        public void start(BiFunction<Task, Runnable, Cancellable> executor)
        {
            Caches caches = commandStore.cachesExclusive();
            state(SCANNING_RANGES);
            startInternal(caches);
            executor.apply(AccordTask.this, this);
        }

        void startInternal(Caches caches)
        {
            summaryLoader = commandStore.diskCommandsForRanges().loader(preLoadContext.primaryTxnId(), preLoadContext.keyHistory(), keysOrRanges);
            summaryLoader.forEachInCache(summary -> summaries.put(summary.txnId, summary), caches);
            caches.commands().register(commandWatcher);
        }

        public void scannedExclusive()
        {
            Invariants.checkState(state == SCANNING_RANGES, "Expected SCANNING_RANGES; found %s", AccordTask.this, AccordTask::toDescription);
            scanned = true;
            scannedInternal();
            if (loading == null) state(WAITING_TO_RUN);
            else if (waitingToLoad == null) state(LOADING);
            else state(State.WAITING_TO_LOAD);
        }

        void scannedInternal()
        {
        }

        void cleanup(Caches caches)
        {
            caches.commands().tryUnregister(commandWatcher);
        }

        CommandsForRanges finish(Caches caches)
        {
            caches.commands().unregister(commandWatcher);
            return new CommandsForRanges(summaries);
        }
    }

}
