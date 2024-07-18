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
package org.apache.cassandra.service.accord.async;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import accord.api.Key;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordSafeCommand;
import org.apache.cassandra.service.accord.AccordSafeCommandStore;
import org.apache.cassandra.service.accord.AccordSafeCommandsForKey;
import org.apache.cassandra.service.accord.AccordSafeCommandsForRanges;
import org.apache.cassandra.service.accord.AccordSafeState;
import org.apache.cassandra.service.accord.AccordSafeTimestampsForKey;
import org.apache.cassandra.service.accord.SavedCommand;

import static org.apache.cassandra.service.accord.async.AsyncLoader.txnIds;
import static org.apache.cassandra.service.accord.async.AsyncOperation.State.COMPLETING;
import static org.apache.cassandra.service.accord.async.AsyncOperation.State.FAILED;
import static org.apache.cassandra.service.accord.async.AsyncOperation.State.FINISHED;
import static org.apache.cassandra.service.accord.async.AsyncOperation.State.INITIALIZED;
import static org.apache.cassandra.service.accord.async.AsyncOperation.State.LOADING;
import static org.apache.cassandra.service.accord.async.AsyncOperation.State.PREPARING;
import static org.apache.cassandra.service.accord.async.AsyncOperation.State.RUNNING;

public abstract class AsyncOperation<R> extends AsyncChains.Head<R> implements Runnable, Function<SafeCommandStore, R>
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncOperation.class);

    private static class LoggingProps
    {
        private static final String COMMAND_STORE = "command_store";
        private static final String ASYNC_OPERATION = "async_op";
    }

    static class Context
    {
        final HashMap<TxnId, AccordSafeCommand> commands = new HashMap<>();
        final TreeMap<Key, AccordSafeTimestampsForKey> timestampsForKey = new TreeMap<>();
        final TreeMap<Key, AccordSafeCommandsForKey> commandsForKey = new TreeMap<>();
        @Nullable
        AccordSafeCommandsForRanges commandsForRanges = null;

        void releaseResources(AccordCommandStore commandStore)
        {
            // TODO (expected): we should destructively iterate to avoid invoking second time in fail; or else read and set to null
            commands.values().forEach(commandStore.commandCache()::release);
            commands.clear();
            timestampsForKey.values().forEach(commandStore.timestampsForKeyCache()::release);
            timestampsForKey.clear();
            commandsForKey.values().forEach(commandStore.commandsForKeyCache()::release);
            commandsForKey.clear();
        }

        void revertChanges()
        {
            commands.values().forEach(AccordSafeState::revert);
            timestampsForKey.values().forEach(AccordSafeState::revert);
            commandsForKey.values().forEach(AccordSafeState::revert);
            if (commandsForRanges != null)
                commandsForRanges.revert();
        }
    }

    enum State
    {
        INITIALIZED, LOADING, PREPARING, RUNNING, COMPLETING, AWAITING_FLUSH, FINISHED, FAILED;

        boolean isComplete()
        {
            return this == FINISHED || this == FAILED;
        }
    }

    private State state = INITIALIZED;
    private final AccordCommandStore commandStore;
    private final PreLoadContext preLoadContext;
    private final Context context = new Context();
    private AccordSafeCommandStore safeStore;
    private final AsyncLoader loader;
    private R result;
    private final String loggingId;
    private BiConsumer<? super R, Throwable> callback;

    private List<Command> sanityCheck = null;

    private void setLoggingIds()
    {
        MDC.put(LoggingProps.COMMAND_STORE, commandStore.loggingId);
        MDC.put(LoggingProps.ASYNC_OPERATION, loggingId);
    }

    private void clearLoggingIds()
    {
        MDC.remove(LoggingProps.COMMAND_STORE);
        MDC.remove(LoggingProps.ASYNC_OPERATION);
    }

    public AsyncOperation(AccordCommandStore commandStore, PreLoadContext preLoadContext)
    {
        this.loggingId = "0x" + Integer.toHexString(System.identityHashCode(this));
        this.commandStore = commandStore;
        this.preLoadContext = preLoadContext;
        this.loader = createAsyncLoader(commandStore, preLoadContext);

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
        return "AsyncOperation{" + state + "}-" + loggingId;
    }

    AsyncLoader createAsyncLoader(AccordCommandStore commandStore, PreLoadContext preLoadContext)
    {
        return new AsyncLoader(commandStore, txnIds(preLoadContext), preLoadContext.keys(), preLoadContext.keyHistory());
    }

    private void onLoaded(Object o, Throwable throwable)
    {
        if (throwable != null)
        {
            logger.error(String.format("Operation %s failed", this), throwable);
            fail(throwable);
        }
        else
        {
            run();
        }
    }

    private void state(State state)
    {
        this.state = state;
    }

    private void finish(R result, Throwable failure)
    {
        try
        {
            if (callback != null)
                callback.accept(result, failure);
        }
        finally
        {
            state(failure == null ? FINISHED : FAILED);
        }
    }

    @SuppressWarnings("unchecked")
    Seekables<?, ?> keys()
    {
        return preLoadContext.keys();
    }

    private void fail(Throwable throwable)
    {
        commandStore.agent().onUncaughtException(throwable);
        commandStore.checkInStoreThread();
        Invariants.nonNull(throwable);

        if (state.isComplete())
            return;

        try
        {
            switch (state)
            {
                case COMPLETING:
                    break; // everything's cleaned up, invoke callback
                case RUNNING:
                    context.revertChanges();
                case PREPARING:
                    commandStore.abortCurrentOperation();
                case LOADING:
                    context.releaseResources(commandStore);
                case INITIALIZED:
                    break; // nothing to clean up, call callback
            }
            if (commandStore.hasSafeStore())
                commandStore.agent().onUncaughtException(new IllegalStateException(String.format("Failure to cleanup safe store for %s; status=%s", this, state), throwable));
        }
        catch (Throwable cleanup)
        {
            commandStore.agent().onUncaughtException(cleanup);
            throwable.addSuppressed(cleanup);
        }

        finish(null, throwable);
    }

    // return true iff ready to run
    protected boolean runInternal(boolean loadOnly)
    {
        switch (state)
        {
            default: throw new IllegalStateException("Unexpected state " + state);
            case INITIALIZED:
                state(LOADING);
            case LOADING:
                if (!loader.load(context, this::onLoaded))
                    return false;
                state(PREPARING);
                if (loadOnly)
                    return true;
            case PREPARING:
                safeStore = commandStore.beginOperation(preLoadContext, context.commands, context.timestampsForKey, context.commandsForKey, context.commandsForRanges);
                state(RUNNING);
            case RUNNING:

                result = apply(safeStore);
                // TODO (required): currently, we are not very efficient about ensuring that we persist the absolute minimum amount of state. Improve that.
                List<SavedCommand.SavedDiff> diffs = null;
                for (AccordSafeCommand commandState : context.commands.values())
                {
                    SavedCommand.SavedDiff diff = commandState.diff();
                    if (diff != null)
                    {
                        if (diffs == null)
                            diffs = new ArrayList<>(context.commands.size());
                        diffs.add(diff);
                        if (CassandraRelevantProperties.DTEST_ACCORD_JOURNAL_SANITY_CHECK_ENABLED.getBoolean())
                        {
                            if (sanityCheck == null)
                                sanityCheck = new ArrayList<>(context.commands.size());
                            sanityCheck.add(commandState.current());
                        }
                    }
                }

                commandStore.completeOperation(safeStore);
                context.releaseResources(commandStore);
                if (diffs != null)
                {
                    state(COMPLETING);
                    this.commandStore.appendCommands(diffs, sanityCheck, () -> finish(result, null));
                    return false;
                }

                state(COMPLETING);
            case COMPLETING:
                finish(result, null);
            case FINISHED:
            case FAILED:
                break;
        }

        return false;
    }

    @Override
    public void run()
    {
        setLoggingIds();
        logger.trace("Running {} with state {}", this, state);
        try
        {
            commandStore.checkInStoreThread();
            commandStore.setCurrentOperation(this);
            try
            {
                runInternal(false);
            }
            catch (Throwable t)
            {
                logger.error(String.format("Operation %s failed", this), t);
                fail(t);
            }
            finally
            {
                commandStore.unsetCurrentOperation(this);
            }
        }
        finally
        {
            logger.trace("Exiting {}", this);
            clearLoggingIds();
        }
    }

    private boolean preRun()
    {
        commandStore.checkInStoreThread();
        try
        {
            return runInternal(true);
        }
        catch (Throwable t)
        {
            logger.error("Operation {} failed", this, t);
            fail(t);
            return false;
        }
    }

    @Override
    public void start(BiConsumer<? super R, Throwable> callback)
    {
        Invariants.checkState(this.callback == null);
        this.callback = callback;
        if (!commandStore.inStore() || preRun())
            commandStore.executor().execute(this);
    }

    static class ForFunction<R> extends AsyncOperation<R>
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

    public static <T> AsyncOperation<T> create(CommandStore commandStore, PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function)
    {
        return new ForFunction<>((AccordCommandStore) commandStore, loadCtx, function);
    }

    // TODO (desired): these anonymous ops are somewhat tricky to debug. We may want to at least give them names.
    static class ForConsumer extends AsyncOperation<Void>
    {
        private final Consumer<? super SafeCommandStore> consumer;

        public ForConsumer(AccordCommandStore commandStore, PreLoadContext loadCtx, Consumer<? super SafeCommandStore> consumer)
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

    public static AsyncOperation<Void> create(CommandStore commandStore, PreLoadContext loadCtx, Consumer<? super SafeCommandStore> consumer)
    {
        return new ForConsumer((AccordCommandStore) commandStore, loadCtx, consumer);
    }
}
