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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import accord.local.CommandStore;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandStore.SafeAccordCommandStore;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

public abstract class AsyncOperation<R> extends AsyncPromise<R> implements Runnable, Function<SafeCommandStore, R>, BiConsumer<Object, Throwable>
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncOperation.class);

    private static class LoggingProps
    {
        private static final String COMMAND_STORE = "command_store";
        private static final String ASYNC_OPERATION = "async_op";
    }

    enum State
    {
        INITIALIZED,
        LOADING,
        RUNNING,
        SAVING,
        AWAITING_SAVE,
        COMPLETING,
        FINISHED,
        FAILED
    }

    public interface Context
    {

    }

    private State state = State.INITIALIZED;
    private final AccordCommandStore commandStore;
    private final AsyncLoader loader;
    private final AsyncWriter writer;
    private final AsyncContext context = new AsyncContext();
    private R result;
    private final String loggingId;

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

    public AsyncOperation(AccordCommandStore commandStore, Iterable<TxnId> commandsToLoad, Iterable<PartitionKey> keyCommandsToLoad)
    {
        this.loggingId = "0x" + Integer.toHexString(System.identityHashCode(this));
        this.commandStore = commandStore;
        this.loader = createAsyncLoader(commandStore, commandsToLoad, keyCommandsToLoad);
        setLoggingIds();
        this.writer = createAsyncWriter(commandStore);
        logger.trace("Created {} on {}", this, commandStore);
        clearLoggingIds();
    }

    @Override
    public String toString()
    {
        return "AsyncOperation{" + state + "}-" + loggingId;
    }

    AsyncWriter createAsyncWriter(AccordCommandStore commandStore)
    {
        return new AsyncWriter(commandStore);
    }

    AsyncLoader createAsyncLoader(AccordCommandStore commandStore, Iterable<TxnId> txnIds, Iterable<PartitionKey> keys)
    {
        return new AsyncLoader(commandStore, txnIds, keys);
    }

    @VisibleForTesting
    State state()
    {
        return state;
    }

    @VisibleForTesting
    protected void setState(State state)
    {
        this.state = state;
    }

    /**
     * callback for loader and writer
     */
    @Override
    public void accept(Object o, Throwable throwable)
    {
        if (throwable != null)
        {
            logger.error(String.format("Operation %s failed", this), throwable);
            state = State.FAILED;
            tryFailure(throwable);
        }
        else
            run();
    }

    protected void runInternal()
    {
        SafeAccordCommandStore safeStore = commandStore.safeStore(context);
        switch (state)
        {
            case INITIALIZED:
                state = State.LOADING;
            case LOADING:
                if (!loader.load(context, this))
                    return;

                state = State.RUNNING;
                result = apply(safeStore);

                state = State.SAVING;
            case SAVING:
            case AWAITING_SAVE:
                boolean updatesPersisted = writer.save(context, this);

                if (state != State.AWAITING_SAVE)
                {
                    // with any updates on the way to disk, release resources so operations waiting
                    // to use these objects don't have issues with fields marked as unsaved
                    context.releaseResources(commandStore);
                    state = State.AWAITING_SAVE;
                }

                if (!updatesPersisted)
                    return;

                state = State.COMPLETING;
                setSuccess(result);
                state = State.FINISHED;
            case FINISHED:
                break;
            default:
                throw new IllegalStateException();
        }
    }


    @Override
    public void run()
    {
        setLoggingIds();
        logger.trace("Running {} with state {}", this, state);
        try
        {
            commandStore.checkInStoreThread();
            commandStore.setContext(context);
            try
            {
                runInternal();
            }
            catch (Throwable t)
            {
                logger.error(String.format("Operation %s failed", this), t);
                tryFailure(t);
            }
            finally
            {
                commandStore.unsetContext(context);
            }
        }
        finally
        {
            logger.trace("Exiting {}", this);
            clearLoggingIds();
        }
    }

    private static Iterable<PartitionKey> toPartitionKeys(Seekables<?, ?> keys)
    {
        switch (keys.domain())
        {
            default: throw new AssertionError();
            case Key:
                return (Iterable<PartitionKey>) keys;
            case Range:
                // TODO (required): implement
                throw new UnsupportedOperationException();
        }
    }

    static class ForFunction<R> extends AsyncOperation<R>
    {
        private final Function<? super SafeCommandStore, R> function;

        public ForFunction(AccordCommandStore commandStore, Iterable<TxnId> txnIds, Iterable<PartitionKey> keys, Function<? super SafeCommandStore, R> function)
        {
            super(commandStore, txnIds, keys);
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
        return new ForFunction<>((AccordCommandStore) commandStore, loadCtx.txnIds(), AsyncOperation.toPartitionKeys(loadCtx.keys()), function);
    }

    static class ForConsumer extends AsyncOperation<Void>
    {
        private final Consumer<? super SafeCommandStore> consumer;

        public ForConsumer(AccordCommandStore commandStore, Iterable<TxnId> txnIds, Iterable<PartitionKey> keys, Consumer<? super SafeCommandStore> consumer)
        {
            super(commandStore, txnIds, keys);
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
        return new ForConsumer((AccordCommandStore) commandStore, loadCtx.txnIds(), AsyncOperation.toPartitionKeys(loadCtx.keys()), consumer);
    }
}
