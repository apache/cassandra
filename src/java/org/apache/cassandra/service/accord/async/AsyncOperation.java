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

import java.util.function.Consumer;
import java.util.function.Function;

import accord.local.CommandStore;
import accord.local.TxnOperation;
import accord.txn.TxnId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

public abstract class AsyncOperation<R> extends AsyncPromise<R> implements Runnable
{
    enum State
    {
        INITIALIZED,
        LOADING,
        RUNNING,
        SAVING,
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
    private AsyncContext context = new AsyncContext();
    private R result;

    public AsyncOperation(AccordCommandStore commandStore, AsyncLoader loader)
    {
        this.commandStore = commandStore;
        this.loader = loader;
        this.writer = new AsyncWriter(commandStore);
    }

    public AsyncOperation(AccordCommandStore commandStore, Iterable<TxnId> commandsToLoad, Iterable<PartitionKey> keyCommandsToLoad)
    {
        this(commandStore, new AsyncLoader(commandStore, commandsToLoad, keyCommandsToLoad));
    }

    private void callback(Object unused, Throwable throwable)
    {
        if (throwable != null)
        {
            state = State.FAILED;
            tryFailure(throwable);
        }
        else
            run();
    }

    abstract R calculateResult(CommandStore commandStore);

    @Override
    public void run()
    {
        commandStore.checkThreadId();
        commandStore.setContext(context);
        try
        {
            switch (state)
            {
                case INITIALIZED:
                    state = State.LOADING;
                case LOADING:
                    if (!loader.load(context, this::callback))
                        return;

                    state = State.RUNNING;
                    result = calculateResult(commandStore);
                    // FIXME: if there is now a read or write to do, prevent other processes from attempting to perform them also

                    state = State.SAVING;
                case SAVING:
                    if (!writer.save(context, this::callback))
                        return;
                    state = State.COMPLETING;
                    context.releaseResources(commandStore);
                    setSuccess(result);
                    state = State.FINISHED;
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
        catch (Throwable t)
        {
            tryFailure(t);
        }
        finally
        {
            commandStore.unsetContext(context);
        }
    }

    private static Iterable<PartitionKey> toPartitionKeys(Iterable<?> iterable)
    {
        return (Iterable<PartitionKey>) iterable;
    }

    static class ForFunction<R> extends AsyncOperation<R>
    {
        private final Function<? super CommandStore, R> function;

        public ForFunction(AccordCommandStore commandStore, Iterable<TxnId> commandsToLoad, Iterable<PartitionKey> keyCommandsToLoad, Function<? super CommandStore, R> function)
        {
            super(commandStore, commandsToLoad, keyCommandsToLoad);
            this.function = function;
        }

        @Override
        R calculateResult(CommandStore commandStore)
        {
            return function.apply(commandStore);
        }
    }

    public static <T> AsyncOperation<T> create(CommandStore commandStore, TxnOperation scope, Function<? super CommandStore, T> function)
    {
        return new ForFunction<>((AccordCommandStore) commandStore, scope.expectedTxnIds(), AsyncOperation.toPartitionKeys(scope.expectedKeys()), function);
    }

    static class ForConsumer  extends AsyncOperation<Void>
    {
        private final Consumer<? super CommandStore> consumer;

        public ForConsumer(AccordCommandStore commandStore, Iterable<TxnId> commandsToLoad, Iterable<PartitionKey> keyCommandsToLoad, Consumer<? super CommandStore> consumer)
        {
            super(commandStore, commandsToLoad, keyCommandsToLoad);
            this.consumer = consumer;
        }

        @Override
        Void calculateResult(CommandStore commandStore)
        {
            consumer.accept(commandStore);
            return null;
        }
    }

    public static AsyncOperation<Void> create(CommandStore commandStore, TxnOperation scope, Consumer<? super CommandStore> consumer)
    {
        return new ForConsumer((AccordCommandStore) commandStore, scope.expectedTxnIds(), AsyncOperation.toPartitionKeys(scope.expectedKeys()), consumer);
    }
}
