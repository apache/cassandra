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

import java.util.function.Function;

import accord.local.CommandStore;
import accord.local.TxnOperation;
import accord.txn.TxnId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

public class AsyncOperation<R> extends AsyncPromise<R> implements Runnable
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
    private final Function<? super CommandStore, R> function;
    private AsyncContext context = new AsyncContext();
    private R result;

    public AsyncOperation(AccordCommandStore commandStore, AsyncLoader loader, Function<? super CommandStore, R> function)
    {
        this.commandStore = commandStore;
        this.loader = loader;
        this.writer = new AsyncWriter(commandStore);
        this.function = function;
    }

    public AsyncOperation(AccordCommandStore commandStore, Iterable<TxnId> commandsToLoad, Iterable<PartitionKey> keyCommandsToLoad, Function<? super CommandStore, R> function)
    {
        this(commandStore, new AsyncLoader(commandStore, commandsToLoad, keyCommandsToLoad), function);
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
                    result = function.apply(commandStore);
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

    public static <R, S> AsyncOperation<R> operationFor(CommandStore commandStore, S scope, Function<? super CommandStore, R> function)
    {
        AccordCommandStore store = (AccordCommandStore) commandStore;
        if (scope instanceof TxnOperation)
        {
            TxnOperation op = (TxnOperation) scope;
            return new AsyncOperation<>(store, op.expectedTxnIds(), toPartitionKeys(op.expectedKeys()), function);
        }
        throw new IllegalArgumentException("Unhandled scope: " + scope);
    }
}
