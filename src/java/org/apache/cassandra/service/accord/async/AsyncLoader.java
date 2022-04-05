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
import java.util.List;
import java.util.function.BiConsumer;

import accord.txn.TxnId;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.service.accord.AccordCommand;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

import static com.google.common.collect.Iterables.all;


public class AsyncLoader
{
    enum State
    {
        INITIALIZED,
        ACQUIRING,
        DISPATCHING,
        LOADING,
        FINISHED
    }

    private State state = State.INITIALIZED;
    protected final AccordCommandStore commandStore;
    protected final Iterable<TxnId> commandsToLoad;
    protected final Iterable<PartitionKey> keyCommandsToLoad;

    protected Future<?> readFuture;

    public AsyncLoader(AccordCommandStore commandStore, Iterable<TxnId> commandsToLoad, Iterable<PartitionKey> keyCommandsToLoad)
    {
        this.commandStore = commandStore;
        this.commandsToLoad = commandsToLoad;
        this.keyCommandsToLoad = keyCommandsToLoad;
    }

    protected boolean acquireCommand(TxnId txnId, AsyncContext context)
    {
        AccordCommand command = commandStore.commandCache().acquire(txnId);
        if (command == null)
            return false;
        context.commands.put(command.txnId(), command);
        return true;
    }

    protected boolean acquireCommandsForKey(PartitionKey key, AsyncContext context)
    {
        AccordCommandsForKey commandsForKey = commandStore.commandsForKeyCache().acquire(key);
        if (commandsForKey == null)
            return false;
        context.keyCommands.put(key, commandsForKey);
        return true;
    }

    private boolean attemptAcquire(AsyncContext context)
    {
        if (!all(commandsToLoad, txnId -> acquireCommand(txnId, context)))
        {
            context.releaseResources(commandStore);
            return false;
        }

        if (!all(keyCommandsToLoad, key -> acquireCommandsForKey(key, context)))
        {
            context.releaseResources(commandStore);
            return false;
        }
        return true;
    }

    private Future<?> maybeDispatchReads(AsyncContext context)
    {
        List<Future<?>> futures = null;
        for (AccordCommand command : context.commands.values())
        {
            if (command.isLoaded())
                continue;

            if (futures == null) futures = new ArrayList<>();
            Future<?> future = Stage.READ.submit(() -> AccordKeyspace.loadCommand(command));
            futures.add(future);
        }

        for (AccordCommandsForKey commandsForKey : context.keyCommands.values())
        {
            if (commandsForKey.isLoaded())
                continue;
            if (futures == null) futures = new ArrayList<>();
            Future<?> future = Stage.READ.submit(() -> AccordKeyspace.loadCommandsForKey(commandsForKey));
            futures.add(future);
        }

        return futures != null ? FutureCombiner.allOf(futures) : null;
    }


    public boolean load(AsyncContext context, BiConsumer<Object, Throwable> callback)
    {
        commandStore.checkThreadId();

        switch (state)
        {
            case INITIALIZED:
                state = State.ACQUIRING;
            case ACQUIRING:
                if (!attemptAcquire(context))
                {
                    commandStore.executor().execute(() -> callback.accept(null, null));
                    break;
                }
                state = State.DISPATCHING;
            case DISPATCHING:
                readFuture = maybeDispatchReads(context);
                state = State.LOADING;

                /*
                    TODO: do we need to acquire locks to the commands referenced by the commands per keys?
                      I believe we only ever read the state of them, so it may be safe. The exception being races where
                      we take an action based on stale state, although I'd expect notify listeners to prevent any issues
                      there. .... Really, do we even need to acquire locks? We need to avoid evicting cached commands we
                      expect to be in memory, but since processing is single threaded, we only need to worry about scenarios
                      where there is a context switch before something is completed. I think Execution/Apply will be the
                      only scenarios where that might happen (today) so we may only need to acquire locks then. We do
                      need to wait until all changes are synced to disk before completing the future, but that is
                      something different

                 */
            case LOADING:
                if (readFuture != null && !readFuture.isDone())
                {
                    // FIXME: this should call back into the AsyncOperation so a separate executor task isn't submitted
                    readFuture.addCallback(callback, commandStore.executor());
                    break;
                }
                // FIXME: the transition from loading to finished feels awkward
                state = State.FINISHED;
            case FINISHED:
                break;
            default:
                throw new IllegalStateException();
        }

        return state == State.FINISHED;
    }
}
