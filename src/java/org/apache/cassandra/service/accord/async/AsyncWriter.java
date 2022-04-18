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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.google.common.base.Preconditions;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

public class AsyncWriter
{
    enum State
    {
        INITIALIZED,
        DISPATCHING,
        SAVING,
        FINISHED
    }

    private State state = State.INITIALIZED;
    protected Future<?> writeFuture;
    private final AccordCommandStore commandStore;

    public AsyncWriter(AccordCommandStore commandStore)
    {
        this.commandStore = commandStore;
    }

    private static <K, V extends AccordStateCache.AccordState<K, V>> List<Future<?>> dispatchWrites(Iterable<V> items,
                                                                                                    AccordStateCache.Instance<K, V> cache,
                                                                                                    Function<V, Mutation> mutationFunction,
                                                                                                    List<Future<?>> futures)
    {
        for (V item : items)
        {
            if (!item.hasModifications())
                continue;

            if (futures == null) futures = new ArrayList<>();
            Mutation mutation = mutationFunction.apply(item);
            Future<?> future = Stage.MUTATION.submit((Runnable) mutation::apply);
            cache.setSaveFuture(item.key(), future);
            futures.add(future);
        }
        return futures;
    }

    private Future<?> maybeDispatchWrites(AsyncContext context) throws IOException
    {
        List<Future<?>> futures = null;

        futures = dispatchWrites(context.commands.values(),
                                 commandStore.commandCache(),
                                 AccordKeyspace::getCommandMutation,
                                 futures);

        futures = dispatchWrites(context.keyCommands.values(),
                                 commandStore.commandsForKeyCache(),
                                 AccordKeyspace::getCommandsForKeyMutation,
                                 futures);

        return futures != null ? FutureCombiner.allOf(futures) : null;
    }

    public boolean save(AsyncContext context, BiConsumer<Object, Throwable> callback)
    {
        commandStore.checkInStoreThread();

        try
        {
            switch (state)
            {
                case INITIALIZED:
                    state = State.DISPATCHING;
                case DISPATCHING:
                    context.summaries.values().forEach(summary -> Preconditions.checkState(!summary.hasModifications(),
                                                                                           "Summaries cannot be modified"));
                    if (writeFuture == null)
                        writeFuture = maybeDispatchWrites(context);

                    state = State.SAVING;
                case SAVING:
                    if (writeFuture != null && !writeFuture.isDone())
                    {
                        writeFuture.addCallback(callback, commandStore.executor());
                        break;
                    }
                    state = State.FINISHED;
                case FINISHED:
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        return state == State.FINISHED;
    }
}
