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
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

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
    private final Function<CommandStore, R> function;
    private R result;

    public AsyncOperation(AccordCommandStore commandStore, AsyncLoader loader, Function<CommandStore, R> function)
    {
        this.commandStore = commandStore;
        this.loader = loader;
        this.function = function;
    }

    private Future<Void> saveStuff()
    {
        throw new UnsupportedOperationException("TODO");
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
        try
        {
            switch (state)
            {
                case INITIALIZED:
                    if (!loader.load(this::callback))
                        return;
                case LOADING:
                    state = State.RUNNING;
                    result = function.apply(commandStore);

                    state = State.SAVING;
                    Future<?> saveFuture = saveStuff();
                    saveFuture.addCallback(this::callback, commandStore.executor());
                    break;
                case SAVING:
                    state = State.COMPLETING;
                    loader.releaseResources();
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
    }
}
