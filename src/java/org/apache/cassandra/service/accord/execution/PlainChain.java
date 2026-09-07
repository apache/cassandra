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

import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import static org.apache.cassandra.service.accord.execution.Task.RunState.RUN_FAILED;

class PlainChain<V> extends Plain
{
    final Callable<? extends V> call;
    final @Nullable ExclusiveExecutor exclusiveExecutor;
    volatile BiConsumer<? super V, Throwable> callback;

    PlainChain(AccordExecutor executor, Callable<? extends V> call, BiConsumer<? super V, Throwable> callback, ExclusiveExecutor exclusiveExecutor, ExclusiveGroup group)
    {
        super(executor, group);
        this.call = call;
        this.callback = callback;
        this.exclusiveExecutor = exclusiveExecutor;
    }

    @Override
    public String description()
    {
        return call.toString();
    }

    @Override
    public String briefDescription()
    {
        return description();
    }

    @Override
    boolean runMayThrow()
    {
        V result;
        try
        {
            result = call.call();
        }
        catch (Throwable fail)
        {
            try { callback.accept(null, fail); }
            catch (Throwable t)
            {
                try { fail.addSuppressed(t); }
                catch (Throwable t2) { /* nothing more to be safely done */ }
                unhandledException(fail);
            }
            setRunState(RUN_FAILED);
            return false;
        }

        // a callback failure does not affect the execution being treated as a success
        try { callback.accept(result, null); }
        catch (Throwable t) { unhandledException(t); }
        return true;
    }

    @Override
    void reportFailureMayThrow(Throwable fail)
    {
        callback.accept(null, fail);
    }

    @Override
    ExclusiveExecutor exclusiveExecutor()
    {
        return exclusiveExecutor;
    }
}
