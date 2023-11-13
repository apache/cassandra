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

package org.apache.cassandra.concurrent;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.WithResources;

/*
 * This class only knows about Tracing and ClientWarn, so if any different executor locals are added, it must be
 * updated.
 *
 * We don't enumerate the ExecutorLocal.all array each time because it would be much slower.
 */
public class ExecutorLocals implements WithResources, Closeable
{
    private static final ExecutorLocals none = new ExecutorLocals(null, null);
    private static final FastThreadLocal<ExecutorLocals> locals = new FastThreadLocal<ExecutorLocals>()
    {
        @Override
        protected ExecutorLocals initialValue()
        {
            return none;
        }
    };

    public static class Impl
    {
        protected static void set(TraceState traceState, ClientWarn.State clientWarnState)
        {
            if (traceState == null && clientWarnState == null) locals.set(none);
            else locals.set(new ExecutorLocals(traceState, clientWarnState));
        }
    }

    public final TraceState traceState;
    public final ClientWarn.State clientWarnState;

    protected ExecutorLocals(TraceState traceState, ClientWarn.State clientWarnState)
    {
        this.traceState = traceState;
        this.clientWarnState = clientWarnState;
    }

    /**
     * @return an ExecutorLocals object which has the current trace state and client warn state.
     */
    public static ExecutorLocals current()
    {
        return locals.get();
    }

    /**
     * The {@link #current}Locals, if any; otherwise {@link WithResources#none()}.
     * Used to propagate current to other executors as a {@link WithResources}.
     */
    public static WithResources propagate()
    {
        ExecutorLocals locals = current();
        return locals == none ? WithResources.none() : locals;
    }

    public static ExecutorLocals create(TraceState traceState)
    {
        ExecutorLocals current = locals.get();
        return current.traceState == traceState ? current : new ExecutorLocals(traceState, current.clientWarnState);
    }

    public static void clear()
    {
        locals.set(none);
    }

    /**
     * Overwrite current locals, and return the previous ones
     */
    public Closeable get()
    {
        ExecutorLocals old = current();
        if (old != this)
            locals.set(this);
        return old;
    }

    public void close()
    {
        locals.set(this);
    }
}
