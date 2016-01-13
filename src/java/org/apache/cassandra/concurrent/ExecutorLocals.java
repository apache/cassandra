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

import java.util.Arrays;

import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;

/*
 * This class only knows about Tracing and ClientWarn, so if any different executor locals are added, it must be
 * updated.
 *
 * We don't enumerate the ExecutorLocal.all array each time because it would be much slower.
 */
public class ExecutorLocals
{
    private static final ExecutorLocal<TraceState> tracing = Tracing.instance;
    private static final ExecutorLocal<ClientWarn.State> clientWarn = ClientWarn.instance;

    public final TraceState traceState;
    public final ClientWarn.State clientWarnState;

    private ExecutorLocals(TraceState traceState, ClientWarn.State clientWarnState)
    {
        this.traceState = traceState;
        this.clientWarnState = clientWarnState;
    }

    static
    {
        assert Arrays.equals(ExecutorLocal.all, new ExecutorLocal[]{ tracing, clientWarn })
        : "ExecutorLocals has not been updated to reflect new ExecutorLocal.all";
    }

    /**
     * This creates a new ExecutorLocals object based on what is already set.
     *
     * @return an ExecutorLocals object which has the trace state and client warn state captured if either has been set,
     *         or null if both are unset. The null result short-circuits logic in
     *         {@link AbstractLocalAwareExecutorService#newTaskFor(Runnable, Object, ExecutorLocals)}, preventing
     *         unnecessarily calling {@link ExecutorLocals#set(ExecutorLocals)}.
     */
    public static ExecutorLocals create()
    {
        TraceState traceState = tracing.get();
        ClientWarn.State clientWarnState = clientWarn.get();
        if (traceState == null && clientWarnState == null)
            return null;
        else
            return new ExecutorLocals(traceState, clientWarnState);
    }

    public static ExecutorLocals create(TraceState traceState)
    {
        ClientWarn.State clientWarnState = clientWarn.get();
        return new ExecutorLocals(traceState, clientWarnState);
    }

    public static void set(ExecutorLocals locals)
    {
        TraceState traceState = locals == null ? null : locals.traceState;
        ClientWarn.State clientWarnState = locals == null ? null : locals.clientWarnState;
        tracing.set(traceState);
        clientWarn.set(clientWarnState);
    }
}
