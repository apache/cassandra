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

package org.apache.cassandra.simulator.systems;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;

import org.apache.cassandra.simulator.systems.SimulatedAction.Kind;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.concurrent.RunnableFuture;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

// some kind of bug in javac(?) sometimes causes this to not be found if not fully-qualified
@Shared(scope = SIMULATION)
public interface InterceptorOfExecution
{
    InterceptExecution intercept();

    @Shared(scope = SIMULATION)
    interface InterceptExecution
    {
        <V, T extends RunnableFuture<V>> T addTask(T task, InterceptingExecutor executor);
        <T> ScheduledFuture<T> schedule(Kind kind, long delayNanos, long deadlineNanos, Callable<T> runnable, InterceptingExecutor executor);
        Thread start(Kind kind, Function<Runnable, InterceptibleThread> factory, Runnable run);
    }
}
