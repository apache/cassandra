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

import java.util.function.BiConsumer;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableCallable;
import org.apache.cassandra.simulator.systems.InterceptedExecution.InterceptedTaskExecution;

public abstract class SimulatedActionCallable<O> extends SimulatedAction implements BiConsumer<O, Throwable>
{
    private final IInvokableInstance on;
    private SerializableCallable<? extends O> execute;

    public SimulatedActionCallable(Object description, Modifiers self, Modifiers children, SimulatedSystems simulated, IInvokableInstance on, SerializableCallable<? extends O> execute)
    {
        super(description, self, children, null, simulated);
        this.execute = execute;
        this.on = on;
    }

    @Override
    protected InterceptedTaskExecution task()
    {
        return new InterceptedTaskExecution((InterceptingExecutor) on.executor())
        {
            public void run()
            {
                // we'll be invoked on the node's executor, but we need to ensure the task is loaded on its classloader
                try { accept(on.unsafeCallOnThisThread(execute), null); }
                catch (Throwable t) { accept(null, t); }
                finally { execute = null; }
            }
        };
    }
}
