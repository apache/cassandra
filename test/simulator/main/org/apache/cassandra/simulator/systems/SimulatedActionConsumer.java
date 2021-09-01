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

import java.util.function.Consumer;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableConsumer;
import org.apache.cassandra.simulator.systems.InterceptedExecution.InterceptedTaskExecution;

public class SimulatedActionConsumer<I> extends SimulatedAction
{
    private final IInvokableInstance on;
    private Consumer<Throwable> onFailure;
    private SerializableConsumer<I> execute;
    private I parameter;

    public SimulatedActionConsumer(Object description, Modifiers self, Modifiers children, SimulatedSystems simulated, IInvokableInstance on, SerializableConsumer<I> execute, I parameter)
    {
        super(description, self, children, null, simulated);
        this.onFailure = simulated.failures;
        this.execute = execute;
        this.on = on;
        this.parameter = parameter;
    }

    @Override
    protected InterceptedTaskExecution task()
    {
        return new InterceptedTaskExecution((InterceptingExecutor) on.executor())
        {
            public void run()
            {
                // we'll be invoked on the node's executor, but we need to ensure the task is loaded on its classloader
                try { on.unsafeAcceptOnThisThread(execute, parameter); }
                catch (Throwable t) { onFailure.accept(t); }
                finally { execute = null; parameter = null; onFailure = null; }
            }
        };
    }
}
