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

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableRunnable;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.OrderOn;
import org.apache.cassandra.simulator.systems.InterceptedExecution.InterceptedTaskExecution;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.simulator.systems.SimulatedAction.Kind.TASK;

public class SimulatedActionTask extends SimulatedAction implements Runnable
{
    InterceptedExecution task;

    public SimulatedActionTask(Object description, Modifiers self, Modifiers transitive, Verb forVerb, SimulatedSystems simulated, InterceptedExecution task)
    {
        this(description, TASK, OrderOn.NONE, self, transitive, Collections.emptyMap(), forVerb, simulated, task);
    }

    public SimulatedActionTask(Object description, Kind kind, OrderOn orderOn, Modifiers self, Modifiers transitive, Map<Verb, Modifiers> verbModifiers, Verb forVerb, SimulatedSystems simulated, InterceptedExecution task)
    {
        super(description, kind, orderOn, self, transitive, verbModifiers, forVerb, simulated);
        this.task = task;
        task.onCancel(this);
    }

    public SimulatedActionTask(Object description, Modifiers self, Modifiers children, SimulatedSystems simulated, IInvokableInstance on, SerializableRunnable run)
    {
        super(description, self, children, null, simulated);
        this.task = unsafeAsTask(on, asSafeRunnable(on, run), simulated.failures);
        task.onCancel(this);
    }

    private SimulatedActionTask(Object description, Modifiers self, Modifiers children, SimulatedSystems simulated, IInvokableInstance on, InterceptedExecution task)
    {
        super(description, self, children, null, simulated);
        this.task = task;
        task.onCancel(this);
    }

    /**
     * To be used to create actions on runnable that are not serializable but are anyway safe to invoke
     */
    public static SimulatedActionTask unsafeTask(Object description, Modifiers self, Modifiers transitive, SimulatedSystems simulated, IInvokableInstance on, Runnable run)
    {
        return new SimulatedActionTask(description, self, transitive, simulated, on, unsafeAsTask(on, run, simulated.failures));
    }

    protected static Runnable asSafeRunnable(IInvokableInstance on, SerializableRunnable run)
    {
        return () -> on.unsafeRunOnThisThread(run);
    }

    protected static InterceptedTaskExecution unsafeAsTask(IInvokableInstance on, Runnable runnable, Consumer<? super Throwable> onFailure)
    {
        return new InterceptedTaskExecution((InterceptingExecutor) on.executor())
        {
            public void run()
            {
                // we'll be invoked on the node's executor, but we need to ensure the task is loaded on its classloader
                try { runnable.run(); }
                catch (Throwable t) { onFailure.accept(t); }
            }
        };
    }

    @Override
    protected InterceptedExecution task()
    {
        return task;
    }

    @Override
    protected ActionList performAndRegister()
    {
        try
        {
            return super.performAndRegister();
        }
        finally
        {
            task = null;
        }
    }

    @Override
    protected Throwable safeInvalidate(boolean isCancellation)
    {
        try
        {
            if (task != null)
            {
                task.onCancel(null);
                task.cancel();
                task = null;
            }

            return super.safeInvalidate(isCancellation);
        }
        catch (Throwable t)
        {
            return Throwables.merge(t, super.safeInvalidate(isCancellation));
        }
    }

    @Override
    public void run()
    {
        // cancellation invoked on the task by the application
        task = null;
        super.cancel();
    }
}
