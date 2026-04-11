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

public class ForwardingExecutorFactory implements ExecutorFactory
{
    private final ExecutorFactory delegate;

    public ForwardingExecutorFactory(ExecutorFactory delegate)
    {
        this.delegate = delegate;
    }

    protected ExecutorFactory delegate()
    {
        return delegate;
    }

    @Override
    public ExecutorPlus pooled(String name, int threads)
    {
        return delegate().pooled(name, threads);
    }

    @Override
    public ExecutorBuilderFactory<ExecutorPlus, SequentialExecutorPlus> withJmxInternal()
    {
        return delegate().withJmxInternal();
    }

    @Override
    public ScheduledExecutorPlus scheduled(String name)
    {
        return delegate().scheduled(name);
    }

    @Override
    public ScheduledExecutorPlus scheduled(String name, SimulatorSemantics simulatorSemantics)
    {
        return delegate().scheduled(name, simulatorSemantics);
    }

    @Override
    public ScheduledExecutorPlus scheduled(boolean executeOnShutdown, String name)
    {
        return delegate().scheduled(executeOnShutdown, name);
    }

    @Override
    public ScheduledExecutorPlus scheduled(boolean executeOnShutdown, String name, int priority)
    {
        return delegate().scheduled(executeOnShutdown, name, priority);
    }

    @Override
    public Thread startThread(String name, Runnable runnable)
    {
        return delegate().startThread(name, runnable);
    }

    @Override
    public Interruptible infiniteLoop(String name, Interruptible.SimpleTask task, InfiniteLoopExecutor.SimulatorSafe simulatorSafe)
    {
        return delegate().infiniteLoop(name, task, simulatorSafe);
    }

    @Override
    public ExecutorBuilder<? extends SequentialExecutorPlus> configureSequential(String name)
    {
        return delegate().configureSequential(name);
    }

    @Override
    public SequentialExecutorPlus sequential(String name)
    {
        return delegate().sequential(name);
    }

    @Override
    public ExecutorBuilder<? extends ExecutorPlus> configurePooled(String name, int threads)
    {
        return delegate().configurePooled(name, threads);
    }

    @Override
    public ExecutorBuilderFactory<ExecutorPlus, SequentialExecutorPlus> withJmx(String jmxPath)
    {
        return delegate().withJmx(jmxPath);
    }

    @Override
    public LocalAwareSubFactory localAware()
    {
        return delegate().localAware();
    }

    @Override
    public ScheduledExecutorPlus scheduled(boolean executeOnShutdown, String name, int priority, SimulatorSemantics simulatorSemantics)
    {
        return delegate().scheduled(executeOnShutdown, name, priority, simulatorSemantics);
    }

    @Override
    public Thread startThread(String name, Runnable runnable, SystemThreadTag systemTag, SimulatorThreadTag simulatorTag)
    {
        return delegate().startThread(name, runnable, systemTag, simulatorTag);
    }

    @Override
    public Interruptible infiniteLoop(String name, Interruptible.Task task, InfiniteLoopExecutor.SimulatorSafe simulatorSafe, SystemThreadTag systemTag, InfiniteLoopExecutor.Interrupts interrupts)
    {
        return delegate().infiniteLoop(name, task, simulatorSafe, systemTag, interrupts);
    }

    @Override
    public ThreadGroup newThreadGroup(String name)
    {
        return delegate().newThreadGroup(name);
    }
}
