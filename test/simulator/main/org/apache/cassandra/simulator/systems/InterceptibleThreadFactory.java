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

import java.io.Serializable;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import org.apache.cassandra.concurrent.NamedThreadFactory;

public interface InterceptibleThreadFactory extends ThreadFactory
{
    public interface MetaFactory<F extends ThreadFactory> extends Serializable
    {
        F create(String id, int priority, ClassLoader contextClassLoader, Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
                 ThreadGroup threadGroup, Runnable onTermination, SimulatedTime.LocalTime time, InterceptingExecutorFactory parent, Object extraToStringInfo, Supplier<Long> idSupplier);
    }

    public static class ConcreteInterceptibleThreadFactory extends NamedThreadFactory implements InterceptibleThreadFactory
    {
        final InterceptingExecutorFactory parent;
        final Runnable onTermination;
        final SimulatedTime.LocalTime time;
        final Object extraToStringInfo;
        final Supplier<Long> idSupplier;

        public ConcreteInterceptibleThreadFactory(String id, int priority, ClassLoader contextClassLoader, Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
                                                  ThreadGroup threadGroup, Runnable onTermination, SimulatedTime.LocalTime time,
                                                  InterceptingExecutorFactory parent, Object extraToStringInfo, Supplier<Long> idSupplier)
        {
            super(id, priority, contextClassLoader, threadGroup, uncaughtExceptionHandler);
            this.onTermination = onTermination;
            this.time = time;
            this.parent = parent;
            this.extraToStringInfo = extraToStringInfo;
            this.idSupplier = idSupplier;
        }

        @Override
        public InterceptibleThread newThread(Runnable runnable)
        {
            return (InterceptibleThread) super.newThread(runnable);
        }

        @Override
        protected synchronized InterceptibleThread newThread(ThreadGroup threadGroup, Runnable runnable, String name)
        {
            // Can not use NamedThreadFactory.globalPrefix() as this method runs in the App class loader and not the Instance class loader; the ThreadGroup's name can act as a proxy for this.
            String threadName = threadGroup.getName() + '_' + name;
            InterceptibleThread thread = new InterceptibleThread(threadGroup, runnable, threadName, extraToStringInfo, onTermination, parent.interceptorOfGlobalMethods, time, idSupplier.get());
            if (parent.isClosed)
                thread.trapInterrupts(false);
            return setupThread(thread);
        }
    }

    public static class PlainThreadFactory extends NamedThreadFactory
    {
        final Runnable onTermination;

        public PlainThreadFactory(String id, int priority, ClassLoader contextClassLoader, Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
                                  ThreadGroup threadGroup, Runnable onTermination, SimulatedTime.LocalTime time, InterceptingExecutorFactory parent, Object extraToStringInfo, Supplier<Long> idSupplier)
        {
            super(id, priority, contextClassLoader, threadGroup, uncaughtExceptionHandler);
            this.onTermination = onTermination;
        }

        @Override
        protected Thread newThread(ThreadGroup threadGroup, Runnable runnable, String name)
        {
            return super.newThread(threadGroup, () -> { try { runnable.run(); } finally { onTermination.run();} }, threadGroup.getName() + '_' + name);
        }
    }

    InterceptibleThread newThread(Runnable runnable);
}
