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

import org.apache.cassandra.concurrent.NamedThreadFactory;

public interface InterceptibleThreadFactory extends ThreadFactory
{
    public interface MetaFactory<F extends ThreadFactory> extends Serializable
    {
        F create(String id, int priority, ClassLoader contextClassLoader, Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
                 ThreadGroup threadGroup, Runnable onTermination, SimulatedTime.LocalTime time, InterceptingExecutorFactory parent, Object extraToStringInfo);
    }

    public static class ConcreteInterceptibleThreadFactory extends NamedThreadFactory implements InterceptibleThreadFactory
    {
        final InterceptingExecutorFactory parent;
        final Runnable onTermination;
        final SimulatedTime.LocalTime time;
        final Object extraToStringInfo;

        public ConcreteInterceptibleThreadFactory(String id, int priority, ClassLoader contextClassLoader, Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
                                                  ThreadGroup threadGroup, Runnable onTermination, SimulatedTime.LocalTime time,
                                                  InterceptingExecutorFactory parent, Object extraToStringInfo)
        {
            super(id, priority, contextClassLoader, threadGroup, uncaughtExceptionHandler);
            this.onTermination = onTermination;
            this.time = time;
            this.parent = parent;
            this.extraToStringInfo = extraToStringInfo;
        }

        @Override
        public InterceptibleThread newThread(Runnable runnable)
        {
            return (InterceptibleThread) super.newThread(runnable);
        }

        @Override
        protected synchronized InterceptibleThread newThread(ThreadGroup threadGroup, Runnable runnable, String name)
        {
            InterceptibleThread thread = new InterceptibleThread(threadGroup, runnable, name, extraToStringInfo, onTermination, parent.interceptorOfGlobalMethods, time);
            if (parent.isClosed)
                thread.trapInterrupts(false);
            return setupThread(thread);
        }
    }

    public static class PlainThreadFactory extends NamedThreadFactory
    {
        final Runnable onTermination;

        public PlainThreadFactory(String id, int priority, ClassLoader contextClassLoader, Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
                                  ThreadGroup threadGroup, Runnable onTermination, SimulatedTime.LocalTime time, InterceptingExecutorFactory parent, Object extraToStringInfo)
        {
            super(id, priority, contextClassLoader, threadGroup, uncaughtExceptionHandler);
            this.onTermination = onTermination;
        }

        @Override
        protected Thread newThread(ThreadGroup threadGroup, Runnable runnable, String name)
        {
            return super.newThread(threadGroup, () -> { try { runnable.run(); } finally { onTermination.run();} }, name);
        }
    }

    InterceptibleThread newThread(Runnable runnable);
}
