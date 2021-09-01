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

import org.apache.cassandra.concurrent.NamedThreadFactory;

public class InterceptibleThreadFactory extends NamedThreadFactory
{
    final InterceptingExecutorFactory parent;
    final Object extraToStringInfo;
    public InterceptibleThreadFactory(String id, int priority, ClassLoader contextClassLoader, Thread.UncaughtExceptionHandler uncaughtExceptionHandler, ThreadGroup threadGroup, InterceptingExecutorFactory parent, Object extraToStringInfo)
    {
        super(id, priority, contextClassLoader, threadGroup, uncaughtExceptionHandler);
        this.parent = parent;
        this.extraToStringInfo = extraToStringInfo;
    }

    @Override
    public InterceptibleThread newThread(Runnable runnable)
    {
        return (InterceptibleThread) super.newThread(runnable);
    }

    @Override
    protected InterceptibleThread newThreadRaw(ThreadGroup threadGroup, Runnable runnable, String name)
    {
        InterceptibleThread thread = new InterceptibleThread(threadGroup, runnable, name, extraToStringInfo);
        if (parent.isClosed)
            thread.trapInterrupts(false);
        return thread;
    }
}
