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

package org.apache.cassandra.service.accord.execution;

import org.apache.cassandra.concurrent.CassandraThread;
import org.apache.cassandra.concurrent.DebuggableTask;

public interface TaskRunner
{
    AccordExecutor accordActiveExecutor();
    void setAccordActiveExecutor(AccordExecutor newExecutor);

    AccordExecutor accordLockedExecutor();
    boolean tryEnterAccordLockedExecutor(AccordExecutor newLockedExecutor);
    void exitAccordLockedExecutor();

    // to be called only by the thread itself, so can (eventually) avoid any memory barriers
    Task accordActiveSelfTask();
    Task accordActiveTask();
    void setAccordActiveTask(Task newActiveTask);

    static TaskRunner get()
    {
        return get(Thread.currentThread());
    }

    static TaskRunner get(Thread thread)
    {
        return thread instanceof CassandraThread ? (CassandraThread) thread : ThreadLocalTaskRunner.threadLocal.get();
    }

    final class ThreadLocalTaskRunner implements TaskRunner
    {
        private AccordExecutor lockedExecutor;
        private int lockedExecutorDepth;
        private AccordExecutor activeExecutor;
        volatile Task activeTask;

        private static final ThreadLocal<ThreadLocalTaskRunner> threadLocal = ThreadLocal.withInitial(ThreadLocalTaskRunner::new);

        @Override
        public AccordExecutor accordActiveExecutor()
        {
            return activeExecutor;
        }

        @Override
        public void setAccordActiveExecutor(AccordExecutor newExecutor)
        {
            activeExecutor = newExecutor;
        }

        @Override
        public AccordExecutor accordLockedExecutor()
        {
            return lockedExecutor;
        }

        @Override
        public boolean tryEnterAccordLockedExecutor(AccordExecutor newLockedExecutor)
        {
            if (lockedExecutor == null) lockedExecutor = newLockedExecutor;
            else if (lockedExecutor != newLockedExecutor) return false;
            ++lockedExecutorDepth;
            return true;
        }

        @Override
        public void exitAccordLockedExecutor()
        {
            if (--lockedExecutorDepth == 0)
                lockedExecutor = null;
        }

        @Override
        public void setAccordActiveTask(Task newActiveTask)
        {
            activeTask = newActiveTask;
        }

        @Override
        public Task accordActiveTask()
        {
            return activeTask;
        }

        @Override
        public Task accordActiveSelfTask()
        {
            return activeTask;
        }
    }

    abstract class DebuggableWrapper implements DebuggableTask.DebuggableTaskRunner
    {
        private volatile TaskRunner wrapped;

        protected void setWrapped(TaskRunner wrapped)
        {
            this.wrapped = wrapped;
        }

        @Override
        public DebuggableTask running()
        {
            Task running = wrapped.accordActiveTask();
            return running == null ? null : running.debuggable();
        }
    }
}
