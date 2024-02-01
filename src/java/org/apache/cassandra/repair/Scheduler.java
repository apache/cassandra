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

package org.apache.cassandra.repair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;

import org.agrona.collections.LongArrayList;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

public interface Scheduler
{
    void schedule(TimeUUID sessionId, Executor taskExecutor, Task<?> tasks);

    static Scheduler build(int concurrentValidations, SharedContext ctx)
    {
        return concurrentValidations <= 0
               ? new NoopScheduler()
               : new LimitedConcurrentScheduler(concurrentValidations, ctx);
    }

    final class NoopScheduler implements Scheduler
    {
        @Override
        public void schedule(TimeUUID sessionId, Executor taskExecutor, Task<?> tasks)
        {
            taskExecutor.execute(tasks);
        }
    }

    final class LimitedConcurrentScheduler implements Scheduler
    {
        private final int concurrentValidations;
        private final SharedContext ctx;
        @GuardedBy("this")
        private int inflight = 0;
        @GuardedBy("this")
        private final Map<TimeUUID, Group> groups = new HashMap<>();

        LimitedConcurrentScheduler(int concurrentValidations, SharedContext ctx)
        {
            this.concurrentValidations = concurrentValidations;
            this.ctx = ctx;
        }

        @Override
        public synchronized void schedule(TimeUUID sessionId, Executor taskExecutor, Task<?> tasks)
        {
            groups.computeIfAbsent(sessionId, ignore -> new Group(sessionId, taskExecutor)).add(tasks);
            maybeSchedule();
        }

        private synchronized void onDone(Group group, long durationNs)
        {
            group.update(durationNs);
            inflight--;
            maybeSchedule();
        }

        private void maybeSchedule()
        {
            if (inflight == concurrentValidations)
                return;
            Group smallest = null;
            long smallestScore = -1;
            for (var g : groups.values())
            {
                if (g.isEmpty())
                    continue;
                if (smallest == null)
                {
                    smallest = g;
                    smallestScore = g.score();
                }
                else
                {
                    var score = g.score();
                    if (score < smallestScore)
                    {
                        smallest = g;
                        smallestScore = score;
                    }
                }
            }
            if (smallest == null)
                return;
            inflight++;
            smallest.executeNext();
        }

        private class Group
        {
            private final TimeUUID sessionId;
            private final Executor taskExecutor;
            private final List<Task<?>> tasks = new ArrayList<>();
            private final LongArrayList durations = new LongArrayList();
            private int inflight = 0;
            private int completed = 0;

            private Group(TimeUUID sessionId, Executor taskExecutor)
            {
                this.sessionId = sessionId;
                this.taskExecutor = taskExecutor;
            }

            public long score()
            {
                if (tasks.isEmpty())
                    return -1;
                long avgDuration = (long) durations.longStream().average().orElse(TimeUnit.HOURS.toNanos(1));
                return tasks.size() * avgDuration;
            }

            public void executeNext()
            {
                Task<?> task = tasks.get(0);
                tasks.remove(0);
                inflight++;
                var startNs = ctx.clock().nanoTime();
                task.addCallback((s, f) -> onDone(this, ctx.clock().nanoTime() - startNs));
                taskExecutor.execute(task);
            }

            public void add(Task<?> task)
            {
                tasks.add(task);
            }

            private void update(long durationNs)
            {
                durations.add(durationNs);
                inflight--;
                completed++;
            }

            public boolean isEmpty()
            {
                return tasks.isEmpty();
            }

            @Override
            public String toString()
            {
                return "Group{" +
                       "sessionId=" + sessionId +
                       ", tasks=" + tasks.size() +
                       ", durations=" + durations.longStream().average().orElse(-1) +
                       ", score=" + score() +
                       ", inflight=" + inflight +
                       ", completed=" + completed +
                       '}';
            }
        }
    }

    abstract class Task<T> extends AsyncFuture<T> implements Runnable
    {
    }
}