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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * This implementation of TaskQueue lets only one LWT task per partition key run concurrently.
 */
public class LwtKeyPrioritizingTaskQueue implements TaskQueue
{
    private final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();
    private final ReentrantLock lock = new ReentrantLock();

    // Set of partition keys currently running on an active worker thread (using ByteBufferUtil for robust value-based equality):
    private final Set<ByteBuffer> activeLwtKeys = ConcurrentHashMap.newKeySet();

    // Maps actively executing tasks to their acquired partition key:
    private final Map<Runnable, ByteBuffer> runningTasks = new ConcurrentHashMap<>();

    @Override
    public boolean add(Runnable task)
    {
        return queue.add(task);
    }

    @Override
    public Runnable peek()
    {
        return queue.peek();
    }

    @Override
    public boolean isEmpty()
    {
        return queue.isEmpty();
    }

    @Override
    public int size()
    {
        return queue.size();
    }

    @Override
    public Runnable poll()
    {
        lock.lock();
        try
        {
            Iterator<Runnable> iterator = queue.iterator();
            while (iterator.hasNext())
            {
                Runnable task = iterator.next();
                PrioritizableTask pt = PrioritizableTask.get(task);

                // If not an LWT or key is null, it's non-contended: execute immediately!
                if (pt == null || !pt.isLWT() || pt.partitionKey() == null)
                {
                    iterator.remove();
                    return task;
                }

                // Normalize buffer to ensure array content comparison is independent of position/limit markers
                ByteBuffer key = ByteBufferUtil.clone(pt.partitionKey());
                if (activeLwtKeys.add(key))
                {
                    iterator.remove();
                    runningTasks.put(task, key);
                    return task;
                }
            }

            return null;
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public void onTaskCompleted(Runnable task)
    {
        lock.lock();
        try
        {
            ByteBuffer key = runningTasks.remove(task);
            if (key != null)
            {
                activeLwtKeys.remove(key);
            }
        }
        finally
        {
            lock.unlock();
        }
    }
}
