package org.apache.cassandra.scheduler;

/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;

import org.apache.cassandra.config.RequestSchedulerOptions;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A very basic Round Robin implementation of the RequestScheduler. It handles 
 * request groups identified on user/keyspace by placing them in separate 
 * queues and servicing a request from each queue in a RoundRobin fashion. 
 */
public class RoundRobinScheduler implements IRequestScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(RoundRobinScheduler.class);
    private final NonBlockingHashMap<String, SynchronousQueue<Thread>> queues;
    private static boolean started = false;

    private final Semaphore taskCount;

    // Used by the the scheduler thread so we don't need to busy-wait until there is a request to process
    private final Semaphore queueSize = new Semaphore(0, false);

    public RoundRobinScheduler(RequestSchedulerOptions options)
    {
        assert !started;

        taskCount = new Semaphore(options.throttle_limit);
        queues = new NonBlockingHashMap<String, SynchronousQueue<Thread>>();
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                while (true)
                {
                    schedule();
                }
            }
        };
        Thread scheduler = new Thread(runnable, "REQUEST-SCHEDULER");
        scheduler.start();
        logger.info("Started the RoundRobin Request Scheduler");
        started = true;
    }

    public void queue(Thread t, String id)
    {
        SynchronousQueue<Thread> queue = getQueue(id);

        try
        {
            queueSize.release();
            queue.put(t);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("Interrupted while queueing requests", e);
        }
    }

    public void release()
    {
        taskCount.release();
    }

    private void schedule()
    {
        queueSize.acquireUninterruptibly();
        for (SynchronousQueue<Thread> queue : queues.values())
        {
            Thread t = queue.poll();
            if (t != null)
            {
                taskCount.acquireUninterruptibly();
                queueSize.acquireUninterruptibly();
            }
        }
        queueSize.release();
    }

    /*
     * Get the Queue for the respective id, if one is not available 
     * create a new queue for that corresponding id and return it
     */
    private SynchronousQueue<Thread> getQueue(String id)
    {
        SynchronousQueue<Thread> queue = queues.get(id);
        if (queue != null)
            // queue existed
            return queue;

        SynchronousQueue<Thread> maybenew = new SynchronousQueue<Thread>(true);
        queue = queues.putIfAbsent(id, maybenew);
        if (queue == null)
            // created new queue
            return maybenew;

        // another thread created the queue
        return queue;
    }

    Semaphore getTaskCount()
    {
        return taskCount;
    }
}
