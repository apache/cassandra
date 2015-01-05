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
package org.apache.cassandra.scheduler;


import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.RequestSchedulerOptions;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * A very basic Round Robin implementation of the RequestScheduler. It handles
 * request groups identified on user/keyspace by placing them in separate
 * queues and servicing a request from each queue in a RoundRobin fashion.
 * It optionally adds weights for each round.
 */
public class RoundRobinScheduler implements IRequestScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(RoundRobinScheduler.class);

    //Map of queue id to weighted queue
    private final NonBlockingHashMap<String, WeightedQueue> queues;

    private final Semaphore taskCount;

    // Tracks the count of threads available in all queues
    // Used by the the scheduler thread so we don't need to busy-wait until there is a request to process
    private final Semaphore queueSize = new Semaphore(0, false);

    private final int defaultWeight;
    private final Map<String, Integer> weights;

    public RoundRobinScheduler(RequestSchedulerOptions options)
    {
        defaultWeight = options.default_weight;
        weights = options.weights;

        // the task count is acquired for the first time _after_ releasing a thread, so we pre-decrement
        taskCount = new Semaphore(options.throttle_limit - 1);

        queues = new NonBlockingHashMap<String, WeightedQueue>();
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
    }

    public void queue(Thread t, String id, long timeoutMS) throws TimeoutException
    {
        WeightedQueue weightedQueue = getWeightedQueue(id);

        try
        {
            queueSize.release();
            try
            {
                weightedQueue.put(t, timeoutMS);
                // the scheduler will release us when a slot is available
            }
            catch (TimeoutException e)
            {
                queueSize.acquireUninterruptibly();
                throw e;
            }
            catch (InterruptedException e)
            {
                queueSize.acquireUninterruptibly();
                throw e;
            }
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
        for (Map.Entry<String,WeightedQueue> request : queues.entrySet())
        {
            WeightedQueue queue = request.getValue();
            //Using the weight, process that many requests at a time (for that scheduler id)
            for (int i=0; i<queue.weight; i++)
            {
                Thread t = queue.poll();
                if (t == null)
                    break;
                else
                {
                    taskCount.acquireUninterruptibly();
                    queueSize.acquireUninterruptibly();
                }
            }
        }
        queueSize.release();
    }

    /*
     * Get the Queue for the respective id, if one is not available
     * create a new queue for that corresponding id and return it
     */
    private WeightedQueue getWeightedQueue(String id)
    {
        WeightedQueue weightedQueue = queues.get(id);
        if (weightedQueue != null)
            // queue existed
            return weightedQueue;

        WeightedQueue maybenew = new WeightedQueue(id, getWeight(id));
        weightedQueue = queues.putIfAbsent(id, maybenew);
        if (weightedQueue == null)
        {
            return maybenew;
        }

        // another thread created the queue
        return weightedQueue;
    }

    Semaphore getTaskCount()
    {
        return taskCount;
    }

    private int getWeight(String weightingVar)
    {
        return (weights != null && weights.containsKey(weightingVar))
                ? weights.get(weightingVar)
                : defaultWeight;
    }
}
