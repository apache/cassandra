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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.RequestSchedulerOptions;
import org.apache.cassandra.utils.Pair;
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

    //The Pair is the weighted queue - the left is the weight and the right is the queue
    private final NonBlockingHashMap<String, Pair<Integer, SynchronousQueue<Thread>>> queues;
    private static boolean started = false;

    private final Semaphore taskCount;

    // Used by the the scheduler thread so we don't need to busy-wait until there is a request to process
    private final Semaphore queueSize = new Semaphore(0, false);

    private Integer defaultWeight;
    private Map<String, Integer> weights;

    public RoundRobinScheduler(RequestSchedulerOptions options)
    {
        assert !started;

        defaultWeight = options.default_weight;
        weights = options.weights;

        taskCount = new Semaphore(options.throttle_limit);
        queues = new NonBlockingHashMap<String, Pair<Integer, SynchronousQueue<Thread>>>();
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
        Pair<Integer, SynchronousQueue<Thread>> weightedQueue = getWeightedQueue(id);

        try
        {
            queueSize.release();
            weightedQueue.right.put(t);
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
        int weight;
        SynchronousQueue<Thread> queue;
        Thread t;

        queueSize.acquireUninterruptibly();
        for (Map.Entry<String,Pair<Integer, SynchronousQueue<Thread>>> request : queues.entrySet())
        {
            weight = request.getValue().left;
            queue = request.getValue().right;
            //Using the weight, process that many requests at a time (for that scheduler id)
            for (int i=0; i<weight; i++)
            {
                t = queue.poll();
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
    private Pair<Integer, SynchronousQueue<Thread>> getWeightedQueue(String id)
    {
        Pair<Integer, SynchronousQueue<Thread>> weightedQueue = queues.get(id);
        if (weightedQueue != null)
            // queue existed
            return weightedQueue;

        Pair<Integer, SynchronousQueue<Thread>> maybenew = new Pair(getWeight(id), new SynchronousQueue<Thread>(true));
        weightedQueue = queues.putIfAbsent(id, maybenew);
        if (weightedQueue == null)
            // created new queue
            return maybenew;

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
