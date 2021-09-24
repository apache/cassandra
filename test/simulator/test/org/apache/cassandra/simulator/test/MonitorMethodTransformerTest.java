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

package org.apache.cassandra.simulator.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;

// A simple demonstration of manipulating the order of JVM level events using the simulation framework
public class MonitorMethodTransformerTest extends SimulationTestBase
{
    @Test
    public void testSynchronizedMethod()
    {
        // This verifies that the simulation does introduce some change to the order in which
        // system events are scheduled. Ordinarily, we would expect each runnable to execute
        // in a purely serial manner. However, injecting them into the simulated system adds
        // a degree of (pseudorandom) non-determinism around the ordering of:
        //   * the scheduling of the threads executing the tasks, when they park / unpark
        //   * the fairness of acquiring the monitor needed to enter the synchronized method
        // so we expect to see an interleaving of the thread ids executing the synchronized
        // method.
        // The simulated system doesn't alter the semantics of the JVM however, so
        // synchronization still ensures single threaded access to critical sections. The
        // synchronized method itself includes a check to verify this.
        ClassWithSynchronizedMethods.executions.clear();
        IIsolatedExecutor.SerializableRunnable[] runnables = new IIsolatedExecutor.SerializableRunnable[10];
        for (int j = 0; j < 10; j++)
        {
            int thread = j;
            runnables[j] = () -> {
                for (int iteration = 0; iteration < 10; iteration++)
                {
                    ClassWithSynchronizedMethods.synchronizedMethodWithParams(thread, iteration);
                }
            };
        }

        simulate(runnables,
                 () -> checkInterleavings());
    }

    @Test
    public void testSynchronizedMethodMultiThreaded()
    {
        // Similar to the method above, but this test adds submission of tasks to an Executor
        // within the simulated system, effectively adding another layer of scheduling to be
        // perturbed. The same invariants should preserved as in the previous test.
        simulate(() -> {
                     ExecutorPlus executor = ExecutorFactory.Global.executorFactory().pooled("name", 10);
                     int threads = 10;
                     for (int i = 0; i < threads; i++)
                     {
                         int thread = i;
                         executor.submit(() -> {
                             for (int iteration = 0; iteration < 10; iteration++)
                                 ClassWithSynchronizedMethods.synchronizedMethodWithParams(thread, iteration);
                         });
                     }
                 },
                 () -> checkInterleavings());
    }

    public static void checkInterleavings()
    {
        List<Integer> seenThreads = new ArrayList<>();
        // check if synchronized methods called by different threads did interleave
        for (ClassWithSynchronizedMethods.Execution execution : ClassWithSynchronizedMethods.executions)
        {
            int idx = seenThreads.indexOf(execution.thread);
            if (seenThreads.size() > 1)
            {
                // This thread was present in the list, and it is not the last one
                if (idx > 0 && idx != seenThreads.size() - 1)
                {
                    System.out.println(String.format("Detected interleaving between %d and %d",
                                       execution.thread, seenThreads.get(seenThreads.size() - 1)));
                    return;
                }
            }

            if (idx == -1)
                seenThreads.add(execution.thread);
        }
        Assert.fail("No interleavings detected");
    }
}

