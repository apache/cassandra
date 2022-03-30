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

package org.apache.cassandra.distributed.test;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableConsumer;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableRunnable;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DrainingTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(DrainingTest.class);

    @Test
    public void drainingTest() throws Throwable
    {
        try (ICluster<IInvokableInstance> cluster = init(builder().withNodes(1)
                                                                  .withConfig(c -> c.with(Feature.GOSSIP,
                                                                                          Feature.NETWORK,
                                                                                          Feature.NATIVE_PROTOCOL))
                                                                  .start()))
        {
            SerializableRunnable counter = new RunsCounter();
            cluster.get(1)
                   .acceptsOnInstance((SerializableConsumer<SerializableRunnable>) (r) ->
                                                                                   ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(r, 0, 20, TimeUnit.SECONDS))
                   .accept(counter);

            // runnable will be executed at time 0 (0 delay), then after 20s (t = 20) and the last time at t = 40.
            Thread.sleep(45_000);

            assertEquals(3, RunsCounter.numberOfRuns);

            cluster.get(1).runOnInstance((SerializableRunnable) () -> {
                try
                {
                    StorageService.instance.drain();
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    fail("error while draining the node!");
                }
            });

            logger.info("Sleeping for 45 seconds to verify scheduled executor was shut down.");
            Thread.sleep(45_000);

            assertEquals(3, RunsCounter.numberOfRuns);

            cluster.get(1).runOnInstance((SerializableRunnable) () -> {
                assertTrue(ScheduledExecutors.scheduledTasks.isTerminated());
                assertTrue(ScheduledExecutors.nonPeriodicTasks.isTerminated());
                assertTrue(ScheduledExecutors.scheduledFastTasks.isTerminated());
                assertTrue(ScheduledExecutors.optionalTasks.isTerminated());
            });
        }
    }

    public static class RunsCounter implements SerializableRunnable
    {
        public static volatile int numberOfRuns = 0;

        @Override
        public void run()
        {
            numberOfRuns++;
        }
    }
}
