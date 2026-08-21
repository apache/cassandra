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

package org.apache.cassandra.distributed.test.accord;

import accord.local.Node;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.AccordTimeService;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertTrue;

public class AccordMonotonicTimeStampsOnCrashTest extends TestBaseImpl
{
    @Test
    public void monotonicTimeStampsOnCrashTest() throws Throwable
    {
        try (Cluster cluster = Cluster.build().withNodes(3)
                                      .withInstanceInitializer(BBHelper::install)
                                      .withConfig(config -> config
                                                            .with(NETWORK, GOSSIP))
                                      .start())
        {
            cluster.get(1).shutdown(false).get();
            State.beforeRestart.set(false);

            cluster.get(1).startup();

            cluster.get(1).runOnInstance( () -> {
                AccordService.instance().node().uniqueNow();
            });
        }
    }

    @Shared
    public static class State
    {
        public static AtomicBoolean beforeRestart = new AtomicBoolean(true);
        public static AtomicLong timestamp = new AtomicLong(0);
    }

    public static class BBHelper
    {

        static void install(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 1)
            {
                new ByteBuddy().rebase(AccordTimeService.class)
                               .method(named("now"))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);

                new ByteBuddy().rebase(Node.class)
                               .method(named("uniqueNow"))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static long now(@SuperCall Callable<Long> r) throws Exception
        {
            if (State.beforeRestart.get())
                return r.call();

            // Simulate clock skew on restart to be 100 seconds backwards
            return r.call() - 100000000L;
        }

        @SuppressWarnings("unused")
        public static long uniqueNow(@SuperCall Callable<Long> r) throws Exception
        {
            long newTimestamp = r.call();
            assertTrue(State.timestamp.get() < newTimestamp);
            State.timestamp.set(newTimestamp);

            return newTimestamp;
        }
    }
}