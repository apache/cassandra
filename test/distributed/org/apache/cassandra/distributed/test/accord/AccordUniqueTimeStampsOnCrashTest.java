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
import org.apache.cassandra.distributed.test.TestBaseImpl;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.AccordTimeService;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class AccordUniqueTimeStampsOnCrashTest extends TestBaseImpl
{
    @Test
    public void uniqueTimeStampsOnCrashTest() throws Throwable
    {
        try (Cluster cluster = Cluster.build().withNodes(3)
                                      .withoutVNodes()
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                      .withInstanceInitializer(BBHelper::install)
                                      .withConfig(config -> config
                                                            .with(NETWORK, GOSSIP))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':3}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'");
            cluster.coordinator(1).execute(wrapInTxn("INSERT INTO ks.tbl (k, c, v) VALUES (?, ?, ?)"), ConsistencyLevel.SERIAL, 1, 1, 2);

            cluster.get(1).shutdown(false).get();
            State.beforeRestart.set(false);

            cluster.get(1).startup();

            cluster.get(1).runOnInstance( () -> {
                AccordService.instance().node().uniqueNow(0);
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
                               .method(named("now").and(takesArguments(0)))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);

                new ByteBuddy().rebase(Node.class)
                               .method(named("uniqueNow").and(takesArguments(1)))
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
        public static long uniqueNow(long greaterThan, @SuperCall Callable<Long> r) throws Exception
        {
            long newTimestamp = r.call();
            assert State.timestamp.get() < newTimestamp;
            State.timestamp.set(newTimestamp);

            return r.call();
        }
    }
}