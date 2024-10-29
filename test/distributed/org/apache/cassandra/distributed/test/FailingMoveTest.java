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

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class FailingMoveTest extends TestBaseImpl
{
    @Test
    public void testResumeMove() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withoutVNodes()
                                           .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                           .withInstanceInitializer(BB::install)
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl(id int primary key);"));
            for (int i=0; i<30; i++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (id) VALUES (?)"),
                                               ConsistencyLevel.ALL, i);
            String oldToken = getToken(cluster.get(3));
            String moveToToken = "2305843009213693949";
            assertNotEquals(oldToken, moveToToken);
            cluster.get(3).nodetoolResult("move", moveToToken).asserts().failure();
            cluster.get(3).runOnInstance(() -> {
                assertEquals(StorageService.Mode.MOVE_FAILED, StorageService.instance.operationMode());
                BB.shouldFail.set(false);
            });

            cluster.get(3).nodetoolResult("move", "--resume").asserts().success();
            cluster.get(3).runOnInstance(() -> assertEquals(StorageService.Mode.NORMAL, StorageService.instance.operationMode()));
            assertEquals(moveToToken, getToken(cluster.get(3)));
        }
    }

    @Test
    public void testAbortMove() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withoutVNodes()
                                           .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                           .withInstanceInitializer(BB::install)
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl(id int primary key);"));
            for (int i=0; i<30; i++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (id) VALUES (?)"),
                                               ConsistencyLevel.ALL, i);
            String oldToken = getToken(cluster.get(3));
            String moveToToken = "2305843009213693949";
            assertNotEquals(oldToken, moveToToken);
            cluster.get(3).nodetoolResult("move", moveToToken).asserts().failure();
            cluster.get(3).runOnInstance(() -> {
                assertEquals(StorageService.Mode.MOVE_FAILED, StorageService.instance.operationMode());
                BB.shouldFail.set(false);
            });

            cluster.get(3).nodetoolResult("move", "--abort").asserts().success();
            cluster.get(3).runOnInstance(() -> assertEquals(StorageService.Mode.NORMAL, StorageService.instance.operationMode()));
            assertNotEquals(moveToToken, getToken(cluster.get(3)));
        }
    }

    private String getToken(IInvokableInstance instance)
    {
        return instance.callsOnInstance(() -> {
            NodeId self = ClusterMetadata.current().myNodeId();
            return ClusterMetadata.current().tokenMap.tokens(self).iterator().next().toString();
        }).call();
    }

    public static class BB
    {
        static AtomicBoolean shouldFail = new AtomicBoolean(true);
        public static void install(ClassLoader classLoader, Integer num)
        {
            new ByteBuddy().rebase(StreamPlan.class)
                           .method(named("execute"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static StreamResultFuture execute(@SuperCall Callable<StreamResultFuture> zuper)
        {
            if (shouldFail.get())
                throw new RuntimeException("failing stream");

            try
            {
                return zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

}
