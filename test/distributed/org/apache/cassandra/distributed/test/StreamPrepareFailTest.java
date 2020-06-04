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

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.transport.messages.ResultMessage;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamPrepareFailTest extends TestBaseImpl
{
    @Test
    public void streamPrepareFailTest() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(2)
                                          .withInstanceInitializer(StreamFailHelper::install)
                                          .withConfig(config -> config.with(NETWORK, GOSSIP))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            try
            {
                cluster.get(1).runOnInstance(() -> StorageService.instance.rebuild(null));
                fail("rebuild should throw exception");
            }
            catch (RuntimeException e)
            {
                cluster.get(2).runOnInstance(() -> assertTrue(StreamFailHelper.thrown.get()));
                assertTrue(e.getMessage().contains("Stream failed"));
            }
        }
    }

    public static class StreamFailHelper
    {
        static AtomicBoolean thrown = new AtomicBoolean();
        static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().redefine(StreamSession.class)
                           .method(named("prepareAsync"))
                           .intercept(MethodDelegation.to(StreamFailHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }
        public static ResultMessage prepareAsync()
        {
            thrown.set(true);
            throw new RuntimeException();
        }
    }

}
