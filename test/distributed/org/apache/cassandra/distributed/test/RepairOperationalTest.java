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

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class RepairOperationalTest extends TestBaseImpl
{
    @Test
    public void compactionBehindTest() throws IOException
    {
        try(Cluster cluster = init(Cluster.build(2)
                                          .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                          .withInstanceInitializer(ByteBuddyHelper::install)
                                          .start()))
        {
            cluster.schemaChange("create table "+KEYSPACE+".tbl (id int primary key, x int)");
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("insert into "+KEYSPACE+".tbl (id, x) VALUES (?,?)", ConsistencyLevel.ALL, i, i);
            cluster.forEach(i -> i.flush(KEYSPACE));
            cluster.forEach(i -> i.nodetoolResult("repair", "--full").asserts().success());
            cluster.get(2).runOnInstance(() -> {
                ByteBuddyHelper.pendingCompactions = 1000;
                DatabaseDescriptor.setRepairPendingCompactionRejectThreshold(500);
            });
            // make sure repair gets rejected on both nodes if pendingCompactions > threshold:
            cluster.forEach(i -> i.nodetoolResult("repair", "--full").asserts().failure());
            cluster.get(2).runOnInstance(() -> ByteBuddyHelper.pendingCompactions = 499);
            cluster.forEach(i -> i.nodetoolResult("repair", "--full").asserts().success());
        }
    }

    public static class ByteBuddyHelper
    {
        public static volatile int pendingCompactions = 0;
        static void install(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 2)
            {
                new ByteBuddy().redefine(CompactionManager.class)
                               .method(named("getPendingTasks"))
                               .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static int getPendingTasks()
        {
            return pendingCompactions;
        }
    }
}
