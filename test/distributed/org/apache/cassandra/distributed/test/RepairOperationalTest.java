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
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.assertj.core.api.Assertions;

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

    public void repairUnreplicatedKStest() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(4)
                                          .withDCs(2)
                                          .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("alter keyspace "+KEYSPACE+" with replication = {'class': 'NetworkTopologyStrategy', 'datacenter1':2, 'datacenter2':0}");
            cluster.schemaChange("create table "+KEYSPACE+".tbl (id int primary key, i int)");
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("insert into "+KEYSPACE+".tbl (id, i) values (?, ?)", ConsistencyLevel.ALL, i, i);
            cluster.forEach(i -> i.flush(KEYSPACE));

            cluster.get(3).nodetoolResult("repair", "-full", KEYSPACE , "tbl", "-st", "0", "-et", "1000")
                   .asserts()
                   .failure()
                   .errorContains("Nothing to repair for (0,1000] in distributed_test_keyspace - aborting");
            cluster.get(3).nodetoolResult("repair", "-full", KEYSPACE , "tbl", "-st", "0", "-et", "1000", "--ignore-unreplicated-keyspaces")
                   .asserts()
                   .success()
                   .notificationContains("unreplicated keyspace is ignored since repair was called with --ignore-unreplicated-keyspaces");

        }
    }

    @Test
    public void dcFilterOnEmptyDC() throws IOException
    {
        try (Cluster cluster = Cluster.build().withRacks(2, 1, 2).start())
        {
            // 1-2 : datacenter1
            // 3-4 : datacenter2
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1':2, 'datacenter2':0}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int PRIMARY KEY, i int)");
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (id, i) VALUES (?, ?)", ConsistencyLevel.ALL, i, i);
            cluster.forEach(i -> i.flush(KEYSPACE));

            // choose a node in the DC that doesn't have any replicas
            IInvokableInstance node = cluster.get(3);
            Assertions.assertThat(node.config().localDatacenter()).isEqualTo("datacenter2");
            // fails with "the local data center must be part of the repair"
            node.nodetoolResult("repair", "-full",
                                "-dc", "datacenter1", "-dc", "datacenter2",
                                "--ignore-unreplicated-keyspaces",
                                "-st", "0", "-et", "1000",
                                KEYSPACE, "tbl")
                .asserts().success();
        }
    }

    @Test
    public void hostFilterDifferentDC() throws IOException
    {
        try (Cluster cluster = Cluster.build().withRacks(2, 1, 2).start())
        {
            // 1-2 : datacenter1
            // 3-4 : datacenter2
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1':2, 'datacenter2':0}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int PRIMARY KEY, i int)");
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (id, i) VALUES (?, ?)", ConsistencyLevel.ALL, i, i);
            cluster.forEach(i -> i.flush(KEYSPACE));

            // choose a node in the DC that doesn't have any replicas
            IInvokableInstance node = cluster.get(3);
            Assertions.assertThat(node.config().localDatacenter()).isEqualTo("datacenter2");
            // fails with "Specified hosts [127.0.0.3, 127.0.0.1] do not share range (0,1000] needed for repair. Either restrict repair ranges with -st/-et options, or specify one of the neighbors that share this range with this node: [].. Check the logs on the repair participants for further details"
            node.nodetoolResult("repair", "-full",
                                "-hosts", cluster.get(1).broadcastAddress().getAddress().getHostAddress(),
                                "-hosts", node.broadcastAddress().getAddress().getHostAddress(),
                                "--ignore-unreplicated-keyspaces",
                                "-st", "0", "-et", "1000",
                                KEYSPACE, "tbl")
                .asserts().success();
        }
    }

    @Test
    public void emptyDC() throws IOException
    {
        try (Cluster cluster = Cluster.build().withRacks(2, 1, 2).start())
        {
            // 1-2 : datacenter1
            // 3-4 : datacenter2
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1':2, 'datacenter2':0}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int PRIMARY KEY, i int)");
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (id, i) VALUES (?, ?)", ConsistencyLevel.ALL, i, i);
            cluster.forEach(i -> i.flush(KEYSPACE));

            // choose a node in the DC that doesn't have any replicas
            IInvokableInstance node = cluster.get(3);
            Assertions.assertThat(node.config().localDatacenter()).isEqualTo("datacenter2");
            // fails with [2020-09-10 11:30:04,139] Repair command #1 failed with error Nothing to repair for (0,1000] in distributed_test_keyspace - aborting. Check the logs on the repair participants for further details
            node.nodetoolResult("repair", "-full",
                                "--ignore-unreplicated-keyspaces",
                                "-st", "0", "-et", "1000",
                                KEYSPACE, "tbl")
                .asserts().success();
        }
    }

    @Test
    public void mainDC() throws IOException
    {
        try (Cluster cluster = Cluster.build().withRacks(2, 1, 2).start())
        {
            // 1-2 : datacenter1
            // 3-4 : datacenter2
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1':2, 'datacenter2':0}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int PRIMARY KEY, i int)");
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (id, i) VALUES (?, ?)", ConsistencyLevel.ALL, i, i);
            cluster.forEach(i -> i.flush(KEYSPACE));

            // choose a node in the DC that doesn't have any replicas
            IInvokableInstance node = cluster.get(1);
            Assertions.assertThat(node.config().localDatacenter()).isEqualTo("datacenter1");
            node.nodetoolResult("repair", "-full",
                                "--ignore-unreplicated-keyspaces",
                                "-st", "0", "-et", "1000",
                                KEYSPACE, "tbl")
                .asserts().success();
        }
    }

}
