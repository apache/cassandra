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

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

public class RepairOperationalTest extends TestBaseImpl
{
    @Test
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
            assertEquals("datacenter2", node.config().localDatacenter());
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
            assertEquals("datacenter2", node.config().localDatacenter());
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
            assertEquals("datacenter2", node.config().localDatacenter());
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
            assertEquals("datacenter1", node.config().localDatacenter());
            node.nodetoolResult("repair", "-full",
                                "--ignore-unreplicated-keyspaces",
                                "-st", "0", "-et", "1000",
                                KEYSPACE, "tbl")
                .asserts().success();
        }
    }

}
