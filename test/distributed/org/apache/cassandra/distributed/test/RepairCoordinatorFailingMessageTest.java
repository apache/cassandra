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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.DistributedRepairUtils.RepairType;
import org.apache.cassandra.net.Verb;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.IMessageFilters.Matcher.of;

@RunWith(Parameterized.class)
@Ignore("Until CASSANDRA-15566 is in these tests all time out")
public class RepairCoordinatorFailingMessageTest extends TestBaseImpl implements Serializable
{
    private static Cluster CLUSTER;

    private final RepairType repairType;
    private final boolean withNotifications;

    public RepairCoordinatorFailingMessageTest(RepairType repairType, boolean withNotifications)
    {
        this.repairType = repairType;
        this.withNotifications = withNotifications;
    }

    @Parameterized.Parameters(name = "{0}/{1}")
    public static Collection<Object[]> messages()
    {
        List<Object[]> tests = new ArrayList<>();
        for (RepairType type : RepairType.values())
        {
            tests.add(new Object[] { type, true });
            tests.add(new Object[] { type, false });
        }
        return tests;
    }

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        // streaming requires networking ATM
        // streaming also requires gossip or isn't setup properly
        CLUSTER = init(Cluster.build(3) // set to 3 so streaming hits non-local case
                              .withConfig(c -> c.with(Feature.NETWORK)
                                                .with(Feature.GOSSIP))
                              .start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    private String tableName(String prefix) {
        return prefix + "_" + postfix() + "_" + withNotifications;
    }

    private String postfix()
    {
        return repairType.name().toLowerCase();
    }

    private NodeToolResult repair(int node, String... args) {
        return DistributedRepairUtils.repair(CLUSTER, node, repairType, withNotifications, args);
    }

    @Test(timeout = 1 * 60 * 1000)
    public void prepareIrFailure()
    {
        Assume.assumeTrue("The Verb.PREPARE_CONSISTENT_REQ is only for incremental, so disable in non-incremental", repairType == RepairType.INCREMENTAL);
        // Wait, isn't this copy paste of RepairCoordinatorTest::prepareFailure?  NO!
        // Incremental repair sends the PREPARE message the same way full does, but then after it does it sends
        // a consistent prepare message... and that one doesn't handle errors...
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".prepareirfailure (key text, value text, PRIMARY KEY (key))");
        IMessageFilters.Filter filter = CLUSTER.verbs(Verb.PREPARE_CONSISTENT_REQ).messagesMatching(of(m -> {
            throw new RuntimeException("prepare fail");
        })).drop();
        try
        {
            NodeToolResult result = repair(1, KEYSPACE, "prepareirfailure");
            result.asserts()
                  .failure()
                  .errorContains("error prepare fail")
                  .notificationContains(NodeToolResult.ProgressEventType.ERROR, "error prepare fail")
                  .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");
        }
        finally
        {
            filter.off();
        }
    }

    //TODO failure reply murkle tree
    //TODO failure reply murkle tree IR

    @Test(timeout = 1 * 60 * 1000)
    public void validationFailure()
    {
        String table = tableName("validationfailure");
        CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
        IMessageFilters.Filter filter = CLUSTER.verbs(Verb.VALIDATION_REQ).messagesMatching(of(m -> {
            throw new RuntimeException("validation fail");
        })).drop();
        try
        {
            NodeToolResult result = repair(1, KEYSPACE, table);
            result.asserts()
                  .failure()
                  .errorContains("Some repair failed")
                  .notificationContains(NodeToolResult.ProgressEventType.ERROR, "Some repair failed")
                  .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");
        }
        finally
        {
            filter.off();
        }
    }

    @Test(timeout = 1 * 60 * 1000)
    public void streamFailure()
    {
        String table = tableName("streamfailure");
        CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
        // there needs to be a difference to cause streaming to happen, so add to one node
        CLUSTER.get(2).executeInternal(format("INSERT INTO %s.%s (key) VALUES (?)", KEYSPACE, table), "some data");
        IMessageFilters.Filter filter = CLUSTER.verbs(Verb.SYNC_REQ).messagesMatching(of(m -> {
            throw new RuntimeException("stream fail");
        })).drop();
        try
        {
            NodeToolResult result = repair(1, KEYSPACE, table);
            result.asserts()
                  .failure()
                  .errorContains("Some repair failed")
                  .notificationContains(NodeToolResult.ProgressEventType.ERROR, "Some repair failed")
                  .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");
        }
        finally
        {
            filter.off();
        }
    }
}
