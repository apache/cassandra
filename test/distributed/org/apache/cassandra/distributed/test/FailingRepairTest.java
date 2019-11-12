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
import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.impl.MessageFilters;
import org.apache.cassandra.distributed.util.RepairUtil;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.ActiveRepairService.ParentRepairStatus;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Tests check the different code paths expected to cause a repair to fail
 */
public class FailingRepairTest extends DistributedTestBase implements Serializable
{
    private static Cluster CLUSTER;

    @BeforeClass
    public static void before()
    {
        // ;_;
        DatabaseDescriptor.clientInitialization();
    }

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        // streaming requires networking ATM
        // streaming also requires gossip or isn't setup properly
        CLUSTER = init(Cluster.build(2)
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

    @Test(timeout = 1 * 60 * 1000, expected = AssertionError.class)
    public void missingKeyspace()
    {
        // if the keyspace doesn't exist, then repair fails early, before anything gets registered
        // for this reason a assert is thrown rather than a failure response
        RepairUtil.runRepairAndAwait(CLUSTER.get(2), "doesnotexist", RepairUtil.fullRange());
    }

    @Test(timeout = 1 * 60 * 1000)
    public void missingTable()
    {
        RepairUtil.Result result = RepairUtil.runRepairAndAwait(CLUSTER.get(2), KEYSPACE, RepairUtil.forTables("doesnotexist"));
        result.assertStatus(ParentRepairStatus.FAILED);
    }

    @Test(timeout = 1 * 60 * 1000)
    public void noTablesToRepair()
    {
        // index CF currently don't support repair, so they get dropped when listed
        // this is done in this test to cause the keyspace to have 0 tables to repair, which causes repair to no-op
        // early and skip.
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".withindex (key text, value text, PRIMARY KEY (key))");
        CLUSTER.schemaChange("CREATE INDEX value ON " + KEYSPACE + ".withindex (value)");
        // if CF has a . in it, it is assumed to be a 2i which rejects repairs
        RepairUtil.Result result = RepairUtil.runRepairAndAwait(CLUSTER.get(2), KEYSPACE, RepairUtil.forTables("withindex.value"));
        result.assertStatus(ParentRepairStatus.COMPLETED);
    }

    @Test(timeout = 1 * 60 * 1000)
    public void intersectingRange()
    {
        // this test exists to show that this case will cause repair to finish; success or failure isn't imporant
        // if repair is enhanced to allow intersecting ranges w/ local then this test will fail saying that we expected
        // repair to fail but it didn't, this would be fine and this test should be updated to reflect the new
        // semantic
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".intersectingrange (key text, value text, PRIMARY KEY (key))");

        String tokenRange = CLUSTER.get(2).callOnInstance(() -> {
            Set<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE).ranges();
            Range<Token> range = Iterables.getFirst(ranges, null);
            long right = (long) range.right.getTokenValue();
            return (right - 7) + ":" + (right + 7);
        });

        RepairUtil.Result result = RepairUtil.runRepairAndAwait(CLUSTER.get(2), KEYSPACE, RepairUtil.forTables("intersectingrange").withRanges(tokenRange));
        result.assertStatus(ParentRepairStatus.FAILED);
    }

    @Test(timeout = 1 * 60 * 1000)
    public void unknownHost()
    {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".unknownhost (key text, value text, PRIMARY KEY (key))");

        RepairUtil.Result result = RepairUtil.runRepairAndAwait(CLUSTER.get(2), KEYSPACE, RepairUtil.forTables("unknownhost").withHosts("thisreally.should.not.exist.apache.org"));
        result.assertStatus(ParentRepairStatus.FAILED);
    }

    @Test(timeout = 1 * 60 * 1000)
    public void desiredHostNotCoordinator()
    {
        // current limitation is that the coordinator must be apart of the repair, so as long as that exists this test
        // verifies that the validation logic will termniate the repair properly
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".desiredhostnotcoordinator (key text, value text, PRIMARY KEY (key))");

        RepairUtil.Result result = RepairUtil.runRepairAndAwait(CLUSTER.get(2), KEYSPACE, RepairUtil.forTables("desiredhostnotcoordinator").withHosts("localhost"));
        result.assertStatus(ParentRepairStatus.FAILED);
    }

    @Test(timeout = 1 * 60 * 1000)
    public void onlyCoordinator()
    {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".onlycoordinator (key text, value text, PRIMARY KEY (key))");

        RepairUtil.Result result = RepairUtil.runRepairAndAwait(CLUSTER.get(1), KEYSPACE, RepairUtil.forTables("onlycoordinator").withHosts("localhost"));
        result.assertStatus(ParentRepairStatus.FAILED);
    }

    @Test(timeout = 1 * 60 * 1000, expected = AssertionError.class)
    public void replicationFactorOne()
    {
        CLUSTER.schemaChange("CREATE KEYSPACE replicationfactor WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        CLUSTER.schemaChange("CREATE TABLE replicationfactor.one (key text, value text, PRIMARY KEY (key))");

        RepairUtil.runRepairAndAwait(CLUSTER.get(1), "replicationfactor", RepairUtil.forTables("one"));
    }

    @Test(timeout = 1 * 60 * 1000)
    public void prepareRPCTimeout()
    {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".preparerpctimeout (key text, value text, PRIMARY KEY (key))");
        MessageFilters.Filter verbFilter = CLUSTER.verbs(Verb.PREPARE_MSG).drop();
        try
        {
            RepairUtil.Result result = RepairUtil.runRepairAndAwait(CLUSTER.get(1), KEYSPACE, RepairUtil.forTables("preparerpctimeout"));
            result.assertStatus(ParentRepairStatus.FAILED);
        }
        finally
        {
            verbFilter.restore();
        }
    }

    @Test(timeout = 1 * 60 * 1000)
    public void neighbourDown() throws ExecutionException, InterruptedException
    {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".neighbourdown (key text, value text, PRIMARY KEY (key))");
        Future<Void> shutdownFuture = CLUSTER.get(2).shutdown();
        String downNodeAddress = CLUSTER.get(2).callOnInstance(() -> FBUtilities.getBroadcastAddressAndPort().toString());
        try
        {
            // wait for the node to stop
            shutdownFuture.get();
            // wait for the failure detector to detect this
            CLUSTER.get(1).runOnInstance(() -> {
                InetAddressAndPort neighbor;
                try
                {
                    neighbor = InetAddressAndPort.getByName(downNodeAddress);
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
                while (FailureDetector.instance.isAlive(neighbor))
                    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            });


            RepairUtil.Result result = RepairUtil.runRepairAndAwait(CLUSTER.get(1), KEYSPACE, RepairUtil.forTables("neighbourdown"));
            result.assertStatus(ParentRepairStatus.FAILED);
            Assert.assertTrue("failure should have been caused by node 2 not being alive; " + result.fullStatus, result.fullStatus.stream().anyMatch(s -> s.contains("Endpoint not alive")));
        }
        finally
        {
            CLUSTER.get(2).startup(CLUSTER);
        }
    }
}
