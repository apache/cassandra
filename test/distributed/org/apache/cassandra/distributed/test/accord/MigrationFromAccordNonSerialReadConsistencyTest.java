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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config.PaxosVariant;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.TransactionalMigrationFromMode;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MigrationFromAccordNonSerialReadConsistencyTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationFromAccordNonSerialReadConsistencyTest.class);

    private static final int STALE_NODE = 3;

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        ServerTestUtils.daemonInitialization();
        AccordTestBase.setupCluster(builder -> builder.appendConfig(config -> config
                                    .with(Feature.NETWORK)
                                    // We turn dynamic_snitch off to not interfere with the routing of the read to STALE_NODE
                                    .set("dynamic_snitch", false)
                                    .set("paxos_variant", PaxosVariant.v2.name())), 3);
    }

    @Test
    public void migratingReadAtConsistencyLevelOneIsUpgradedToQuorum() throws Exception
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int PRIMARY KEY, v int) WITH read_repair = 'NONE' AND speculative_retry = 'NONE' AND " + transactionalMode.asCqlParam(), cluster -> {
            // Prevent STALE_NODE from applying write
            cluster.filters()
                   .inbound()
                   .to(STALE_NODE)
                   .verbs(Verb.ACCORD_APPLY_REQ.id,
                          Verb.ACCORD_APPLY_AND_WAIT_REQ.id,
                          Verb.ACCORD_INTEROP_APPLY_REQ.id,
                          Verb.ACCORD_FETCH_DATA_RSP.id,
                          Verb.ACCORD_CHECK_STATUS_RSP.id)
                   .drop();

            cluster.coordinator(1).execute(format("INSERT INTO %s (k, v) VALUES (1, 1)", qualifiedAccordTableName), ALL);

            // Ensure that write to key 1 is only applied on nodes 1 & nodes 2
            Util.spinUntilTrue(() -> cluster.get(1).executeInternal(selectCQL(1)).length == 1, 30);
            Util.spinUntilTrue(() -> cluster.get(2).executeInternal(selectCQL(1)).length == 1, 30);
            assertEquals(0, cluster.get(STALE_NODE).executeInternal(selectCQL(1)).length);

            alterTableTransactionalMode(TransactionalMode.off, TransactionalMigrationFromMode.full);
            nodetool(cluster.coordinator(1), "consensus_admin", "begin-migration", KEYSPACE, accordTableName);

            assertTrue(readPlanOnlyIncludesSelfForConsistencyLevelOne(cluster.get(STALE_NODE), accordTableName));
            assertEquals(0, cluster.get(STALE_NODE).executeInternal(selectCQL(1)).length);
            Object[][] row = cluster.coordinator(STALE_NODE).execute(selectCQL(1), ONE);
            AssertUtils.assertRows(row, AssertUtils.row(1, 1));
        });
    }

    @Test
    public void testMigratedRangesAreRoutedUsingConsistencyLevelOne() throws Throwable
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int PRIMARY KEY, v int) WITH read_repair = 'NONE' AND speculative_retry = 'NONE' AND " + transactionalMode.asCqlParam(), cluster -> {
            String table = accordTableName;

            // Prevent STALE_NODE from applying write
            cluster.filters()
                   .inbound()
                   .to(STALE_NODE)
                   .verbs(Verb.ACCORD_APPLY_REQ.id,
                          Verb.ACCORD_APPLY_AND_WAIT_REQ.id,
                          Verb.ACCORD_INTEROP_APPLY_REQ.id,
                          Verb.ACCORD_FETCH_DATA_RSP.id,
                          Verb.ACCORD_CHECK_STATUS_RSP.id)
                   .drop();

            cluster.coordinator(1).execute("INSERT INTO " + qualifiedAccordTableName + " (k, v) VALUES (1, 1)", ALL);

            // Ensure that write to key 1 is only applied on nodes 1 & nodes 2
            Util.spinUntilTrue(() -> cluster.get(1).executeInternal(selectCQL(1)).length == 1, 30);
            Util.spinUntilTrue(() -> cluster.get(2).executeInternal(selectCQL(1)).length == 1, 30);
            assertEquals(0, cluster.get(STALE_NODE).executeInternal(selectCQL(1)).length);

            alterTableTransactionalMode(TransactionalMode.off, TransactionalMigrationFromMode.full);

            long tokenOfMigratedKey = cluster.get(1).callOnInstance(() ->
                Schema.instance.getTableMetadata(KEYSPACE, table)
                               .partitioner.getToken(Int32Type.instance.decompose(1)).getLongValue());

            // We reset the filter so that Accord repair can finish
            cluster.filters().reset();

            // We perform repair for key 1
            cluster.get(1).nodetoolResult("consensus_admin", "finish-migration",
                                          "-st", Long.toString(tokenOfMigratedKey - 1),
                                          "-et", Long.toString(tokenOfMigratedKey),
                                          KEYSPACE, accordTableName)
                   .asserts().success();


            // Node 3, now has key 1
            assertEquals(1, cluster.get(STALE_NODE).executeInternal(selectCQL(1)).length);

            AtomicInteger readRequestsFromStaleNode = new AtomicInteger();
            cluster.filters()
                   .outbound()
                   .from(STALE_NODE)
                   .verbs(Verb.READ_REQ.id)
                   .messagesMatching((from, to, message) -> cluster.get(to).callsOnInstance(() -> {
                       ReadCommand readCommand = (ReadCommand) Instance.deserializeMessage(message).payload;
                       if (readCommand.metadata().keyspace.equals(KEYSPACE))
                           readRequestsFromStaleNode.incrementAndGet();
                       return false;
                   }).call())
                   .drop();

            readRequestsFromStaleNode.set(0);

            assertTrue(readPlanOnlyIncludesSelfForConsistencyLevelOne(cluster.get(STALE_NODE), accordTableName));
            AssertUtils.assertRows(cluster.coordinator(STALE_NODE).execute(selectCQL(1), ONE), AssertUtils.row(1, 1));

            // Since the key 1 is migrated, the read should be served at CL.ONE instead of CL.QUORUM
            assertEquals(0, readRequestsFromStaleNode.get());
        });
    }

    private static boolean readPlanOnlyIncludesSelfForConsistencyLevelOne(IInvokableInstance instance, String table)
    {
        return instance.callOnInstance(() -> {
            Keyspace keyspace = Keyspace.open(KEYSPACE);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(table);
            Token token = cfs.decorateKey(Int32Type.instance.decompose(1)).getToken();

            ReplicaPlan.ForTokenRead replicaPlan = ReplicaPlans.forRead(ClusterMetadata.current(),
                                                                        keyspace,
                                                                        cfs.getTableId(),
                                                                        token,
                                                                        null,
                                                                        org.apache.cassandra.db.ConsistencyLevel.ONE,
                                                                        cfs.metadata().params.speculativeRetry,
                                                                        ReadCoordinator.DEFAULT);
            EndpointsForToken contactedReplicas = replicaPlan.contacts();
            InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();

            return contactedReplicas.size() == 1 && contactedReplicas.get(0).endpoint().equals(self);
        });
    }

    private String selectCQL(int key)
    {
        return format("SELECT k, v FROM %s WHERE k = %d", qualifiedAccordTableName, key);
    }
}
