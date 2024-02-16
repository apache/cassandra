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

package org.apache.cassandra.service;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.metrics.ClientRequestsMetrics;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.Util.dk;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(BMUnitRunner.class)
public class MutatorTest
{
    private static final String KEYSPACE = "ks_test";
    private static final String KEYSPACE_RF2 = "ks_rf2";
    private static final String KEYSPACE_TRANSIENT = "ks_transient";
    private static final String TABLE0 = "table_0";
    private static final String TABLE1 = "table_1";
    private static final String TABLE2 = "table_2";

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE0),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE2));
        SchemaLoader.createKeyspace(KEYSPACE_RF2,
                                    KeyspaceParams.simple(2),
                                    SchemaLoader.standardCFMD(KEYSPACE_RF2, TABLE0),
                                    SchemaLoader.standardCFMD(KEYSPACE_RF2, TABLE1),
                                    SchemaLoader.standardCFMD(KEYSPACE_RF2, TABLE2));
        SchemaLoader.createKeyspace(KEYSPACE_TRANSIENT,
                                    KeyspaceParams.nts("datacenter1", "3/1"),
                                    SchemaLoader.standardCFMD(KEYSPACE_TRANSIENT, TABLE0),
                                    SchemaLoader.standardCFMD(KEYSPACE_TRANSIENT, TABLE1),
                                    SchemaLoader.standardCFMD(KEYSPACE_TRANSIENT, TABLE2));

        Token token = ByteOrderedPartitioner.instance.getToken(ByteBufferUtil.bytes(1));
        StorageService.instance.getTokenMetadata().updateNormalToken(token, FBUtilities.getBroadcastAddressAndPort());
    }

    @Before
    public void clearBatchlog()
    {
        Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).truncateBlocking();
    }

    @Test
    public void testMutatateAtomically()
    {
        Mutator mutator = new StorageProxy.DefaultMutator();

        long now = System.nanoTime();
        Collection<Mutation> mutations = Arrays.asList(createMutation(KEYSPACE, "1", now), createMutation(KEYSPACE, "2", now));
        ConsistencyLevel consistency = ConsistencyLevel.EACH_QUORUM;
        ClientRequestsMetrics metrics = new ClientRequestsMetrics("test");
        ClientState clientState = ClientState.forInternalCalls();

        long count = metrics.writeMetrics.executionTimeMetrics.latency.getCount();
        long countServiceTime = metrics.writeMetrics.serviceTimeMetrics.latency.getCount();

        mutator.mutateAtomically(mutations, consistency, true, now, metrics, clientState);
        assertThat(metrics.writeMetrics.executionTimeMetrics.latency.getCount()).isEqualTo(count + 1);
        assertThat(metrics.writeMetrics.serviceTimeMetrics.latency.getCount()).isEqualTo(countServiceTime + 1);

        // verify batchlog is removed
        Util.assertEmpty(Util.cmd(cfs(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.BATCHES)).withNowInSeconds(FBUtilities.nowInSeconds()).build());

        // verify data is inserted
        Util.getOnlyRow(Util.cmd(cfs(KEYSPACE, TABLE0), dk("1")).includeRow("column0").withNowInSeconds(FBUtilities.nowInSeconds()).build());
        Util.getOnlyRow(Util.cmd(cfs(KEYSPACE, TABLE0), dk("2")).includeRow("column0").withNowInSeconds(FBUtilities.nowInSeconds()).build());
    }

    /**
     * Require 2 batchlog replicas, but only local node available
     */
    @Test
    public void testMutatateAtomicallyInsufficientBatchlogReplica()
    {
        Mutator mutator = new StorageProxy.DefaultMutator();

        long now = System.nanoTime();
        Collection<Mutation> mutations = Arrays.asList(createMutation(KEYSPACE_RF2, "1", now), createMutation(KEYSPACE_RF2, "2", now));
        ConsistencyLevel consistency = ConsistencyLevel.EACH_QUORUM;
        ClientRequestsMetrics metrics = new ClientRequestsMetrics("test");
        ClientState clientState = ClientState.forInternalCalls();

        long count = metrics.writeMetrics.unavailables.getCount();

        assertThatThrownBy(() -> mutator.mutateAtomically(mutations, consistency, true, now, metrics, clientState))
        .isInstanceOf(UnavailableException.class)
        .hasMessageContaining("Cannot achieve consistency level EACH_QUORUM");

        assertThat(metrics.writeMetrics.unavailables.getCount()).isEqualTo(count + 1);

        // verify batchlog is not stored
        Util.assertEmpty(Util.cmd(cfs(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.BATCHES)).withNowInSeconds(FBUtilities.nowInSeconds()).build());
    }

    /**
     * Require 2 write replicas, but only local node available
     */
    @Test
    public void testMutatateAtomicallyInsufficientWriteReplica()
    {
        Mutator mutator = new StorageProxy.DefaultMutator();

        long now = System.nanoTime();
        Collection<Mutation> mutations = Arrays.asList(createMutation(KEYSPACE_RF2, "1", now), createMutation(KEYSPACE_RF2, "2", now));
        ConsistencyLevel consistency = ConsistencyLevel.ALL;
        ClientRequestsMetrics metrics = new ClientRequestsMetrics("test");
        ClientState clientState = ClientState.forInternalCalls();

        assertThatThrownBy(() -> mutator.mutateAtomically(mutations, consistency, false, now, metrics, clientState))
        .isInstanceOf(UnavailableException.class)
        .hasMessageContaining("Cannot achieve consistency level ALL");

        // verify batchlog is not stored
        Util.assertEmpty(Util.cmd(cfs(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.BATCHES)).withNowInSeconds(FBUtilities.nowInSeconds()).build());
    }

    @Test
    @BMRule(name = "fail mutation write",
    targetClass = "org.apache.cassandra.db.Keyspace",
    targetMethod = "apply",
    targetLocation = "AT ENTRY",
    condition = "$1.getKeyspaceName().endsWith(\"ks_test\")",
    action = "throw new RuntimeException(\"Byteman Exception\")")
    public void testMutatateAtomicallyWriteFailure()
    {
        Mutator mutator = new StorageProxy.DefaultMutator();

        long now = System.nanoTime();
        Collection<Mutation> mutations = Arrays.asList(createMutation(KEYSPACE, "1", now), createMutation(KEYSPACE, "2", now));
        ConsistencyLevel consistency = ConsistencyLevel.ALL;
        ClientRequestsMetrics metrics = new ClientRequestsMetrics("test");
        ClientState clientState = ClientState.forInternalCalls();

        assertThatThrownBy(() -> mutator.mutateAtomically(mutations, consistency, false, now, metrics, clientState))
        .isInstanceOf(WriteFailureException.class)
        .hasMessageContaining("received 0 responses and 1 failures");

        // verify batchlog is stored
        Util.getOnlyRow(Util.cmd(cfs(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.BATCHES)).withNowInSeconds(FBUtilities.nowInSeconds()).build());
    }

    /**
     * logged batch doesn't support transient replica
     */
    @Test
    public void testMutatateAtomicallyWithTransientReplica()
    {
        Mutator mutator = new StorageProxy.DefaultMutator();

        long now = System.nanoTime();
        Collection<Mutation> mutations = Arrays.asList(createMutation(KEYSPACE_TRANSIENT, "1", now), createMutation(KEYSPACE_TRANSIENT, "2", now));
        ConsistencyLevel consistency = ConsistencyLevel.EACH_QUORUM;
        ClientRequestsMetrics metrics = new ClientRequestsMetrics("test");
        ClientState clientState = ClientState.forInternalCalls();

        assertThatThrownBy(() -> mutator.mutateAtomically(mutations, consistency, false, now, metrics, clientState))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("Logged batches are unsupported with transient replication");
    }

    private static ColumnFamilyStore cfs(String keyspace, String table)
    {
        return Keyspace.open(keyspace).getColumnFamilyStore(table);
    }

    private static Mutation createMutation(String kesypace, String key, long now)
    {
        Mutation.SimpleBuilder builder = Mutation.simpleBuilder(kesypace, dk(key));

        builder.update(Schema.instance.getTableMetadata(kesypace, TABLE0))
               .timestamp(now)
               .row("column0")
               .add("val", "value0");

        return builder.build();
    }
}
