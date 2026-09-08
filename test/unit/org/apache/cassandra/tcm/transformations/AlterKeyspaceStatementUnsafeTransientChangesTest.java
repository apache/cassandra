/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.cassandra.tcm.transformations;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_UNSAFE_TRANSIENT_CHANGES;
import static org.apache.cassandra.cql3.CQLTester.schemaChange;
import static org.junit.Assert.assertTrue;

/**
 * The transient replication validation rules in AlterKeyspaceStatement are skipped when
 * cassandra.allow_unsafe_transient_changes is set, which is how an operator opts into an unsafe
 * alteration. The flag is read into a static final field at class initialization, so it has to be set
 * before AlterKeyspaceStatement loads. That is why these cases need their own class rather than
 * living in AlterSchemaMutationTrackingTest, which asserts the same alterations are rejected.
 */
public class AlterKeyspaceStatementUnsafeTransientChangesTest
{
    private static final AtomicInteger ksCounter = new AtomicInteger();

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        ALLOW_UNSAFE_TRANSIENT_CHANGES.setBoolean(true);
        CassandraRelevantProperties.PARTITIONER.setString(Murmur3Partitioner.class.getName());
        ServerTestUtils.daemonInitialization();
        ServerTestUtils.prepareServer();
        MutationJournal.start();
    }

    private static String nextKsName()
    {
        return "ks" + ksCounter.incrementAndGet();
    }

    @Test
    public void testAddingWitnessesDuringMigrationAllowedWhenOptedIn()
    {
        String ksName = nextKsName();
        schemaChange("CREATE KEYSPACE " + ksName +
                     " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}" +
                     " AND replication_type = 'untracked'");
        schemaChange(String.format("CREATE TABLE %s.tbl (pk int PRIMARY KEY, val int)", ksName));
        schemaChange(String.format("ALTER KEYSPACE %s WITH replication_type = 'tracked'", ksName));
        assertTrue(ClusterMetadata.current().mutationTrackingMigrationState.isMigrating(ksName));

        schemaChange("ALTER KEYSPACE " + ksName +
                     " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3/1'}");

        assertTrue(ClusterMetadata.current().schema.getKeyspaceMetadata(ksName)
                                 .replicationStrategy.getReplicationFactor().hasTransientReplicas());
    }

    /** Enabling tracking and adding witnesses in a single statement. */
    @Test
    public void testEnablingTrackingWithWitnessesAllowedWhenOptedIn()
    {
        String ksName = nextKsName();
        schemaChange("CREATE KEYSPACE " + ksName +
                     " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}" +
                     " AND replication_type = 'untracked'");

        schemaChange("ALTER KEYSPACE " + ksName +
                     " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3/1'}" +
                     " AND replication_type = 'tracked'");

        assertTrue(ClusterMetadata.current().schema.getKeyspaceMetadata(ksName)
                                 .replicationStrategy.getReplicationFactor().hasTransientReplicas());
    }
}
