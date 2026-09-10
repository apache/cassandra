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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.cql3.CQLTester.schemaChange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Which replication factor and replication type transitions a keyspace with witnesses (transient
 * replicas) permits. Each rejected transition names the rule that rejects it, because several rules
 * overlap and a test asserting only that the statement failed can pass for the wrong reason.
 */
@RunWith(Parameterized.class)
public class AlterKeyspaceWitnessTransitionTest
{
    private static final AtomicInteger ksCounter = new AtomicInteger();

    /** The state a keyspace starts in, including whether a mutation tracking migration is in flight. */
    enum Start
    {
        TRACKED_3("3", "tracked", false),
        TRACKED_3_1("3/1", "tracked", false),
        UNTRACKED_3("3", "untracked", false),
        MIGRATING_3("3", "untracked", true);

        final String replicationFactor;
        final String replicationType;
        final boolean startMigration;

        Start(String replicationFactor, String replicationType, boolean startMigration)
        {
            this.replicationFactor = replicationFactor;
            this.replicationType = replicationType;
            this.startMigration = startMigration;
        }
    }

    @Parameterized.Parameter(0)
    public Start start;

    @Parameterized.Parameter(1)
    public String proposedReplicationFactor;

    @Parameterized.Parameter(2)
    public String proposedReplicationType;

    /** The rejection this transition must produce, or null when it is permitted. */
    @Parameterized.Parameter(3)
    public String expectedRejection;

    @Parameterized.Parameter(4)
    public String description;

    @Parameterized.Parameters(name = "{4}")
    public static Collection<Object[]> transitions()
    {
        List<Object[]> rows = new ArrayList<>();

        // A settled tracked keyspace accepts witnesses: the guard is on the migration, not on transient
        // replicas as such
        rows.add(row(Start.TRACKED_3, "3/1", null, null, "settled tracked keyspace accepts witnesses"));

        // Enabling tracking and adding witnesses at once starts a migration, so the migration state
        // cannot yet show it and the transition itself has to be recognised
        rows.add(row(Start.UNTRACKED_3, "3/1", "tracked", "Cannot enable mutation tracking on",
                     "enabling tracking and adding witnesses at once"));

        // Reads for a pending range take the untracked path and would contact a transient replica
        rows.add(row(Start.MIGRATING_3, "3/1", null, "Cannot add transient replicas to",
                     "adding witnesses while a migration is in flight"));

        return rows;
    }

    private static Object[] row(Start start, String rf, String type, String rejection, String description)
    {
        return new Object[]{ start, rf, type, rejection, description };
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        CassandraRelevantProperties.PARTITIONER.setString(Murmur3Partitioner.class.getName());
        ServerTestUtils.daemonInitialization();
        ServerTestUtils.prepareServer();
        MutationJournal.start();
    }

    @Test
    public void testTransition()
    {
        String ksName = "ks" + ksCounter.incrementAndGet();
        schemaChange("CREATE KEYSPACE " + ksName +
                     " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '" + start.replicationFactor + "'}" +
                     " AND replication_type = '" + start.replicationType + "'");

        if (start.startMigration)
        {
            schemaChange(String.format("CREATE TABLE %s.tbl (pk int PRIMARY KEY, val int)", ksName));
            schemaChange(String.format("ALTER KEYSPACE %s WITH replication_type = 'tracked'", ksName));
            assertThat(ClusterMetadata.current().mutationTrackingMigrationState.isMigrating(ksName)).isTrue();
        }

        String alter = "ALTER KEYSPACE " + ksName +
                       " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '" + proposedReplicationFactor + "'}" +
                       (proposedReplicationType == null ? "" : " AND replication_type = '" + proposedReplicationType + "'");

        if (expectedRejection != null)
        {
            assertThatThrownBy(() -> schemaChange(alter))
                .hasRootCauseInstanceOf(ConfigurationException.class)
                .hasRootCauseMessage(rejectionMessage(ksName));
            assertUnchanged(ksName);
        }
        else
        {
            schemaChange(alter);
            assertProposedStateApplied(ksName);
        }
    }

    /**
     * The rules name the keyspace, so the expected message is completed here rather than in the table.
     * Matching the whole message keeps overlapping rules distinguishable: several of these transitions
     * are rejected by more than one rule in principle, and only one of them should fire.
     */
    private String rejectionMessage(String ksName)
    {
        switch (expectedRejection)
        {
            case "Cannot enable mutation tracking on":
                return String.format("Cannot enable mutation tracking on %s and add transient replicas in the same " +
                                     "statement, because doing so starts a migration. Set replication_type = " +
                                     "'tracked' first, wait for the migration to complete, then alter the " +
                                     "replication factor.", ksName);
            case "Cannot add transient replicas to":
                return String.format("Cannot add transient replicas to %s while its mutation tracking migration is " +
                                     "in progress. Wait for the migration to complete, then alter the replication " +
                                     "factor.", ksName);
            default:
                throw new AssertionError("unhandled rejection: " + expectedRejection);
        }
    }

    private void assertUnchanged(String ksName)
    {
        KeyspaceMetadata ksm = ClusterMetadata.current().schema.getKeyspaceMetadata(ksName);
        ReplicationFactor rf = ksm.replicationStrategy.getReplicationFactor();
        assertThat(rf.toString()).isEqualTo(expectedReplicationFactor(start.replicationFactor).toString());
        assertThat(ksm.params.replicationType.isTracked()).isEqualTo(startedTracked());
    }

    private void assertProposedStateApplied(String ksName)
    {
        KeyspaceMetadata ksm = ClusterMetadata.current().schema.getKeyspaceMetadata(ksName);
        ReplicationFactor rf = ksm.replicationStrategy.getReplicationFactor();
        assertThat(rf.toString()).isEqualTo(expectedReplicationFactor(proposedReplicationFactor).toString());
        if (proposedReplicationType != null)
            assertThat(ksm.params.replicationType.isTracked()).isEqualTo("tracked".equals(proposedReplicationType));
    }

    private boolean startedTracked()
    {
        return "tracked".equals(start.replicationType) || start.startMigration;
    }

    private static ReplicationFactor expectedReplicationFactor(String spec)
    {
        return ReplicationFactor.fromString(spec);
    }
}
