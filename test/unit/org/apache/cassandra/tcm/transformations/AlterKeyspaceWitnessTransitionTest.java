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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;

import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_UNSAFE_TRANSIENT_CHANGES;
import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_UNSAFE_WITNESS_PROMOTION;
import static org.apache.cassandra.cql3.CQLTester.schemaChange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Which replication changes a keyspace with witnesses (transient replicas) permits.
 *
 * Each rejected transition asserts the whole rejection message rather than just that the statement
 * failed. Several rules overlap here -- witness promotion, the migration guard, the pre-existing
 * replica-count rules, and the requirement that transient replication implies mutation tracking -- so a
 * test asserting only failure can pass because the wrong rule fired.
 *
 * Datacenter-aware rows are present because {@link org.apache.cassandra.locator.NetworkTopologyStrategy}
 * reports the sum of its per-datacenter factors: comparing aggregates both misses a promotion in one
 * datacenter offset by a reduction in another, and rejects two independently legal per-datacenter
 * changes whose aggregates resemble a promotion.
 */
@RunWith(Parameterized.class)
public class AlterKeyspaceWitnessTransitionTest
{
    private static final String SIMPLE_3 = "{'class': 'SimpleStrategy', 'replication_factor': '3'}";
    private static final String SIMPLE_3_1 = "{'class': 'SimpleStrategy', 'replication_factor': '3/1'}";
    private static final String NTS_3_1_AND_3 = "{'class': 'NetworkTopologyStrategy', 'DC1': '3/1', 'DC2': '3'}";
    private static final String NTS_3_1_AND_2 = "{'class': 'NetworkTopologyStrategy', 'DC1': '3/1', 'DC2': '2'}";

    private static final AtomicInteger ksCounter = new AtomicInteger();

    private static final String TRACKED = "tracked";
    private static final String UNTRACKED = "untracked";
    private static final boolean MIGRATING = true;
    private static final boolean NO_MIGRATION = false;

    @Parameterized.Parameter(0)
    public String startReplication;

    /** The replication type the keyspace is created with. */
    @Parameterized.Parameter(1)
    public String startReplicationType;

    /** Whether to leave a mutation tracking migration in flight before the alteration under test. */
    @Parameterized.Parameter(2)
    public boolean startMigration;

    @Parameterized.Parameter(3)
    public String proposedReplication;

    /** The replication type to propose, or null to leave it unchanged. */
    @Parameterized.Parameter(4)
    public String proposedReplicationType;

    /** The rejection this change must produce, or null when it is permitted. */
    @Parameterized.Parameter(5)
    public Rejection expectedRejection;

    @Parameterized.Parameter(6)
    public String description;

    enum Rejection
    {
        WITNESS_PROMOTION("Cannot promote a transient replica of %s to a full replica: it holds no data for the " +
                          "range it witnessed. Set " + ALLOW_UNSAFE_WITNESS_PROMOTION.getKey() + "=true over JMX, " +
                          "or " + ALLOW_UNSAFE_TRANSIENT_CHANGES.getKey() + "=true at startup, to allow it, then " +
                          "run a full repair to distribute the data."),
        MIGRATION_IN_FLIGHT("Cannot add transient replicas to %s while its mutation tracking migration is in " +
                            "progress. Wait for the migration to complete, then alter the replication factor."),
        WOULD_START_MIGRATION("Cannot enable mutation tracking on %s and add transient replicas in the same " +
                              "statement, because doing so starts a migration. Set replication_type = 'tracked' " +
                              "first, wait for the migration to complete, then alter the replication factor."),
        NEEDS_MUTATION_TRACKING("Transient replication requires mutation tracking"),
        REPLICA_COUNT_RULE("Can't add full replicas if there are any transient replicas. You must first remove all " +
                           "transient replicas, then change the # of full replicas, then add back the transient " +
                           "replicas");

        private final String template;

        Rejection(String template)
        {
            this.template = template;
        }

        String message(String keyspace)
        {
            return template.contains("%s") ? String.format(template, keyspace) : template;
        }
    }

    @Parameterized.Parameters(name = "{6}")
    public static Collection<Object[]> transitions()
    {
        List<Object[]> rows = new ArrayList<>();

        // The migration guard is on the migration, not on transient replicas as such
        rows.add(row(SIMPLE_3, TRACKED, NO_MIGRATION, "3/1", null, null,
                     "settled tracked keyspace accepts witnesses"));
        rows.add(row(SIMPLE_3, UNTRACKED, MIGRATING, "3/1", null, Rejection.MIGRATION_IN_FLIGHT,
                     "adding witnesses while a migration is in flight"));
        rows.add(row(SIMPLE_3, UNTRACKED, NO_MIGRATION, "3/1", TRACKED, Rejection.WOULD_START_MIGRATION,
                     "enabling tracking and adding witnesses at once"));

        // Dropping a witness needs no data movement: replica ordering puts the transient replica last, so
        // lowering the factor removes it from the replica set rather than promoting it
        rows.add(row(SIMPLE_3_1, TRACKED, NO_MIGRATION, "2", null, null, "dropping a witness"));
        rows.add(row(SIMPLE_3_1, TRACKED, NO_MIGRATION, "2", UNTRACKED, null,
                     "dropping a witness and tracking at once"));
        rows.add(row(SIMPLE_3, TRACKED, NO_MIGRATION, "3", UNTRACKED, null,
                     "leaving tracked replication without witnesses"));
        rows.add(row(SIMPLE_3_1, TRACKED, NO_MIGRATION, "3/1", UNTRACKED, Rejection.NEEDS_MUTATION_TRACKING,
                     "retaining witnesses while leaving tracked replication"));

        // A promoted witness holds no data for the range it witnessed, and quorum reads would count it
        rows.add(row(SIMPLE_3_1, TRACKED, NO_MIGRATION, "3", null, Rejection.WITNESS_PROMOTION,
                     "promoting a witness to a full replica"));
        rows.add(row(SIMPLE_3_1, TRACKED, NO_MIGRATION, "3", UNTRACKED, Rejection.WITNESS_PROMOTION,
                     "promoting a witness while leaving tracked replication"));

        // Per-datacenter comparison: the aggregate full replica count is 5 either side of this change
        rows.add(row(NTS_3_1_AND_3, TRACKED, NO_MIGRATION,
                     "{'class': 'NetworkTopologyStrategy', 'DC1': '3', 'DC2': '2'}", null,
                     Rejection.WITNESS_PROMOTION, "promotion masked by a reduction in another datacenter"));
        rows.add(row(NTS_3_1_AND_3, TRACKED, NO_MIGRATION,
                     "{'class': 'NetworkTopologyStrategy', 'DC1': '3', 'DC2': '3'}", null,
                     Rejection.WITNESS_PROMOTION, "promotion in a single datacenter"));
        // Dropping a witness in one datacenter and adding a full replica in another are legal alone; their
        // aggregates resemble a promotion
        rows.add(row(NTS_3_1_AND_2, TRACKED, NO_MIGRATION,
                     "{'class': 'NetworkTopologyStrategy', 'DC1': '2', 'DC2': '3'}", null, null,
                     "legal changes in separate datacenters"));
        // Not the promotion guard: removing a datacenter drops the aggregate full count while a transient
        // replica remains, which the pre-existing replica-count rule rejects
        rows.add(row(NTS_3_1_AND_3, TRACKED, NO_MIGRATION,
                     "{'class': 'NetworkTopologyStrategy', 'DC2': '3'}", null,
                     Rejection.REPLICA_COUNT_RULE, "removing a witness datacenter entirely"));

        return rows;
    }

    private static Object[] row(String startReplication, String startType, boolean startMigration,
                                String proposedReplication, String proposedType, Rejection rejection,
                                String description)
    {
        return new Object[]{ startReplication, startType, startMigration, proposedReplication, proposedType,
                             rejection, description };
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        CassandraRelevantProperties.PARTITIONER.setString(Murmur3Partitioner.class.getName());
        ServerTestUtils.daemonInitialization();
        ServerTestUtils.prepareServer();
        MutationJournal.start();

        // Datacenter-aware rows need endpoints in both datacenters for their options to be recognised
        addEndpoints(new byte[]{ 10, 0, 0 }, new Location("DC1", "Rack1"));
        addEndpoints(new byte[]{ 10, 20, 114 }, new Location("DC2", "Rack1"));
    }

    private static void addEndpoints(byte[] prefix, Location location) throws UnknownHostException
    {
        for (byte last = 10; last < 14; last++)
        {
            InetAddressAndPort addr = InetAddressAndPort.getByAddress(new byte[]{ prefix[0], prefix[1], prefix[2], last });
            ClusterMetadataTestHelper.addEndpoint(addr, Murmur3Partitioner.instance.getRandomToken(), location);
        }
    }

    @Test
    public void testTransition()
    {
        String ksName = "ks" + ksCounter.incrementAndGet();
        schemaChange("CREATE KEYSPACE " + ksName + " WITH replication = " + startReplication +
                     " AND replication_type = '" + startReplicationType + "'");

        if (startMigration)
        {
            schemaChange(String.format("CREATE TABLE %s.tbl (pk int PRIMARY KEY, val int)", ksName));
            schemaChange(String.format("ALTER KEYSPACE %s WITH replication_type = 'tracked'", ksName));
            assertThat(ClusterMetadata.current().mutationTrackingMigrationState.isMigrating(ksName)).isTrue();
        }

        Map<String, String> before = replicationOf(ksName);
        String proposed = proposedReplication.startsWith("{")
                          ? proposedReplication
                          : "{'class': 'SimpleStrategy', 'replication_factor': '" + proposedReplication + "'}";
        String alter = "ALTER KEYSPACE " + ksName + " WITH replication = " + proposed +
                       (proposedReplicationType == null ? "" : " AND replication_type = '" + proposedReplicationType + "'");

        if (expectedRejection != null)
        {
            assertThatThrownBy(() -> schemaChange(alter))
                .hasRootCauseInstanceOf(ConfigurationException.class)
                .hasRootCauseMessage(expectedRejection.message(ksName));
            assertThat(replicationOf(ksName)).isEqualTo(before);
            assertThat(trackedOf(ksName)).isEqualTo(TRACKED.equals(startReplicationType) || startMigration);
        }
        else
        {
            schemaChange(alter);
            assertThat(replicationOf(ksName)).isEqualTo(optionsOf(proposed));
            if (proposedReplicationType != null)
                assertThat(trackedOf(ksName)).isEqualTo("tracked".equals(proposedReplicationType));
        }
    }

    /**
     * The options in a CQL replication map, in the form {@link ReplicationParams#asMap} returns them:
     * the strategy class is fully qualified there.
     */
    private static Map<String, String> optionsOf(String cqlReplicationMap)
    {
        Map<String, String> options = new HashMap<>();
        for (String entry : cqlReplicationMap.replaceAll("[{}']", "").split(","))
        {
            String[] keyAndValue = entry.split(":");
            String key = keyAndValue[0].trim();
            String value = keyAndValue[1].trim();
            options.put(key, ReplicationParams.CLASS.equals(key) ? "org.apache.cassandra.locator." + value : value);
        }
        return options;
    }

    private static Map<String, String> replicationOf(String ksName)
    {
        return keyspace(ksName).params.replication.asMap();
    }

    private static boolean trackedOf(String ksName)
    {
        return keyspace(ksName).params.replicationType.isTracked();
    }

    private static KeyspaceMetadata keyspace(String ksName)
    {
        return ClusterMetadata.current().schema.getKeyspaceMetadata(ksName);
    }
}
