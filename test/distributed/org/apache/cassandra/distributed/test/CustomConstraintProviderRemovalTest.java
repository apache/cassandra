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

import org.junit.Test;

import org.apache.cassandra.constraints.ConstraintsProviderTest.CustomConstraintProvider;
import org.apache.cassandra.cql3.constraints.ConstraintProvider;
import org.apache.cassandra.cql3.constraints.ConstraintResolver;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.tcm.ClusterMetadataService;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;

/**
 * Tests what happens with a node when {@link ConstraintProvider} of custom constraints is removed from its class path.
 * <p>
 * A schema change is stored in the cluster metadata log as a CQL statement, and a snapshot of cluster metadata
 * contains the constraints of all tables, so a node needs the provider on startup for as long as its latest
 * snapshot, or any log entry after that snapshot, mentions a custom constraint. To remove a provider, its constraints
 * have to be dropped from all tables first, and a snapshot of cluster metadata has to be taken after that.
 * <p>
 * A provider is set on a running node here, as there is none on the class path of the tests. A node which is restarted
 * gets a new class loader, so it starts without that provider, as if its jar was removed.
 */
public class CustomConstraintProviderRemovalTest extends TestBaseImpl
{
    private static final String TABLE = KEYSPACE + ".tbl";
    private static final String UNRECOGNIZED = "Unrecognized constraint function: my_custom_constraint";

    @Test
    public void testNodeDoesNotStartWithoutProviderOfConstraintInUse() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            createTableWithCustomConstraint(cluster);

            cluster.get(1).shutdown().get();

            assertThatThrownBy(() -> cluster.get(1).startup()).hasMessage(UNRECOGNIZED);
        }
    }

    @Test
    public void testNodeDoesNotStartWithoutProviderWhenSnapshotContainsConstraint() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            createTableWithCustomConstraint(cluster);
            // this snapshot can not be read without the provider, the node then replays the whole log instead,
            // where it can not read the statement which created the constraint
            snapshotClusterMetadata(cluster);

            cluster.get(1).shutdown().get();

            assertThatThrownBy(() -> cluster.get(1).startup()).hasMessage(UNRECOGNIZED);
        }
    }

    @Test
    public void testNodeDoesNotStartWithoutProviderWhenLogAfterSnapshotContainsConstraint() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            createTableWithCustomConstraint(cluster);
            // the constraint is not in the schema anymore, but the statement which created it is still replayed
            cluster.schemaChange("ALTER TABLE " + TABLE + " ALTER v DROP CHECK");

            cluster.get(1).shutdown().get();

            assertThatThrownBy(() -> cluster.get(1).startup()).hasMessage(UNRECOGNIZED);
        }
    }

    @Test
    public void testNodeStartsWithoutProviderAfterConstraintIsDroppedAndSnapshotTaken() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            createTableWithCustomConstraint(cluster);
            cluster.schemaChange("ALTER TABLE " + TABLE + " ALTER v DROP CHECK");
            snapshotClusterMetadata(cluster);

            cluster.get(1).shutdown().get();
            cluster.get(1).startup();

            assertFalse(cluster.get(1).callOnInstance(() -> ConstraintResolver.customConstraintProvider != null));
            // a value the dropped constraint would reject
            cluster.coordinator(1).execute("INSERT INTO " + TABLE + " (pk, v) VALUES (2, 'a')", ConsistencyLevel.ALL);
        }
    }

    private static void createTableWithCustomConstraint(Cluster cluster)
    {
        cluster.get(1).runOnInstance(() -> ConstraintResolver.customConstraintProvider = new CustomConstraintProvider());
        cluster.schemaChange("CREATE TABLE " + TABLE + " (pk int PRIMARY KEY, v text CHECK MY_CUSTOM_CONSTRAINT() > 1)");

        cluster.coordinator(1).execute("INSERT INTO " + TABLE + " (pk, v) VALUES (1, 'abc')", ConsistencyLevel.ALL);
        assertThatThrownBy(() -> cluster.coordinator(1).execute("INSERT INTO " + TABLE + " (pk, v) VALUES (2, 'a')", ConsistencyLevel.ALL))
        .hasMessageContaining("It has a length of 1 and it should be > 1");
    }

    private static void snapshotClusterMetadata(Cluster cluster)
    {
        cluster.get(1).runOnInstance(() -> ClusterMetadataService.instance().triggerSnapshot());
    }
}
