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

package org.apache.cassandra.distributed.test.tracking;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.virtual.MutationTrackingTables;
import org.apache.cassandra.db.virtual.SystemViewsKeyspace;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.AssertionUtils;

import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.replication.MutationTrackingService.DISABLED_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class MutationTrackingFeatureFlagTest extends TestBaseImpl
{
    @Test
    public void shouldHideMutationTracking()  throws IOException
    {
        try (Cluster cluster = builder().withNodes(3).withConfig(c -> c.with(NETWORK).set("mutation_tracking.enabled", false)).start())
        {
            // We shouldn't be able to create a tracked keyspace: 
            Assertions.assertThatThrownBy(() -> cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + 
                                                                     " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}" +
                                                                     " AND replication_type='tracked'"))
                      .has(AssertionUtils.isThrowableInstanceof(InvalidRequestException.class))
                      .hasMessage(DISABLED_MESSAGE);

            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='untracked'");

            // We shouldn't be able to alter a keyspace to make it tracked: 
            Assertions.assertThatThrownBy(() -> cluster.schemaChange("ALTER KEYSPACE " + KEYSPACE + " WITH replication_type='tracked'"))
                      .has(AssertionUtils.isThrowableInstanceof(InvalidRequestException.class))
                      .hasMessage(DISABLED_MESSAGE);

            // Mutation tracking system keyspace tables always exist, so commit log replay works if the flag is toggled:
            assertNotNull(cluster.get(1).callOnInstance(() -> Schema.instance.getTableMetadata(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.HOST_LOG_ID)));
            assertNotNull(cluster.get(1).callOnInstance(() -> Schema.instance.getTableMetadata(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SHARDS)));
            assertNotNull(cluster.get(1).callOnInstance(() -> Schema.instance.getTableMetadata(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.COORDINATOR_LOGS)));

            // Make sure virtual tables don't exist:
            IIsolatedExecutor.SerializableCallable<Stream<VirtualTable>> hasMutationTrackingTables =
                    () -> SystemViewsKeyspace.instance.tables().stream().filter(t -> t.getClass().equals(MutationTrackingTables.MutationTrackingShardsTable.class)
                                                                                             || t.getClass().equals(MutationTrackingTables.MutationJournalTable.class));
            List<VirtualTable> tables = cluster.get(1).callOnInstance(hasMutationTrackingTables).collect(Collectors.toList());
            assertEquals(Collections.emptyList(), tables);

            String journalDirectoryPath = cluster.get(1).callOnInstance(DatabaseDescriptor::getMutationTrackingJournalDirectory);
            File journalDirectory = new File(journalDirectoryPath);
            assertFalse("Journal directory should not be created when mutation tracking is disabled", journalDirectory.exists());
        }
    }

    @Test
    public void shouldStartOnBounceWhenDisabledAfterEnabled() throws Exception
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withConfig(c -> c.with(Feature.NETWORK).set("mutation_tracking.enabled", true))
                                        .start())
        {
            // Create a tracked keyspace/table while MT is enabled:
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} AND replication_type='tracked'"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int)"));

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"), ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);

            IInvokableInstance bouncingNode = cluster.get(1);
            bouncingNode.shutdown().get();
            bouncingNode.config().set("mutation_tracking.enabled", false);
            Config badConfig = YamlConfigurationLoader.fromMap(bouncingNode.config().getParams(), true, Config.class);
            Config.setOverrideLoadConfig(() -> badConfig);

            bouncingNode.startup(cluster);

            // Flushed data written while tracking was enabled stays on disk. Unflushed data lives in the journal, which is not replayed while disabled.
            Object[][] rows = bouncingNode.executeInternal(withKeyspace("SELECT k, v FROM %s.tbl WHERE k = 1"));
            assertEquals(1, rows.length);
        }
    }
}
