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

package org.apache.cassandra.simulator.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.consensus.migration.TransactionalMigrationFromMode;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;

import static org.apache.cassandra.simulator.Action.Modifiers.NONE;

class OnInstanceStartConsensusMigration extends ClusterAction
{
    public OnInstanceStartConsensusMigration(KeyspaceActions actions, int on, Map.Entry<String, String> startMigrationRange)
    {
        this(actions, on, NONE, NONE, startMigrationRange);
    }

    public OnInstanceStartConsensusMigration(KeyspaceActions actions, int on, Modifiers self, Modifiers transitive, Map.Entry<String, String> startMigrationRange)
    {
        super("Start consensus migration on " + on, self, transitive, actions, on, invokableBlockingStartConsensusMigration(actions.keyspace, actions.table, startMigrationRange));
    }

    private static IIsolatedExecutor.SerializableRunnable invokableBlockingStartConsensusMigration(String keyspaceName, String cfName, Map.Entry<String, String> range)
    {
        return () -> {
            List<String> keyspaces = new ArrayList<>();
            keyspaces.add(keyspaceName);
            List<String> tables = new ArrayList<>();
            tables.add(cfName);
            ClusterMetadata clusterMetadata = ClusterMetadata.current();
            TransactionalMigrationFromMode migrationFrom = clusterMetadata.schema.getTableMetadata(keyspaceName, cfName).params.transactionalMigrationFrom;
            // The alter enabling migration could be on another node, need to wait for it to be visible
            // This assumes there are no other sources of epoch changes which won't work with topology changes
            if (!migrationFrom.isMigrating())
                ClusterMetadataService.instance().fetchLogFromCMS(clusterMetadata.epoch.nextEpoch());
            StorageService.instance.migrateConsensusProtocol(keyspaces, tables, range.getKey() + ":" + range.getValue());
        };
    }
}
