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
package org.apache.cassandra.db.virtual;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.index.sai.virtual.StorageAttachedIndexTables;

import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_VIEWS;

public final class SystemViewsKeyspace extends VirtualKeyspace
{
    public static SystemViewsKeyspace instance = new SystemViewsKeyspace();

    private SystemViewsKeyspace()
    {
        super(VIRTUAL_VIEWS, new ImmutableList.Builder<VirtualTable>()
                    .add(new CachesTable(VIRTUAL_VIEWS))
                    .add(new ClientsTable(VIRTUAL_VIEWS))
                    .add(new SettingsTable(VIRTUAL_VIEWS))
                    .add(new SystemPropertiesTable(VIRTUAL_VIEWS))
                    .add(new SSTableTasksTable(VIRTUAL_VIEWS))
                    .add(new ThreadPoolsTable(VIRTUAL_VIEWS))
                    .add(new InternodeOutboundTable(VIRTUAL_VIEWS))
                    .add(new InternodeInboundTable(VIRTUAL_VIEWS))
                    .add(new PendingHintsTable(VIRTUAL_VIEWS))
                    .addAll(TableMetricTables.getAll(VIRTUAL_VIEWS))
                    .add(new CredentialsCacheKeysTable(VIRTUAL_VIEWS))
                    .add(new JmxPermissionsCacheKeysTable(VIRTUAL_VIEWS))
                    .add(new NetworkPermissionsCacheKeysTable(VIRTUAL_VIEWS))
                    .add(new PermissionsCacheKeysTable(VIRTUAL_VIEWS))
                    .add(new RolesCacheKeysTable(VIRTUAL_VIEWS))
                    .add(new CQLMetricsTable(VIRTUAL_VIEWS))
                    .add(new BatchMetricsTable(VIRTUAL_VIEWS))
                    .add(new StreamingVirtualTable(VIRTUAL_VIEWS))
                    .add(new GossipInfoTable(VIRTUAL_VIEWS))
                    .add(new QueriesTable(VIRTUAL_VIEWS))
                    .add(new LogMessagesTable(VIRTUAL_VIEWS))
                    .add(new SnapshotsTable(VIRTUAL_VIEWS))
                    .addAll(LocalRepairTables.getAll(VIRTUAL_VIEWS))
                    .addAll(CIDRFilteringMetricsTable.getAll(VIRTUAL_VIEWS))
                    .addAll(StorageAttachedIndexTables.getAll(VIRTUAL_VIEWS))
                    .build());
    }
}
