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

package org.apache.cassandra.tcm.transformations;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.AlterTableStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.ClusterMetadataService.State;
import org.apache.cassandra.tcm.listeners.ChangeListener;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.utils.CassandraVersion;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.exceptions.ExceptionCode.ALREADY_EXISTS;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;

// Update distributed system tables after TCM is running on a fully upgraded cluster
public class UpdateDistributedSystemTables implements ChangeListener.Async
{
    private static final Logger logger = LoggerFactory.getLogger(UpdateDistributedSystemTables.class);
    private static final CassandraVersion MIN_TO_UPDATE = new CassandraVersion("5.0");

    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        maybeUpdateDistributedSystemTables(prev, next);
    }

    // This listener waits until TCM is activated and all nodes are upgraded to the minimum version before
    // doing the schema change. A 4.X to 5.X upgrade is forced to wait for an entire cluster upgrade and TCM activation
    // before it can change the schema, but 5.X to later versions can use maybeUpdateDistributedSystemTables
    // to change the schema when the first node is upgraded.
    private static void maybeUpdateDistributedSystemTables(@Nullable ClusterMetadata prev, ClusterMetadata next)
    {
        if (next.directory.clusterMinVersion.cassandraVersion.compareTo(MIN_TO_UPDATE) < 0)
            return;
        // Avoid expensive schema comparisons when nothing changes
        if (prev != null && next.directory.clusterMinVersion == prev.directory.clusterMinVersion)
            return;

        for (KeyspaceMetadata keyspace : ImmutableList.of(TraceKeyspace.metadata(), SystemDistributedKeyspace.metadata(), AuthKeyspace.metadata()))
        {
            for (TableMetadata table : keyspace.tables)
            {
                if (maybeCreateTable(next, keyspace, table))
                    maybeAlterTable(next, keyspace, table);
            }
        }
    }

    // Make it possible for 5.X upgrades to later versions to update system tables without waiting for the cluster to reach some minimum version
    public static void maybeUpdateDistributedSystemTables()
    {
        State state = ClusterMetadataService.state();
        Set<State> migratedStates = ImmutableSet.of(State.LOCAL, State.REMOTE);
        if (!ClusterMetadataService.instance().isMigrating() && migratedStates.contains(state))
            maybeUpdateDistributedSystemTables(null, ClusterMetadata.current());
    }

    private static boolean maybeCreateTable(ClusterMetadata metadata, KeyspaceMetadata keyspace, TableMetadata table)
    {
        KeyspaceMetadata existingKeyspace = metadata.schema.getKeyspaces().getNullable(keyspace.name);
        checkState(existingKeyspace != null, "Keyspace should already exist");
        if (existingKeyspace.getTableNullable(table.name) != null)
            return true;

        String createCql = table.toCqlString(true, false);

        logger.info("Maybe creating {}.{}", keyspace.name, table.name);
        CreateTableStatement stmt = (CreateTableStatement) QueryProcessor.getStatement(createCql, ClientState.forInternalCalls());
        AlterSchema transform = new AlterSchema(stmt, Schema.instance);
        ClusterMetadataService.instance().commit(transform,
                                                 (m) -> m,
                                                 (code, message) ->
                                                 {
                                                     if (code != ALREADY_EXISTS)
                                                         logger.warn("Error initializing {}.{} ({})",
                                                                     AUTH_KEYSPACE_NAME,
                                                                     table,
                                                                     message);
                                                     else
                                                         logger.info("{}.{} already exists", AUTH_KEYSPACE_NAME, table);

                                                     return ClusterMetadata.current();
                                                 });
        return false;
    }

    // Add columns to a system table, won't alter or drop
    private static @Nullable String addColumnsCQL(TableMetadata existing, TableMetadata desired)
    {
        checkState(existing.partitionKeyColumns().equals(desired.partitionKeyColumns()), "Can't change partition key columns");
        checkState(existing.clusteringColumns().equals(desired.clusteringColumns()), "Can't change clustering columns");

        List<ColumnMetadata> columnsToAdd = new ArrayList<>();
        for (ColumnMetadata column : desired.regularAndStaticColumns())
        {
            ColumnMetadata existingColumn = existing.getColumn(column.name);
            if (existingColumn == null)
                columnsToAdd.add(column);
            else
                checkState(existingColumn.type.equals(column.type)
                           && existingColumn.isStatic() == column.isStatic()
                           && existingColumn.isMasked() == column.isMasked(), "Can't alter existing column");
        }

        if (columnsToAdd.isEmpty())
            return null;

        CqlBuilder cqlBuilder = new CqlBuilder();
        cqlBuilder.append("ALTER TABLE ")
                  .append(desired.keyspace)
                  .append('.')
                  .append(desired.name)
                  .append(" ADD ")
                  .appendWithSeparators(columnsToAdd, (b, c) -> c.appendCqlTo(b), ", ")
        .append(';');

        return cqlBuilder.toString();
    }

    // Only adds columns
    private static void maybeAlterTable(ClusterMetadata metadata, KeyspaceMetadata keyspace, TableMetadata table)
    {
        KeyspaceMetadata existingKeyspace = metadata.schema.getKeyspaces().getNullable(keyspace.name);
        checkState(existingKeyspace != null, "Keyspace should already exist");
        TableMetadata existingTable = existingKeyspace.tables.get(table.name).get();

        String alterCql = addColumnsCQL(existingTable, table);
        if (alterCql == null)
            return;

        logger.info("Maybe altering {}.{}", keyspace.name, table.name);
        AlterTableStatement stmt = (AlterTableStatement) QueryProcessor.getStatement(alterCql, ClientState.forInternalCalls());
        AlterSchema transform = new AlterSchema(stmt, Schema.instance);
        ClusterMetadataService.instance().commit(transform,
                                                 (m) -> m,
                                                 (code, message) ->
                                                 {
                                                     if (code != ALREADY_EXISTS)
                                                         logger.warn("Error initializing {}.{} ({})",
                                                                     AUTH_KEYSPACE_NAME,
                                                                     table,
                                                                     message);
                                                     else
                                                         logger.info("{}.{} already exists", AUTH_KEYSPACE_NAME, table);

                                                     return ClusterMetadata.current();
                                                 });
    }
}