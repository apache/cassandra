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

package org.apache.cassandra.db;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Migrate 3.0 versions of some tables to 4.1. In this case it's just extra columns and some keys
 * that are changed.
 * <p>
 * Can't just add the additional columns because they are primary key columns and C* doesn't support changing
 * key columns even if it's just clustering columns.
 */
public class SystemKeyspaceMigrator41
{
    private static final Logger logger = LoggerFactory.getLogger(SystemKeyspaceMigrator41.class);

    private SystemKeyspaceMigrator41()
    {
    }

    public static void migrate()
    {
        migratePeers();
        migratePeerEvents();
        migrateTransferredRanges();
        migrateAvailableRanges();
        migrateSSTableActivity();
        migrateCompactionHistory();
    }

    @VisibleForTesting
    static void migratePeers()
    {
        migrateTable(false,
                     SystemKeyspace.LEGACY_PEERS,
                     SystemKeyspace.PEERS_V2,
                     new String[]{ "peer",
                                   "peer_port",
                                   "data_center",
                                   "host_id",
                                   "preferred_ip",
                                   "preferred_port",
                                   "rack",
                                   "release_version",
                                   "native_address",
                                   "native_port",
                                   "schema_version",
                                   "tokens" },
                     row -> Collections.singletonList(new Object[]{ row.has("peer") ? row.getInetAddress("peer") : null,
                                                                    DatabaseDescriptor.getStoragePort(),
                                                                    row.has("data_center") ? row.getString("data_center") : null,
                                                                    row.has("host_id") ? row.getUUID("host_id") : null,
                                                                    row.has("preferred_ip") ? row.getInetAddress("preferred_ip") : null,
                                                                    DatabaseDescriptor.getStoragePort(),
                                                                    row.has("rack") ? row.getString("rack") : null,
                                                                    row.has("release_version") ? row.getString("release_version") : null,
                                                                    row.has("rpc_address") ? row.getInetAddress("rpc_address") : null,
                                                                    DatabaseDescriptor.getNativeTransportPort(),
                                                                    row.has("schema_version") ? row.getUUID("schema_version") : null,
                                                                    row.has("tokens") ? row.getSet("tokens", UTF8Type.instance) : null }));
    }

    @VisibleForTesting
    static void migratePeerEvents()
    {
        migrateTable(false,
                     SystemKeyspace.LEGACY_PEER_EVENTS,
                     SystemKeyspace.PEER_EVENTS_V2,
                     new String[]{ "peer",
                                   "peer_port",
                                   "hints_dropped" },
                     row -> Collections.singletonList(
                     new Object[]{ row.has("peer") ? row.getInetAddress("peer") : null,
                                   DatabaseDescriptor.getStoragePort(),
                                   row.has("hints_dropped") ? row.getMap("hints_dropped", TimeUUIDType.instance, Int32Type.instance) : null }
                     ));
    }

    @VisibleForTesting
    static void migrateTransferredRanges()
    {
        migrateTable(false,
                     SystemKeyspace.LEGACY_TRANSFERRED_RANGES,
                     SystemKeyspace.TRANSFERRED_RANGES_V2,
                     new String[]{ "operation", "peer", "peer_port", "keyspace_name", "ranges" },
                     row -> Collections.singletonList(new Object[]{ row.has("operation") ? row.getString("operation") : null,
                                                                    row.has("peer") ? row.getInetAddress("peer") : null,
                                                                    DatabaseDescriptor.getStoragePort(),
                                                                    row.has("keyspace_name") ? row.getString("keyspace_name") : null,
                                                                    row.has("ranges") ? row.getSet("ranges", BytesType.instance) : null }));
    }

    @VisibleForTesting
    static void migrateAvailableRanges()
    {
        migrateTable(false,
                     SystemKeyspace.LEGACY_AVAILABLE_RANGES,
                     SystemKeyspace.AVAILABLE_RANGES_V2,
                     new String[]{ "keyspace_name", "full_ranges", "transient_ranges" },
                     row -> Collections.singletonList(new Object[]{ row.getString("keyspace_name"),
                                                                    Optional.ofNullable(row.getSet("ranges", BytesType.instance)).orElse(Collections.emptySet()),
                                                                    Collections.emptySet() }));
    }

    @VisibleForTesting
    static void migrateSSTableActivity()
    {
        String prevVersionString = FBUtilities.getPreviousReleaseVersionString();
        CassandraVersion prevVersion = prevVersionString != null ? new CassandraVersion(prevVersionString) : CassandraVersion.NULL_VERSION;

        // if we are upgrading from pre 4.1, we want to force repopulate the table; this is for the case when we
        // upgraded from pre 4.1, then downgraded to pre 4.1 and then upgraded again
        migrateTable(CassandraVersion.CASSANDRA_4_1.compareTo(prevVersion) > 0,
                     SystemKeyspace.LEGACY_SSTABLE_ACTIVITY,
                     SystemKeyspace.SSTABLE_ACTIVITY_V2,
                     new String[]{ "keyspace_name", "table_name", "id", "rate_120m", "rate_15m" },
                     row ->
                     Collections.singletonList(new Object[]{ row.getString("keyspace_name"),
                                                             row.getString("columnfamily_name"),
                                                             new SequenceBasedSSTableId(row.getInt("generation")).toString(),
                                                             row.has("rate_120m") ? row.getDouble("rate_120m") : null,
                                                             row.has("rate_15m") ? row.getDouble("rate_15m") : null
                     })
        );
    }
    
    @VisibleForTesting
    static void migrateCompactionHistory()
    {
        migrateTable(false,
                     SystemKeyspace.COMPACTION_HISTORY,
                     SystemKeyspace.COMPACTION_HISTORY,
                     new String[]{ "id",
                                   "bytes_in",
                                   "bytes_out",
                                   "columnfamily_name",
                                   "compacted_at",
                                   "keyspace_name",
                                   "rows_merged",
                                   "compaction_properties" },
                     row -> Collections.singletonList(new Object[]{ row.getTimeUUID("id") ,
                                                                    row.has("bytes_in") ? row.getLong("bytes_in") : null,
                                                                    row.has("bytes_out") ? row.getLong("bytes_out") : null,
                                                                    row.has("columnfamily_name") ? row.getString("columnfamily_name") : null,
                                                                    row.has("compacted_at") ? row.getTimestamp("compacted_at") : null,
                                                                    row.has("keyspace_name") ? row.getString("keyspace_name") : null,
                                                                    row.has("rows_merged") ? row.getMap("rows_merged", Int32Type.instance, LongType.instance) : null,
                                                                    row.has("compaction_properties") ? row.getMap("compaction_properties", UTF8Type.instance, UTF8Type.instance) : ImmutableMap.of() })
        );
    }

    /**
     * Perform table migration by reading data from the old table, converting it, and adding to the new table.
     * If oldName and newName are same, it means data in the table will be refreshed.
     * 
     * @param truncateIfExists truncate the existing table if it exists before migration; if it is disabled
     *                         and the new table is not empty and oldName is not equal to newName, no migration is performed
     * @param oldName          old table name
     * @param newName          new table name
     * @param columns          columns to fill in the new table in the same order as returned by the transformation
     * @param transformation   transformation function which gets the row from the old table and returns a row for the new table
     */
    @VisibleForTesting
    static void migrateTable(boolean truncateIfExists, String oldName, String newName, String[] columns, Function<UntypedResultSet.Row, Collection<Object[]>> transformation)
    {
        ColumnFamilyStore newTable = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(newName);

        if (!newTable.isEmpty() && !truncateIfExists && !oldName.equals(newName))
            return;

        if (truncateIfExists)
            newTable.truncateBlockingWithoutSnapshot();

        logger.info("{} table was empty, migrating legacy {}, if this fails you should fix the issue and then truncate {} to have it try again.",
                    newName, oldName, newName);

        String query = String.format("SELECT * FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, oldName);
        String insert = String.format("INSERT INTO %s.%s (%s) VALUES (%s)", SchemaConstants.SYSTEM_KEYSPACE_NAME, newName,
                                      StringUtils.join(columns, ", "), StringUtils.repeat("?", ", ", columns.length));

        UntypedResultSet rows = QueryProcessor.executeInternal(query);

        assert rows != null : String.format("Migrating rows from legacy %s to %s was not done as returned rows from %s are null!", oldName, newName, oldName);
        
        int transferred = 0;
        logger.info("Migrating rows from legacy {} to {}", oldName, newName);
        for (UntypedResultSet.Row row : rows)
        {
            logger.debug("Transferring row {}", transferred);
            for (Object[] newRow : transformation.apply(row))
                QueryProcessor.executeInternal(insert, newRow);
            transferred++;
        }

        logger.info("Migrated {} rows from legacy {} to {}", transferred, oldName, newName);
    }
}
