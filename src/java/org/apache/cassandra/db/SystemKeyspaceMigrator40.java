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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;

/**
 * Migrate 3.0 versions of some tables to 4.0. In this case it's just extra columns and some keys
 * that are changed.
 *
 * Can't just add the additional columns because they are primary key columns and C* doesn't support changing
 * key columns even if it's just clustering columns.
 */
public class SystemKeyspaceMigrator40
{
    static final String legacyPeersName = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_PEERS);
    static final String peersName = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PEERS_V2);
    static final String legacyPeerEventsName = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_PEER_EVENTS);
    static final String peerEventsName = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PEER_EVENTS_V2);
    static final String legacyTransferredRangesName = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_TRANSFERRED_RANGES);
    static final String transferredRangesName = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.TRANSFERRED_RANGES_V2);
    static final String legacyAvailableRangesName = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_AVAILABLE_RANGES);
    static final String availableRangesName = String.format("%s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.AVAILABLE_RANGES_V2);


    private static final Logger logger = LoggerFactory.getLogger(SystemKeyspaceMigrator40.class);

    private SystemKeyspaceMigrator40() {}

    public static void migrate()
    {
        migratePeers();
        migratePeerEvents();
        migrateTransferredRanges();
        migrateAvailableRanges();
    }

    private static void migratePeers()
    {
        ColumnFamilyStore newPeers = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.PEERS_V2);

        if (!newPeers.isEmpty())
             return;

        logger.info("{} table was empty, migrating legacy {}, if this fails you should fix the issue and then truncate {} to have it try again.",
                                  peersName, legacyPeersName, peersName);

        String query = String.format("SELECT * FROM %s",
                                     legacyPeersName);

        String insert = String.format("INSERT INTO %s ( "
                                      + "peer, "
                                      + "peer_port, "
                                      + "data_center, "
                                      + "host_id, "
                                      + "preferred_ip, "
                                      + "preferred_port, "
                                      + "rack, "
                                      + "release_version, "
                                      + "native_address, "
                                      + "native_port, "
                                      + "schema_version, "
                                      + "tokens) "
                                      + " values ( ?, ?, ? , ? , ?, ?, ?, ?, ?, ?, ?, ?)",
                                      peersName);

        UntypedResultSet rows = QueryProcessor.executeInternalWithPaging(query, 1000);
        int transferred = 0;
        logger.info("Migrating rows from legacy {} to {}", legacyPeersName, peersName);
        for (UntypedResultSet.Row row : rows)
        {
            logger.debug("Transferring row {}", transferred);
            QueryProcessor.executeInternal(insert,
                                           row.has("peer") ? row.getInetAddress("peer") : null,
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
                                           row.has("tokens") ? row.getSet("tokens", UTF8Type.instance) : null);
            transferred++;
        }
        logger.info("Migrated {} rows from legacy {} to {}", transferred, legacyPeersName, peersName);
    }

    private static void migratePeerEvents()
    {
        ColumnFamilyStore newPeerEvents = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.PEER_EVENTS_V2);

        if (!newPeerEvents.isEmpty())
            return;

        logger.info("{} table was empty, migrating legacy {} to {}", peerEventsName, legacyPeerEventsName, peerEventsName);

        String query = String.format("SELECT * FROM %s",
                                     legacyPeerEventsName);

        String insert = String.format("INSERT INTO %s ( "
                                      + "peer, "
                                      + "peer_port, "
                                      + "hints_dropped) "
                                      + " values ( ?, ?, ? )",
                                      peerEventsName);

        UntypedResultSet rows = QueryProcessor.executeInternalWithPaging(query, 1000);
        int transferred = 0;
        for (UntypedResultSet.Row row : rows)
        {
            logger.debug("Transferring row {}", transferred);
            QueryProcessor.executeInternal(insert,
                                           row.has("peer") ? row.getInetAddress("peer") : null,
                                           DatabaseDescriptor.getStoragePort(),
                                           row.has("hints_dropped") ? row.getMap("hints_dropped", UUIDType.instance, Int32Type.instance) : null);
            transferred++;
        }
        logger.info("Migrated {} rows from legacy {} to {}", transferred, legacyPeerEventsName, peerEventsName);
    }

    static void migrateTransferredRanges()
    {
        ColumnFamilyStore newTransferredRanges = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.TRANSFERRED_RANGES_V2);

        if (!newTransferredRanges.isEmpty())
            return;

        logger.info("{} table was empty, migrating legacy {} to {}", transferredRangesName, legacyTransferredRangesName, transferredRangesName);

        String query = String.format("SELECT * FROM %s",
                                     legacyTransferredRangesName);

        String insert = String.format("INSERT INTO %s ("
                                      + "operation, "
                                      + "peer, "
                                      + "peer_port, "
                                      + "keyspace_name, "
                                      + "ranges) "
                                      + " values ( ?, ?, ? , ?, ?)",
                                      transferredRangesName);

        UntypedResultSet rows = QueryProcessor.executeInternalWithPaging(query, 1000);
        int transferred = 0;
        for (UntypedResultSet.Row row : rows)
        {
            logger.debug("Transferring row {}", transferred);
            QueryProcessor.executeInternal(insert,
                                           row.has("operation") ? row.getString("operation") : null,
                                           row.has("peer") ? row.getInetAddress("peer") : null,
                                           DatabaseDescriptor.getStoragePort(),
                                           row.has("keyspace_name") ? row.getString("keyspace_name") : null,
                                           row.has("ranges") ? row.getSet("ranges", BytesType.instance) : null);
            transferred++;
        }

        logger.info("Migrated {} rows from legacy {} to {}", transferred, legacyTransferredRangesName, transferredRangesName);
    }

    static void migrateAvailableRanges()
    {
        ColumnFamilyStore newAvailableRanges = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.AVAILABLE_RANGES_V2);

        if (!newAvailableRanges.isEmpty())
            return;

        logger.info("{} table was empty, migrating legacy {} to {}", availableRangesName, legacyAvailableRangesName, availableRangesName);

        String query = String.format("SELECT * FROM %s",
                                     legacyAvailableRangesName);

        String insert = String.format("INSERT INTO %s ("
                                      + "keyspace_name, "
                                      + "full_ranges, "
                                      + "transient_ranges) "
                                      + " values ( ?, ?, ? )",
                                      availableRangesName);

        UntypedResultSet rows = QueryProcessor.executeInternalWithPaging(query, 1000);
        int transferred = 0;
        for (UntypedResultSet.Row row : rows)
        {
            logger.debug("Transferring row {}", transferred);
            String keyspace = row.getString("keyspace_name");
            Set<ByteBuffer> ranges = Optional.ofNullable(row.getSet("ranges", BytesType.instance)).orElse(Collections.emptySet());
            QueryProcessor.executeInternal(insert,
                                           keyspace,
                                           ranges,
                                           Collections.emptySet());
            transferred++;
        }

        logger.info("Migrated {} rows from legacy {} to {}", transferred, legacyAvailableRangesName, availableRangesName);
    }

}
