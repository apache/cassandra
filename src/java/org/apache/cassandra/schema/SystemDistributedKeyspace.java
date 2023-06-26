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
package org.apache.cassandra.schema;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.CommonRange;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static java.lang.String.format;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public final class SystemDistributedKeyspace
{
    private SystemDistributedKeyspace()
    {
    }

    public static final String NAME = "system_distributed";

    private static final int DEFAULT_RF = CassandraRelevantProperties.SYSTEM_DISTRIBUTED_DEFAULT_RF.getInt();
    private static final Logger logger = LoggerFactory.getLogger(SystemDistributedKeyspace.class);

    /**
     * Generation is used as a timestamp for automatic table creation on startup.
     * If you make any changes to the tables below, make sure to increment the
     * generation and document your change here.
     *
     * gen 0: original definition in 2.2
     * gen 1: (pre-)add options column to parent_repair_history in 3.0, 3.11
     * gen 2: (pre-)add coordinator_port and participants_v2 columns to repair_history in 3.0, 3.11, 4.0
     * gen 3: gc_grace_seconds raised from 0 to 10 days in CASSANDRA-12954 in 3.11.0
     * gen 4: compression chunk length reduced to 16KiB, memtable_flush_period_in_ms now unset on all tables in 4.0
     * gen 5: add ttl and TWCS to repair_history tables
     * gen 6: add denylist table
     */
    public static final long GENERATION = 6;

    public static final String REPAIR_HISTORY = "repair_history";

    public static final String PARENT_REPAIR_HISTORY = "parent_repair_history";

    public static final String VIEW_BUILD_STATUS = "view_build_status";

    public static final String PARTITION_DENYLIST_TABLE = "partition_denylist";

    public static final Set<String> TABLE_NAMES = ImmutableSet.of(REPAIR_HISTORY, PARENT_REPAIR_HISTORY, VIEW_BUILD_STATUS, PARTITION_DENYLIST_TABLE);
    
    private static final TableMetadata RepairHistory =
        parse(REPAIR_HISTORY,
                "Repair history",
                "CREATE TABLE %s ("
                     + "keyspace_name text,"
                     + "columnfamily_name text,"
                     + "id timeuuid,"
                     + "parent_id timeuuid,"
                     + "range_begin text,"
                     + "range_end text,"
                     + "coordinator inet,"
                     + "coordinator_port int,"
                     + "participants set<inet>,"
                     + "participants_v2 set<text>,"
                     + "exception_message text,"
                     + "exception_stacktrace text,"
                     + "status text,"
                     + "started_at timestamp,"
                     + "finished_at timestamp,"
                     + "PRIMARY KEY ((keyspace_name, columnfamily_name), id))")
        .defaultTimeToLive((int) TimeUnit.DAYS.toSeconds(30))
        .compaction(CompactionParams.twcs(ImmutableMap.of("compaction_window_unit","DAYS",
                                                          "compaction_window_size","1")))
        .build();

    private static final TableMetadata ParentRepairHistory =
        parse(PARENT_REPAIR_HISTORY,
                "Repair history",
                "CREATE TABLE %s ("
                     + "parent_id timeuuid,"
                     + "keyspace_name text,"
                     + "columnfamily_names set<text>,"
                     + "started_at timestamp,"
                     + "finished_at timestamp,"
                     + "exception_message text,"
                     + "exception_stacktrace text,"
                     + "requested_ranges set<text>,"
                     + "successful_ranges set<text>,"
                     + "options map<text, text>,"
                     + "PRIMARY KEY (parent_id))")
        .defaultTimeToLive((int) TimeUnit.DAYS.toSeconds(30))
        .compaction(CompactionParams.twcs(ImmutableMap.of("compaction_window_unit","DAYS",
                                                          "compaction_window_size","1")))
        .build();

    private static final TableMetadata ViewBuildStatus =
        parse(VIEW_BUILD_STATUS,
            "Materialized View build status",
            "CREATE TABLE %s ("
                     + "keyspace_name text,"
                     + "view_name text,"
                     + "host_id uuid,"
                     + "status text,"
                     + "PRIMARY KEY ((keyspace_name, view_name), host_id))").build();

    public static final TableMetadata PartitionDenylistTable =
    parse(PARTITION_DENYLIST_TABLE,
          "Partition keys which have been denied access",
          "CREATE TABLE %s ("
          + "ks_name text,"
          + "table_name text,"
          + "key blob,"
          + "PRIMARY KEY ((ks_name, table_name), key))")
    .build();

    private static TableMetadata.Builder parse(String table, String description, String cql)
    {
        return CreateTableStatement.parse(format(cql, table), SchemaConstants.DISTRIBUTED_KEYSPACE_NAME)
                                   .id(TableId.forSystemTable(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, table))
                                   .comment(description);
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, KeyspaceParams.simple(Math.max(DEFAULT_RF, DatabaseDescriptor.getDefaultKeyspaceRF())), Tables.of(RepairHistory, ParentRepairHistory, ViewBuildStatus, PartitionDenylistTable));
    }

    public static void startParentRepair(TimeUUID parent_id, String keyspaceName, String[] cfnames, RepairOption options)
    {
        Collection<Range<Token>> ranges = options.getRanges();
        String query = "INSERT INTO %s.%s (parent_id, keyspace_name, columnfamily_names, requested_ranges, started_at,          options)"+
                                 " VALUES (%s,        '%s',          { '%s' },           { '%s' },          to_timestamp(now()), { %s })";
        String fmtQry = format(query,
                                      SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                      PARENT_REPAIR_HISTORY,
                                      parent_id.toString(),
                                      keyspaceName,
                                      Joiner.on("','").join(cfnames),
                                      Joiner.on("','").join(ranges),
                                      toCQLMap(options.asMap(), RepairOption.RANGES_KEY, RepairOption.COLUMNFAMILIES_KEY));
        processSilent(fmtQry);
    }

    private static String toCQLMap(Map<String, String> options, String ... ignore)
    {
        Set<String> toIgnore = Sets.newHashSet(ignore);
        StringBuilder map = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : options.entrySet())
        {
            if (!toIgnore.contains(entry.getKey()))
            {
                if (!first)
                    map.append(',');
                first = false;
                map.append(format("'%s': '%s'", entry.getKey(), entry.getValue()));
            }
        }
        return map.toString();
    }

    public static void failParentRepair(TimeUUID parent_id, Throwable t)
    {
        String query = "UPDATE %s.%s SET finished_at = to_timestamp(now()), exception_message=?, exception_stacktrace=? WHERE parent_id=%s";

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        String fmtQuery = format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, PARENT_REPAIR_HISTORY, parent_id.toString());
        String message = t.getMessage();
        processSilent(fmtQuery, message != null ? message : "", sw.toString());
    }

    public static void successfulParentRepair(TimeUUID parent_id, Collection<Range<Token>> successfulRanges)
    {
        String query = "UPDATE %s.%s SET finished_at = to_timestamp(now()), successful_ranges = {'%s'} WHERE parent_id=%s";
        String fmtQuery = format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, PARENT_REPAIR_HISTORY, Joiner.on("','").join(successfulRanges), parent_id.toString());
        processSilent(fmtQuery);
    }

    public static void startRepairs(TimeUUID id, TimeUUID parent_id, String keyspaceName, String[] cfnames, CommonRange commonRange)
    {
        // Don't record repair history if an upgrade is in progress as version 3 nodes generates errors
        // due to schema differences
        boolean includeNewColumns = !Gossiper.instance.hasMajorVersion3Nodes();

        InetAddressAndPort coordinator = FBUtilities.getBroadcastAddressAndPort();
        Set<String> participants = Sets.newHashSet();
        Set<String> participants_v2 = Sets.newHashSet();

        for (InetAddressAndPort endpoint : commonRange.endpoints)
        {
            participants.add(endpoint.getHostAddress(false));
            participants_v2.add(endpoint.getHostAddressAndPort());
        }

        String query =
                "INSERT INTO %s.%s (keyspace_name, columnfamily_name, id, parent_id, range_begin, range_end, coordinator, coordinator_port, participants, participants_v2, status, started_at) " +
                        "VALUES (   '%s',          '%s',              %s, %s,        '%s',        '%s',      '%s',        %d,               { '%s' },     { '%s' },        '%s',   to_timestamp(now()))";
        String queryWithoutNewColumns =
                "INSERT INTO %s.%s (keyspace_name, columnfamily_name, id, parent_id, range_begin, range_end, coordinator, participants, status, started_at) " +
                        "VALUES (   '%s',          '%s',              %s, %s,        '%s',        '%s',      '%s',               { '%s' },        '%s',   to_timestamp(now()))";

        for (String cfname : cfnames)
        {
            for (Range<Token> range : commonRange.ranges)
            {
                String fmtQry;
                if (includeNewColumns)
                {
                    fmtQry = format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, REPAIR_HISTORY,
                                    keyspaceName,
                                    cfname,
                                    id.toString(),
                                    parent_id.toString(),
                                    range.left.toString(),
                                    range.right.toString(),
                                    coordinator.getHostAddress(false),
                                    coordinator.getPort(),
                                    Joiner.on("', '").join(participants),
                                    Joiner.on("', '").join(participants_v2),
                                    RepairState.STARTED.toString());
                }
                else
                {
                    fmtQry = format(queryWithoutNewColumns, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, REPAIR_HISTORY,
                                    keyspaceName,
                                    cfname,
                                    id.toString(),
                                    parent_id.toString(),
                                    range.left.toString(),
                                    range.right.toString(),
                                    coordinator.getHostAddress(false),
                                    Joiner.on("', '").join(participants),
                                    RepairState.STARTED.toString());
                }
                processSilent(fmtQry);
            }
        }
    }

    public static void failRepairs(TimeUUID id, String keyspaceName, String[] cfnames, Throwable t)
    {
        for (String cfname : cfnames)
            failedRepairJob(id, keyspaceName, cfname, t);
    }

    public static void successfulRepairJob(TimeUUID id, String keyspaceName, String cfname)
    {
        String query = "UPDATE %s.%s SET status = '%s', finished_at = to_timestamp(now()) WHERE keyspace_name = '%s' AND columnfamily_name = '%s' AND id = %s";
        String fmtQuery = format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, REPAIR_HISTORY,
                                        RepairState.SUCCESS.toString(),
                                        keyspaceName,
                                        cfname,
                                        id.toString());
        processSilent(fmtQuery);
    }

    public static void failedRepairJob(TimeUUID id, String keyspaceName, String cfname, Throwable t)
    {
        String query = "UPDATE %s.%s SET status = '%s', finished_at = to_timestamp(now()), exception_message=?, exception_stacktrace=? WHERE keyspace_name = '%s' AND columnfamily_name = '%s' AND id = %s";
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        String fmtQry = format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, REPAIR_HISTORY,
                                      RepairState.FAILED.toString(),
                                      keyspaceName,
                                      cfname,
                                      id.toString());
        String message = t.getMessage();
        if (message == null)
            message = t.getClass().getName();
        processSilent(fmtQry, message, sw.toString());
    }

    public static void startViewBuild(String keyspace, String view, UUID hostId)
    {
        String query = "INSERT INTO %s.%s (keyspace_name, view_name, host_id, status) VALUES (?, ?, ?, ?)";
        QueryProcessor.process(format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, VIEW_BUILD_STATUS),
                               ConsistencyLevel.ONE,
                               Lists.newArrayList(bytes(keyspace),
                                                  bytes(view),
                                                  bytes(hostId),
                                                  bytes(BuildStatus.STARTED.toString())));
    }

    public static void successfulViewBuild(String keyspace, String view, UUID hostId)
    {
        String query = "UPDATE %s.%s SET status = ? WHERE keyspace_name = ? AND view_name = ? AND host_id = ?";
        QueryProcessor.process(format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, VIEW_BUILD_STATUS),
                               ConsistencyLevel.ONE,
                               Lists.newArrayList(bytes(BuildStatus.SUCCESS.toString()),
                                                  bytes(keyspace),
                                                  bytes(view),
                                                  bytes(hostId)));
    }

    public static Map<UUID, String> viewStatus(String keyspace, String view)
    {
        String query = "SELECT host_id, status FROM %s.%s WHERE keyspace_name = ? AND view_name = ?";
        UntypedResultSet results;
        try
        {
            results = QueryProcessor.execute(format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, VIEW_BUILD_STATUS),
                                             ConsistencyLevel.ONE,
                                             keyspace,
                                             view);
        }
        catch (Exception e)
        {
            return Collections.emptyMap();
        }


        Map<UUID, String> status = new HashMap<>();
        for (UntypedResultSet.Row row : results)
        {
            status.put(row.getUUID("host_id"), row.getString("status"));
        }
        return status;
    }

    public static void setViewRemoved(String keyspaceName, String viewName)
    {
        String buildReq = "DELETE FROM %s.%s WHERE keyspace_name = ? AND view_name = ?";
        QueryProcessor.executeInternal(format(buildReq, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, VIEW_BUILD_STATUS), keyspaceName, viewName);
        forceBlockingFlush(VIEW_BUILD_STATUS, ColumnFamilyStore.FlushReason.INTERNALLY_FORCED);
    }

    private static void processSilent(String fmtQry, String... values)
    {
        try
        {
            List<ByteBuffer> valueList = new ArrayList<>(values.length);
            for (String v : values)
            {
                valueList.add(bytes(v));
            }
            QueryProcessor.process(fmtQry, ConsistencyLevel.ANY, valueList);
        }
        catch (Throwable t)
        {
            logger.error("Error executing query "+fmtQry, t);
        }
    }

    public static void forceBlockingFlush(String table, ColumnFamilyStore.FlushReason reason)
    {
        if (!DatabaseDescriptor.isUnsafeSystem())
            FBUtilities.waitOnFuture(Keyspace.open(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME)
                                             .getColumnFamilyStore(table)
                                             .forceFlush(reason));
    }

    private enum RepairState
    {
        STARTED, SUCCESS, FAILED
    }

    private enum BuildStatus
    {
        UNKNOWN, STARTED, SUCCESS
    }
}
