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
package org.apache.cassandra.repair;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public final class SystemDistributedKeyspace
{
    private SystemDistributedKeyspace()
    {
    }

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
     */
    public static final long GENERATION = 3;

    public static final String REPAIR_HISTORY = "repair_history";

    public static final String PARENT_REPAIR_HISTORY = "parent_repair_history";

    public static final String VIEW_BUILD_STATUS = "view_build_status";

    private static final CFMetaData RepairHistory =
        compile(REPAIR_HISTORY,
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
                     + "PRIMARY KEY ((keyspace_name, columnfamily_name), id))");

    private static final CFMetaData ParentRepairHistory =
        compile(PARENT_REPAIR_HISTORY,
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
                     + "PRIMARY KEY (parent_id))");

    private static final CFMetaData ViewBuildStatus =
    compile(VIEW_BUILD_STATUS,
            "Materialized View build status",
            "CREATE TABLE %s ("
                     + "keyspace_name text,"
                     + "view_name text,"
                     + "host_id uuid,"
                     + "status text,"
                     + "PRIMARY KEY ((keyspace_name, view_name), host_id))");

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), SchemaConstants.DISTRIBUTED_KEYSPACE_NAME)
                         .comment(description)
                         .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(10));
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, KeyspaceParams.simple(3), Tables.of(RepairHistory, ParentRepairHistory, ViewBuildStatus));
    }

    public static void startParentRepair(UUID parent_id, String keyspaceName, String[] cfnames, RepairOption options)
    {
        Collection<Range<Token>> ranges = options.getRanges();
        String query = "INSERT INTO %s.%s (parent_id, keyspace_name, columnfamily_names, requested_ranges, started_at,          options)"+
                                 " VALUES (%s,        '%s',          { '%s' },           { '%s' },          toTimestamp(now()), { %s })";
        String fmtQry = String.format(query,
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
                map.append(String.format("'%s': '%s'", entry.getKey(), entry.getValue()));
            }
        }
        return map.toString();
    }

    public static void failParentRepair(UUID parent_id, Throwable t)
    {
        String query = "UPDATE %s.%s SET finished_at = toTimestamp(now()), exception_message=?, exception_stacktrace=? WHERE parent_id=%s";

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        String fmtQuery = String.format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, PARENT_REPAIR_HISTORY, parent_id.toString());
        processSilent(fmtQuery, t.getMessage(), sw.toString());
    }

    public static void successfulParentRepair(UUID parent_id, Collection<Range<Token>> successfulRanges)
    {
        String query = "UPDATE %s.%s SET finished_at = toTimestamp(now()), successful_ranges = {'%s'} WHERE parent_id=%s";
        String fmtQuery = String.format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, PARENT_REPAIR_HISTORY, Joiner.on("','").join(successfulRanges), parent_id.toString());
        processSilent(fmtQuery);
    }

    public static void startRepairs(UUID id, UUID parent_id, String keyspaceName, String[] cfnames, Collection<Range<Token>> ranges, Iterable<InetAddress> endpoints)
    {
        String coordinator = FBUtilities.getBroadcastAddress().getHostAddress();
        Set<String> participants = Sets.newHashSet(coordinator);

        for (InetAddress endpoint : endpoints)
            participants.add(endpoint.getHostAddress());

        String query =
                "INSERT INTO %s.%s (keyspace_name, columnfamily_name, id, parent_id, range_begin, range_end, coordinator, participants, status, started_at) " +
                        "VALUES (   '%s',          '%s',              %s, %s,        '%s',        '%s',      '%s',        { '%s' },     '%s',   toTimestamp(now()))";

        for (String cfname : cfnames)
        {
            for (Range<Token> range : ranges)
            {
                String fmtQry = String.format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, REPAIR_HISTORY,
                                              keyspaceName,
                                              cfname,
                                              id.toString(),
                                              parent_id.toString(),
                                              range.left.toString(),
                                              range.right.toString(),
                                              coordinator,
                                              Joiner.on("', '").join(participants),
                                              RepairState.STARTED.toString());
                processSilent(fmtQry);
            }
        }
    }

    public static void failRepairs(UUID id, String keyspaceName, String[] cfnames, Throwable t)
    {
        for (String cfname : cfnames)
            failedRepairJob(id, keyspaceName, cfname, t);
    }

    public static void successfulRepairJob(UUID id, String keyspaceName, String cfname)
    {
        String query = "UPDATE %s.%s SET status = '%s', finished_at = toTimestamp(now()) WHERE keyspace_name = '%s' AND columnfamily_name = '%s' AND id = %s";
        String fmtQuery = String.format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, REPAIR_HISTORY,
                                        RepairState.SUCCESS.toString(),
                                        keyspaceName,
                                        cfname,
                                        id.toString());
        processSilent(fmtQuery);
    }

    public static void failedRepairJob(UUID id, String keyspaceName, String cfname, Throwable t)
    {
        String query = "UPDATE %s.%s SET status = '%s', finished_at = toTimestamp(now()), exception_message=?, exception_stacktrace=? WHERE keyspace_name = '%s' AND columnfamily_name = '%s' AND id = %s";
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        String fmtQry = String.format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, REPAIR_HISTORY,
                                      RepairState.FAILED.toString(),
                                      keyspaceName,
                                      cfname,
                                      id.toString());
        processSilent(fmtQry, t.getMessage(), sw.toString());
    }

    public static void startViewBuild(String keyspace, String view, UUID hostId)
    {
        String query = "INSERT INTO %s.%s (keyspace_name, view_name, host_id, status) VALUES (?, ?, ?, ?)";
        QueryProcessor.process(String.format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, VIEW_BUILD_STATUS),
                               ConsistencyLevel.ONE,
                               Lists.newArrayList(bytes(keyspace),
                                                  bytes(view),
                                                  bytes(hostId),
                                                  bytes(BuildStatus.STARTED.toString())));
    }

    public static void successfulViewBuild(String keyspace, String view, UUID hostId)
    {
        String query = "UPDATE %s.%s SET status = ? WHERE keyspace_name = ? AND view_name = ? AND host_id = ?";
        QueryProcessor.process(String.format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, VIEW_BUILD_STATUS),
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
            results = QueryProcessor.execute(String.format(query, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, VIEW_BUILD_STATUS),
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
        QueryProcessor.executeInternal(String.format(buildReq, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, VIEW_BUILD_STATUS), keyspaceName, viewName);
        forceBlockingFlush(VIEW_BUILD_STATUS);
    }

    private static void processSilent(String fmtQry, String... values)
    {
        try
        {
            List<ByteBuffer> valueList = new ArrayList<>();
            for (String v : values)
            {
                valueList.add(bytes(v));
            }
            QueryProcessor.process(fmtQry, ConsistencyLevel.ONE, valueList);
        }
        catch (Throwable t)
        {
            logger.error("Error executing query "+fmtQry, t);
        }
    }

    public static void forceBlockingFlush(String table)
    {
        if (!DatabaseDescriptor.isUnsafeSystem())
            FBUtilities.waitOnFuture(Keyspace.open(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME).getColumnFamilyStore(table).forceFlush());
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
