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
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public final class SystemDistributedKeyspace
{
    private SystemDistributedKeyspace()
    {
    }

    private static final Logger logger = LoggerFactory.getLogger(SystemDistributedKeyspace.class);

    public static final String NAME = "system_distributed";

    public static final String REPAIR_HISTORY = "repair_history";

    public static final String PARENT_REPAIR_HISTORY = "parent_repair_history";

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
                     + "participants set<inet>,"
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
                     + "PRIMARY KEY (parent_id))");

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), NAME)
                         .comment(description);
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(NAME, KeyspaceParams.simple(3), Tables.of(RepairHistory, ParentRepairHistory));
    }

    public static void startParentRepair(UUID parent_id, String keyspaceName, String[] cfnames, Collection<Range<Token>> ranges)
    {

        String query = "INSERT INTO %s.%s (parent_id, keyspace_name, columnfamily_names, requested_ranges, started_at)"+
                                 " VALUES (%s,        '%s',          { '%s' },           { '%s' },          toTimestamp(now()))";
        String fmtQry = String.format(query, NAME, PARENT_REPAIR_HISTORY, parent_id.toString(), keyspaceName, Joiner.on("','").join(cfnames), Joiner.on("','").join(ranges));
        processSilent(fmtQry);
    }

    public static void failParentRepair(UUID parent_id, Throwable t)
    {
        String query = "UPDATE %s.%s SET finished_at = toTimestamp(now()), exception_message=?, exception_stacktrace=? WHERE parent_id=%s";

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        String fmtQuery = String.format(query, NAME, PARENT_REPAIR_HISTORY, parent_id.toString());
        processSilent(fmtQuery, t.getMessage(), sw.toString());
    }

    public static void successfulParentRepair(UUID parent_id, Collection<Range<Token>> successfulRanges)
    {
        String query = "UPDATE %s.%s SET finished_at = toTimestamp(now()), successful_ranges = {'%s'} WHERE parent_id=%s";
        String fmtQuery = String.format(query, NAME, PARENT_REPAIR_HISTORY, Joiner.on("','").join(successfulRanges), parent_id.toString());
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
                String fmtQry = String.format(query, NAME, REPAIR_HISTORY,
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
        String fmtQuery = String.format(query, NAME, REPAIR_HISTORY,
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
        String fmtQry = String.format(query, NAME, REPAIR_HISTORY,
                RepairState.FAILED.toString(),
                keyspaceName,
                cfname,
                id.toString());
        processSilent(fmtQry, t.getMessage(), sw.toString());
    }

    private static void processSilent(String fmtQry, String... values)
    {
        try
        {
            List<ByteBuffer> valueList = new ArrayList<>();
            for (String v : values)
            {
                valueList.add(ByteBufferUtil.bytes(v));
            }
            QueryProcessor.process(fmtQry, ConsistencyLevel.ONE, valueList);
        }
        catch (Throwable t)
        {
            logger.error("Error executing query "+fmtQry, t);
        }
    }


    private enum RepairState
    {
        STARTED, SUCCESS, FAILED
    }
}
