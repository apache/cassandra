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

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import java.util.concurrent.Future;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Futures;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.CompactionHistoryTabularData;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.thrift.cassandraConstants;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.*;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.io.util.FileUtils.visitDirectory;

public final class SystemKeyspace
{
    private SystemKeyspace()
    {
    }

    private static final Logger logger = LoggerFactory.getLogger(SystemKeyspace.class);

    // Used to indicate that there was a previous version written to the legacy (pre 1.2)
    // system.Versions table, but that we cannot read it. Suffice to say, any upgrade should
    // proceed through 1.2.x before upgrading to the current version.
    public static final CassandraVersion UNREADABLE_VERSION = new CassandraVersion("0.0.0-unknown");

    // Used to indicate that no previous version information was found. When encountered, we assume that
    // Cassandra was not previously installed and we're in the process of starting a fresh node.
    public static final CassandraVersion NULL_VERSION = new CassandraVersion("0.0.0-absent");

    public static final String BATCHES = "batches";
    public static final String PAXOS = "paxos";
    public static final String BUILT_INDEXES = "IndexInfo";
    public static final String LOCAL = "local";
    public static final String PEERS = "peers";
    public static final String PEER_EVENTS = "peer_events";
    public static final String RANGE_XFERS = "range_xfers";
    public static final String COMPACTION_HISTORY = "compaction_history";
    public static final String SSTABLE_ACTIVITY = "sstable_activity";
    public static final String SIZE_ESTIMATES = "size_estimates";
    public static final String AVAILABLE_RANGES = "available_ranges";
    public static final String TRANSFERRED_RANGES = "transferred_ranges";
    public static final String VIEWS_BUILDS_IN_PROGRESS = "views_builds_in_progress";
    public static final String BUILT_VIEWS = "built_views";
    public static final String PREPARED_STATEMENTS = "prepared_statements";

    @Deprecated public static final String LEGACY_HINTS = "hints";
    @Deprecated public static final String LEGACY_BATCHLOG = "batchlog";
    @Deprecated public static final String LEGACY_KEYSPACES = "schema_keyspaces";
    @Deprecated public static final String LEGACY_COLUMNFAMILIES = "schema_columnfamilies";
    @Deprecated public static final String LEGACY_COLUMNS = "schema_columns";
    @Deprecated public static final String LEGACY_TRIGGERS = "schema_triggers";
    @Deprecated public static final String LEGACY_USERTYPES = "schema_usertypes";
    @Deprecated public static final String LEGACY_FUNCTIONS = "schema_functions";
    @Deprecated public static final String LEGACY_AGGREGATES = "schema_aggregates";

    public static final CFMetaData Batches =
        compile(BATCHES,
                "batches awaiting replay",
                "CREATE TABLE %s ("
                + "id timeuuid,"
                + "mutations list<blob>,"
                + "version int,"
                + "PRIMARY KEY ((id)))")
                .copy(new LocalPartitioner(TimeUUIDType.instance))
                .compaction(CompactionParams.scts(singletonMap("min_threshold", "2")))
                .gcGraceSeconds(0);

    private static final CFMetaData Paxos =
        compile(PAXOS,
                "in-progress paxos proposals",
                "CREATE TABLE %s ("
                + "row_key blob,"
                + "cf_id UUID,"
                + "in_progress_ballot timeuuid,"
                + "most_recent_commit blob,"
                + "most_recent_commit_at timeuuid,"
                + "most_recent_commit_version int,"
                + "proposal blob,"
                + "proposal_ballot timeuuid,"
                + "proposal_version int,"
                + "PRIMARY KEY ((row_key), cf_id))")
                .compaction(CompactionParams.lcs(emptyMap()));

    private static final CFMetaData BuiltIndexes =
        compile(BUILT_INDEXES,
                "built column indexes",
                "CREATE TABLE \"%s\" ("
                + "table_name text," // table_name here is the name of the keyspace - don't be fooled
                + "index_name text,"
                + "PRIMARY KEY ((table_name), index_name)) "
                + "WITH COMPACT STORAGE");

    private static final CFMetaData Local =
        compile(LOCAL,
                "information about the local node",
                "CREATE TABLE %s ("
                + "key text,"
                + "bootstrapped text,"
                + "broadcast_address inet,"
                + "cluster_name text,"
                + "cql_version text,"
                + "data_center text,"
                + "gossip_generation int,"
                + "host_id uuid,"
                + "listen_address inet,"
                + "native_protocol_version text,"
                + "partitioner text,"
                + "rack text,"
                + "release_version text,"
                + "rpc_address inet,"
                + "schema_version uuid,"
                + "thrift_version text,"
                + "tokens set<varchar>,"
                + "truncated_at map<uuid, blob>,"
                + "PRIMARY KEY ((key)))");

    private static final CFMetaData Peers =
        compile(PEERS,
                "information about known peers in the cluster",
                "CREATE TABLE %s ("
                + "peer inet,"
                + "data_center text,"
                + "host_id uuid,"
                + "preferred_ip inet,"
                + "rack text,"
                + "release_version text,"
                + "rpc_address inet,"
                + "schema_version uuid,"
                + "tokens set<varchar>,"
                + "PRIMARY KEY ((peer)))");

    private static final CFMetaData PeerEvents =
        compile(PEER_EVENTS,
                "events related to peers",
                "CREATE TABLE %s ("
                + "peer inet,"
                + "hints_dropped map<uuid, int>,"
                + "PRIMARY KEY ((peer)))");

    private static final CFMetaData RangeXfers =
        compile(RANGE_XFERS,
                "ranges requested for transfer",
                "CREATE TABLE %s ("
                + "token_bytes blob,"
                + "requested_at timestamp,"
                + "PRIMARY KEY ((token_bytes)))");

    private static final CFMetaData CompactionHistory =
        compile(COMPACTION_HISTORY,
                "week-long compaction history",
                "CREATE TABLE %s ("
                + "id uuid,"
                + "bytes_in bigint,"
                + "bytes_out bigint,"
                + "columnfamily_name text,"
                + "compacted_at timestamp,"
                + "keyspace_name text,"
                + "rows_merged map<int, bigint>,"
                + "PRIMARY KEY ((id)))")
                .defaultTimeToLive((int) TimeUnit.DAYS.toSeconds(7));

    private static final CFMetaData SSTableActivity =
        compile(SSTABLE_ACTIVITY,
                "historic sstable read rates",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "columnfamily_name text,"
                + "generation int,"
                + "rate_120m double,"
                + "rate_15m double,"
                + "PRIMARY KEY ((keyspace_name, columnfamily_name, generation)))");

    private static final CFMetaData SizeEstimates =
        compile(SIZE_ESTIMATES,
                "per-table primary range size estimates",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "table_name text,"
                + "range_start text,"
                + "range_end text,"
                + "mean_partition_size bigint,"
                + "partitions_count bigint,"
                + "PRIMARY KEY ((keyspace_name), table_name, range_start, range_end))")
                .gcGraceSeconds(0);

    private static final CFMetaData AvailableRanges =
        compile(AVAILABLE_RANGES,
                "available keyspace/ranges during bootstrap/replace that are ready to be served",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "ranges set<blob>,"
                + "PRIMARY KEY ((keyspace_name)))");

    private static final CFMetaData TransferredRanges =
        compile(TRANSFERRED_RANGES,
                "record of transferred ranges for streaming operation",
                "CREATE TABLE %s ("
                + "operation text,"
                + "peer inet,"
                + "keyspace_name text,"
                + "ranges set<blob>,"
                + "PRIMARY KEY ((operation, keyspace_name), peer))");

    private static final CFMetaData ViewsBuildsInProgress =
        compile(VIEWS_BUILDS_IN_PROGRESS,
                "views builds current progress",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "view_name text,"
                + "last_token varchar,"
                + "generation_number int,"
                + "PRIMARY KEY ((keyspace_name), view_name))");

    private static final CFMetaData BuiltViews =
        compile(BUILT_VIEWS,
                "built views",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "view_name text,"
                + "status_replicated boolean,"
                + "PRIMARY KEY ((keyspace_name), view_name))");

    private static final CFMetaData PreparedStatements =
        compile(PREPARED_STATEMENTS,
                "prepared statements",
                "CREATE TABLE %s ("
                + "prepared_id blob,"
                + "logged_keyspace text,"
                + "query_string text,"
                + "PRIMARY KEY ((prepared_id)))");

    @Deprecated
    public static final CFMetaData LegacyHints =
        compile(LEGACY_HINTS,
                "*DEPRECATED* hints awaiting delivery",
                "CREATE TABLE %s ("
                + "target_id uuid,"
                + "hint_id timeuuid,"
                + "message_version int,"
                + "mutation blob,"
                + "PRIMARY KEY ((target_id), hint_id, message_version)) "
                + "WITH COMPACT STORAGE")
                .compaction(CompactionParams.scts(singletonMap("enabled", "false")))
                .gcGraceSeconds(0);

    @Deprecated
    public static final CFMetaData LegacyBatchlog =
        compile(LEGACY_BATCHLOG,
                "*DEPRECATED* batchlog entries",
                "CREATE TABLE %s ("
                + "id uuid,"
                + "data blob,"
                + "version int,"
                + "written_at timestamp,"
                + "PRIMARY KEY ((id)))")
                .compaction(CompactionParams.scts(singletonMap("min_threshold", "2")))
                .gcGraceSeconds(0);

    @Deprecated
    public static final CFMetaData LegacyKeyspaces =
        compile(LEGACY_KEYSPACES,
                "*DEPRECATED* keyspace definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "durable_writes boolean,"
                + "strategy_class text,"
                + "strategy_options text,"
                + "PRIMARY KEY ((keyspace_name))) "
                + "WITH COMPACT STORAGE");

    @Deprecated
    public static final CFMetaData LegacyColumnfamilies =
        compile(LEGACY_COLUMNFAMILIES,
                "*DEPRECATED* table definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "columnfamily_name text,"
                + "bloom_filter_fp_chance double,"
                + "caching text,"
                + "cf_id uuid," // post-2.1 UUID cfid
                + "comment text,"
                + "compaction_strategy_class text,"
                + "compaction_strategy_options text,"
                + "comparator text,"
                + "compression_parameters text,"
                + "default_time_to_live int,"
                + "default_validator text,"
                + "dropped_columns map<text, bigint>,"
                + "gc_grace_seconds int,"
                + "is_dense boolean,"
                + "key_validator text,"
                + "local_read_repair_chance double,"
                + "max_compaction_threshold int,"
                + "max_index_interval int,"
                + "memtable_flush_period_in_ms int,"
                + "min_compaction_threshold int,"
                + "min_index_interval int,"
                + "read_repair_chance double,"
                + "speculative_retry text,"
                + "subcomparator text,"
                + "type text,"
                + "PRIMARY KEY ((keyspace_name), columnfamily_name))");

    @Deprecated
    public static final CFMetaData LegacyColumns =
        compile(LEGACY_COLUMNS,
                "*DEPRECATED* column definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "columnfamily_name text,"
                + "column_name text,"
                + "component_index int,"
                + "index_name text,"
                + "index_options text,"
                + "index_type text,"
                + "type text,"
                + "validator text,"
                + "PRIMARY KEY ((keyspace_name), columnfamily_name, column_name))");

    @Deprecated
    public static final CFMetaData LegacyTriggers =
        compile(LEGACY_TRIGGERS,
                "*DEPRECATED* trigger definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "columnfamily_name text,"
                + "trigger_name text,"
                + "trigger_options map<text, text>,"
                + "PRIMARY KEY ((keyspace_name), columnfamily_name, trigger_name))");

    @Deprecated
    public static final CFMetaData LegacyUsertypes =
        compile(LEGACY_USERTYPES,
                "*DEPRECATED* user defined type definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "type_name text,"
                + "field_names list<text>,"
                + "field_types list<text>,"
                + "PRIMARY KEY ((keyspace_name), type_name))");

    @Deprecated
    public static final CFMetaData LegacyFunctions =
        compile(LEGACY_FUNCTIONS,
                "*DEPRECATED* user defined function definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "function_name text,"
                + "signature frozen<list<text>>,"
                + "argument_names list<text>,"
                + "argument_types list<text>,"
                + "body text,"
                + "language text,"
                + "return_type text,"
                + "called_on_null_input boolean,"
                + "PRIMARY KEY ((keyspace_name), function_name, signature))");

    @Deprecated
    public static final CFMetaData LegacyAggregates =
        compile(LEGACY_AGGREGATES,
                "*DEPRECATED* user defined aggregate definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "aggregate_name text,"
                + "signature frozen<list<text>>,"
                + "argument_types list<text>,"
                + "final_func text,"
                + "initcond blob,"
                + "return_type text,"
                + "state_func text,"
                + "state_type text,"
                + "PRIMARY KEY ((keyspace_name), aggregate_name, signature))");

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), SchemaConstants.SYSTEM_KEYSPACE_NAME)
                         .comment(description);
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(SchemaConstants.SYSTEM_KEYSPACE_NAME, KeyspaceParams.local(), tables(), Views.none(), Types.none(), functions());
    }

    private static Tables tables()
    {
        return Tables.of(BuiltIndexes,
                         Batches,
                         Paxos,
                         Local,
                         Peers,
                         PeerEvents,
                         RangeXfers,
                         CompactionHistory,
                         SSTableActivity,
                         SizeEstimates,
                         AvailableRanges,
                         TransferredRanges,
                         ViewsBuildsInProgress,
                         BuiltViews,
                         LegacyHints,
                         LegacyBatchlog,
                         PreparedStatements,
                         LegacyKeyspaces,
                         LegacyColumnfamilies,
                         LegacyColumns,
                         LegacyTriggers,
                         LegacyUsertypes,
                         LegacyFunctions,
                         LegacyAggregates);
    }

    private static Functions functions()
    {
        return Functions.builder()
                        .add(UuidFcts.all())
                        .add(TimeFcts.all())
                        .add(BytesConversionFcts.all())
                        .add(AggregateFcts.all())
                        .add(CastFcts.all())
                        .build();
    }

    private static volatile Map<UUID, Pair<CommitLogPosition, Long>> truncationRecords;

    public enum BootstrapState
    {
        NEEDS_BOOTSTRAP,
        COMPLETED,
        IN_PROGRESS,
        DECOMMISSIONED
    }

    public static void finishStartup()
    {
        SchemaKeyspace.saveSystemKeyspacesSchema();
    }

    public static void persistLocalMetadata()
    {
        String req = "INSERT INTO system.%s (" +
                     "key," +
                     "cluster_name," +
                     "release_version," +
                     "cql_version," +
                     "thrift_version," +
                     "native_protocol_version," +
                     "data_center," +
                     "rack," +
                     "partitioner," +
                     "rpc_address," +
                     "broadcast_address," +
                     "listen_address" +
                     ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        executeOnceInternal(String.format(req, LOCAL),
                            LOCAL,
                            DatabaseDescriptor.getClusterName(),
                            FBUtilities.getReleaseVersionString(),
                            QueryProcessor.CQL_VERSION.toString(),
                            cassandraConstants.VERSION,
                            String.valueOf(ProtocolVersion.CURRENT.asInt()),
                            snitch.getDatacenter(FBUtilities.getBroadcastAddress()),
                            snitch.getRack(FBUtilities.getBroadcastAddress()),
                            DatabaseDescriptor.getPartitioner().getClass().getName(),
                            DatabaseDescriptor.getRpcAddress(),
                            FBUtilities.getBroadcastAddress(),
                            FBUtilities.getLocalAddress());
    }

    public static void updateCompactionHistory(String ksname,
                                               String cfname,
                                               long compactedAt,
                                               long bytesIn,
                                               long bytesOut,
                                               Map<Integer, Long> rowsMerged)
    {
        // don't write anything when the history table itself is compacted, since that would in turn cause new compactions
        if (ksname.equals("system") && cfname.equals(COMPACTION_HISTORY))
            return;
        String req = "INSERT INTO system.%s (id, keyspace_name, columnfamily_name, compacted_at, bytes_in, bytes_out, rows_merged) VALUES (?, ?, ?, ?, ?, ?, ?)";
        executeInternal(String.format(req, COMPACTION_HISTORY),
                        UUIDGen.getTimeUUID(),
                        ksname,
                        cfname,
                        ByteBufferUtil.bytes(compactedAt),
                        bytesIn,
                        bytesOut,
                        rowsMerged);
    }

    public static TabularData getCompactionHistory() throws OpenDataException
    {
        UntypedResultSet queryResultSet = executeInternal(String.format("SELECT * from system.%s", COMPACTION_HISTORY));
        return CompactionHistoryTabularData.from(queryResultSet);
    }

    public static boolean isViewBuilt(String keyspaceName, String viewName)
    {
        String req = "SELECT view_name FROM %s.\"%s\" WHERE keyspace_name=? AND view_name=?";
        UntypedResultSet result = executeInternal(String.format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_VIEWS), keyspaceName, viewName);
        return !result.isEmpty();
    }

    public static boolean isViewStatusReplicated(String keyspaceName, String viewName)
    {
        String req = "SELECT status_replicated FROM %s.\"%s\" WHERE keyspace_name=? AND view_name=?";
        UntypedResultSet result = executeInternal(String.format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_VIEWS), keyspaceName, viewName);

        if (result.isEmpty())
            return false;
        UntypedResultSet.Row row = result.one();
        return row.has("status_replicated") && row.getBoolean("status_replicated");
    }

    public static void setViewBuilt(String keyspaceName, String viewName, boolean replicated)
    {
        String req = "INSERT INTO %s.\"%s\" (keyspace_name, view_name, status_replicated) VALUES (?, ?, ?)";
        executeInternal(String.format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_VIEWS), keyspaceName, viewName, replicated);
        forceBlockingFlush(BUILT_VIEWS);
    }

    public static void setViewRemoved(String keyspaceName, String viewName)
    {
        String buildReq = "DELETE FROM %S.%s WHERE keyspace_name = ? AND view_name = ?";
        executeInternal(String.format(buildReq, SchemaConstants.SYSTEM_KEYSPACE_NAME, VIEWS_BUILDS_IN_PROGRESS), keyspaceName, viewName);
        forceBlockingFlush(VIEWS_BUILDS_IN_PROGRESS);

        String builtReq = "DELETE FROM %s.\"%s\" WHERE keyspace_name = ? AND view_name = ?";
        executeInternal(String.format(builtReq, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_VIEWS), keyspaceName, viewName);
        forceBlockingFlush(BUILT_VIEWS);
    }

    public static void beginViewBuild(String ksname, String viewName, int generationNumber)
    {
        executeInternal(String.format("INSERT INTO system.%s (keyspace_name, view_name, generation_number) VALUES (?, ?, ?)", VIEWS_BUILDS_IN_PROGRESS),
                        ksname,
                        viewName,
                        generationNumber);
    }

    public static void finishViewBuildStatus(String ksname, String viewName)
    {
        // We flush the view built first, because if we fail now, we'll restart at the last place we checkpointed
        // view build.
        // If we flush the delete first, we'll have to restart from the beginning.
        // Also, if writing to the built_view succeeds, but the view_builds_in_progress deletion fails, we will be able
        // to skip the view build next boot.
        setViewBuilt(ksname, viewName, false);
        executeInternal(String.format("DELETE FROM system.%s WHERE keyspace_name = ? AND view_name = ?", VIEWS_BUILDS_IN_PROGRESS), ksname, viewName);
        forceBlockingFlush(VIEWS_BUILDS_IN_PROGRESS);
    }

    public static void setViewBuiltReplicated(String ksname, String viewName)
    {
        setViewBuilt(ksname, viewName, true);
    }

    public static void updateViewBuildStatus(String ksname, String viewName, Token token)
    {
        String req = "INSERT INTO system.%s (keyspace_name, view_name, last_token) VALUES (?, ?, ?)";
        Token.TokenFactory factory = ViewsBuildsInProgress.partitioner.getTokenFactory();
        executeInternal(String.format(req, VIEWS_BUILDS_IN_PROGRESS), ksname, viewName, factory.toString(token));
    }

    public static Pair<Integer, Token> getViewBuildStatus(String ksname, String viewName)
    {
        String req = "SELECT generation_number, last_token FROM system.%s WHERE keyspace_name = ? AND view_name = ?";
        UntypedResultSet queryResultSet = executeInternal(String.format(req, VIEWS_BUILDS_IN_PROGRESS), ksname, viewName);
        if (queryResultSet == null || queryResultSet.isEmpty())
            return null;

        UntypedResultSet.Row row = queryResultSet.one();

        Integer generation = null;
        Token lastKey = null;
        if (row.has("generation_number"))
            generation = row.getInt("generation_number");
        if (row.has("last_key"))
        {
            Token.TokenFactory factory = ViewsBuildsInProgress.partitioner.getTokenFactory();
            lastKey = factory.fromString(row.getString("last_key"));
        }

        return Pair.create(generation, lastKey);
    }

    public static synchronized void saveTruncationRecord(ColumnFamilyStore cfs, long truncatedAt, CommitLogPosition position)
    {
        String req = "UPDATE system.%s SET truncated_at = truncated_at + ? WHERE key = '%s'";
        executeInternal(String.format(req, LOCAL, LOCAL), truncationAsMapEntry(cfs, truncatedAt, position));
        truncationRecords = null;
        forceBlockingFlush(LOCAL);
    }

    /**
     * This method is used to remove information about truncation time for specified column family
     */
    public static synchronized void removeTruncationRecord(UUID cfId)
    {
        String req = "DELETE truncated_at[?] from system.%s WHERE key = '%s'";
        executeInternal(String.format(req, LOCAL, LOCAL), cfId);
        truncationRecords = null;
        forceBlockingFlush(LOCAL);
    }

    private static Map<UUID, ByteBuffer> truncationAsMapEntry(ColumnFamilyStore cfs, long truncatedAt, CommitLogPosition position)
    {
        try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
        {
            CommitLogPosition.serializer.serialize(position, out);
            out.writeLong(truncatedAt);
            return singletonMap(cfs.metadata.cfId, out.asNewBuffer());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static CommitLogPosition getTruncatedPosition(UUID cfId)
    {
        Pair<CommitLogPosition, Long> record = getTruncationRecord(cfId);
        return record == null ? null : record.left;
    }

    public static long getTruncatedAt(UUID cfId)
    {
        Pair<CommitLogPosition, Long> record = getTruncationRecord(cfId);
        return record == null ? Long.MIN_VALUE : record.right;
    }

    private static synchronized Pair<CommitLogPosition, Long> getTruncationRecord(UUID cfId)
    {
        if (truncationRecords == null)
            truncationRecords = readTruncationRecords();
        return truncationRecords.get(cfId);
    }

    private static Map<UUID, Pair<CommitLogPosition, Long>> readTruncationRecords()
    {
        UntypedResultSet rows = executeInternal(String.format("SELECT truncated_at FROM system.%s WHERE key = '%s'", LOCAL, LOCAL));

        Map<UUID, Pair<CommitLogPosition, Long>> records = new HashMap<>();

        if (!rows.isEmpty() && rows.one().has("truncated_at"))
        {
            Map<UUID, ByteBuffer> map = rows.one().getMap("truncated_at", UUIDType.instance, BytesType.instance);
            for (Map.Entry<UUID, ByteBuffer> entry : map.entrySet())
                records.put(entry.getKey(), truncationRecordFromBlob(entry.getValue()));
        }

        return records;
    }

    private static Pair<CommitLogPosition, Long> truncationRecordFromBlob(ByteBuffer bytes)
    {
        try (RebufferingInputStream in = new DataInputBuffer(bytes, true))
        {
            return Pair.create(CommitLogPosition.serializer.deserialize(in), in.available() > 0 ? in.readLong() : Long.MIN_VALUE);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Record tokens being used by another node
     */
    public static Future<?> updateTokens(final InetAddress ep, final Collection<Token> tokens, ExecutorService executorService)
    {
        if (ep.equals(FBUtilities.getBroadcastAddress()))
            return Futures.immediateFuture(null);

        String req = "INSERT INTO system.%s (peer, tokens) VALUES (?, ?)";
        return executorService.submit((Runnable) () -> executeInternal(String.format(req, PEERS), ep, tokensAsSet(tokens)));
    }

    public static void updatePreferredIP(InetAddress ep, InetAddress preferred_ip)
    {
        String req = "INSERT INTO system.%s (peer, preferred_ip) VALUES (?, ?)";
        executeInternal(String.format(req, PEERS), ep, preferred_ip);
        forceBlockingFlush(PEERS);
    }

    public static Future<?> updatePeerInfo(final InetAddress ep, final String columnName, final Object value, ExecutorService executorService)
    {
        if (ep.equals(FBUtilities.getBroadcastAddress()))
            return Futures.immediateFuture(null);

        String req = "INSERT INTO system.%s (peer, %s) VALUES (?, ?)";
        return executorService.submit((Runnable) () -> executeInternal(String.format(req, PEERS, columnName), ep, value));
    }

    public static synchronized void updateHintsDropped(InetAddress ep, UUID timePeriod, int value)
    {
        // with 30 day TTL
        String req = "UPDATE system.%s USING TTL 2592000 SET hints_dropped[ ? ] = ? WHERE peer = ?";
        executeInternal(String.format(req, PEER_EVENTS), timePeriod, value, ep);
    }

    public static synchronized void updateSchemaVersion(UUID version)
    {
        String req = "INSERT INTO system.%s (key, schema_version) VALUES ('%s', ?)";
        executeInternal(String.format(req, LOCAL, LOCAL), version);
    }

    private static Set<String> tokensAsSet(Collection<Token> tokens)
    {
        if (tokens.isEmpty())
            return Collections.emptySet();
        Token.TokenFactory factory = StorageService.instance.getTokenFactory();
        Set<String> s = new HashSet<>(tokens.size());
        for (Token tk : tokens)
            s.add(factory.toString(tk));
        return s;
    }

    private static Collection<Token> deserializeTokens(Collection<String> tokensStrings)
    {
        Token.TokenFactory factory = StorageService.instance.getTokenFactory();
        List<Token> tokens = new ArrayList<>(tokensStrings.size());
        for (String tk : tokensStrings)
            tokens.add(factory.fromString(tk));
        return tokens;
    }

    /**
     * Remove stored tokens being used by another node
     */
    public static void removeEndpoint(InetAddress ep)
    {
        String req = "DELETE FROM system.%s WHERE peer = ?";
        executeInternal(String.format(req, PEERS), ep);
        forceBlockingFlush(PEERS);
    }

    /**
     * This method is used to update the System Keyspace with the new tokens for this node
    */
    public static synchronized void updateTokens(Collection<Token> tokens)
    {
        assert !tokens.isEmpty() : "removeEndpoint should be used instead";
        String req = "INSERT INTO system.%s (key, tokens) VALUES ('%s', ?)";
        executeInternal(String.format(req, LOCAL, LOCAL), tokensAsSet(tokens));
        forceBlockingFlush(LOCAL);
    }

    public static void forceBlockingFlush(String cfname)
    {
        if (!DatabaseDescriptor.isUnsafeSystem())
            FBUtilities.waitOnFuture(Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(cfname).forceFlush());
    }

    /**
     * Return a map of stored tokens to IP addresses
     *
     */
    public static SetMultimap<InetAddress, Token> loadTokens()
    {
        SetMultimap<InetAddress, Token> tokenMap = HashMultimap.create();
        for (UntypedResultSet.Row row : executeInternal("SELECT peer, tokens FROM system." + PEERS))
        {
            InetAddress peer = row.getInetAddress("peer");
            if (row.has("tokens"))
                tokenMap.putAll(peer, deserializeTokens(row.getSet("tokens", UTF8Type.instance)));
        }

        return tokenMap;
    }

    /**
     * Return a map of store host_ids to IP addresses
     *
     */
    public static Map<InetAddress, UUID> loadHostIds()
    {
        Map<InetAddress, UUID> hostIdMap = new HashMap<>();
        for (UntypedResultSet.Row row : executeInternal("SELECT peer, host_id FROM system." + PEERS))
        {
            InetAddress peer = row.getInetAddress("peer");
            if (row.has("host_id"))
            {
                hostIdMap.put(peer, row.getUUID("host_id"));
            }
        }
        return hostIdMap;
    }

    /**
     * Get preferred IP for given endpoint if it is known. Otherwise this returns given endpoint itself.
     *
     * @param ep endpoint address to check
     * @return Preferred IP for given endpoint if present, otherwise returns given ep
     */
    public static InetAddress getPreferredIP(InetAddress ep)
    {
        String req = "SELECT preferred_ip FROM system.%s WHERE peer=?";
        UntypedResultSet result = executeInternal(String.format(req, PEERS), ep);
        if (!result.isEmpty() && result.one().has("preferred_ip"))
            return result.one().getInetAddress("preferred_ip");
        return ep;
    }

    /**
     * Return a map of IP addresses containing a map of dc and rack info
     */
    public static Map<InetAddress, Map<String,String>> loadDcRackInfo()
    {
        Map<InetAddress, Map<String, String>> result = new HashMap<>();
        for (UntypedResultSet.Row row : executeInternal("SELECT peer, data_center, rack from system." + PEERS))
        {
            InetAddress peer = row.getInetAddress("peer");
            if (row.has("data_center") && row.has("rack"))
            {
                Map<String, String> dcRack = new HashMap<>();
                dcRack.put("data_center", row.getString("data_center"));
                dcRack.put("rack", row.getString("rack"));
                result.put(peer, dcRack);
            }
        }
        return result;
    }

    /**
     * Get release version for given endpoint.
     * If release version is unknown, then this returns null.
     *
     * @param ep endpoint address to check
     * @return Release version or null if version is unknown.
     */
    public static CassandraVersion getReleaseVersion(InetAddress ep)
    {
        try
        {
            if (FBUtilities.getBroadcastAddress().equals(ep))
            {
                return new CassandraVersion(FBUtilities.getReleaseVersionString());
            }
            String req = "SELECT release_version FROM system.%s WHERE peer=?";
            UntypedResultSet result = executeInternal(String.format(req, PEERS), ep);
            if (result != null && result.one().has("release_version"))
            {
                return new CassandraVersion(result.one().getString("release_version"));
            }
            // version is unknown
            return null;
        }
        catch (IllegalArgumentException e)
        {
            // version string cannot be parsed
            return null;
        }
    }

    /**
     * One of three things will happen if you try to read the system keyspace:
     * 1. files are present and you can read them: great
     * 2. no files are there: great (new node is assumed)
     * 3. files are present but you can't read them: bad
     * @throws ConfigurationException
     */
    public static void checkHealth() throws ConfigurationException
    {
        Keyspace keyspace;
        try
        {
            keyspace = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        }
        catch (AssertionError err)
        {
            // this happens when a user switches from OPP to RP.
            ConfigurationException ex = new ConfigurationException("Could not read system keyspace!");
            ex.initCause(err);
            throw ex;
        }
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(LOCAL);

        String req = "SELECT cluster_name FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(String.format(req, LOCAL, LOCAL));

        if (result.isEmpty() || !result.one().has("cluster_name"))
        {
            // this is a brand new node
            if (!cfs.getLiveSSTables().isEmpty())
                throw new ConfigurationException("Found system keyspace files, but they couldn't be loaded!");

            // no system files.  this is a new node.
            return;
        }

        String savedClusterName = result.one().getString("cluster_name");
        if (!DatabaseDescriptor.getClusterName().equals(savedClusterName))
            throw new ConfigurationException("Saved cluster name " + savedClusterName + " != configured name " + DatabaseDescriptor.getClusterName());
    }

    public static Collection<Token> getSavedTokens()
    {
        String req = "SELECT tokens FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(String.format(req, LOCAL, LOCAL));
        return result.isEmpty() || !result.one().has("tokens")
             ? Collections.<Token>emptyList()
             : deserializeTokens(result.one().getSet("tokens", UTF8Type.instance));
    }

    public static int incrementAndGetGeneration()
    {
        String req = "SELECT gossip_generation FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(String.format(req, LOCAL, LOCAL));

        int generation;
        if (result.isEmpty() || !result.one().has("gossip_generation"))
        {
            // seconds-since-epoch isn't a foolproof new generation
            // (where foolproof is "guaranteed to be larger than the last one seen at this ip address"),
            // but it's as close as sanely possible
            generation = (int) (System.currentTimeMillis() / 1000);
        }
        else
        {
            // Other nodes will ignore gossip messages about a node that have a lower generation than previously seen.
            final int storedGeneration = result.one().getInt("gossip_generation") + 1;
            final int now = (int) (System.currentTimeMillis() / 1000);
            if (storedGeneration >= now)
            {
                logger.warn("Using stored Gossip Generation {} as it is greater than current system time {}.  See CASSANDRA-3654 if you experience problems",
                            storedGeneration, now);
                generation = storedGeneration;
            }
            else
            {
                generation = now;
            }
        }

        req = "INSERT INTO system.%s (key, gossip_generation) VALUES ('%s', ?)";
        executeInternal(String.format(req, LOCAL, LOCAL), generation);
        forceBlockingFlush(LOCAL);

        return generation;
    }

    public static BootstrapState getBootstrapState()
    {
        String req = "SELECT bootstrapped FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(String.format(req, LOCAL, LOCAL));

        if (result.isEmpty() || !result.one().has("bootstrapped"))
            return BootstrapState.NEEDS_BOOTSTRAP;

        return BootstrapState.valueOf(result.one().getString("bootstrapped"));
    }

    public static boolean bootstrapComplete()
    {
        return getBootstrapState() == BootstrapState.COMPLETED;
    }

    public static boolean bootstrapInProgress()
    {
        return getBootstrapState() == BootstrapState.IN_PROGRESS;
    }

    public static boolean wasDecommissioned()
    {
        return getBootstrapState() == BootstrapState.DECOMMISSIONED;
    }

    public static void setBootstrapState(BootstrapState state)
    {
        String req = "INSERT INTO system.%s (key, bootstrapped) VALUES ('%s', ?)";
        executeInternal(String.format(req, LOCAL, LOCAL), state.name());
        forceBlockingFlush(LOCAL);
    }

    public static boolean isIndexBuilt(String keyspaceName, String indexName)
    {
        String req = "SELECT index_name FROM %s.\"%s\" WHERE table_name=? AND index_name=?";
        UntypedResultSet result = executeInternal(String.format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName, indexName);
        return !result.isEmpty();
    }

    public static void setIndexBuilt(String keyspaceName, String indexName)
    {
        String req = "INSERT INTO %s.\"%s\" (table_name, index_name) VALUES (?, ?)";
        executeInternal(String.format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName, indexName);
        forceBlockingFlush(BUILT_INDEXES);
    }

    public static void setIndexRemoved(String keyspaceName, String indexName)
    {
        String req = "DELETE FROM %s.\"%s\" WHERE table_name = ? AND index_name = ?";
        executeInternal(String.format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName, indexName);
        forceBlockingFlush(BUILT_INDEXES);
    }

    public static List<String> getBuiltIndexes(String keyspaceName, Set<String> indexNames)
    {
        List<String> names = new ArrayList<>(indexNames);
        String req = "SELECT index_name from %s.\"%s\" WHERE table_name=? AND index_name IN ?";
        UntypedResultSet results = executeInternal(String.format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName, names);
        return StreamSupport.stream(results.spliterator(), false)
                            .map(r -> r.getString("index_name"))
                            .collect(Collectors.toList());
    }

    /**
     * Read the host ID from the system keyspace, creating (and storing) one if
     * none exists.
     */
    public static UUID getLocalHostId()
    {
        String req = "SELECT host_id FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(String.format(req, LOCAL, LOCAL));

        // Look up the Host UUID (return it if found)
        if (!result.isEmpty() && result.one().has("host_id"))
            return result.one().getUUID("host_id");

        // ID not found, generate a new one, persist, and then return it.
        UUID hostId = UUID.randomUUID();
        logger.warn("No host ID found, created {} (Note: This should happen exactly once per node).", hostId);
        return setLocalHostId(hostId);
    }

    /**
     * Sets the local host ID explicitly.  Should only be called outside of SystemTable when replacing a node.
     */
    public static UUID setLocalHostId(UUID hostId)
    {
        String req = "INSERT INTO system.%s (key, host_id) VALUES ('%s', ?)";
        executeInternal(String.format(req, LOCAL, LOCAL), hostId);
        return hostId;
    }

    /**
     * Gets the stored rack for the local node, or null if none have been set yet.
     */
    public static String getRack()
    {
        String req = "SELECT rack FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(String.format(req, LOCAL, LOCAL));

        // Look up the Rack (return it if found)
        if (!result.isEmpty() && result.one().has("rack"))
            return result.one().getString("rack");

        return null;
    }

    /**
     * Gets the stored data center for the local node, or null if none have been set yet.
     */
    public static String getDatacenter()
    {
        String req = "SELECT data_center FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(String.format(req, LOCAL, LOCAL));

        // Look up the Data center (return it if found)
        if (!result.isEmpty() && result.one().has("data_center"))
            return result.one().getString("data_center");

        return null;
    }

    public static PaxosState loadPaxosState(DecoratedKey key, CFMetaData metadata, int nowInSec)
    {
        String req = "SELECT * FROM system.%s WHERE row_key = ? AND cf_id = ?";
        UntypedResultSet results = QueryProcessor.executeInternalWithNow(nowInSec, System.nanoTime(), String.format(req, PAXOS), key.getKey(), metadata.cfId);
        if (results.isEmpty())
            return new PaxosState(key, metadata);
        UntypedResultSet.Row row = results.one();
        Commit promised = row.has("in_progress_ballot")
                        ? new Commit(row.getUUID("in_progress_ballot"), new PartitionUpdate(metadata, key, metadata.partitionColumns(), 1))
                        : Commit.emptyCommit(key, metadata);
        // either we have both a recently accepted ballot and update or we have neither
        int proposalVersion = row.has("proposal_version") ? row.getInt("proposal_version") : MessagingService.VERSION_21;
        Commit accepted = row.has("proposal")
                        ? new Commit(row.getUUID("proposal_ballot"), PartitionUpdate.fromBytes(row.getBytes("proposal"), proposalVersion, key))
                        : Commit.emptyCommit(key, metadata);
        // either most_recent_commit and most_recent_commit_at will both be set, or neither
        int mostRecentVersion = row.has("most_recent_commit_version") ? row.getInt("most_recent_commit_version") : MessagingService.VERSION_21;
        Commit mostRecent = row.has("most_recent_commit")
                          ? new Commit(row.getUUID("most_recent_commit_at"), PartitionUpdate.fromBytes(row.getBytes("most_recent_commit"), mostRecentVersion, key))
                          : Commit.emptyCommit(key, metadata);
        return new PaxosState(promised, accepted, mostRecent);
    }

    public static void savePaxosPromise(Commit promise)
    {
        String req = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET in_progress_ballot = ? WHERE row_key = ? AND cf_id = ?";
        executeInternal(String.format(req, PAXOS),
                        UUIDGen.microsTimestamp(promise.ballot),
                        paxosTtlSec(promise.update.metadata()),
                        promise.ballot,
                        promise.update.partitionKey().getKey(),
                        promise.update.metadata().cfId);
    }

    public static void savePaxosProposal(Commit proposal)
    {
        executeInternal(String.format("UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET proposal_ballot = ?, proposal = ?, proposal_version = ? WHERE row_key = ? AND cf_id = ?", PAXOS),
                        UUIDGen.microsTimestamp(proposal.ballot),
                        paxosTtlSec(proposal.update.metadata()),
                        proposal.ballot,
                        PartitionUpdate.toBytes(proposal.update, MessagingService.current_version),
                        MessagingService.current_version,
                        proposal.update.partitionKey().getKey(),
                        proposal.update.metadata().cfId);
    }

    public static int paxosTtlSec(CFMetaData metadata)
    {
        // keep paxos state around for at least 3h
        return Math.max(3 * 3600, metadata.params.gcGraceSeconds);
    }

    public static void savePaxosCommit(Commit commit)
    {
        // We always erase the last proposal (with the commit timestamp to no erase more recent proposal in case the commit is old)
        // even though that's really just an optimization  since SP.beginAndRepairPaxos will exclude accepted proposal older than the mrc.
        String cql = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET proposal_ballot = null, proposal = null, most_recent_commit_at = ?, most_recent_commit = ?, most_recent_commit_version = ? WHERE row_key = ? AND cf_id = ?";
        executeInternal(String.format(cql, PAXOS),
                        UUIDGen.microsTimestamp(commit.ballot),
                        paxosTtlSec(commit.update.metadata()),
                        commit.ballot,
                        PartitionUpdate.toBytes(commit.update, MessagingService.current_version),
                        MessagingService.current_version,
                        commit.update.partitionKey().getKey(),
                        commit.update.metadata().cfId);
    }

    /**
     * Returns a RestorableMeter tracking the average read rate of a particular SSTable, restoring the last-seen rate
     * from values in system.sstable_activity if present.
     * @param keyspace the keyspace the sstable belongs to
     * @param table the table the sstable belongs to
     * @param generation the generation number for the sstable
     */
    public static RestorableMeter getSSTableReadMeter(String keyspace, String table, int generation)
    {
        String cql = "SELECT * FROM system.%s WHERE keyspace_name=? and columnfamily_name=? and generation=?";
        UntypedResultSet results = executeInternal(String.format(cql, SSTABLE_ACTIVITY), keyspace, table, generation);

        if (results.isEmpty())
            return new RestorableMeter();

        UntypedResultSet.Row row = results.one();
        double m15rate = row.getDouble("rate_15m");
        double m120rate = row.getDouble("rate_120m");
        return new RestorableMeter(m15rate, m120rate);
    }

    /**
     * Writes the current read rates for a given SSTable to system.sstable_activity
     */
    public static void persistSSTableReadMeter(String keyspace, String table, int generation, RestorableMeter meter)
    {
        // Store values with a one-day TTL to handle corner cases where cleanup might not occur
        String cql = "INSERT INTO system.%s (keyspace_name, columnfamily_name, generation, rate_15m, rate_120m) VALUES (?, ?, ?, ?, ?) USING TTL 864000";
        executeInternal(String.format(cql, SSTABLE_ACTIVITY),
                        keyspace,
                        table,
                        generation,
                        meter.fifteenMinuteRate(),
                        meter.twoHourRate());
    }

    /**
     * Clears persisted read rates from system.sstable_activity for SSTables that have been deleted.
     */
    public static void clearSSTableReadMeter(String keyspace, String table, int generation)
    {
        String cql = "DELETE FROM system.%s WHERE keyspace_name=? AND columnfamily_name=? and generation=?";
        executeInternal(String.format(cql, SSTABLE_ACTIVITY), keyspace, table, generation);
    }

    /**
     * Writes the current partition count and size estimates into SIZE_ESTIMATES_CF
     */
    public static void updateSizeEstimates(String keyspace, String table, Map<Range<Token>, Pair<Long, Long>> estimates)
    {
        long timestamp = FBUtilities.timestampMicros();
        PartitionUpdate update = new PartitionUpdate(SizeEstimates, UTF8Type.instance.decompose(keyspace), SizeEstimates.partitionColumns(), estimates.size());
        Mutation mutation = new Mutation(update);

        // delete all previous values with a single range tombstone.
        int nowInSec = FBUtilities.nowInSeconds();
        update.add(new RangeTombstone(Slice.make(SizeEstimates.comparator, table), new DeletionTime(timestamp - 1, nowInSec)));

        // add a CQL row for each primary token range.
        for (Map.Entry<Range<Token>, Pair<Long, Long>> entry : estimates.entrySet())
        {
            Range<Token> range = entry.getKey();
            Pair<Long, Long> values = entry.getValue();
            update.add(Rows.simpleBuilder(SizeEstimates, table, range.left.toString(), range.right.toString())
                           .timestamp(timestamp)
                           .add("partitions_count", values.left)
                           .add("mean_partition_size", values.right)
                           .build());
        }

        mutation.apply();
    }

    /**
     * Clears size estimates for a table (on table drop)
     */
    public static void clearSizeEstimates(String keyspace, String table)
    {
        String cql = String.format("DELETE FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", SchemaConstants.SYSTEM_KEYSPACE_NAME, SIZE_ESTIMATES);
        executeInternal(cql, keyspace, table);
    }

    /**
     * Clears size estimates for a keyspace (used to manually clean when we miss a keyspace drop)
     */
    public static void clearSizeEstimates(String keyspace)
    {
        String cql = String.format("DELETE FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SYSTEM_KEYSPACE_NAME, SIZE_ESTIMATES);
        executeInternal(cql, keyspace);
    }

    /**
     * @return A multimap from keyspace to table for all tables with entries in size estimates
     */

    public static synchronized SetMultimap<String, String> getTablesWithSizeEstimates()
    {
        SetMultimap<String, String> keyspaceTableMap = HashMultimap.create();
        String cql = String.format("SELECT keyspace_name, table_name FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SIZE_ESTIMATES);
        UntypedResultSet rs = executeInternal(cql);
        for (UntypedResultSet.Row row : rs)
        {
            keyspaceTableMap.put(row.getString("keyspace_name"), row.getString("table_name"));
        }

        return keyspaceTableMap;
    }

    public static synchronized void updateAvailableRanges(String keyspace, Collection<Range<Token>> completedRanges)
    {
        String cql = "UPDATE system.%s SET ranges = ranges + ? WHERE keyspace_name = ?";
        Set<ByteBuffer> rangesToUpdate = new HashSet<>(completedRanges.size());
        for (Range<Token> range : completedRanges)
        {
            rangesToUpdate.add(rangeToBytes(range));
        }
        executeInternal(String.format(cql, AVAILABLE_RANGES), rangesToUpdate, keyspace);
    }

    public static synchronized Set<Range<Token>> getAvailableRanges(String keyspace, IPartitioner partitioner)
    {
        Set<Range<Token>> result = new HashSet<>();
        String query = "SELECT * FROM system.%s WHERE keyspace_name=?";
        UntypedResultSet rs = executeInternal(String.format(query, AVAILABLE_RANGES), keyspace);
        for (UntypedResultSet.Row row : rs)
        {
            Set<ByteBuffer> rawRanges = row.getSet("ranges", BytesType.instance);
            for (ByteBuffer rawRange : rawRanges)
            {
                result.add(byteBufferToRange(rawRange, partitioner));
            }
        }
        return ImmutableSet.copyOf(result);
    }

    public static void resetAvailableRanges()
    {
        ColumnFamilyStore availableRanges = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(AVAILABLE_RANGES);
        availableRanges.truncateBlocking();
    }

    public static synchronized void updateTransferredRanges(String description,
                                                         InetAddress peer,
                                                         String keyspace,
                                                         Collection<Range<Token>> streamedRanges)
    {
        String cql = "UPDATE system.%s SET ranges = ranges + ? WHERE operation = ? AND peer = ? AND keyspace_name = ?";
        Set<ByteBuffer> rangesToUpdate = new HashSet<>(streamedRanges.size());
        for (Range<Token> range : streamedRanges)
        {
            rangesToUpdate.add(rangeToBytes(range));
        }
        executeInternal(String.format(cql, TRANSFERRED_RANGES), rangesToUpdate, description, peer, keyspace);
    }

    public static synchronized Map<InetAddress, Set<Range<Token>>> getTransferredRanges(String description, String keyspace, IPartitioner partitioner)
    {
        Map<InetAddress, Set<Range<Token>>> result = new HashMap<>();
        String query = "SELECT * FROM system.%s WHERE operation = ? AND keyspace_name = ?";
        UntypedResultSet rs = executeInternal(String.format(query, TRANSFERRED_RANGES), description, keyspace);
        for (UntypedResultSet.Row row : rs)
        {
            InetAddress peer = row.getInetAddress("peer");
            Set<ByteBuffer> rawRanges = row.getSet("ranges", BytesType.instance);
            Set<Range<Token>> ranges = Sets.newHashSetWithExpectedSize(rawRanges.size());
            for (ByteBuffer rawRange : rawRanges)
            {
                ranges.add(byteBufferToRange(rawRange, partitioner));
            }
            result.put(peer, ranges);
        }
        return ImmutableMap.copyOf(result);
    }

    /**
     * Compare the release version in the system.local table with the one included in the distro.
     * If they don't match, snapshot all tables in the system keyspace. This is intended to be
     * called at startup to create a backup of the system tables during an upgrade
     *
     * @throws IOException
     */
    public static boolean snapshotOnVersionChange() throws IOException
    {
        String previous = getPreviousVersionString();
        String next = FBUtilities.getReleaseVersionString();

        // if we're restarting after an upgrade, snapshot the system keyspace
        if (!previous.equals(NULL_VERSION.toString()) && !previous.equals(next))

        {
            logger.info("Detected version upgrade from {} to {}, snapshotting system keyspace", previous, next);
            String snapshotName = Keyspace.getTimestampedSnapshotName(String.format("upgrade-%s-%s",
                                                                                    previous,
                                                                                    next));
            Keyspace systemKs = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME);
            systemKs.snapshot(snapshotName, null);
            return true;
        }

        return false;
    }

    /**
     * Try to determine what the previous version, if any, was installed on this node.
     * Primary source of truth is the release version in system.local. If the previous
     * version cannot be determined by looking there then either:
     * * the node never had a C* install before
     * * the was a very old version (pre 1.2) installed, which did not include system.local
     *
     * @return either a version read from the system.local table or one of two special values
     * indicating either no previous version (SystemUpgrade.NULL_VERSION) or an unreadable,
     * legacy version (SystemUpgrade.UNREADABLE_VERSION).
     */
    private static String getPreviousVersionString()
    {
        String req = "SELECT release_version FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(String.format(req, SystemKeyspace.LOCAL, SystemKeyspace.LOCAL));
        if (result.isEmpty() || !result.one().has("release_version"))
        {
            // it isn't inconceivable that one might try to upgrade a node straight from <= 1.1 to whatever
            // the current version is. If we couldn't read a previous version from system.local we check for
            // the existence of the legacy system.Versions table. We don't actually attempt to read a version
            // from there, but it informs us that this isn't a completely new node.
            for (File dataDirectory : Directories.getKSChildDirectories(SchemaConstants.SYSTEM_KEYSPACE_NAME))
            {
                if (dataDirectory.getName().equals("Versions") && dataDirectory.listFiles().length > 0)
                {
                    logger.trace("Found unreadable versions info in pre 1.2 system.Versions table");
                    return UNREADABLE_VERSION.toString();
                }
            }

            // no previous version information found, we can assume that this is a new node
            return NULL_VERSION.toString();
        }
        // report back whatever we found in the system table
        return result.one().getString("release_version");
    }

    /**
     * Check data directories for old files that can be removed when migrating from 2.1 or 2.2 to 3.0,
     * these checks can be removed in 4.0, see CASSANDRA-7066
     */
    public static void migrateDataDirs()
    {
        Iterable<String> dirs = Arrays.asList(DatabaseDescriptor.getAllDataFileLocations());
        for (String dataDir : dirs)
        {
            logger.debug("Checking {} for legacy files", dataDir);
            File dir = new File(dataDir);
            assert dir.exists() : dir + " should have been created by startup checks";

            visitDirectory(dir.toPath(),
                           File::isDirectory,
                           ksdir ->
                           {
                               logger.trace("Checking {} for legacy files", ksdir);
                               visitDirectory(ksdir.toPath(),
                                              File::isDirectory,
                                              cfdir ->
                                              {
                                                  logger.trace("Checking {} for legacy files", cfdir);

                                                  if (Descriptor.isLegacyFile(cfdir))
                                                  {
                                                      FileUtils.deleteRecursive(cfdir);
                                                  }
                                                  else
                                                  {
                                                      visitDirectory(cfdir.toPath(),
                                                                     Descriptor::isLegacyFile,
                                                                     FileUtils::delete);
                                                  }
                                              });
                           });
        }
    }

    private static ByteBuffer rangeToBytes(Range<Token> range)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Range.tokenSerializer.serialize(range, out, MessagingService.VERSION_22);
            return out.buffer();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Range<Token> byteBufferToRange(ByteBuffer rawRange, IPartitioner partitioner)
    {
        try
        {
            return (Range<Token>) Range.tokenSerializer.deserialize(ByteStreams.newDataInput(ByteBufferUtil.getArray(rawRange)),
                                                                    partitioner,
                                                                    MessagingService.VERSION_22);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public static void writePreparedStatement(String loggedKeyspace, MD5Digest key, String cql)
    {
        executeInternal(String.format("INSERT INTO %s.%s"
                                      + " (logged_keyspace, prepared_id, query_string) VALUES (?, ?, ?)",
                                      SchemaConstants.SYSTEM_KEYSPACE_NAME, PREPARED_STATEMENTS),
                        loggedKeyspace, key.byteBuffer(), cql);
        logger.debug("stored prepared statement for logged keyspace '{}': '{}'", loggedKeyspace, cql);
    }

    public static void removePreparedStatement(MD5Digest key)
    {
        executeInternal(String.format("DELETE FROM %s.%s"
                                      + " WHERE prepared_id = ?",
                                      SchemaConstants.SYSTEM_KEYSPACE_NAME, PREPARED_STATEMENTS),
                        key.byteBuffer());
    }

    public static void resetPreparedStatements()
    {
        ColumnFamilyStore availableRanges = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(PREPARED_STATEMENTS);
        availableRanges.truncateBlocking();
    }

    public static List<Pair<String, String>> loadPreparedStatements()
    {
        String query = String.format("SELECT logged_keyspace, query_string FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, PREPARED_STATEMENTS);
        UntypedResultSet resultSet = executeOnceInternal(query);
        List<Pair<String, String>> r = new ArrayList<>();
        for (UntypedResultSet.Row row : resultSet)
            r.add(Pair.create(row.has("logged_keyspace") ? row.getString("logged_keyspace") : null,
                              row.getString("query_string")));
        return r;
    }
}
