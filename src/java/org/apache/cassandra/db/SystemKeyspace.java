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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.CompactionHistoryTabularData;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.*;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;

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
    public static final String PEERS_V2 = "peers_v2";
    public static final String PEER_EVENTS_V2 = "peer_events_v2";
    public static final String RANGE_XFERS = "range_xfers";
    public static final String COMPACTION_HISTORY = "compaction_history";
    public static final String SSTABLE_ACTIVITY = "sstable_activity";
    public static final String SIZE_ESTIMATES = "size_estimates";
    public static final String AVAILABLE_RANGES = "available_ranges";
    public static final String TRANSFERRED_RANGES = "transferred_ranges";
    public static final String TRANSFERRED_RANGES_V2 = "transferred_ranges_v2";
    public static final String VIEW_BUILDS_IN_PROGRESS = "view_builds_in_progress";
    public static final String BUILT_VIEWS = "built_views";
    public static final String PREPARED_STATEMENTS = "prepared_statements";
    public static final String REPAIRS = "repairs";

    @Deprecated public static final String LEGACY_PEERS = "peers";
    @Deprecated public static final String LEGACY_PEER_EVENTS = "peer_events";
    @Deprecated public static final String LEGACY_TRANSFERRED_RANGES = "transferred_ranges";

    public static final TableMetadata Batches =
        parse(BATCHES,
                "batches awaiting replay",
                "CREATE TABLE %s ("
                + "id timeuuid,"
                + "mutations list<blob>,"
                + "version int,"
                + "PRIMARY KEY ((id)))")
                .partitioner(new LocalPartitioner(TimeUUIDType.instance))
                .compaction(CompactionParams.scts(singletonMap("min_threshold", "2")))
                .build();

    private static final TableMetadata Paxos =
        parse(PAXOS,
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
                .compaction(CompactionParams.lcs(emptyMap()))
                .build();

    private static final TableMetadata BuiltIndexes =
        parse(BUILT_INDEXES,
              "built column indexes",
              "CREATE TABLE \"%s\" ("
              + "table_name text," // table_name here is the name of the keyspace - don't be fooled
              + "index_name text,"
              + "value blob," // Table used to be compact in previous versions
              + "PRIMARY KEY ((table_name), index_name)) ")
              .build();

    private static final TableMetadata Local =
        parse(LOCAL,
                "information about the local node",
                "CREATE TABLE %s ("
                + "key text,"
                + "bootstrapped text,"
                + "broadcast_address inet,"
                + "broadcast_port int,"
                + "cluster_name text,"
                + "cql_version text,"
                + "data_center text,"
                + "gossip_generation int,"
                + "host_id uuid,"
                + "listen_address inet,"
                + "listen_port int,"
                + "native_protocol_version text,"
                + "partitioner text,"
                + "rack text,"
                + "release_version text,"
                + "rpc_address inet,"
                + "rpc_port int,"
                + "schema_version uuid,"
                + "tokens set<varchar>,"
                + "truncated_at map<uuid, blob>,"
                + "PRIMARY KEY ((key)))"
                ).recordDeprecatedSystemColumn("thrift_version", UTF8Type.instance)
                .build();

    private static final TableMetadata PeersV2 =
        parse(PEERS_V2,
                "information about known peers in the cluster",
                "CREATE TABLE %s ("
                + "peer inet,"
                + "peer_port int,"
                + "data_center text,"
                + "host_id uuid,"
                + "preferred_ip inet,"
                + "preferred_port int,"
                + "rack text,"
                + "release_version text,"
                + "native_address inet,"
                + "native_port int,"
                + "schema_version uuid,"
                + "tokens set<varchar>,"
                + "PRIMARY KEY ((peer), peer_port))")
                .build();

    private static final TableMetadata PeerEventsV2 =
        parse(PEER_EVENTS_V2,
                "events related to peers",
                "CREATE TABLE %s ("
                + "peer inet,"
                + "peer_port int,"
                + "hints_dropped map<uuid, int>,"
                + "PRIMARY KEY ((peer), peer_port))")
                .build();

    private static final TableMetadata RangeXfers =
        parse(RANGE_XFERS,
                "ranges requested for transfer",
                "CREATE TABLE %s ("
                + "token_bytes blob,"
                + "requested_at timestamp,"
                + "PRIMARY KEY ((token_bytes)))")
                .build();

    private static final TableMetadata CompactionHistory =
        parse(COMPACTION_HISTORY,
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
                .defaultTimeToLive((int) TimeUnit.DAYS.toSeconds(7))
                .build();

    private static final TableMetadata SSTableActivity =
        parse(SSTABLE_ACTIVITY,
                "historic sstable read rates",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "columnfamily_name text,"
                + "generation int,"
                + "rate_120m double,"
                + "rate_15m double,"
                + "PRIMARY KEY ((keyspace_name, columnfamily_name, generation)))")
                .build();

    private static final TableMetadata SizeEstimates =
        parse(SIZE_ESTIMATES,
                "per-table primary range size estimates",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "table_name text,"
                + "range_start text,"
                + "range_end text,"
                + "mean_partition_size bigint,"
                + "partitions_count bigint,"
                + "PRIMARY KEY ((keyspace_name), table_name, range_start, range_end))")
                .build();

    private static final TableMetadata AvailableRanges =
        parse(AVAILABLE_RANGES,
                "available keyspace/ranges during bootstrap/replace that are ready to be served",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "ranges set<blob>,"
                + "PRIMARY KEY ((keyspace_name)))")
                .build();

    private static final TableMetadata TransferredRangesV2 =
        parse(TRANSFERRED_RANGES_V2,
                "record of transferred ranges for streaming operation",
                "CREATE TABLE %s ("
                + "operation text,"
                + "peer inet,"
                + "peer_port int,"
                + "keyspace_name text,"
                + "ranges set<blob>,"
                + "PRIMARY KEY ((operation, keyspace_name), peer, peer_port))")
                .build();

    private static final TableMetadata ViewBuildsInProgress =
        parse(VIEW_BUILDS_IN_PROGRESS,
              "views builds current progress",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "view_name text,"
              + "start_token varchar,"
              + "end_token varchar,"
              + "last_token varchar,"
              + "keys_built bigint,"
              + "PRIMARY KEY ((keyspace_name), view_name, start_token, end_token))")
              .build();

    private static final TableMetadata BuiltViews =
        parse(BUILT_VIEWS,
                "built views",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "view_name text,"
                + "status_replicated boolean,"
                + "PRIMARY KEY ((keyspace_name), view_name))")
                .build();

    private static final TableMetadata PreparedStatements =
        parse(PREPARED_STATEMENTS,
                "prepared statements",
                "CREATE TABLE %s ("
                + "prepared_id blob,"
                + "logged_keyspace text,"
                + "query_string text,"
                + "PRIMARY KEY ((prepared_id)))")
                .build();

    private static final TableMetadata Repairs =
        parse(REPAIRS,
          "repairs",
          "CREATE TABLE %s ("
          + "parent_id timeuuid, "
          + "started_at timestamp, "
          + "last_update timestamp, "
          + "repaired_at timestamp, "
          + "state int, "
          + "coordinator inet, "
          + "coordinator_port int,"
          + "participants set<inet>,"
          + "participants_wp set<text>,"
          + "ranges set<blob>, "
          + "cfids set<uuid>, "
          + "PRIMARY KEY (parent_id))").build();

    @Deprecated
    private static final TableMetadata LegacyPeers =
        parse(LEGACY_PEERS,
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
            + "PRIMARY KEY ((peer)))")
            .build();

    @Deprecated
    private static final TableMetadata LegacyPeerEvents =
        parse(LEGACY_PEER_EVENTS,
            "events related to peers",
            "CREATE TABLE %s ("
            + "peer inet,"
            + "hints_dropped map<uuid, int>,"
            + "PRIMARY KEY ((peer)))")
            .build();

    @Deprecated
    private static final TableMetadata LegacyTransferredRanges =
        parse(LEGACY_TRANSFERRED_RANGES,
            "record of transferred ranges for streaming operation",
            "CREATE TABLE %s ("
            + "operation text,"
            + "peer inet,"
            + "keyspace_name text,"
            + "ranges set<blob>,"
            + "PRIMARY KEY ((operation, keyspace_name), peer))")
            .build();

    private static TableMetadata.Builder parse(String table, String description, String cql)
    {
        return CreateTableStatement.parse(format(cql, table), SchemaConstants.SYSTEM_KEYSPACE_NAME)
                                   .id(TableId.forSystemTable(SchemaConstants.SYSTEM_KEYSPACE_NAME, table))
                                   .dcLocalReadRepairChance(0.0)
                                   .gcGraceSeconds(0)
                                   .memtableFlushPeriod((int) TimeUnit.HOURS.toMillis(1))
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
                         PeersV2,
                         LegacyPeers,
                         PeerEventsV2,
                         LegacyPeerEvents,
                         RangeXfers,
                         CompactionHistory,
                         SSTableActivity,
                         SizeEstimates,
                         AvailableRanges,
                         TransferredRangesV2,
                         LegacyTransferredRanges,
                         ViewBuildsInProgress,
                         BuiltViews,
                         PreparedStatements,
                         Repairs);
    }

    private static Functions functions()
    {
        return Functions.builder()
                        .add(UuidFcts.all())
                        .add(TimeFcts.all())
                        .add(BytesConversionFcts.all())
                        .add(AggregateFcts.all())
                        .add(CastFcts.all())
                        .add(OperationFcts.all())
                        .build();
    }

    private static volatile Map<TableId, Pair<CommitLogPosition, Long>> truncationRecords;

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
                     "native_protocol_version," +
                     "data_center," +
                     "rack," +
                     "partitioner," +
                     "rpc_address," +
                     "rpc_port," +
                     "broadcast_address," +
                     "broadcast_port," +
                     "listen_address," +
                     "listen_port" +
                     ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        executeOnceInternal(format(req, LOCAL),
                            LOCAL,
                            DatabaseDescriptor.getClusterName(),
                            FBUtilities.getReleaseVersionString(),
                            QueryProcessor.CQL_VERSION.toString(),
                            String.valueOf(ProtocolVersion.CURRENT.asInt()),
                            snitch.getDatacenter(FBUtilities.getBroadcastAddressAndPort()),
                            snitch.getRack(FBUtilities.getBroadcastAddressAndPort()),
                            DatabaseDescriptor.getPartitioner().getClass().getName(),
                            DatabaseDescriptor.getRpcAddress(),
                            DatabaseDescriptor.getNativeTransportPort(),
                            FBUtilities.getJustBroadcastAddress(),
                            DatabaseDescriptor.getStoragePort(),
                            FBUtilities.getJustLocalAddress(),
                            DatabaseDescriptor.getStoragePort());
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
        executeInternal(format(req, COMPACTION_HISTORY),
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
        UntypedResultSet queryResultSet = executeInternal(format("SELECT * from system.%s", COMPACTION_HISTORY));
        return CompactionHistoryTabularData.from(queryResultSet);
    }

    public static boolean isViewBuilt(String keyspaceName, String viewName)
    {
        String req = "SELECT view_name FROM %s.\"%s\" WHERE keyspace_name=? AND view_name=?";
        UntypedResultSet result = executeInternal(format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_VIEWS), keyspaceName, viewName);
        return !result.isEmpty();
    }

    public static boolean isViewStatusReplicated(String keyspaceName, String viewName)
    {
        String req = "SELECT status_replicated FROM %s.\"%s\" WHERE keyspace_name=? AND view_name=?";
        UntypedResultSet result = executeInternal(format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_VIEWS), keyspaceName, viewName);

        if (result.isEmpty())
            return false;
        UntypedResultSet.Row row = result.one();
        return row.has("status_replicated") && row.getBoolean("status_replicated");
    }

    public static void setViewBuilt(String keyspaceName, String viewName, boolean replicated)
    {
        if (isViewBuilt(keyspaceName, viewName) && isViewStatusReplicated(keyspaceName, viewName) == replicated)
            return;

        String req = "INSERT INTO %s.\"%s\" (keyspace_name, view_name, status_replicated) VALUES (?, ?, ?)";
        executeInternal(format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_VIEWS), keyspaceName, viewName, replicated);
        forceBlockingFlush(BUILT_VIEWS);
    }

    public static void setViewRemoved(String keyspaceName, String viewName)
    {
        String buildReq = "DELETE FROM %s.%s WHERE keyspace_name = ? AND view_name = ?";
        executeInternal(String.format(buildReq, SchemaConstants.SYSTEM_KEYSPACE_NAME, VIEW_BUILDS_IN_PROGRESS), keyspaceName, viewName);

        String builtReq = "DELETE FROM %s.\"%s\" WHERE keyspace_name = ? AND view_name = ? IF EXISTS";
        executeInternal(String.format(builtReq, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_VIEWS), keyspaceName, viewName);
        forceBlockingFlush(VIEW_BUILDS_IN_PROGRESS, BUILT_VIEWS);
    }

    public static void finishViewBuildStatus(String ksname, String viewName)
    {
        // We flush the view built first, because if we fail now, we'll restart at the last place we checkpointed
        // view build.
        // If we flush the delete first, we'll have to restart from the beginning.
        // Also, if writing to the built_view succeeds, but the view_builds_in_progress deletion fails, we will be able
        // to skip the view build next boot.
        setViewBuilt(ksname, viewName, false);
        executeInternal(String.format("DELETE FROM system.%s WHERE keyspace_name = ? AND view_name = ?", VIEW_BUILDS_IN_PROGRESS), ksname, viewName);
        forceBlockingFlush(VIEW_BUILDS_IN_PROGRESS);
    }

    public static void setViewBuiltReplicated(String ksname, String viewName)
    {
        setViewBuilt(ksname, viewName, true);
    }

    public static void updateViewBuildStatus(String ksname, String viewName, Range<Token> range, Token lastToken, long keysBuilt)
    {
        String req = "INSERT INTO system.%s (keyspace_name, view_name, start_token, end_token, last_token, keys_built) VALUES (?, ?, ?, ?, ?, ?)";
        Token.TokenFactory factory = ViewBuildsInProgress.partitioner.getTokenFactory();
        executeInternal(format(req, VIEW_BUILDS_IN_PROGRESS),
                        ksname,
                        viewName,
                        factory.toString(range.left),
                        factory.toString(range.right),
                        factory.toString(lastToken),
                        keysBuilt);
    }

    public static Map<Range<Token>, Pair<Token, Long>> getViewBuildStatus(String ksname, String viewName)
    {
        String req = "SELECT start_token, end_token, last_token, keys_built FROM system.%s WHERE keyspace_name = ? AND view_name = ?";
        Token.TokenFactory factory = ViewBuildsInProgress.partitioner.getTokenFactory();
        UntypedResultSet rs = executeInternal(format(req, VIEW_BUILDS_IN_PROGRESS), ksname, viewName);

        if (rs == null || rs.isEmpty())
            return Collections.emptyMap();

        Map<Range<Token>, Pair<Token, Long>> status = new HashMap<>();
        for (UntypedResultSet.Row row : rs)
        {
            Token start = factory.fromString(row.getString("start_token"));
            Token end = factory.fromString(row.getString("end_token"));
            Range<Token> range = new Range<>(start, end);

            Token lastToken = row.has("last_token") ? factory.fromString(row.getString("last_token")) : null;
            long keysBuilt = row.has("keys_built") ? row.getLong("keys_built") : 0;

            status.put(range, Pair.create(lastToken, keysBuilt));
        }
        return status;
    }

    public static synchronized void saveTruncationRecord(ColumnFamilyStore cfs, long truncatedAt, CommitLogPosition position)
    {
        String req = "UPDATE system.%s SET truncated_at = truncated_at + ? WHERE key = '%s'";
        executeInternal(format(req, LOCAL, LOCAL), truncationAsMapEntry(cfs, truncatedAt, position));
        truncationRecords = null;
        forceBlockingFlush(LOCAL);
    }

    /**
     * This method is used to remove information about truncation time for specified column family
     */
    public static synchronized void removeTruncationRecord(TableId id)
    {
        Pair<CommitLogPosition, Long> truncationRecord = getTruncationRecord(id);
        if (truncationRecord == null)
            return;

        String req = "DELETE truncated_at[?] from system.%s WHERE key = '%s'";
        executeInternal(format(req, LOCAL, LOCAL), id.asUUID());
        truncationRecords = null;
        forceBlockingFlush(LOCAL);
    }

    private static Map<UUID, ByteBuffer> truncationAsMapEntry(ColumnFamilyStore cfs, long truncatedAt, CommitLogPosition position)
    {
        try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
        {
            CommitLogPosition.serializer.serialize(position, out);
            out.writeLong(truncatedAt);
            return singletonMap(cfs.metadata.id.asUUID(), out.asNewBuffer());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static CommitLogPosition getTruncatedPosition(TableId id)
    {
        Pair<CommitLogPosition, Long> record = getTruncationRecord(id);
        return record == null ? null : record.left;
    }

    public static long getTruncatedAt(TableId id)
    {
        Pair<CommitLogPosition, Long> record = getTruncationRecord(id);
        return record == null ? Long.MIN_VALUE : record.right;
    }

    private static synchronized Pair<CommitLogPosition, Long> getTruncationRecord(TableId id)
    {
        if (truncationRecords == null)
            truncationRecords = readTruncationRecords();
        return truncationRecords.get(id);
    }

    private static Map<TableId, Pair<CommitLogPosition, Long>> readTruncationRecords()
    {
        UntypedResultSet rows = executeInternal(format("SELECT truncated_at FROM system.%s WHERE key = '%s'", LOCAL, LOCAL));

        Map<TableId, Pair<CommitLogPosition, Long>> records = new HashMap<>();

        if (!rows.isEmpty() && rows.one().has("truncated_at"))
        {
            Map<UUID, ByteBuffer> map = rows.one().getMap("truncated_at", UUIDType.instance, BytesType.instance);
            for (Map.Entry<UUID, ByteBuffer> entry : map.entrySet())
                records.put(TableId.fromUUID(entry.getKey()), truncationRecordFromBlob(entry.getValue()));
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
    public static synchronized void updateTokens(InetAddressAndPort ep, Collection<Token> tokens)
    {
        if (ep.equals(FBUtilities.getBroadcastAddressAndPort()))
            return;

        String req = "INSERT INTO system.%s (peer, tokens) VALUES (?, ?)";
        executeInternal(String.format(req, LEGACY_PEERS), ep.address, tokensAsSet(tokens));
        req = "INSERT INTO system.%s (peer, peer_port, tokens) VALUES (?, ?, ?)";
        executeInternal(String.format(req, PEERS_V2), ep.address, ep.port, tokensAsSet(tokens));
    }

    public static synchronized void updatePreferredIP(InetAddressAndPort ep, InetAddressAndPort preferred_ip)
    {
        if (getPreferredIP(ep) == preferred_ip)
            return;

        String req = "INSERT INTO system.%s (peer, preferred_ip) VALUES (?, ?)";
        executeInternal(String.format(req, LEGACY_PEERS), ep.address, preferred_ip.address);
        req = "INSERT INTO system.%s (peer, peer_port, preferred_ip, preferred_port) VALUES (?, ?, ?, ?)";
        executeInternal(String.format(req, PEERS_V2), ep.address, ep.port, preferred_ip.address, preferred_ip.port);
        forceBlockingFlush(LEGACY_PEERS, PEERS_V2);
    }

    public static synchronized void updatePeerInfo(InetAddressAndPort ep, String columnName, Object value)
    {
        if (ep.equals(FBUtilities.getBroadcastAddressAndPort()))
            return;

        String req = "INSERT INTO system.%s (peer, %s) VALUES (?, ?)";
        executeInternal(String.format(req, LEGACY_PEERS, columnName), ep.address, value);
        //This column doesn't match across the two tables
        if (columnName.equals("rpc_address"))
        {
            columnName = "native_address";
        }
        req = "INSERT INTO system.%s (peer, peer_port, %s) VALUES (?, ?, ?)";
        executeInternal(String.format(req, PEERS_V2, columnName), ep.address, ep.port, value);
    }

    public static synchronized void updatePeerNativeAddress(InetAddressAndPort ep, InetAddressAndPort address)
    {
        if (ep.equals(FBUtilities.getBroadcastAddressAndPort()))
            return;

        String req = "INSERT INTO system.%s (peer, rpc_address) VALUES (?, ?)";
        executeInternal(String.format(req, LEGACY_PEERS), ep.address, address.address);
        req = "INSERT INTO system.%s (peer, peer_port, native_address, native_port) VALUES (?, ?, ?, ?)";
        executeInternal(String.format(req, PEERS_V2), ep.address, ep.port, address.address, address.port);
    }


    public static synchronized void updateHintsDropped(InetAddressAndPort ep, UUID timePeriod, int value)
    {
        // with 30 day TTL
        String req = "UPDATE system.%s USING TTL 2592000 SET hints_dropped[ ? ] = ? WHERE peer = ?";
        executeInternal(String.format(req, LEGACY_PEER_EVENTS), timePeriod, value, ep.address);
        req = "UPDATE system.%s USING TTL 2592000 SET hints_dropped[ ? ] = ? WHERE peer = ? AND peer_port = ?";
        executeInternal(String.format(req, PEER_EVENTS_V2), timePeriod, value, ep.address, ep.port);
    }

    public static synchronized void updateSchemaVersion(UUID version)
    {
        String req = "INSERT INTO system.%s (key, schema_version) VALUES ('%s', ?)";
        executeInternal(format(req, LOCAL, LOCAL), version);
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
    public static synchronized void removeEndpoint(InetAddressAndPort ep)
    {
        String req = "DELETE FROM system.%s WHERE peer = ?";
        executeInternal(String.format(req, LEGACY_PEERS), ep.address);
        req = String.format("DELETE FROM system.%s WHERE peer = ? AND peer_port = ?", PEERS_V2);
        executeInternal(req, ep.address, ep.port);
        forceBlockingFlush(LEGACY_PEERS, PEERS_V2);
    }

    /**
     * This method is used to update the System Keyspace with the new tokens for this node
    */
    public static synchronized void updateTokens(Collection<Token> tokens)
    {
        assert !tokens.isEmpty() : "removeEndpoint should be used instead";

        Collection<Token> savedTokens = getSavedTokens();
        if (tokens.containsAll(savedTokens) && tokens.size() == savedTokens.size())
            return;

        String req = "INSERT INTO system.%s (key, tokens) VALUES ('%s', ?)";
        executeInternal(format(req, LOCAL, LOCAL), tokensAsSet(tokens));
        forceBlockingFlush(LOCAL);
    }

    public static void forceBlockingFlush(String ...cfnames)
    {
        if (!DatabaseDescriptor.isUnsafeSystem())
        {
            List<ListenableFuture<CommitLogPosition>> futures = new ArrayList<>();

            for (String cfname : cfnames)
            {
                futures.add(Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(cfname).forceFlush());
            }
            FBUtilities.waitOnFutures(futures);
        }
    }

    /**
     * Return a map of stored tokens to IP addresses
     *
     */
    public static SetMultimap<InetAddressAndPort, Token> loadTokens()
    {
        SetMultimap<InetAddressAndPort, Token> tokenMap = HashMultimap.create();
        for (UntypedResultSet.Row row : executeInternal("SELECT peer, peer_port, tokens FROM system." + PEERS_V2))
        {
            InetAddress address = row.getInetAddress("peer");
            Integer port = row.getInt("peer_port");
            InetAddressAndPort peer = InetAddressAndPort.getByAddressOverrideDefaults(address, port);
            if (row.has("tokens"))
                tokenMap.putAll(peer, deserializeTokens(row.getSet("tokens", UTF8Type.instance)));
        }

        return tokenMap;
    }

    /**
     * Return a map of store host_ids to IP addresses
     *
     */
    public static Map<InetAddressAndPort, UUID> loadHostIds()
    {
        Map<InetAddressAndPort, UUID> hostIdMap = new HashMap<>();
        for (UntypedResultSet.Row row : executeInternal("SELECT peer, peer_port, host_id FROM system." + PEERS_V2))
        {
            InetAddress address = row.getInetAddress("peer");
            Integer port = row.getInt("peer_port");
            InetAddressAndPort peer = InetAddressAndPort.getByAddressOverrideDefaults(address, port);
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
    public static InetAddressAndPort getPreferredIP(InetAddressAndPort ep)
    {
        String req = "SELECT preferred_ip, preferred_port FROM system.%s WHERE peer=? AND peer_port = ?";
        UntypedResultSet result = executeInternal(String.format(req, PEERS_V2), ep.address, ep.port);
        if (!result.isEmpty() && result.one().has("preferred_ip"))
        {
            UntypedResultSet.Row row = result.one();
            return InetAddressAndPort.getByAddressOverrideDefaults(row.getInetAddress("preferred_ip"), row.getInt("preferred_port"));
        }
        return ep;
    }

    /**
     * Return a map of IP addresses containing a map of dc and rack info
     */
    public static Map<InetAddressAndPort, Map<String,String>> loadDcRackInfo()
    {
        Map<InetAddressAndPort, Map<String, String>> result = new HashMap<>();
        for (UntypedResultSet.Row row : executeInternal("SELECT peer, peer_port, data_center, rack from system." + PEERS_V2))
        {
            InetAddress address = row.getInetAddress("peer");
            Integer port = row.getInt("peer_port");
            InetAddressAndPort peer = InetAddressAndPort.getByAddressOverrideDefaults(address, port);
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
    public static CassandraVersion getReleaseVersion(InetAddressAndPort ep)
    {
        try
        {
            if (FBUtilities.getBroadcastAddressAndPort().equals(ep))
            {
                return new CassandraVersion(FBUtilities.getReleaseVersionString());
            }
            String req = "SELECT release_version FROM system.%s WHERE peer=? AND peer_port=?";
            UntypedResultSet result = executeInternal(String.format(req, PEERS_V2), ep.address, ep.port);
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
        UntypedResultSet result = executeInternal(format(req, LOCAL, LOCAL));

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
        UntypedResultSet result = executeInternal(format(req, LOCAL, LOCAL));
        return result.isEmpty() || !result.one().has("tokens")
             ? Collections.<Token>emptyList()
             : deserializeTokens(result.one().getSet("tokens", UTF8Type.instance));
    }

    public static int incrementAndGetGeneration()
    {
        String req = "SELECT gossip_generation FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(format(req, LOCAL, LOCAL));

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
        executeInternal(format(req, LOCAL, LOCAL), generation);
        forceBlockingFlush(LOCAL);

        return generation;
    }

    public static BootstrapState getBootstrapState()
    {
        String req = "SELECT bootstrapped FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(format(req, LOCAL, LOCAL));

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
        if (getBootstrapState() == state)
            return;

        String req = "INSERT INTO system.%s (key, bootstrapped) VALUES ('%s', ?)";
        executeInternal(format(req, LOCAL, LOCAL), state.name());
        forceBlockingFlush(LOCAL);
    }

    public static boolean isIndexBuilt(String keyspaceName, String indexName)
    {
        String req = "SELECT index_name FROM %s.\"%s\" WHERE table_name=? AND index_name=?";
        UntypedResultSet result = executeInternal(format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName, indexName);
        return !result.isEmpty();
    }

    public static void setIndexBuilt(String keyspaceName, String indexName)
    {
        String req = "INSERT INTO %s.\"%s\" (table_name, index_name) VALUES (?, ?) IF NOT EXISTS;";
        executeInternal(String.format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName, indexName);
        forceBlockingFlush(BUILT_INDEXES);
    }

    public static void setIndexRemoved(String keyspaceName, String indexName)
    {
        String req = "DELETE FROM %s.\"%s\" WHERE table_name = ? AND index_name = ? IF EXISTS";
        executeInternal(String.format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName, indexName);
        forceBlockingFlush(BUILT_INDEXES);
    }

    public static List<String> getBuiltIndexes(String keyspaceName, Set<String> indexNames)
    {
        List<String> names = new ArrayList<>(indexNames);
        String req = "SELECT index_name from %s.\"%s\" WHERE table_name=? AND index_name IN ?";
        UntypedResultSet results = executeInternal(format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName, names);
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
        UntypedResultSet result = executeInternal(format(req, LOCAL, LOCAL));

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
        executeInternal(format(req, LOCAL, LOCAL), hostId);
        return hostId;
    }

    /**
     * Gets the stored rack for the local node, or null if none have been set yet.
     */
    public static String getRack()
    {
        String req = "SELECT rack FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(format(req, LOCAL, LOCAL));

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
        UntypedResultSet result = executeInternal(format(req, LOCAL, LOCAL));

        // Look up the Data center (return it if found)
        if (!result.isEmpty() && result.one().has("data_center"))
            return result.one().getString("data_center");

        return null;
    }

    public static PaxosState loadPaxosState(DecoratedKey key, TableMetadata metadata, int nowInSec)
    {
        String req = "SELECT * FROM system.%s WHERE row_key = ? AND cf_id = ?";
        UntypedResultSet results = QueryProcessor.executeInternalWithNow(nowInSec, System.nanoTime(), format(req, PAXOS), key.getKey(), metadata.id.asUUID());
        if (results.isEmpty())
            return new PaxosState(key, metadata);
        UntypedResultSet.Row row = results.one();

        Commit promised = row.has("in_progress_ballot")
                        ? new Commit(row.getUUID("in_progress_ballot"), new PartitionUpdate.Builder(metadata, key, metadata.regularAndStaticColumns(), 1).build())
                        : Commit.emptyCommit(key, metadata);
        // either we have both a recently accepted ballot and update or we have neither
        Commit accepted = row.has("proposal_version") && row.has("proposal")
                        ? new Commit(row.getUUID("proposal_ballot"),
                                     PartitionUpdate.fromBytes(row.getBytes("proposal"), row.getInt("proposal_version")))
                        : Commit.emptyCommit(key, metadata);
        // either most_recent_commit and most_recent_commit_at will both be set, or neither
        Commit mostRecent = row.has("most_recent_commit_version") && row.has("most_recent_commit")
                          ? new Commit(row.getUUID("most_recent_commit_at"),
                                       PartitionUpdate.fromBytes(row.getBytes("most_recent_commit"), row.getInt("most_recent_commit_version")))
                          : Commit.emptyCommit(key, metadata);
        return new PaxosState(promised, accepted, mostRecent);
    }

    public static void savePaxosPromise(Commit promise)
    {
        String req = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET in_progress_ballot = ? WHERE row_key = ? AND cf_id = ?";
        executeInternal(format(req, PAXOS),
                        UUIDGen.microsTimestamp(promise.ballot),
                        paxosTtlSec(promise.update.metadata()),
                        promise.ballot,
                        promise.update.partitionKey().getKey(),
                        promise.update.metadata().id.asUUID());
    }

    public static void savePaxosProposal(Commit proposal)
    {
        executeInternal(format("UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET proposal_ballot = ?, proposal = ?, proposal_version = ? WHERE row_key = ? AND cf_id = ?", PAXOS),
                        UUIDGen.microsTimestamp(proposal.ballot),
                        paxosTtlSec(proposal.update.metadata()),
                        proposal.ballot,
                        PartitionUpdate.toBytes(proposal.update, MessagingService.current_version),
                        MessagingService.current_version,
                        proposal.update.partitionKey().getKey(),
                        proposal.update.metadata().id.asUUID());
    }

    public static int paxosTtlSec(TableMetadata metadata)
    {
        // keep paxos state around for at least 3h
        return Math.max(3 * 3600, metadata.params.gcGraceSeconds);
    }

    public static void savePaxosCommit(Commit commit)
    {
        // We always erase the last proposal (with the commit timestamp to no erase more recent proposal in case the commit is old)
        // even though that's really just an optimization  since SP.beginAndRepairPaxos will exclude accepted proposal older than the mrc.
        String cql = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET proposal_ballot = null, proposal = null, most_recent_commit_at = ?, most_recent_commit = ?, most_recent_commit_version = ? WHERE row_key = ? AND cf_id = ?";
        executeInternal(format(cql, PAXOS),
                        UUIDGen.microsTimestamp(commit.ballot),
                        paxosTtlSec(commit.update.metadata()),
                        commit.ballot,
                        PartitionUpdate.toBytes(commit.update, MessagingService.current_version),
                        MessagingService.current_version,
                        commit.update.partitionKey().getKey(),
                        commit.update.metadata().id.asUUID());
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
        UntypedResultSet results = executeInternal(format(cql, SSTABLE_ACTIVITY), keyspace, table, generation);

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
        executeInternal(format(cql, SSTABLE_ACTIVITY),
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
        executeInternal(format(cql, SSTABLE_ACTIVITY), keyspace, table, generation);
    }

    /**
     * Writes the current partition count and size estimates into SIZE_ESTIMATES_CF
     */
    public static void updateSizeEstimates(String keyspace, String table, Map<Range<Token>, Pair<Long, Long>> estimates)
    {
        long timestamp = FBUtilities.timestampMicros();
        PartitionUpdate.Builder update = new PartitionUpdate.Builder(SizeEstimates, UTF8Type.instance.decompose(keyspace), SizeEstimates.regularAndStaticColumns(), estimates.size());
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
        new Mutation(update.build()).apply();
    }

    /**
     * Clears size estimates for a table (on table drop)
     */
    public static void clearSizeEstimates(String keyspace, String table)
    {
        String cql = format("DELETE FROM %s WHERE keyspace_name = ? AND table_name = ?", SizeEstimates.toString());
        executeInternal(cql, keyspace, table);
    }

    public static synchronized void updateAvailableRanges(String keyspace, Collection<Range<Token>> completedRanges)
    {
        String cql = "UPDATE system.%s SET ranges = ranges + ? WHERE keyspace_name = ?";
        Set<ByteBuffer> rangesToUpdate = new HashSet<>(completedRanges.size());
        for (Range<Token> range : completedRanges)
        {
            rangesToUpdate.add(rangeToBytes(range));
        }
        executeInternal(format(cql, AVAILABLE_RANGES), rangesToUpdate, keyspace);
    }

    public static synchronized Set<Range<Token>> getAvailableRanges(String keyspace, IPartitioner partitioner)
    {
        Set<Range<Token>> result = new HashSet<>();
        String query = "SELECT * FROM system.%s WHERE keyspace_name=?";
        UntypedResultSet rs = executeInternal(format(query, AVAILABLE_RANGES), keyspace);
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

    public static synchronized void updateTransferredRanges(StreamOperation streamOperation,
                                                         InetAddressAndPort peer,
                                                         String keyspace,
                                                         Collection<Range<Token>> streamedRanges)
    {
        String cql = "UPDATE system.%s SET ranges = ranges + ? WHERE operation = ? AND peer = ? AND keyspace_name = ?";
        Set<ByteBuffer> rangesToUpdate = new HashSet<>(streamedRanges.size());
        for (Range<Token> range : streamedRanges)
        {
            rangesToUpdate.add(rangeToBytes(range));
        }
        executeInternal(format(cql, LEGACY_TRANSFERRED_RANGES), rangesToUpdate, streamOperation.getDescription(), peer.address, keyspace);
        cql = "UPDATE system.%s SET ranges = ranges + ? WHERE operation = ? AND peer = ? AND peer_port = ? AND keyspace_name = ?";
        executeInternal(String.format(cql, TRANSFERRED_RANGES_V2), rangesToUpdate, streamOperation.getDescription(), peer.address, peer.port, keyspace);
    }

    public static synchronized Map<InetAddressAndPort, Set<Range<Token>>> getTransferredRanges(String description, String keyspace, IPartitioner partitioner)
    {
        Map<InetAddressAndPort, Set<Range<Token>>> result = new HashMap<>();
        String query = "SELECT * FROM system.%s WHERE operation = ? AND keyspace_name = ?";
        UntypedResultSet rs = executeInternal(String.format(query, TRANSFERRED_RANGES_V2), description, keyspace);
        for (UntypedResultSet.Row row : rs)
        {
            InetAddress peerAddress = row.getInetAddress("peer");
            int port = row.getInt("peer_port");
            InetAddressAndPort peer = InetAddressAndPort.getByAddressOverrideDefaults(peerAddress, port);
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
            String snapshotName = Keyspace.getTimestampedSnapshotName(format("upgrade-%s-%s",
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
        UntypedResultSet result = executeInternal(format(req, SystemKeyspace.LOCAL, SystemKeyspace.LOCAL));
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

    private static ByteBuffer rangeToBytes(Range<Token> range)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            // The format with which token ranges are serialized in the system tables is the pre-3.0 serialization
            // formot for ranges, so we should maintain that for now. And while we don't really support pre-3.0
            // messaging versions, we know AbstractBounds.Serializer still support it _exactly_ for this use case, so we
            // pass 0 as the version to trigger that legacy code.
            // In the future, it might be worth switching to a stable text format for the ranges to 1) save that and 2)
            // be more user friendly (the serialization format we currently use is pretty custom).
            Range.tokenSerializer.serialize(range, out, 0);
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
            // See rangeToBytes above for why version is 0.
            return (Range<Token>) Range.tokenSerializer.deserialize(ByteStreams.newDataInput(ByteBufferUtil.getArray(rawRange)),
                                                                    partitioner,
                                                                    0);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public static void writePreparedStatement(String loggedKeyspace, MD5Digest key, String cql)
    {
        executeInternal(format("INSERT INTO %s (logged_keyspace, prepared_id, query_string) VALUES (?, ?, ?)",
                               PreparedStatements.toString()),
                        loggedKeyspace, key.byteBuffer(), cql);
        logger.debug("stored prepared statement for logged keyspace '{}': '{}'", loggedKeyspace, cql);
    }

    public static void removePreparedStatement(MD5Digest key)
    {
        executeInternal(format("DELETE FROM %s WHERE prepared_id = ?", PreparedStatements.toString()),
                        key.byteBuffer());
    }

    public static void resetPreparedStatements()
    {
        ColumnFamilyStore availableRanges = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(PREPARED_STATEMENTS);
        availableRanges.truncateBlocking();
    }

    public static List<Pair<String, String>> loadPreparedStatements()
    {
        String query = format("SELECT logged_keyspace, query_string FROM %s", PreparedStatements.toString());
        UntypedResultSet resultSet = executeOnceInternal(query);
        List<Pair<String, String>> r = new ArrayList<>();
        for (UntypedResultSet.Row row : resultSet)
            r.add(Pair.create(row.has("logged_keyspace") ? row.getString("logged_keyspace") : null,
                              row.getString("query_string")));
        return r;
    }
}
