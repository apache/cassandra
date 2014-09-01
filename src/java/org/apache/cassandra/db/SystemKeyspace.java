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

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

import com.google.common.base.Function;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.compaction.CompactionHistoryTabularData;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.LegacySchemaTables;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.thrift.cassandraConstants;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;

public final class SystemKeyspace
{
    private static final Logger logger = LoggerFactory.getLogger(SystemKeyspace.class);

    // Used to indicate that there was a previous version written to the legacy (pre 1.2)
    // system.Versions table, but that we cannot read it. Suffice to say, any upgrade should
    // proceed through 1.2.x before upgrading to the current version.
    public static final CassandraVersion UNREADABLE_VERSION = new CassandraVersion("0.0.0-unknown");

    // Used to indicate that no previous version information was found. When encountered, we assume that
    // Cassandra was not previously installed and we're in the process of starting a fresh node.
    public static final CassandraVersion NULL_VERSION = new CassandraVersion("0.0.0-absent");

    public static final String NAME = "system";

    public static final String HINTS = "hints";
    public static final String BATCHLOG = "batchlog";
    public static final String PAXOS = "paxos";
    public static final String BUILT_INDEXES = "IndexInfo";
    public static final String LOCAL = "local";
    public static final String PEERS = "peers";
    public static final String PEER_EVENTS = "peer_events";
    public static final String RANGE_XFERS = "range_xfers";
    public static final String COMPACTIONS_IN_PROGRESS = "compactions_in_progress";
    public static final String COMPACTION_HISTORY = "compaction_history";
    public static final String SSTABLE_ACTIVITY = "sstable_activity";
    public static final String SIZE_ESTIMATES = "size_estimates";
    public static final String AVAILABLE_RANGES = "available_ranges";

    public static final CFMetaData Hints =
        compile(HINTS,
                "hints awaiting delivery",
                "CREATE TABLE %s ("
                + "target_id uuid,"
                + "hint_id timeuuid,"
                + "message_version int,"
                + "mutation blob,"
                + "PRIMARY KEY ((target_id), hint_id, message_version)) "
                + "WITH COMPACT STORAGE")
                .compactionStrategyOptions(Collections.singletonMap("enabled", "false"))
                .gcGraceSeconds(0);

    public static final CFMetaData Batchlog =
        compile(BATCHLOG,
                "batches awaiting replay",
                "CREATE TABLE %s ("
                + "id uuid,"
                + "data blob,"
                + "version int,"
                + "written_at timestamp,"
                + "PRIMARY KEY ((id)))")
                .compactionStrategyOptions(Collections.singletonMap("min_threshold", "2"))
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
                .compactionStrategyClass(LeveledCompactionStrategy.class);

    // TODO: make private
    public static final CFMetaData BuiltIndexes =
        compile(BUILT_INDEXES,
                "built column indexes",
                "CREATE TABLE \"%s\" ("
                + "table_name text,"
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

    private static final CFMetaData CompactionsInProgress =
        compile(COMPACTIONS_IN_PROGRESS,
                "unfinished compactions",
                "CREATE TABLE %s ("
                + "id uuid,"
                + "columnfamily_name text,"
                + "inputs set<int>,"
                + "keyspace_name text,"
                + "PRIMARY KEY ((id)))");

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
                "Available keyspace/ranges during bootstrap/replace that are ready to be served",
                "CREATE TABLE %s ("
                        + "keyspace_name text PRIMARY KEY,"
                        + "ranges set<blob>"
                        + ")");

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), NAME)
                         .comment(description);
    }

    public static KSMetaData definition()
    {
        Iterable<CFMetaData> tables =
            Iterables.concat(LegacySchemaTables.All,
                             Arrays.asList(BuiltIndexes,
                                           Hints,
                                           Batchlog,
                                           Paxos,
                                           Local,
                                           Peers,
                                           PeerEvents,
                                           RangeXfers,
                                           CompactionsInProgress,
                                           CompactionHistory,
                                           SSTableActivity,
                                           SizeEstimates,
                                           AvailableRanges));
        return new KSMetaData(NAME, LocalStrategy.class, Collections.<String, String>emptyMap(), true, tables);
    }

    private static volatile Map<UUID, Pair<ReplayPosition, Long>> truncationRecords;

    public enum BootstrapState
    {
        NEEDS_BOOTSTRAP,
        COMPLETED,
        IN_PROGRESS,
        DECOMMISSIONED
    }

    private static DecoratedKey decorate(ByteBuffer key)
    {
        return StorageService.getPartitioner().decorateKey(key);
    }

    public static void finishStartup()
    {
        persistLocalMetadata();
        LegacySchemaTables.saveSystemKeyspaceSchema();
    }

    private static void persistLocalMetadata()
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
                     "broadcast_address" +
                     ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        executeOnceInternal(String.format(req, LOCAL),
                            LOCAL,
                            DatabaseDescriptor.getClusterName(),
                            FBUtilities.getReleaseVersionString(),
                            QueryProcessor.CQL_VERSION.toString(),
                            cassandraConstants.VERSION,
                            String.valueOf(Server.CURRENT_VERSION),
                            snitch.getDatacenter(FBUtilities.getBroadcastAddress()),
                            snitch.getRack(FBUtilities.getBroadcastAddress()),
                            DatabaseDescriptor.getPartitioner().getClass().getName(),
                            DatabaseDescriptor.getRpcAddress(),
                            FBUtilities.getBroadcastAddress());
    }

    /**
     * Write compaction log, except columfamilies under system keyspace.
     *
     * @param cfs cfs to compact
     * @param toCompact sstables to compact
     * @return compaction task id or null if cfs is under system keyspace
     */
    public static UUID startCompaction(ColumnFamilyStore cfs, Iterable<SSTableReader> toCompact)
    {
        if (NAME.equals(cfs.keyspace.getName()))
            return null;

        UUID compactionId = UUIDGen.getTimeUUID();
        Iterable<Integer> generations = Iterables.transform(toCompact, new Function<SSTableReader, Integer>()
        {
            public Integer apply(SSTableReader sstable)
            {
                return sstable.descriptor.generation;
            }
        });
        String req = "INSERT INTO system.%s (id, keyspace_name, columnfamily_name, inputs) VALUES (?, ?, ?, ?)";
        executeInternal(String.format(req, COMPACTIONS_IN_PROGRESS), compactionId, cfs.keyspace.getName(), cfs.name, Sets.newHashSet(generations));
        forceBlockingFlush(COMPACTIONS_IN_PROGRESS);
        return compactionId;
    }

    /**
     * Deletes the entry for this compaction from the set of compactions in progress.  The compaction does not need
     * to complete successfully for this to be called.
     * @param taskId what was returned from {@code startCompaction}
     */
    public static void finishCompaction(UUID taskId)
    {
        assert taskId != null;

        executeInternal(String.format("DELETE FROM system.%s WHERE id = ?", COMPACTIONS_IN_PROGRESS), taskId);
        forceBlockingFlush(COMPACTIONS_IN_PROGRESS);
    }

    /**
     * Returns a Map whose keys are KS.CF pairs and whose values are maps from sstable generation numbers to the
     * task ID of the compaction they were participating in.
     */
    public static Map<Pair<String, String>, Map<Integer, UUID>> getUnfinishedCompactions()
    {
        String req = "SELECT * FROM system.%s";
        UntypedResultSet resultSet = executeInternal(String.format(req, COMPACTIONS_IN_PROGRESS));

        Map<Pair<String, String>, Map<Integer, UUID>> unfinishedCompactions = new HashMap<>();
        for (UntypedResultSet.Row row : resultSet)
        {
            String keyspace = row.getString("keyspace_name");
            String columnfamily = row.getString("columnfamily_name");
            Set<Integer> inputs = row.getSet("inputs", Int32Type.instance);
            UUID taskID = row.getUUID("id");

            Pair<String, String> kscf = Pair.create(keyspace, columnfamily);
            Map<Integer, UUID> generationToTaskID = unfinishedCompactions.get(kscf);
            if (generationToTaskID == null)
                generationToTaskID = new HashMap<>(inputs.size());

            for (Integer generation : inputs)
                generationToTaskID.put(generation, taskID);

            unfinishedCompactions.put(kscf, generationToTaskID);
        }
        return unfinishedCompactions;
    }

    public static void discardCompactionsInProgress()
    {
        ColumnFamilyStore compactionLog = Keyspace.open(NAME).getColumnFamilyStore(COMPACTIONS_IN_PROGRESS);
        compactionLog.truncateBlocking();
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
        executeInternal(String.format(req, COMPACTION_HISTORY), UUIDGen.getTimeUUID(), ksname, cfname, ByteBufferUtil.bytes(compactedAt), bytesIn, bytesOut, rowsMerged);
    }

    public static TabularData getCompactionHistory() throws OpenDataException
    {
        UntypedResultSet queryResultSet = executeInternal(String.format("SELECT * from system.%s", COMPACTION_HISTORY));
        return CompactionHistoryTabularData.from(queryResultSet);
    }

    public static synchronized void saveTruncationRecord(ColumnFamilyStore cfs, long truncatedAt, ReplayPosition position)
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

    private static Map<UUID, ByteBuffer> truncationAsMapEntry(ColumnFamilyStore cfs, long truncatedAt, ReplayPosition position)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReplayPosition.serializer.serialize(position, out);
            out.writeLong(truncatedAt);
            return Collections.singletonMap(cfs.metadata.cfId, ByteBuffer.wrap(out.getData(), 0, out.getLength()));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static ReplayPosition getTruncatedPosition(UUID cfId)
    {
        Pair<ReplayPosition, Long> record = getTruncationRecord(cfId);
        return record == null ? null : record.left;
    }

    public static long getTruncatedAt(UUID cfId)
    {
        Pair<ReplayPosition, Long> record = getTruncationRecord(cfId);
        return record == null ? Long.MIN_VALUE : record.right;
    }

    private static synchronized Pair<ReplayPosition, Long> getTruncationRecord(UUID cfId)
    {
        if (truncationRecords == null)
            truncationRecords = readTruncationRecords();
        return truncationRecords.get(cfId);
    }

    private static Map<UUID, Pair<ReplayPosition, Long>> readTruncationRecords()
    {
        UntypedResultSet rows = executeInternal(String.format("SELECT truncated_at FROM system.%s WHERE key = '%s'", LOCAL, LOCAL));

        Map<UUID, Pair<ReplayPosition, Long>> records = new HashMap<>();

        if (!rows.isEmpty() && rows.one().has("truncated_at"))
        {
            Map<UUID, ByteBuffer> map = rows.one().getMap("truncated_at", UUIDType.instance, BytesType.instance);
            for (Map.Entry<UUID, ByteBuffer> entry : map.entrySet())
                records.put(entry.getKey(), truncationRecordFromBlob(entry.getValue()));
        }

        return records;
    }

    private static Pair<ReplayPosition, Long> truncationRecordFromBlob(ByteBuffer bytes)
    {
        try
        {
            DataInputStream in = new DataInputStream(ByteBufferUtil.inputStream(bytes));
            return Pair.create(ReplayPosition.serializer.deserialize(in), in.available() > 0 ? in.readLong() : Long.MIN_VALUE);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Record tokens being used by another node
     */
    public static synchronized void updateTokens(InetAddress ep, Collection<Token> tokens)
    {
        if (ep.equals(FBUtilities.getBroadcastAddress()))
        {
            removeEndpoint(ep);
            return;
        }

        String req = "INSERT INTO system.%s (peer, tokens) VALUES (?, ?)";
        executeInternal(String.format(req, PEERS), ep, tokensAsSet(tokens));
    }

    public static synchronized void updatePreferredIP(InetAddress ep, InetAddress preferred_ip)
    {
        String req = "INSERT INTO system.%s (peer, preferred_ip) VALUES (?, ?)";
        executeInternal(String.format(req, PEERS), ep, preferred_ip);
        forceBlockingFlush(PEERS);
    }

    public static synchronized void updatePeerInfo(InetAddress ep, String columnName, Object value)
    {
        if (ep.equals(FBUtilities.getBroadcastAddress()))
            return;

        String req = "INSERT INTO system.%s (peer, %s) VALUES (?, ?)";
        executeInternal(String.format(req, PEERS, columnName), ep, value);
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
        Token.TokenFactory factory = StorageService.getPartitioner().getTokenFactory();
        Set<String> s = new HashSet<>(tokens.size());
        for (Token tk : tokens)
            s.add(factory.toString(tk));
        return s;
    }

    private static Collection<Token> deserializeTokens(Collection<String> tokensStrings)
    {
        Token.TokenFactory factory = StorageService.getPartitioner().getTokenFactory();
        List<Token> tokens = new ArrayList<>(tokensStrings.size());
        for (String tk : tokensStrings)
            tokens.add(factory.fromString(tk));
        return tokens;
    }

    /**
     * Remove stored tokens being used by another node
     */
    public static synchronized void removeEndpoint(InetAddress ep)
    {
        String req = "DELETE FROM system.%s WHERE peer = ?";
        executeInternal(String.format(req, PEERS), ep);
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

    /**
     * Convenience method to update the list of tokens in the local system keyspace.
     *
     * @param addTokens tokens to add
     * @param rmTokens tokens to remove
     * @return the collection of persisted tokens
     */
    public static synchronized Collection<Token> updateLocalTokens(Collection<Token> addTokens, Collection<Token> rmTokens)
    {
        Collection<Token> tokens = getSavedTokens();
        tokens.removeAll(rmTokens);
        tokens.addAll(addTokens);
        updateTokens(tokens);
        return tokens;
    }

    public static void forceBlockingFlush(String cfname)
    {
        if (!Boolean.getBoolean("cassandra.unsafesystem"))
            FBUtilities.waitOnFuture(Keyspace.open(NAME).getColumnFamilyStore(cfname).forceFlush());
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
            keyspace = Keyspace.open(NAME);
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
            if (!cfs.getSSTables().isEmpty())
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
        UntypedResultSet result = executeInternal(String.format(req, NAME, BUILT_INDEXES), keyspaceName, indexName);
        return !result.isEmpty();
    }

    public static void setIndexBuilt(String keyspaceName, String indexName)
    {
        String req = "INSERT INTO %s.\"%s\" (table_name, index_name) VALUES (?, ?)";
        executeInternal(String.format(req, NAME, BUILT_INDEXES), keyspaceName, indexName);
        forceBlockingFlush(BUILT_INDEXES);
    }

    public static void setIndexRemoved(String keyspaceName, String indexName)
    {
        String req = "DELETE FROM %s.\"%s\" WHERE table_name = ? AND index_name = ?";
        executeInternal(String.format(req, NAME, BUILT_INDEXES), keyspaceName, indexName);
        forceBlockingFlush(BUILT_INDEXES);
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


    public static PaxosState loadPaxosState(DecoratedKey key, CFMetaData metadata)
    {
        String req = "SELECT * FROM system.%s WHERE row_key = ? AND cf_id = ?";
        UntypedResultSet results = executeInternal(String.format(req, PAXOS), key.getKey(), metadata.cfId);
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
                        paxosTtl(promise.update.metadata()),
                        promise.ballot,
                        promise.update.partitionKey().getKey(),
                        promise.update.metadata().cfId);
    }

    public static void savePaxosProposal(Commit proposal)
    {
        executeInternal(String.format("UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET proposal_ballot = ?, proposal = ?, proposal_version = ? WHERE row_key = ? AND cf_id = ?", PAXOS),
                        UUIDGen.microsTimestamp(proposal.ballot),
                        paxosTtl(proposal.update.metadata()),
                        proposal.ballot,
                        PartitionUpdate.toBytes(proposal.update, MessagingService.current_version),
                        MessagingService.current_version,
                        proposal.update.partitionKey().getKey(),
                        proposal.update.metadata().cfId);
    }

    private static int paxosTtl(CFMetaData metadata)
    {
        // keep paxos state around for at least 3h
        return Math.max(3 * 3600, metadata.getGcGraceSeconds());
    }

    public static void savePaxosCommit(Commit commit)
    {
        // We always erase the last proposal (with the commit timestamp to no erase more recent proposal in case the commit is old)
        // even though that's really just an optimization  since SP.beginAndRepairPaxos will exclude accepted proposal older than the mrc.
        String cql = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET proposal_ballot = null, proposal = null, most_recent_commit_at = ?, most_recent_commit = ?, most_recent_commit_version = ? WHERE row_key = ? AND cf_id = ?";
        executeInternal(String.format(cql, PAXOS),
                        UUIDGen.microsTimestamp(commit.ballot),
                        paxosTtl(commit.update.metadata()),
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
        DecoratedKey key = decorate(UTF8Type.instance.decompose(keyspace));
        PartitionUpdate update = new PartitionUpdate(SizeEstimates, key, SizeEstimates.partitionColumns(), estimates.size());
        Mutation mutation = new Mutation(update);

        // delete all previous values with a single range tombstone.
        int nowInSec = FBUtilities.nowInSeconds();
        update.addRangeTombstone(Slice.make(SizeEstimates.comparator, table), new SimpleDeletionTime(timestamp - 1, nowInSec));

        // add a CQL row for each primary token range.
        for (Map.Entry<Range<Token>, Pair<Long, Long>> entry : estimates.entrySet())
        {
            Range<Token> range = entry.getKey();
            Pair<Long, Long> values = entry.getValue();
            new RowUpdateBuilder(SizeEstimates, timestamp, mutation)
                .clustering(table, range.left.toString(), range.right.toString())
                .add("partitions_count", values.left)
                .add("mean_partition_size", values.right)
                .build();
        }

        mutation.apply();
    }

    /**
     * Clears size estimates for a table (on table drop)
     */
    public static void clearSizeEstimates(String keyspace, String table)
    {
        String cql = String.format("DELETE FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", NAME, SIZE_ESTIMATES);
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
        ColumnFamilyStore availableRanges = Keyspace.open(NAME).getColumnFamilyStore(AVAILABLE_RANGES);
        availableRanges.truncateBlocking();
    }

    /**
     * Compare the release version in the system.local table with the one included in the distro.
     * If they don't match, snapshot all tables in the system keyspace. This is intended to be
     * called at startup to create a backup of the system tables during an upgrade
     *
     * @throws IOException
     */
    public static void snapshotOnVersionChange() throws IOException
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
            Keyspace systemKs = Keyspace.open(SystemKeyspace.NAME);
            systemKs.snapshot(snapshotName, null);
        }
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
            for (File dataDirectory : Directories.getKSChildDirectories(SystemKeyspace.NAME))
            {
                if (dataDirectory.getName().equals("Versions") && dataDirectory.listFiles().length > 0)
                {
                    logger.debug("Found unreadable versions info in pre 1.2 system.Versions table");
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

}
