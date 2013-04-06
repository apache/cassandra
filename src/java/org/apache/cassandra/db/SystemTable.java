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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.Constants;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.cql3.QueryProcessor.processInternal;

public class SystemTable
{
    private static final Logger logger = LoggerFactory.getLogger(SystemTable.class);

    // see CFMetaData for schema definitions
    public static final String PEERS_CF = "peers";
    public static final String PEER_EVENTS_CF = "peer_events";
    public static final String LOCAL_CF = "local";
    public static final String INDEX_CF = "IndexInfo";
    public static final String COUNTER_ID_CF = "NodeIdInfo";
    public static final String HINTS_CF = "hints";
    public static final String RANGE_XFERS_CF = "range_xfers";
    public static final String BATCHLOG_CF = "batchlog";
    // see layout description in the DefsTable class header
    public static final String SCHEMA_KEYSPACES_CF = "schema_keyspaces";
    public static final String SCHEMA_COLUMNFAMILIES_CF = "schema_columnfamilies";
    public static final String SCHEMA_COLUMNS_CF = "schema_columns";

    @Deprecated
    public static final String OLD_STATUS_CF = "LocationInfo";
    @Deprecated
    public static final String OLD_HINTS_CF = "HintsColumnFamily";

    private static final String LOCAL_KEY = "local";
    private static final ByteBuffer ALL_LOCAL_NODE_ID_KEY = ByteBufferUtil.bytes("Local");

    public enum BootstrapState
    {
        NEEDS_BOOTSTRAP,
        COMPLETED,
        IN_PROGRESS
    }

    private static DecoratedKey decorate(ByteBuffer key)
    {
        return StorageService.getPartitioner().decorateKey(key);
    }

    public static void finishStartup()
    {
        DefsTable.fixSchemaNanoTimestamps();
        setupVersion();
        try
        {
            upgradeSystemData();
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        // add entries to system schema columnfamilies for the hardcoded system definitions
        for (String ksname : Schema.systemKeyspaceNames)
        {
            KSMetaData ksmd = Schema.instance.getKSMetaData(ksname);

            // delete old, possibly obsolete entries in schema columnfamilies
            for (String cfname : Arrays.asList(SystemTable.SCHEMA_KEYSPACES_CF, SystemTable.SCHEMA_COLUMNFAMILIES_CF, SystemTable.SCHEMA_COLUMNS_CF))
            {
                String req = String.format("DELETE FROM system.%s WHERE keyspace_name = '%s'", cfname, ksmd.name);
                processInternal(req);
            }

            // (+1 to timestamp to make sure we don't get shadowed by the tombstones we just added)
            ksmd.toSchema(FBUtilities.timestampMicros() + 1).apply();
        }
    }

    private static void setupVersion()
    {
        String req = "INSERT INTO system.%s (key, release_version, cql_version, thrift_version, data_center, rack, partitioner) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')";
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        processInternal(String.format(req, LOCAL_CF,
                                         LOCAL_KEY,
                                         FBUtilities.getReleaseVersionString(),
                                         QueryProcessor.CQL_VERSION.toString(),
                                         Constants.VERSION,
                                         snitch.getDatacenter(FBUtilities.getBroadcastAddress()),
                                         snitch.getRack(FBUtilities.getBroadcastAddress()),
                                         DatabaseDescriptor.getPartitioner().getClass().getName()));
    }

    /** if system data becomes incompatible across versions of cassandra, that logic (and associated purging) is managed here */
    private static void upgradeSystemData() throws ExecutionException, InterruptedException
    {
        Table table = Table.open(Table.SYSTEM_KS);
        ColumnFamilyStore oldStatusCfs = table.getColumnFamilyStore(OLD_STATUS_CF);
        if (oldStatusCfs.getSSTables().size() > 0)
        {
            SortedSet<ByteBuffer> cols = new TreeSet<ByteBuffer>(BytesType.instance);
            cols.add(ByteBufferUtil.bytes("ClusterName"));
            cols.add(ByteBufferUtil.bytes("Token"));
            QueryFilter filter = QueryFilter.getNamesFilter(decorate(ByteBufferUtil.bytes("L")), new QueryPath(OLD_STATUS_CF), cols);
            ColumnFamily oldCf = oldStatusCfs.getColumnFamily(filter);
            Iterator<IColumn> oldColumns = oldCf.columns.iterator();

            String clusterName = null;
            try
            {
                clusterName = ByteBufferUtil.string(oldColumns.next().value());
            }
            catch (CharacterCodingException e)
            {
                throw new RuntimeException(e);
            }
            // serialize the old token as a collection of (one )tokens.
            Token token = StorageService.getPartitioner().getTokenFactory().fromByteArray(oldColumns.next().value());
            String tokenBytes = tokensAsSet(Collections.singleton(token));
            // (assume that any node getting upgraded was bootstrapped, since that was stored in a separate row for no particular reason)
            String req = "INSERT INTO system.%s (key, cluster_name, tokens, bootstrapped) VALUES ('%s', '%s', %s, '%s')";
            processInternal(String.format(req, LOCAL_CF, LOCAL_KEY, clusterName, tokenBytes, BootstrapState.COMPLETED.name()));

            oldStatusCfs.truncate();
        }

        ColumnFamilyStore oldHintsCfs = table.getColumnFamilyStore(OLD_HINTS_CF);
        if (oldHintsCfs.getSSTables().size() > 0)
        {
            logger.info("Possible old-format hints found. Truncating");
            oldHintsCfs.truncate();
        }
    }

    public static void saveTruncationPosition(ColumnFamilyStore cfs, ReplayPosition position)
    {
        String req = "UPDATE system.%s SET truncated_at = truncated_at + %s WHERE key = '%s'";
        processInternal(String.format(req, LOCAL_CF, positionAsMapEntry(cfs, position), LOCAL_KEY));
        forceBlockingFlush(LOCAL_CF);
    }

    private static String positionAsMapEntry(ColumnFamilyStore cfs, ReplayPosition position)
    {
        DataOutputBuffer out = new DataOutputBuffer();
        try
        {
            ReplayPosition.serializer.serialize(position, out);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return String.format("{%s: 0x%s}",
                             cfs.metadata.cfId,
                             ByteBufferUtil.bytesToHex(ByteBuffer.wrap(out.getData(), 0, out.getLength())));
    }

    public static Map<UUID, ReplayPosition> getTruncationPositions()
    {
        String req = "SELECT truncated_at FROM system.%s WHERE key = '%s'";
        UntypedResultSet rows = processInternal(String.format(req, LOCAL_CF, LOCAL_KEY));
        if (rows.isEmpty())
            return Collections.emptyMap();

        UntypedResultSet.Row row = rows.one();
        Map<UUID, ByteBuffer> rawMap = row.getMap("truncated_at", UUIDType.instance, BytesType.instance);
        if (rawMap == null)
            return Collections.emptyMap();

        Map<UUID, ReplayPosition> positions = new HashMap<UUID, ReplayPosition>();
        for (Map.Entry<UUID, ByteBuffer> entry : rawMap.entrySet())
        {
            positions.put(entry.getKey(), positionFromBlob(entry.getValue()));
        }
        return positions;
    }

    private static ReplayPosition positionFromBlob(ByteBuffer bytes)
    {
        try
        {
            return ReplayPosition.serializer.deserialize(new DataInputStream(ByteBufferUtil.inputStream(bytes)));
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

        String req = "INSERT INTO system.%s (peer, tokens) VALUES ('%s', %s)";
        processInternal(String.format(req, PEERS_CF, ep.getHostAddress(), tokensAsSet(tokens)));
        forceBlockingFlush(PEERS_CF);
    }

    public static synchronized void updatePeerInfo(InetAddress ep, String columnName, String value)
    {
        if (ep.equals(FBUtilities.getBroadcastAddress()))
            return;

        String req = "INSERT INTO system.%s (peer, %s) VALUES ('%s', %s)";
        processInternal(String.format(req, PEERS_CF, columnName, ep.getHostAddress(), value));
    }

    public static synchronized void updateHintsDropped(InetAddress ep, UUID timePeriod, int value)
    {
        // with 30 day TTL
        String req = "UPDATE system.%s USING TTL 2592000 SET hints_dropped[ %s ] = %s WHERE peer = '%s'";
        processInternal(String.format(req, PEER_EVENTS_CF, timePeriod.toString(), value, ep.getHostAddress()));
    }

    public static synchronized void updateSchemaVersion(UUID version)
    {
        String req = "INSERT INTO system.%s (key, schema_version) VALUES ('%s', %s)";
        processInternal(String.format(req, LOCAL_CF, LOCAL_KEY, version.toString()));
    }

    private static String tokensAsSet(Collection<Token> tokens)
    {
        Token.TokenFactory factory = StorageService.getPartitioner().getTokenFactory();
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        Iterator<Token> iter = tokens.iterator();
        while (iter.hasNext())
        {
            sb.append("'").append(factory.toString(iter.next())).append("'");
            if (iter.hasNext())
                sb.append(",");
        }
        sb.append("}");
        return sb.toString();
    }

    private static Collection<Token> deserializeTokens(Collection<String> tokensStrings)
    {
        Token.TokenFactory factory = StorageService.getPartitioner().getTokenFactory();
        List<Token> tokens = new ArrayList<Token>(tokensStrings.size());
        for (String tk : tokensStrings)
            tokens.add(factory.fromString(tk));
        return tokens;
    }

    /**
     * Remove stored tokens being used by another node
     */
    public static synchronized void removeEndpoint(InetAddress ep)
    {
        String req = "DELETE FROM system.%s WHERE peer = '%s'";
        processInternal(String.format(req, PEERS_CF, ep.getHostAddress()));
        forceBlockingFlush(PEERS_CF);
    }

    /**
     * This method is used to update the System Table with the new tokens for this node
    */
    public static synchronized void updateTokens(Collection<Token> tokens)
    {
        assert !tokens.isEmpty() : "removeEndpoint should be used instead";
        String req = "INSERT INTO system.%s (key, tokens) VALUES ('%s', %s)";
        processInternal(String.format(req, LOCAL_CF, LOCAL_KEY, tokensAsSet(tokens)));
        forceBlockingFlush(LOCAL_CF);
    }

    /**
     * Convenience method to update the list of tokens in the local system table.
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

    private static void forceBlockingFlush(String cfname)
    {
        try
        {
            Table.open(Table.SYSTEM_KS).getColumnFamilyStore(cfname).forceBlockingFlush();
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    /**
     * Return a map of stored tokens to IP addresses
     *
     */
    public static SetMultimap<InetAddress, Token> loadTokens()
    {
        SetMultimap<InetAddress, Token> tokenMap = HashMultimap.create();
        for (UntypedResultSet.Row row : processInternal("SELECT peer, tokens FROM system." + PEERS_CF))
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
        Map<InetAddress, UUID> hostIdMap = new HashMap<InetAddress, UUID>();
        for (UntypedResultSet.Row row : processInternal("SELECT peer, host_id FROM system." + PEERS_CF))
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
     * Return a map of IP addresses containing a map of dc and rack info
     */
    public static Map<InetAddress, Map<String,String>> loadDcRackInfo()
    {
        Map<InetAddress, Map<String, String>> result = new HashMap<InetAddress, Map<String, String>>();
        for (UntypedResultSet.Row row : processInternal("SELECT peer, data_center, rack from system." + PEERS_CF))
        {
            InetAddress peer = row.getInetAddress("peer");
            if (row.has("data_center") && row.has("rack"))
            {
                Map<String, String> dcRack = new HashMap<String, String>();
                dcRack.put("data_center", row.getString("data_center"));
                dcRack.put("rack", row.getString("rack"));
                result.put(peer, dcRack);
            }
        }
        return result;
    }

    /**
     * One of three things will happen if you try to read the system table:
     * 1. files are present and you can read them: great
     * 2. no files are there: great (new node is assumed)
     * 3. files are present but you can't read them: bad
     * @throws ConfigurationException
     */
    public static void checkHealth() throws ConfigurationException
    {
        Table table;
        try
        {
            table = Table.open(Table.SYSTEM_KS);
        }
        catch (AssertionError err)
        {
            // this happens when a user switches from OPP to RP.
            ConfigurationException ex = new ConfigurationException("Could not read system table!");
            ex.initCause(err);
            throw ex;
        }
        ColumnFamilyStore cfs = table.getColumnFamilyStore(LOCAL_CF);

        String req = "SELECT cluster_name FROM system.%s WHERE key='%s'";
        UntypedResultSet result = processInternal(String.format(req, LOCAL_CF, LOCAL_KEY));

        if (result.isEmpty() || !result.one().has("cluster_name"))
        {
            // this is a brand new node
            if (!cfs.getSSTables().isEmpty())
                throw new ConfigurationException("Found system table files, but they couldn't be loaded!");

            // no system files.  this is a new node.
            req = "INSERT INTO system.%s (key, cluster_name) VALUES ('%s', '%s')";
            processInternal(String.format(req, LOCAL_CF, LOCAL_KEY, DatabaseDescriptor.getClusterName()));
            return;
        }

        String savedClusterName = result.one().getString("cluster_name");
        if (!DatabaseDescriptor.getClusterName().equals(savedClusterName))
            throw new ConfigurationException("Saved cluster name " + savedClusterName + " != configured name " + DatabaseDescriptor.getClusterName());
    }

    public static Collection<Token> getSavedTokens()
    {
        String req = "SELECT tokens FROM system.%s WHERE key='%s'";
        UntypedResultSet result = processInternal(String.format(req, LOCAL_CF, LOCAL_KEY));
        return result.isEmpty() || !result.one().has("tokens")
             ? Collections.<Token>emptyList()
             : deserializeTokens(result.one().<String>getSet("tokens", UTF8Type.instance));
    }

    public static int incrementAndGetGeneration()
    {
        String req = "SELECT gossip_generation FROM system.%s WHERE key='%s'";
        UntypedResultSet result = processInternal(String.format(req, LOCAL_CF, LOCAL_KEY));

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

        req = "INSERT INTO system.%s (key, gossip_generation) VALUES ('%s', %d)";
        processInternal(String.format(req, LOCAL_CF, LOCAL_KEY, generation));
        forceBlockingFlush(LOCAL_CF);

        return generation;
    }

    public static BootstrapState getBootstrapState()
    {
        String req = "SELECT bootstrapped FROM system.%s WHERE key='%s'";
        UntypedResultSet result = processInternal(String.format(req, LOCAL_CF, LOCAL_KEY));

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

    public static void setBootstrapState(BootstrapState state)
    {
        String req = "INSERT INTO system.%s (key, bootstrapped) VALUES ('%s', '%s')";
        processInternal(String.format(req, LOCAL_CF, LOCAL_KEY, state.name()));
        forceBlockingFlush(LOCAL_CF);
    }

    public static boolean isIndexBuilt(String table, String indexName)
    {
        ColumnFamilyStore cfs = Table.open(Table.SYSTEM_KS).getColumnFamilyStore(INDEX_CF);
        QueryFilter filter = QueryFilter.getNamesFilter(decorate(ByteBufferUtil.bytes(table)),
                                                        new QueryPath(INDEX_CF),
                                                        ByteBufferUtil.bytes(indexName));
        return ColumnFamilyStore.removeDeleted(cfs.getColumnFamily(filter), Integer.MAX_VALUE) != null;
    }

    public static void setIndexBuilt(String table, String indexName)
    {
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_KS, INDEX_CF);
        cf.addColumn(new Column(ByteBufferUtil.bytes(indexName), ByteBufferUtil.EMPTY_BYTE_BUFFER, FBUtilities.timestampMicros()));
        RowMutation rm = new RowMutation(Table.SYSTEM_KS, ByteBufferUtil.bytes(table));
        rm.add(cf);
        rm.apply();
        forceBlockingFlush(INDEX_CF);
    }

    public static void setIndexRemoved(String table, String indexName)
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_KS, ByteBufferUtil.bytes(table));
        rm.delete(new QueryPath(INDEX_CF, null, ByteBufferUtil.bytes(indexName)), FBUtilities.timestampMicros());
        rm.apply();
        forceBlockingFlush(INDEX_CF);
    }

    /**
     * Read the host ID from the system table, creating (and storing) one if
     * none exists.
     */
    public static UUID getLocalHostId()
    {
        UUID hostId = null;

        String req = "SELECT host_id FROM system.%s WHERE key='%s'";
        UntypedResultSet result = processInternal(String.format(req, LOCAL_CF, LOCAL_KEY));

        // Look up the Host UUID (return it if found)
        if (!result.isEmpty() && result.one().has("host_id"))
        {
            return result.one().getUUID("host_id");
        }

        // ID not found, generate a new one, persist, and then return it.
        hostId = UUID.randomUUID();
        logger.warn("No host ID found, created {} (Note: This should happen exactly once per node).", hostId);

        req = "INSERT INTO system.%s (key, host_id) VALUES ('%s', %s)";
        processInternal(String.format(req, LOCAL_CF, LOCAL_KEY, hostId));
        return hostId;
    }

    /**
     * Read the current local node id from the system table or null if no
     * such node id is recorded.
     */
    public static CounterId getCurrentLocalCounterId()
    {
        Table table = Table.open(Table.SYSTEM_KS);

        // Get the last CounterId (since CounterId are timeuuid is thus ordered from the older to the newer one)
        QueryFilter filter = QueryFilter.getSliceFilter(decorate(ALL_LOCAL_NODE_ID_KEY),
                                                        new QueryPath(COUNTER_ID_CF),
                                                        ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                        ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                        true,
                                                        1);
        ColumnFamily cf = table.getColumnFamilyStore(COUNTER_ID_CF).getColumnFamily(filter);
        if (cf != null && cf.getColumnCount() != 0)
            return CounterId.wrap(cf.iterator().next().name());
        else
            return null;
    }

    /**
     * Write a new current local node id to the system table.
     *
     * @param oldCounterId the previous local node id (that {@code newCounterId}
     * replace) or null if no such node id exists (new node or removed system
     * table)
     * @param newCounterId the new current local node id to record
     * @param now microsecond time stamp.
     */
    public static void writeCurrentLocalCounterId(CounterId oldCounterId, CounterId newCounterId, long now)
    {
        ByteBuffer ip = ByteBuffer.wrap(FBUtilities.getBroadcastAddress().getAddress());

        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_KS, COUNTER_ID_CF);
        cf.addColumn(new Column(newCounterId.bytes(), ip, now));
        RowMutation rm = new RowMutation(Table.SYSTEM_KS, ALL_LOCAL_NODE_ID_KEY);
        rm.add(cf);
        rm.apply();
        forceBlockingFlush(COUNTER_ID_CF);
    }

    public static List<CounterId.CounterIdRecord> getOldLocalCounterIds()
    {
        List<CounterId.CounterIdRecord> l = new ArrayList<CounterId.CounterIdRecord>();

        Table table = Table.open(Table.SYSTEM_KS);
        QueryFilter filter = QueryFilter.getIdentityFilter(decorate(ALL_LOCAL_NODE_ID_KEY), new QueryPath(COUNTER_ID_CF));
        ColumnFamily cf = table.getColumnFamilyStore(COUNTER_ID_CF).getColumnFamily(filter);

        CounterId previous = null;
        for (IColumn c : cf)
        {
            if (previous != null)
                l.add(new CounterId.CounterIdRecord(previous, c.timestamp()));

            // this will ignore the last column on purpose since it is the
            // current local node id
            previous = CounterId.wrap(c.name());
        }
        return l;
    }

    /**
     * @param cfName The name of the ColumnFamily responsible for part of the schema (keyspace, ColumnFamily, columns)
     * @return CFS responsible to hold low-level serialized schema
     */
    public static ColumnFamilyStore schemaCFS(String cfName)
    {
        return Table.open(Table.SYSTEM_KS).getColumnFamilyStore(cfName);
    }

    public static List<Row> serializedSchema()
    {
        List<Row> schema = new ArrayList<Row>(3);

        schema.addAll(serializedSchema(SCHEMA_KEYSPACES_CF));
        schema.addAll(serializedSchema(SCHEMA_COLUMNFAMILIES_CF));
        schema.addAll(serializedSchema(SCHEMA_COLUMNS_CF));

        return schema;
    }

    /**
     * @param schemaCfName The name of the ColumnFamily responsible for part of the schema (keyspace, ColumnFamily, columns)
     * @return low-level schema representation (each row represents individual Keyspace or ColumnFamily)
     */
    public static List<Row> serializedSchema(String schemaCfName)
    {
        Token minToken = StorageService.getPartitioner().getMinimumToken();

        return schemaCFS(schemaCfName).getRangeSlice(null,
                                                     new Range<RowPosition>(minToken.minKeyBound(),
                                                                            minToken.maxKeyBound()),
                                                     Integer.MAX_VALUE,
                                                     new IdentityQueryFilter(),
                                                     null);
    }

    public static Collection<RowMutation> serializeSchema()
    {
        Map<DecoratedKey, RowMutation> mutationMap = new HashMap<DecoratedKey, RowMutation>();

        serializeSchema(mutationMap, SCHEMA_KEYSPACES_CF);
        serializeSchema(mutationMap, SCHEMA_COLUMNFAMILIES_CF);
        serializeSchema(mutationMap, SCHEMA_COLUMNS_CF);

        return mutationMap.values();
    }

    private static void serializeSchema(Map<DecoratedKey, RowMutation> mutationMap, String schemaCfName)
    {
        for (Row schemaRow : serializedSchema(schemaCfName))
        {
            if (Schema.ignoredSchemaRow(schemaRow))
                continue;

            RowMutation mutation = mutationMap.get(schemaRow.key);

            if (mutation == null)
            {
                mutationMap.put(schemaRow.key, new RowMutation(Table.SYSTEM_KS, schemaRow));
                continue;
            }

            mutation.add(schemaRow.cf);
        }
    }

    public static Map<DecoratedKey, ColumnFamily> getSchema(String cfName)
    {
        Map<DecoratedKey, ColumnFamily> schema = new HashMap<DecoratedKey, ColumnFamily>();

        for (Row schemaEntity : SystemTable.serializedSchema(cfName))
            schema.put(schemaEntity.key, schemaEntity.cf);

        return schema;
    }

    public static ByteBuffer getSchemaKSKey(String ksName)
    {
        return AsciiType.instance.fromString(ksName);
    }

    public static Row readSchemaRow(String ksName)
    {
        DecoratedKey key = StorageService.getPartitioner().decorateKey(getSchemaKSKey(ksName));

        ColumnFamilyStore schemaCFS = SystemTable.schemaCFS(SCHEMA_KEYSPACES_CF);
        ColumnFamily result = schemaCFS.getColumnFamily(QueryFilter.getIdentityFilter(key, new QueryPath(SCHEMA_KEYSPACES_CF)));

        return new Row(key, result);
    }

    public static Row readSchemaRow(String ksName, String cfName)
    {
        DecoratedKey key = StorageService.getPartitioner().decorateKey(getSchemaKSKey(ksName));

        ColumnFamilyStore schemaCFS = SystemTable.schemaCFS(SCHEMA_COLUMNFAMILIES_CF);
        ColumnFamily result = schemaCFS.getColumnFamily(key,
                                                        new QueryPath(SCHEMA_COLUMNFAMILIES_CF),
                                                        DefsTable.searchComposite(cfName, true),
                                                        DefsTable.searchComposite(cfName, false),
                                                        false,
                                                        Integer.MAX_VALUE);

        return new Row(key, result);
    }
}
