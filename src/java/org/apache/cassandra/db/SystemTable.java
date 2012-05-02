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

import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql.QueryProcessor;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.Constants;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NodeId;
import org.apache.cassandra.utils.UUIDGen;

public class SystemTable
{
    private static final Logger logger = LoggerFactory.getLogger(SystemTable.class);
    public static final String STATUS_CF = "LocationInfo"; // keep the old CF string for backwards-compatibility
    public static final String INDEX_CF = "IndexInfo";
    public static final String NODE_ID_CF = "NodeIdInfo";
    public static final String VERSION_CF = "Versions";
    public static final String HOST_ID_CF = "HostUUID";
    // see layout description in the DefsTable class header
    public static final String SCHEMA_KEYSPACES_CF = "schema_keyspaces";
    public static final String SCHEMA_COLUMNFAMILIES_CF = "schema_columnfamilies";
    public static final String SCHEMA_COLUMNS_CF = "schema_columns";

    private static final ByteBuffer LOCATION_KEY = ByteBufferUtil.bytes("L");
    private static final ByteBuffer RING_KEY = ByteBufferUtil.bytes("Ring");
    private static final ByteBuffer BOOTSTRAP_KEY = ByteBufferUtil.bytes("Bootstrap");
    private static final ByteBuffer COOKIE_KEY = ByteBufferUtil.bytes("Cookies");
    private static final ByteBuffer BOOTSTRAP = ByteBufferUtil.bytes("B");
    private static final ByteBuffer TOKEN = ByteBufferUtil.bytes("Token");
    private static final ByteBuffer GENERATION = ByteBufferUtil.bytes("Generation");
    private static final ByteBuffer CLUSTERNAME = ByteBufferUtil.bytes("ClusterName");
    private static final ByteBuffer CURRENT_LOCAL_NODE_ID_KEY = ByteBufferUtil.bytes("CurrentLocal");
    private static final ByteBuffer ALL_LOCAL_NODE_ID_KEY = ByteBufferUtil.bytes("Local");
    private static final ByteBuffer HOST_ID_KEY = ByteBufferUtil.bytes("Id");

    private static DecoratedKey decorate(ByteBuffer key)
    {
        return StorageService.getPartitioner().decorateKey(key);
    }

    public static void finishStartup() throws IOException
    {
        setupVersion();
        purgeIncompatibleHints();
    }

    private static void setupVersion() throws IOException
    {
        RowMutation rm;
        ColumnFamily cf;

        rm = new RowMutation(Table.SYSTEM_TABLE, ByteBufferUtil.bytes("build"));
        cf = ColumnFamily.create(Table.SYSTEM_TABLE, VERSION_CF);
        cf.addColumn(new Column(ByteBufferUtil.bytes("version"), ByteBufferUtil.bytes(FBUtilities.getReleaseVersionString()), FBUtilities.timestampMicros()));
        rm.add(cf);
        rm.apply();

        rm = new RowMutation(Table.SYSTEM_TABLE, ByteBufferUtil.bytes("cql"));
        cf = ColumnFamily.create(Table.SYSTEM_TABLE, VERSION_CF);
        cf.addColumn(new Column(ByteBufferUtil.bytes("version"), ByteBufferUtil.bytes(QueryProcessor.CQL_VERSION.toString()), FBUtilities.timestampMicros()));
        rm.add(cf);
        rm.apply();

        rm = new RowMutation(Table.SYSTEM_TABLE, ByteBufferUtil.bytes("thrift"));
        cf = ColumnFamily.create(Table.SYSTEM_TABLE, VERSION_CF);
        cf.addColumn(new Column(ByteBufferUtil.bytes("version"), ByteBufferUtil.bytes(Constants.VERSION), FBUtilities.timestampMicros()));
        rm.add(cf);
        rm.apply();
    }

    /** if hints become incompatible across versions of cassandra, that logic (and associated purging) is managed here. */
    private static void purgeIncompatibleHints() throws IOException
    {
        ByteBuffer upgradeMarker = ByteBufferUtil.bytes("Pre-1.2 hints purged");
        Table table = Table.open(Table.SYSTEM_TABLE);
        QueryFilter filter = QueryFilter.getNamesFilter(decorate(COOKIE_KEY), new QueryPath(STATUS_CF), upgradeMarker);
        ColumnFamily cf = table.getColumnFamilyStore(STATUS_CF).getColumnFamily(filter);
        if (cf != null)
        {
            logger.debug("Pre-1.0 hints already purged");
            return;
        }

        // marker not found.  Snapshot + remove hints and add the marker
        ColumnFamilyStore hintsCfs = Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(HintedHandOffManager.HINTS_CF);
        if (hintsCfs.getSSTables().size() > 0)
        {
            logger.info("Possible old-format hints found. Truncating");
            try
            {
                hintsCfs.truncate();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
        logger.debug("Marking pre-1.0 hints purged");
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, COOKIE_KEY);
        rm.add(new QueryPath(STATUS_CF, null, upgradeMarker), ByteBufferUtil.bytes("oh yes, they were purged"), FBUtilities.timestampMicros());
        rm.apply();
    }

    /**
     * Record token being used by another node
     */
    public static synchronized void updateToken(InetAddress ep, Token token)
    {
        if (ep.equals(FBUtilities.getBroadcastAddress()))
        {
            removeToken(token);
            return;
        }
        IPartitioner p = StorageService.getPartitioner();
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, STATUS_CF);
        cf.addColumn(new Column(p.getTokenFactory().toByteArray(token), ByteBuffer.wrap(ep.getAddress()), FBUtilities.timestampMicros()));
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, RING_KEY);
        rm.add(cf);
        try
        {
            rm.apply();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        forceBlockingFlush(STATUS_CF);
    }

    /**
     * Remove stored token being used by another node
     */
    public static synchronized void removeToken(Token token)
    {
        IPartitioner p = StorageService.getPartitioner();
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, RING_KEY);
        rm.delete(new QueryPath(STATUS_CF, null, p.getTokenFactory().toByteArray(token)), FBUtilities.timestampMicros());
        try
        {
            rm.apply();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        forceBlockingFlush(STATUS_CF);
    }

    /**
     * This method is used to update the System Table with the new token for this node
    */
    public static synchronized void updateToken(Token token)
    {
        IPartitioner p = StorageService.getPartitioner();
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, STATUS_CF);
        cf.addColumn(new Column(SystemTable.TOKEN, p.getTokenFactory().toByteArray(token), FBUtilities.timestampMicros()));
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, LOCATION_KEY);
        rm.add(cf);
        try
        {
            rm.apply();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        forceBlockingFlush(STATUS_CF);
    }

    private static void forceBlockingFlush(String cfname)
    {
        try
        {
            Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(cfname).forceBlockingFlush();
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
    public static HashMap<Token, InetAddress> loadTokens()
    {
        HashMap<Token, InetAddress> tokenMap = new HashMap<Token, InetAddress>();
        IPartitioner p = StorageService.getPartitioner();
        Table table = Table.open(Table.SYSTEM_TABLE);
        QueryFilter filter = QueryFilter.getIdentityFilter(decorate(RING_KEY), new QueryPath(STATUS_CF));
        ColumnFamily cf = ColumnFamilyStore.removeDeleted(table.getColumnFamilyStore(STATUS_CF).getColumnFamily(filter), Integer.MAX_VALUE);
        if (cf != null)
        {
            for (IColumn column : cf.getSortedColumns())
            {
                try
                {
                    ByteBuffer v = column.value();
                    byte[] addr = new byte[v.remaining()];
                    ByteBufferUtil.arrayCopy(v, v.position(), addr, 0, v.remaining());
                    tokenMap.put(p.getTokenFactory().fromByteArray(column.name()), InetAddress.getByAddress(addr));
                }
                catch (UnknownHostException e)
                {
                    throw new IOError(e);
                }
            }
        }
        return tokenMap;
    }

    /**
     * One of three things will happen if you try to read the system table:
     * 1. files are present and you can read them: great
     * 2. no files are there: great (new node is assumed)
     * 3. files are present but you can't read them: bad
     * @throws ConfigurationException
     */
    public static void checkHealth() throws ConfigurationException, IOException
    {
        Table table = null;
        try
        {
            table = Table.open(Table.SYSTEM_TABLE);
        }
        catch (AssertionError err)
        {
            // this happens when a user switches from OPP to RP.
            ConfigurationException ex = new ConfigurationException("Could not read system table!");
            ex.initCause(err);
            throw ex;
        }

        SortedSet<ByteBuffer> cols = new TreeSet<ByteBuffer>(BytesType.instance);
        cols.add(CLUSTERNAME);
        QueryFilter filter = QueryFilter.getNamesFilter(decorate(LOCATION_KEY), new QueryPath(STATUS_CF), cols);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(STATUS_CF);
        ColumnFamily cf = cfs.getColumnFamily(filter);

        if (cf == null)
        {
            // this is a brand new node
            if (!cfs.getSSTables().isEmpty())
                throw new ConfigurationException("Found system table files, but they couldn't be loaded!");

            // no system files.  this is a new node.
            RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, LOCATION_KEY);
            cf = ColumnFamily.create(Table.SYSTEM_TABLE, SystemTable.STATUS_CF);
            cf.addColumn(new Column(CLUSTERNAME, ByteBufferUtil.bytes(DatabaseDescriptor.getClusterName()), FBUtilities.timestampMicros()));
            rm.add(cf);
            rm.apply();

            return;
        }


        IColumn clusterCol = cf.getColumn(CLUSTERNAME);
        assert clusterCol != null;
        String savedClusterName = ByteBufferUtil.string(clusterCol.value());
        if (!DatabaseDescriptor.getClusterName().equals(savedClusterName))
            throw new ConfigurationException("Saved cluster name " + savedClusterName + " != configured name " + DatabaseDescriptor.getClusterName());
    }

    public static Token getSavedToken()
    {
        Table table = Table.open(Table.SYSTEM_TABLE);
        QueryFilter filter = QueryFilter.getNamesFilter(decorate(LOCATION_KEY), new QueryPath(STATUS_CF), TOKEN);
        ColumnFamily cf = table.getColumnFamilyStore(STATUS_CF).getColumnFamily(filter);
        return cf == null ? null : StorageService.getPartitioner().getTokenFactory().fromByteArray(cf.getColumn(TOKEN).value());
    }

    public static int incrementAndGetGeneration() throws IOException
    {
        Table table = Table.open(Table.SYSTEM_TABLE);
        QueryFilter filter = QueryFilter.getNamesFilter(decorate(LOCATION_KEY), new QueryPath(STATUS_CF), GENERATION);
        ColumnFamily cf = table.getColumnFamilyStore(STATUS_CF).getColumnFamily(filter);

        int generation;
        if (cf == null)
        {
            // seconds-since-epoch isn't a foolproof new generation
            // (where foolproof is "guaranteed to be larger than the last one seen at this ip address"),
            // but it's as close as sanely possible
            generation = (int) (System.currentTimeMillis() / 1000);
        }
        else
        {
            // Other nodes will ignore gossip messages about a node that have a lower generation than previously seen.
            final int storedGeneration = ByteBufferUtil.toInt(cf.getColumn(GENERATION).value()) + 1;
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

        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, LOCATION_KEY);
        cf = ColumnFamily.create(Table.SYSTEM_TABLE, SystemTable.STATUS_CF);
        cf.addColumn(new Column(GENERATION, ByteBufferUtil.bytes(generation), FBUtilities.timestampMicros()));
        rm.add(cf);
        rm.apply();
        forceBlockingFlush(STATUS_CF);

        return generation;
    }

    public static boolean isBootstrapped()
    {
        Table table = Table.open(Table.SYSTEM_TABLE);
        QueryFilter filter = QueryFilter.getNamesFilter(decorate(BOOTSTRAP_KEY),
                                                        new QueryPath(STATUS_CF),
                                                        BOOTSTRAP);
        ColumnFamily cf = table.getColumnFamilyStore(STATUS_CF).getColumnFamily(filter);
        if (cf == null)
            return false;
        IColumn c = cf.getColumn(BOOTSTRAP);
        return c.value().get(c.value().position()) == 1;
    }

    public static void setBootstrapped(boolean isBootstrapped)
    {
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, STATUS_CF);
        cf.addColumn(new Column(BOOTSTRAP,
                                ByteBuffer.wrap(new byte[] { (byte) (isBootstrapped ? 1 : 0) }),
                                FBUtilities.timestampMicros()));
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, BOOTSTRAP_KEY);
        rm.add(cf);
        try
        {
            rm.apply();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static boolean isIndexBuilt(String table, String indexName)
    {
        ColumnFamilyStore cfs = Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(INDEX_CF);
        QueryFilter filter = QueryFilter.getNamesFilter(decorate(ByteBufferUtil.bytes(table)),
                                                        new QueryPath(INDEX_CF),
                                                        ByteBufferUtil.bytes(indexName));
        return ColumnFamilyStore.removeDeleted(cfs.getColumnFamily(filter), Integer.MAX_VALUE) != null;
    }

    public static void setIndexBuilt(String table, String indexName)
    {
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, INDEX_CF);
        cf.addColumn(new Column(ByteBufferUtil.bytes(indexName), ByteBufferUtil.EMPTY_BYTE_BUFFER, FBUtilities.timestampMicros()));
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, ByteBufferUtil.bytes(table));
        rm.add(cf);
        try
        {
            rm.apply();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        forceBlockingFlush(INDEX_CF);
    }

    public static void setIndexRemoved(String table, String indexName)
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, ByteBufferUtil.bytes(table));
        rm.delete(new QueryPath(INDEX_CF, null, ByteBufferUtil.bytes(indexName)), FBUtilities.timestampMicros());
        try
        {
            rm.apply();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        forceBlockingFlush(INDEX_CF);
    }

    /**
     * Read the host ID from the system table, creating (and storing) one if
     * none exists.
     */
    public static UUID getLocalHostId()
    {
        UUID hostId = null;

        // Look up the Host UUID (return it if found)
        Table table = Table.open(Table.SYSTEM_TABLE);
        QueryFilter filter = QueryFilter.getIdentityFilter(decorate(HOST_ID_KEY), new QueryPath(HOST_ID_CF));
        ColumnFamily cf = table.getColumnFamilyStore(HOST_ID_CF).getColumnFamily(filter);

        if (cf != null)
        {
            cf = ColumnFamilyStore.removeDeleted(cf, 0);
            assert cf.getColumnCount() <= 1;
            if (cf.getColumnCount() > 0)
                return UUIDGen.getUUID(cf.iterator().next().name());
        }

        // ID not found, generate a new one, persist, and then return it.
        hostId = UUID.randomUUID();
        long now = FBUtilities.timestampMicros();

        logger.warn("No host ID found, created {} (Note: This should happen exactly once per node).", hostId);

        cf = ColumnFamily.create(Table.SYSTEM_TABLE, HOST_ID_CF);
        cf.addColumn(new Column(ByteBuffer.wrap(UUIDGen.decompose(hostId)), ByteBufferUtil.bytes(now), now));

        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, HOST_ID_KEY);
        rm.add(cf);
        try
        {
            rm.apply();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        return hostId;
    }

    /**
     * Read the current local node id from the system table or null if no
     * such node id is recorded.
     */
    public static NodeId getCurrentLocalNodeId()
    {
        ByteBuffer id = null;
        Table table = Table.open(Table.SYSTEM_TABLE);
        QueryFilter filter = QueryFilter.getIdentityFilter(decorate(CURRENT_LOCAL_NODE_ID_KEY),
                new QueryPath(NODE_ID_CF));
        ColumnFamily cf = table.getColumnFamilyStore(NODE_ID_CF).getColumnFamily(filter);
        if (cf != null)
        {
            // Even though gc_grace==0 on System table, we can have a race where we get back tombstones (see CASSANDRA-2824)
            cf = ColumnFamilyStore.removeDeleted(cf, 0);
            assert cf.getColumnCount() <= 1;
            if (cf.getColumnCount() > 0)
                id = cf.iterator().next().name();
        }
        if (id != null)
        {
            return NodeId.wrap(id);
        }
        else
        {
            return null;
        }
    }

    /**
     * Write a new current local node id to the system table.
     *
     * @param oldNodeId the previous local node id (that {@code newNodeId}
     * replace) or null if no such node id exists (new node or removed system
     * table)
     * @param newNodeId the new current local node id to record
     * @param now microsecond time stamp.
     */
    public static void writeCurrentLocalNodeId(NodeId oldNodeId, NodeId newNodeId, long now)
    {
        ByteBuffer ip = ByteBuffer.wrap(FBUtilities.getBroadcastAddress().getAddress());

        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, NODE_ID_CF);
        cf.addColumn(new Column(newNodeId.bytes(), ip, now));
        ColumnFamily cf2 = cf.cloneMe();
        if (oldNodeId != null)
        {
            // previously used (int)(now /1000) for the localDeletionTime
            // tests use single digit long values for now, so use actual time.
            cf2.addColumn(new DeletedColumn(oldNodeId.bytes(), (int)(System.currentTimeMillis() / 1000), now));
        }
        RowMutation rmCurrent = new RowMutation(Table.SYSTEM_TABLE, CURRENT_LOCAL_NODE_ID_KEY);
        RowMutation rmAll = new RowMutation(Table.SYSTEM_TABLE, ALL_LOCAL_NODE_ID_KEY);
        rmCurrent.add(cf2);
        rmAll.add(cf);
        try
        {
            rmCurrent.apply();
            rmAll.apply();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static List<NodeId.NodeIdRecord> getOldLocalNodeIds()
    {
        List<NodeId.NodeIdRecord> l = new ArrayList<NodeId.NodeIdRecord>();

        Table table = Table.open(Table.SYSTEM_TABLE);
        QueryFilter filter = QueryFilter.getIdentityFilter(decorate(ALL_LOCAL_NODE_ID_KEY),
                new QueryPath(NODE_ID_CF));
        ColumnFamily cf = table.getColumnFamilyStore(NODE_ID_CF).getColumnFamily(filter);

        NodeId previous = null;
        for (IColumn c : cf)
        {
            if (previous != null)
                l.add(new NodeId.NodeIdRecord(previous, c.timestamp()));

            // this will ignore the last column on purpose since it is the
            // current local node id
            previous = NodeId.wrap(c.name());
        }
        return l;
    }

    /**
     * @param cfName The name of the ColumnFamily responsible for part of the schema (keyspace, ColumnFamily, columns)
     * @return CFS responsible to hold low-level serialized schema
     */
    public static ColumnFamilyStore schemaCFS(String cfName)
    {
        return Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(cfName);
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
            RowMutation mutation = mutationMap.get(schemaRow.key);

            if (mutation == null)
            {
                mutationMap.put(schemaRow.key, new RowMutation(Table.SYSTEM_TABLE, schemaRow));
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
