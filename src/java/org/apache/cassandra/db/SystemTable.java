/**
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
import java.io.FilenameFilter;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NodeId;

import static com.google.common.base.Charsets.UTF_8;

public class SystemTable
{
    private static Logger logger = LoggerFactory.getLogger(SystemTable.class);
    public static final String STATUS_CF = "LocationInfo"; // keep the old CF string for backwards-compatibility
    public static final String INDEX_CF = "IndexInfo";
    public static final String NODE_ID_CF = "NodeIdInfo";
    private static final ByteBuffer LOCATION_KEY = ByteBufferUtil.bytes("L");
    private static final ByteBuffer RING_KEY = ByteBufferUtil.bytes("Ring");
    private static final ByteBuffer BOOTSTRAP_KEY = ByteBufferUtil.bytes("Bootstrap");
    private static final ByteBuffer COOKIE_KEY = ByteBufferUtil.bytes("Cookies");
    private static final ByteBuffer BOOTSTRAP = ByteBufferUtil.bytes("B");
    private static final ByteBuffer TOKEN = ByteBufferUtil.bytes("Token");
    private static final ByteBuffer GENERATION = ByteBufferUtil.bytes("Generation");
    private static final ByteBuffer CLUSTERNAME = ByteBufferUtil.bytes("ClusterName");
    private static final ByteBuffer PARTITIONER = ByteBufferUtil.bytes("Partioner");
    private static final ByteBuffer CURRENT_LOCAL_NODE_ID_KEY = ByteBufferUtil.bytes("CurrentLocal");
    private static final ByteBuffer ALL_LOCAL_NODE_ID_KEY = ByteBufferUtil.bytes("Local");

    private static DecoratedKey decorate(ByteBuffer key)
    {
        return StorageService.getPartitioner().decorateKey(key);
    }
    
    /* if hints become incompatible across versions of cassandra, that logic (and associated purging) is managed here. */
    public static void purgeIncompatibleHints() throws IOException
    {
        // 0.6->0.7
        final ByteBuffer hintsPurged6to7 = ByteBufferUtil.bytes("Hints purged as part of upgrading from 0.6.x to 0.7");
        Table table = Table.open(Table.SYSTEM_TABLE);
        QueryFilter dotSeven = QueryFilter.getNamesFilter(decorate(COOKIE_KEY), new QueryPath(STATUS_CF), hintsPurged6to7);
        ColumnFamily cf = table.getColumnFamilyStore(STATUS_CF).getColumnFamily(dotSeven);
        if (cf == null)
        {
            // upgrading from 0.6 to 0.7.
            logger.info("Upgrading to 0.7. Purging hints if there are any. Old hints will be snapshotted.");
            new Truncation(Table.SYSTEM_TABLE, HintedHandOffManager.HINTS_CF).apply();
            RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, COOKIE_KEY);
            rm.add(new QueryPath(STATUS_CF, null, hintsPurged6to7), ByteBufferUtil.bytes("oh yes, it they were purged."), System.currentTimeMillis());
            rm.apply();
        }
    }

    /**
     * Record token being used by another node
     */
    public static synchronized void updateToken(InetAddress ep, Token token)
    {
        IPartitioner p = StorageService.getPartitioner();
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, STATUS_CF);
        cf.addColumn(new Column(p.getTokenFactory().toByteArray(token), ByteBuffer.wrap(ep.getAddress()), System.currentTimeMillis()));
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
    }

    /**
     * Remove stored token being used by another node
     */
    public static synchronized void removeToken(Token token)
    {
        IPartitioner p = StorageService.getPartitioner();
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, RING_KEY);
        rm.delete(new QueryPath(STATUS_CF, null, p.getTokenFactory().toByteArray(token)), System.currentTimeMillis());
        try
        {
            rm.apply();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * This method is used to update the System Table with the new token for this node
    */
    public static synchronized void updateToken(Token token)
    {
        IPartitioner p = StorageService.getPartitioner();
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, STATUS_CF);
        cf.addColumn(new Column(SystemTable.TOKEN, p.getTokenFactory().toByteArray(token), System.currentTimeMillis()));
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
        ColumnFamily cf = table.getColumnFamilyStore(STATUS_CF).getColumnFamily(filter);
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
     * 3. files are present but you can't read them: bad (suspect that the partitioner was changed).
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
            ConfigurationException ex = new ConfigurationException("Could not read system table. Did you change partitioners?");
            ex.initCause(err);
            throw ex;
        }
        
        SortedSet<ByteBuffer> cols = new TreeSet<ByteBuffer>(BytesType.instance);
        cols.add(PARTITIONER);
        cols.add(CLUSTERNAME);
        QueryFilter filter = QueryFilter.getNamesFilter(decorate(LOCATION_KEY), new QueryPath(STATUS_CF), cols);
        ColumnFamily cf = table.getColumnFamilyStore(STATUS_CF).getColumnFamily(filter);
        
        if (cf == null)
        {
            // this is either a brand new node (there will be no files), or the partitioner was changed from RP to OPP.
            for (String path : DatabaseDescriptor.getAllDataFileLocationsForTable("system"))
            {
                File[] dbContents = new File(path).listFiles(new FilenameFilter()
                {
                    public boolean accept(File dir, String name)
                    {
                        return name.endsWith(".db");
                    }
                }); 
                if (dbContents.length > 0)
                    throw new ConfigurationException("Found system table files, but they couldn't be loaded. Did you change the partitioner?");
            }

            // no system files.  this is a new node.
            RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, LOCATION_KEY);
            cf = ColumnFamily.create(Table.SYSTEM_TABLE, SystemTable.STATUS_CF);
            cf.addColumn(new Column(PARTITIONER, ByteBuffer.wrap(DatabaseDescriptor.getPartitioner().getClass().getName().getBytes(UTF_8)), FBUtilities.timestampMicros()));
            cf.addColumn(new Column(CLUSTERNAME, ByteBuffer.wrap(DatabaseDescriptor.getClusterName().getBytes()), FBUtilities.timestampMicros()));
            rm.add(cf);
            rm.apply();

            return;
        }
        
        
        IColumn partitionerCol = cf.getColumn(PARTITIONER);
        IColumn clusterCol = cf.getColumn(CLUSTERNAME);
        assert partitionerCol != null;
        assert clusterCol != null;
        if (!DatabaseDescriptor.getPartitioner().getClass().getName().equals(ByteBufferUtil.string(partitionerCol.value(), UTF_8)))
            throw new ConfigurationException("Detected partitioner mismatch! Did you change the partitioner?");
        String savedClusterName = ByteBufferUtil.string(clusterCol.value(), UTF_8);
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
            generation = Math.max(ByteBufferUtil.toInt(cf.getColumn(GENERATION).value()) + 1,
                                  (int) (System.currentTimeMillis() / 1000));
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
                                System.currentTimeMillis()));
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
        QueryFilter filter = QueryFilter.getNamesFilter(decorate(ByteBuffer.wrap(table.getBytes(UTF_8))),
                                                        new QueryPath(INDEX_CF),
                                                        ByteBuffer.wrap(indexName.getBytes(UTF_8)));
        return cfs.getColumnFamily(filter) != null;
    }

    public static void setIndexBuilt(String table, String indexName)
    {
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, INDEX_CF);
        cf.addColumn(new Column(ByteBuffer.wrap(indexName.getBytes(UTF_8)), ByteBufferUtil.EMPTY_BYTE_BUFFER, System.currentTimeMillis()));
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, ByteBuffer.wrap(table.getBytes(UTF_8)));
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
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, ByteBuffer.wrap(table.getBytes(UTF_8)));
        rm.delete(new QueryPath(INDEX_CF, null, ByteBuffer.wrap(indexName.getBytes(UTF_8))), System.currentTimeMillis());
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
     */
    public static void writeCurrentLocalNodeId(NodeId oldNodeId, NodeId newNodeId)
    {
        long now = System.currentTimeMillis();
        ByteBuffer ip = ByteBuffer.wrap(FBUtilities.getLocalAddress().getAddress());

        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, NODE_ID_CF);
        cf.addColumn(new Column(newNodeId.bytes(), ip, now));
        ColumnFamily cf2 = cf.cloneMe();
        if (oldNodeId != null)
        {
            cf2.addColumn(new DeletedColumn(oldNodeId.bytes(), (int) (now / 1000), now));
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
        for (IColumn c : cf.getReverseSortedColumns())
        {
            if (previous != null)
                l.add(new NodeId.NodeIdRecord(previous, c.timestamp()));

            // this will ignore the last column on purpose since it is the
            // current local node id
            previous = NodeId.wrap(c.name());
        }
        return l;
    }
}
