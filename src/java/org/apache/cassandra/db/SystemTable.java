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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.IOError;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.utils.FBUtilities;
import static org.apache.cassandra.utils.FBUtilities.UTF8;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.config.DatabaseDescriptor;

import java.net.InetAddress;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

public class SystemTable
{
    private static Logger logger = LoggerFactory.getLogger(SystemTable.class);
    public static final String STATUS_CF = "LocationInfo"; // keep the old CF string for backwards-compatibility
    private static final byte[] LOCATION_KEY = "L".getBytes(UTF8);
    private static final byte[] BOOTSTRAP_KEY = "Bootstrap".getBytes(UTF8);
    private static final byte[] GRAVEYARD_KEY = "Graveyard".getBytes(UTF8);
    private static final byte[] BOOTSTRAP = "B".getBytes(UTF8);
    private static final byte[] TOKEN = "Token".getBytes(UTF8);
    private static final byte[] GENERATION = "Generation".getBytes(UTF8);
    private static final byte[] CLUSTERNAME = "ClusterName".getBytes(UTF8);
    private static StorageMetadata metadata;

    private static DecoratedKey decorate(byte[] key)
    {
        return StorageService.getPartitioner().decorateKey(key);
    }

    /**
     * Record token being used by another node
     */
    public static synchronized void updateToken(InetAddress ep, Token token)
    {
        IPartitioner p = StorageService.getPartitioner();
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, STATUS_CF);
        cf.addColumn(new Column(ep.getAddress(), p.getTokenFactory().toByteArray(token), new TimestampClock(System.currentTimeMillis())));
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
    }

    /**
     * This method is used to update the System Table with the new token for this node
    */
    public static synchronized void updateToken(Token token)
    {
        assert metadata != null;
        IPartitioner p = StorageService.getPartitioner();
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, STATUS_CF);
        cf.addColumn(new Column(SystemTable.TOKEN, p.getTokenFactory().toByteArray(token), new TimestampClock(System.currentTimeMillis())));
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
        metadata.setToken(token);
    }
    
    /*
     * This method reads the system table and retrieves the metadata
     * associated with this storage instance. Currently we store the
     * metadata in a Column Family called LocatioInfo which has two
     * columns namely "Token" and "Generation". This is the token that
     * gets gossiped around and the generation info is used for FD.
     * We also store whether we're in bootstrap mode in a third column
    */
    public static synchronized StorageMetadata initMetadata() throws IOException
    {
        if (metadata != null)  // guard to protect against being called twice
            return metadata;

        /* Read the system table to retrieve the storage ID and the generation */
        IPartitioner p = StorageService.getPartitioner();
        Table table = Table.open(Table.SYSTEM_TABLE);
        SortedSet<byte[]> columns = new TreeSet<byte[]>(BytesType.instance);
        columns.add(TOKEN);
        columns.add(GENERATION);
        columns.add(CLUSTERNAME);
        QueryFilter filter = QueryFilter.getNamesFilter(decorate(LOCATION_KEY), new QueryPath(STATUS_CF), columns);
        ColumnFamily cf = table.getColumnFamilyStore(STATUS_CF).getColumnFamily(filter);

        if (cf == null)
        {
            Token token;
            String initialToken = DatabaseDescriptor.getInitialToken();
            if (initialToken == null)
                token = p.getRandomToken();
            else
                token = p.getTokenFactory().fromString(initialToken);

            logger.info("Saved Token not found. Using " + token);
            // seconds-since-epoch isn't a foolproof new generation
            // (where foolproof is "guaranteed to be larger than the last one seen at this ip address"),
            // but it's as close as sanely possible
            int generation = (int) (System.currentTimeMillis() / 1000);

            logger.info("Saved ClusterName not found. Using " + DatabaseDescriptor.getClusterName());

            RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, LOCATION_KEY);
            cf = ColumnFamily.create(Table.SYSTEM_TABLE, SystemTable.STATUS_CF);
            cf.addColumn(new Column(TOKEN, p.getTokenFactory().toByteArray(token), TimestampClock.ZERO_VALUE));
            cf.addColumn(new Column(GENERATION, FBUtilities.toByteArray(generation), TimestampClock.ZERO_VALUE));
            cf.addColumn(new Column(CLUSTERNAME, DatabaseDescriptor.getClusterName().getBytes(), TimestampClock.ZERO_VALUE));
            rm.add(cf);
            rm.apply();
            metadata = new StorageMetadata(token, generation, DatabaseDescriptor.getClusterName().getBytes());
            return metadata;
        }

        if (cf.getColumnCount() < 2)
            throw new RuntimeException("Expected both token and generation columns; found " + cf);
        /* we crashed and came back up: make sure new generation is greater than old */
        IColumn tokenColumn = cf.getColumn(TOKEN);
        assert tokenColumn != null : cf;
        Token token = p.getTokenFactory().fromByteArray(tokenColumn.value());
        logger.info("Saved Token found: " + token);

        IColumn generation = cf.getColumn(GENERATION);
        assert generation != null : cf;
        int gen = Math.max(FBUtilities.byteArrayToInt(generation.value()) + 1, (int) (System.currentTimeMillis() / 1000));

        IColumn cluster = cf.getColumn(CLUSTERNAME);

        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, LOCATION_KEY);
        cf = ColumnFamily.create(Table.SYSTEM_TABLE, SystemTable.STATUS_CF);
        TimestampClock genClock = new TimestampClock(((TimestampClock)generation.clock()).timestamp() + 1);
        Column generation2 = new Column(GENERATION, FBUtilities.toByteArray(gen), genClock);
        cf.addColumn(generation2);
        byte[] cname;
        if (cluster != null)
        {
            logger.info("Saved ClusterName found: " + new String(cluster.value()));
            cname = cluster.value();
        }
        else
        {
            Column clustername = new Column(CLUSTERNAME, DatabaseDescriptor.getClusterName().getBytes(), TimestampClock.ZERO_VALUE);
            cf.addColumn(clustername);
            cname = DatabaseDescriptor.getClusterName().getBytes();
            logger.info("Saved ClusterName not found. Using " + DatabaseDescriptor.getClusterName());
        }
        rm.add(cf);
        rm.apply();
        metadata = new StorageMetadata(token, gen, cname);
        return metadata;
    }

    public static boolean isBootstrapped()
    {
        Table table = Table.open(Table.SYSTEM_TABLE);
        QueryFilter filter = QueryFilter.getNamesFilter(decorate(BOOTSTRAP_KEY),
                                                        new QueryPath(STATUS_CF),
                                                        BOOTSTRAP);
        ColumnFamily cf = table.getColumnFamilyStore(STATUS_CF).getColumnFamily(filter);
        return cf != null && cf.getColumn(BOOTSTRAP).value()[0] == 1;
    }

    public static void setBootstrapped(boolean isBootstrapped)
    {
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, STATUS_CF);
        cf.addColumn(new Column(BOOTSTRAP, new byte[] { (byte) (isBootstrapped ? 1 : 0) }, new TimestampClock(System.currentTimeMillis())));
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

    public static ColumnFamily getDroppedCFs() throws IOException
    {
        ColumnFamilyStore cfs = Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(SystemTable.STATUS_CF);
        return cfs.getColumnFamily(QueryFilter.getSliceFilter(decorate(GRAVEYARD_KEY), new QueryPath(STATUS_CF), "".getBytes(), "".getBytes(), null, false, 100));
    }
    
    public static void deleteDroppedCfMarkers(Collection<IColumn> cols) throws IOException
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, GRAVEYARD_KEY);
        long now = System.currentTimeMillis();
        for (IColumn col : cols)
            rm.delete(new QueryPath(STATUS_CF, null, col.name()), new TimestampClock(now));
        rm.apply();
    }
    
    /** when a cf is dropped, it needs to be marked so its files get deleted at some point. */
    public static void markForRemoval(CFMetaData cfm)
    {
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, STATUS_CF);
        cf.addColumn(new Column((cfm.tableName + "-" + cfm.cfName + "-" + cfm.cfId).getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, new TimestampClock(System.currentTimeMillis())));
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, GRAVEYARD_KEY);
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

    public static class StorageMetadata
    {
        private Token token;
        private int generation;
        private byte[] cluster;

        StorageMetadata(Token storageId, int generation, byte[] clustername)
        {
            token = storageId;
            this.generation = generation;
            cluster = clustername;
        }

        public Token getToken()
        {
            return token;
        }

        public void setToken(Token storageId)
        {
            token = storageId;
        }

        public int getGeneration()
        {
            return generation;
        }

        public byte[] getClusterName()
        {
            return cluster;
        }
    }
}
