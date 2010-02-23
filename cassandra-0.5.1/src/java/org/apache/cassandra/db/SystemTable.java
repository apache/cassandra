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

import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.config.DatabaseDescriptor;

import java.net.InetAddress;
import java.util.SortedSet;
import java.util.TreeSet;

public class SystemTable
{
    private static Logger logger = Logger.getLogger(SystemTable.class);
    public static final String STATUS_CF = "LocationInfo"; // keep the old CF string for backwards-compatibility
    private static final String LOCATION_KEY = "L";
    private static final String BOOTSTRAP_KEY = "Bootstrap";
    private static final byte[] BOOTSTRAP = utf8("B");
    private static final byte[] TOKEN = utf8("Token");
    private static final byte[] GENERATION = utf8("Generation");
    private static StorageMetadata metadata;

    private static byte[] utf8(String str)
    {
        try
        {
            return str.getBytes("UTF-8");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Record token being used by another node
     */
    public static synchronized void updateToken(InetAddress ep, Token token)
    {
        IPartitioner p = StorageService.getPartitioner();
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, STATUS_CF);
        cf.addColumn(new Column(ep.getAddress(), p.getTokenFactory().toByteArray(token), System.currentTimeMillis()));
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
        Table table = Table.open(Table.SYSTEM_TABLE);
        SortedSet<byte[]> columns = new TreeSet<byte[]>(new BytesType());
        columns.add(TOKEN);
        columns.add(GENERATION);
        QueryFilter filter = new NamesQueryFilter(LOCATION_KEY, new QueryPath(STATUS_CF), columns);
        ColumnFamily cf = table.getColumnFamilyStore(STATUS_CF).getColumnFamily(filter);

        IPartitioner p = StorageService.getPartitioner();
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

            RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, LOCATION_KEY);
            cf = ColumnFamily.create(Table.SYSTEM_TABLE, SystemTable.STATUS_CF);
            cf.addColumn(new Column(TOKEN, p.getTokenFactory().toByteArray(token)));
            cf.addColumn(new Column(GENERATION, FBUtilities.toByteArray(generation)));
            rm.add(cf);
            rm.apply();
            metadata = new StorageMetadata(token, generation);
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
        
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, LOCATION_KEY);
        cf = ColumnFamily.create(Table.SYSTEM_TABLE, SystemTable.STATUS_CF);
        Column generation2 = new Column(GENERATION, FBUtilities.toByteArray(gen), generation.timestamp() + 1);
        cf.addColumn(generation2);
        rm.add(cf);
        rm.apply();
        metadata = new StorageMetadata(token, gen);
        return metadata;
    }

    public static boolean isBootstrapped()
    {
        Table table = null;
        try
        {
            table = Table.open(Table.SYSTEM_TABLE);
            QueryFilter filter = new NamesQueryFilter(BOOTSTRAP_KEY, new QueryPath(STATUS_CF), BOOTSTRAP);
            ColumnFamily cf = table.getColumnFamilyStore(STATUS_CF).getColumnFamily(filter);
            return cf != null && cf.getColumn(BOOTSTRAP).value()[0] == 1;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void setBootstrapped(boolean isBootstrapped)
    {
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, STATUS_CF);
        cf.addColumn(new Column(BOOTSTRAP, new byte[] { (byte) (isBootstrapped ? 1 : 0) }, System.currentTimeMillis()));
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

    public static class StorageMetadata
    {
        private Token token;
        private int generation;

        StorageMetadata(Token storageId, int generation)
        {
            token = storageId;
            this.generation = generation;
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
    }
}
