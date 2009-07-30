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

import org.apache.log4j.Logger;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.BasicUtilities;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.QueryFilter;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class SystemTable
{
    private static Logger logger_ = Logger.getLogger(SystemTable.class);
    public static final String LOCATION_CF = "LocationInfo";
    private static final String LOCATION_KEY = "L"; // only one row in Location CF
    private static final byte[] TOKEN = utf8("Token");
    private static final byte[] GENERATION = utf8("Generation");

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

    /*
     * This method is used to update the SystemTable with the new token.
    */
    public static void updateToken(Token token) throws IOException
    {
        IPartitioner p = StorageService.getPartitioner();
        Table table = Table.open(Table.SYSTEM_TABLE);
        /* Retrieve the "LocationInfo" column family */
        QueryFilter filter = new NamesQueryFilter(LOCATION_KEY, new QueryPath(LOCATION_CF), TOKEN);
        ColumnFamily cf = table.getColumnFamilyStore(LOCATION_CF).getColumnFamily(filter);
        long oldTokenColumnTimestamp = cf.getColumn(SystemTable.TOKEN).timestamp();
        /* create the "Token" whose value is the new token. */
        IColumn tokenColumn = new Column(SystemTable.TOKEN, p.getTokenFactory().toByteArray(token), oldTokenColumnTimestamp + 1);
        /* replace the old "Token" column with this new one. */
        if (logger_.isDebugEnabled())
          logger_.debug("Replacing old token " + p.getTokenFactory().fromByteArray(cf.getColumn(SystemTable.TOKEN).value()) + " with " + token);
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, LOCATION_KEY);
        cf.addColumn(tokenColumn);
        rm.add(cf);
        rm.apply();
    }

    /*
     * This method reads the system table and retrieves the metadata
     * associated with this storage instance. Currently we store the
     * metadata in a Column Family called LocatioInfo which has two
     * columns namely "Token" and "Generation". This is the token that
     * gets gossiped around and the generation info is used for FD.
    */
    public static StorageMetadata initMetadata() throws IOException
    {
        /* Read the system table to retrieve the storage ID and the generation */
        Table table = Table.open(Table.SYSTEM_TABLE);
        QueryFilter filter = new NamesQueryFilter(LOCATION_KEY, new QueryPath(LOCATION_CF), GENERATION);
        ColumnFamily cf = table.getColumnFamilyStore(LOCATION_CF).getColumnFamily(filter);

        IPartitioner p = StorageService.getPartitioner();
        if (cf == null)
        {
            Token token = p.getDefaultToken();
            int generation = 1;

            RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, LOCATION_KEY);
            cf = ColumnFamily.create(Table.SYSTEM_TABLE, SystemTable.LOCATION_CF);
            cf.addColumn(new Column(TOKEN, p.getTokenFactory().toByteArray(token)));
            cf.addColumn(new Column(GENERATION, BasicUtilities.intToByteArray(generation)) );
            rm.add(cf);
            rm.apply();
            return new StorageMetadata(token, generation);
        }

        /* we crashed and came back up need to bump generation # */
        IColumn tokenColumn = cf.getColumn(TOKEN);
        Token token = p.getTokenFactory().fromByteArray(tokenColumn.value());

        IColumn generation = cf.getColumn(GENERATION);
        int gen = BasicUtilities.byteArrayToInt(generation.value()) + 1;

        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, LOCATION_KEY);
        cf = ColumnFamily.create(Table.SYSTEM_TABLE, SystemTable.LOCATION_CF);
        Column generation2 = new Column(GENERATION, BasicUtilities.intToByteArray(gen), generation.timestamp() + 1);
        cf.addColumn(generation2);
        rm.add(cf);
        rm.apply();
        return new StorageMetadata(token, gen);
    }

    public static class StorageMetadata
    {
        private Token myToken;
        private int generation_;

        StorageMetadata(Token storageId, int generation)
        {
            myToken = storageId;
            generation_ = generation;
        }

        public Token getStorageId()
        {
            return myToken;
        }

        public void setStorageId(Token storageId)
        {
            myToken = storageId;
        }

        public int getGeneration()
        {
            return generation_;
        }
    }
}
