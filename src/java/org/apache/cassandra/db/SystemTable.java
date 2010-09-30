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
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.clock.TimestampReconciler;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static com.google.common.base.Charsets.UTF_8;

public class SystemTable
{
    private static Logger logger = LoggerFactory.getLogger(SystemTable.class);
    public static final String STATUS_CF = "LocationInfo"; // keep the old CF string for backwards-compatibility
    public static final String INDEX_CF = "IndexInfo";
    private static final byte[] LOCATION_KEY = "L".getBytes(UTF_8);
    private static final byte[] BOOTSTRAP_KEY = "Bootstrap".getBytes(UTF_8);
    private static final byte[] COOKIE_KEY = "Cookies".getBytes(UTF_8);
    private static final byte[] BOOTSTRAP = "B".getBytes(UTF_8);
    private static final byte[] TOKEN = "Token".getBytes(UTF_8);
    private static final byte[] GENERATION = "Generation".getBytes(UTF_8);
    private static final byte[] CLUSTERNAME = "ClusterName".getBytes(UTF_8);
    private static final byte[] PARTITIONER = "Partioner".getBytes(UTF_8);

    private static DecoratedKey decorate(byte[] key)
    {
        return StorageService.getPartitioner().decorateKey(key);
    }
    
    /* if hints become incompatible across versions of cassandra, that logic (and associated purging) is managed here. */
    public static void purgeIncompatibleHints() throws IOException
    {
        // 0.6->0.7
        final byte[] hintsPurged6to7 = "Hints purged as part of upgrading from 0.6.x to 0.7".getBytes();
        Table table = Table.open(Table.SYSTEM_TABLE);
        QueryFilter dotSeven = QueryFilter.getNamesFilter(decorate(COOKIE_KEY), new QueryPath(STATUS_CF), hintsPurged6to7);
        ColumnFamily cf = table.getColumnFamilyStore(STATUS_CF).getColumnFamily(dotSeven);
        if (cf == null)
        {
            // upgrading from 0.6 to 0.7.
            logger.info("Upgrading to 0.7. Purging hints if there are any. Old hints will be snapshotted.");
            new Truncation(Table.SYSTEM_TABLE, HintedHandOffManager.HINTS_CF).apply();
            RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, COOKIE_KEY);
            rm.add(new QueryPath(STATUS_CF, null, hintsPurged6to7), "oh yes, it they were purged.".getBytes(), new TimestampClock(System.currentTimeMillis()));
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

        try
        {
            Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(SystemTable.STATUS_CF).forceBlockingFlush();
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
        
        SortedSet<byte[]> cols = new TreeSet<byte[]>(BytesType.instance);
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
            cf.addColumn(new Column(PARTITIONER, DatabaseDescriptor.getPartitioner().getClass().getName().getBytes(UTF_8), new TimestampClock(FBUtilities.timestampMicros())));
            cf.addColumn(new Column(CLUSTERNAME, DatabaseDescriptor.getClusterName().getBytes(), new TimestampClock(FBUtilities.timestampMicros())));
            rm.add(cf);
            rm.apply();

            return;
        }
        
        
        IColumn partitionerCol = cf.getColumn(PARTITIONER);
        IColumn clusterCol = cf.getColumn(CLUSTERNAME);
        assert partitionerCol != null;
        assert clusterCol != null;
        if (!DatabaseDescriptor.getPartitioner().getClass().getName().equals(new String(partitionerCol.value(), UTF_8)))
            throw new ConfigurationException("Detected partitioner mismatch! Did you change the partitioner?");
        if (!DatabaseDescriptor.getClusterName().equals(new String(clusterCol.value())))
            throw new ConfigurationException("Saved cluster name " + new String(clusterCol.value()) + " != configured name " + DatabaseDescriptor.getClusterName());
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
            generation = Math.max(FBUtilities.byteArrayToInt(cf.getColumn(GENERATION).value()) + 1,
                                  (int) (System.currentTimeMillis() / 1000));
        }

        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, LOCATION_KEY);
        cf = ColumnFamily.create(Table.SYSTEM_TABLE, SystemTable.STATUS_CF);
        TimestampClock genClock = new TimestampClock(FBUtilities.timestampMicros());
        cf.addColumn(new Column(GENERATION, FBUtilities.toByteArray(generation), genClock));
        rm.add(cf);
        rm.apply();
        try
        {
            table.getColumnFamilyStore(SystemTable.STATUS_CF).forceBlockingFlush();
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }

        return generation;
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

    public static boolean isIndexBuilt(String table, String indexName)
    {
        ColumnFamilyStore cfs = Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(INDEX_CF);
        QueryFilter filter = QueryFilter.getNamesFilter(decorate(table.getBytes(UTF_8)),
                                                        new QueryPath(INDEX_CF),
                                                        indexName.getBytes(UTF_8));
        return cfs.getColumnFamily(filter) != null;
    }

    public static void setIndexBuilt(String table, String indexName)
    {
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, INDEX_CF);
        cf.addColumn(new Column(indexName.getBytes(UTF_8), ArrayUtils.EMPTY_BYTE_ARRAY, new TimestampClock(System.currentTimeMillis())));
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, table.getBytes(UTF_8));
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
}
