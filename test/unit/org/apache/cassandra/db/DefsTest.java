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

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.migration.AddColumnFamily;
import org.apache.cassandra.db.migration.AddKeyspace;
import org.apache.cassandra.db.migration.DropColumnFamily;
import org.apache.cassandra.db.migration.DropKeyspace;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.db.migration.RenameColumnFamily;
import org.apache.cassandra.db.migration.RenameKeyspace;
import org.apache.cassandra.locator.RackAwareStrategy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.UUIDGen;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class DefsTest extends CleanupHelper
{

    @Test
    public void saveAndRestore() throws IOException
    {
        // verify dump and reload.
        UUID first = UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress());
        DefsTable.dumpToStorage(first);
        List<KSMetaData> defs = new ArrayList<KSMetaData>(DefsTable.loadFromStorage(first));

        assert defs.size() > 0;
        assert defs.size() == DatabaseDescriptor.getNonSystemTables().size();
        for (KSMetaData loaded : defs)
        {
            KSMetaData defined = DatabaseDescriptor.getTableDefinition(loaded.name);
            assert defined.equals(loaded);
        }
    }
     
    @Test
    public void addNewCfToBogusTable() throws InterruptedException
    {
        CFMetaData newCf = new CFMetaData("MadeUpKeyspace", "NewCF", "Standard", new UTF8Type(), null, "new cf", 0, 0);
        try
        {
            new AddColumnFamily(newCf).apply();
            throw new AssertionError("You should't be able to do anything to a keyspace that doesn't exist.");
        }
        catch (ConfigurationException expected)
        {
        }
        catch (IOException unexpected)
        {
            throw new AssertionError("Unexpected exception.");
        }
    }
    
    @Test
    public void testMigrations() throws IOException, ConfigurationException
    {
        // do a save. make sure it doesn't mess with the defs version.
        UUID prior = DatabaseDescriptor.getDefsVersion();
        UUID ver0 = UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress());
        DefsTable.dumpToStorage(ver0);
        assert DatabaseDescriptor.getDefsVersion().equals(prior);
        
        // add a cf.
        CFMetaData newCf1 = new CFMetaData("Keyspace1", "MigrationCf_1", "Standard", new UTF8Type(), null, "Migration CF ", 0, 0);
        Migration m1 = new AddColumnFamily(newCf1);
        m1.apply();
        UUID ver1 = m1.getVersion();
        assert DatabaseDescriptor.getDefsVersion().equals(ver1);
        
        // rename it.
        Migration m2 = new RenameColumnFamily("Keyspace1", "MigrationCf_1", "MigrationCf_2");
        m2.apply();
        UUID ver2 = m2.getVersion();
        assert DatabaseDescriptor.getDefsVersion().equals(ver2);
        
        // drop it.
        Migration m3 = new DropColumnFamily("Keyspace1", "MigrationCf_2", true);
        m3.apply();
        UUID ver3 = m3.getVersion();
        assert DatabaseDescriptor.getDefsVersion().equals(ver3);
        
        // now lets load the older migrations to see if that code works.
        Collection<IColumn> serializedMigrations = Migration.getLocalMigrations(ver1, ver3);
        assert serializedMigrations.size() == 3;
        
        // test deserialization of the migrations.
        Migration[] reconstituded = new Migration[3];
        int i = 0;
        for (IColumn col : serializedMigrations)
        {
            UUID version = UUIDGen.makeType1UUID(col.name());
            reconstituded[i] = Migration.deserialize(new ByteArrayInputStream(col.value()));
            assert version.equals(reconstituded[i].getVersion());
            i++;
        }
        
        assert m1.getClass().equals(reconstituded[0].getClass());
        assert m2.getClass().equals(reconstituded[1].getClass());
        assert m3.getClass().equals(reconstituded[2].getClass());
        
        // verify that the row mutations are the same. rather than exposing the private fields, serialize and verify.
        assert Arrays.equals(getBytes(m1), getBytes(reconstituded[0]));
        assert Arrays.equals(getBytes(m2), getBytes(reconstituded[1]));
        assert Arrays.equals(getBytes(m3), getBytes(reconstituded[2]));
    }
    
    private static byte[] getBytes(Migration m) throws IOException
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        m.getSerializer().serialize(m, dout);
        dout.close();
        return bout.toByteArray();
    }

    @Test
    public void addNewCF() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        final String ks = "Keyspace1";
        final String cf = "BrandNewCf";
        KSMetaData original = DatabaseDescriptor.getTableDefinition(ks);

        CFMetaData newCf = new CFMetaData(original.name, cf, "Standard", new UTF8Type(), null, "A New Column Family", 0, 0);
        int clSegments = CommitLog.instance().getSegmentCount();
        assert !DatabaseDescriptor.getTableDefinition(ks).cfMetaData().containsKey(newCf.cfName);
        new AddColumnFamily(newCf).apply();
        assert CommitLog.instance().getSegmentCount() == clSegments + 1;

        assert DatabaseDescriptor.getTableDefinition(ks).cfMetaData().containsKey(newCf.cfName);
        assert DatabaseDescriptor.getTableDefinition(ks).cfMetaData().get(newCf.cfName).equals(newCf);

        // now read and write to it.
        DecoratedKey dk = Util.dk("key0");
        RowMutation rm = new RowMutation(ks, dk.key);
        rm.add(new QueryPath(cf, null, "col0".getBytes()), "value0".getBytes(), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(ks).getColumnFamilyStore(cf);
        assert store != null;
        store.forceBlockingFlush();
        
        ColumnFamily cfam = store.getColumnFamily(QueryFilter.getNamesFilter(dk, new QueryPath(cf), "col0".getBytes()));
        assert cfam.getColumn("col0".getBytes()) != null;
        IColumn col = cfam.getColumn("col0".getBytes());
        assert Arrays.equals("value0".getBytes(), col.value());
    }

    @Test
    public void dropCf() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        DecoratedKey dk = Util.dk("dropCf");
        // sanity
        final KSMetaData ks = DatabaseDescriptor.getTableDefinition("Keyspace1");
        assert ks != null;
        final CFMetaData cfm = ks.cfMetaData().get("Standard1");
        assert cfm != null;
        
        // write some data, force a flush, then verify that files exist on disk.
        RowMutation rm = new RowMutation(ks.name, dk.key);
        for (int i = 0; i < 100; i++)
            rm.add(new QueryPath(cfm.cfName, null, ("col" + i).getBytes()), "anyvalue".getBytes(), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(cfm.tableName).getColumnFamilyStore(cfm.cfName);
        assert store != null;
        store.forceBlockingFlush();
        store.getFlushPath();
        assert DefsTable.getFiles(cfm.tableName, cfm.cfName).size() > 0;
        
        new DropColumnFamily(ks.name, cfm.cfName, true).apply();
        
        assert !DatabaseDescriptor.getTableDefinition(ks.name).cfMetaData().containsKey(cfm.cfName);
        
        // any write should fail.
        rm = new RowMutation(ks.name, dk.key);
        try
        {
            rm.add(new QueryPath("Standard1", null, "col0".getBytes()), "value0".getBytes(), 1L);
            rm.apply();
            assert false : "This mutation should have failed since the CF no longer exists.";
        }
        catch (Throwable th)
        {
            assert th instanceof IllegalArgumentException;
        }
        
        // verify that the files are gone.
        assert DefsTable.getFiles(cfm.tableName, cfm.cfName).size() == 0;
    }    
    
    @Test
    public void renameCf() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        DecoratedKey dk = Util.dk("key0");
        final KSMetaData ks = DatabaseDescriptor.getTableDefinition("Keyspace2");
        assert ks != null;
        final CFMetaData oldCfm = ks.cfMetaData().get("Standard1");
        assert oldCfm != null;
        
        // write some data, force a flush, then verify that files exist on disk.
        RowMutation rm = new RowMutation(ks.name, dk.key);
        for (int i = 0; i < 100; i++)
            rm.add(new QueryPath(oldCfm.cfName, null, ("col" + i).getBytes()), "anyvalue".getBytes(), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(oldCfm.tableName).getColumnFamilyStore(oldCfm.cfName);
        assert store != null;
        store.forceBlockingFlush();
        int fileCount = DefsTable.getFiles(oldCfm.tableName, oldCfm.cfName).size();
        assert fileCount > 0;
        
        final String cfName = "St4ndard1Replacement";
        new RenameColumnFamily(oldCfm.tableName, oldCfm.cfName, cfName).apply();
        
        assert !DatabaseDescriptor.getTableDefinition(ks.name).cfMetaData().containsKey(oldCfm.cfName);
        assert DatabaseDescriptor.getTableDefinition(ks.name).cfMetaData().containsKey(cfName);
        
        // verify that new files are there.
        assert DefsTable.getFiles(oldCfm.tableName, cfName).size() == fileCount;
        
        // do some reads.
        store = Table.open(oldCfm.tableName).getColumnFamilyStore(cfName);
        assert store != null;
        ColumnFamily cfam = store.getColumnFamily(QueryFilter.getSliceFilter(dk, new QueryPath(cfName), "".getBytes(), "".getBytes(), null, false, 1000));
        assert cfam.getSortedColumns().size() == 100; // should be good enough?
        
        // do some writes
        rm = new RowMutation(ks.name, dk.key);
        rm.add(new QueryPath(cfName, null, "col5".getBytes()), "updated".getBytes(), 2L);
        rm.apply();
        store.forceBlockingFlush();
        
        cfam = store.getColumnFamily(QueryFilter.getNamesFilter(dk, new QueryPath(cfName), "col5".getBytes()));
        assert cfam.getColumnCount() == 1;
        assert Arrays.equals(cfam.getColumn("col5".getBytes()).value(), "updated".getBytes());
    }
    
    @Test
    public void addNewKS() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        DecoratedKey dk = Util.dk("key0");
        CFMetaData newCf = new CFMetaData("NewKeyspace1", "AddedStandard1", "Standard", new UTF8Type(), null, "A new cf for a new ks", 0, 0);
        KSMetaData newKs = new KSMetaData(newCf.tableName, RackAwareStrategy.class, 5, newCf);
        
        int segmentCount = CommitLog.instance().getSegmentCount();
        new AddKeyspace(newKs).apply();
        assert CommitLog.instance().getSegmentCount() == segmentCount + 1;
        
        assert DatabaseDescriptor.getTableDefinition(newCf.tableName) != null;
        assert DatabaseDescriptor.getTableDefinition(newCf.tableName) == newKs;

        // test reads and writes.
        RowMutation rm = new RowMutation(newCf.tableName, dk.key);
        rm.add(new QueryPath(newCf.cfName, null, "col0".getBytes()), "value0".getBytes(), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(newCf.tableName).getColumnFamilyStore(newCf.cfName);
        assert store != null;
        store.forceBlockingFlush();
        
        ColumnFamily cfam = store.getColumnFamily(QueryFilter.getNamesFilter(dk, new QueryPath(newCf.cfName), "col0".getBytes()));
        assert cfam.getColumn("col0".getBytes()) != null;
        IColumn col = cfam.getColumn("col0".getBytes());
        assert Arrays.equals("value0".getBytes(), col.value());
    }
    
    @Test
    public void dropKS() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        DecoratedKey dk = Util.dk("dropKs");
        // sanity
        final KSMetaData ks = DatabaseDescriptor.getTableDefinition("Keyspace1");
        assert ks != null;
        final CFMetaData cfm = ks.cfMetaData().get("Standard2");
        assert cfm != null;
        
        // write some data, force a flush, then verify that files exist on disk.
        RowMutation rm = new RowMutation(ks.name, dk.key);
        for (int i = 0; i < 100; i++)
            rm.add(new QueryPath(cfm.cfName, null, ("col" + i).getBytes()), "anyvalue".getBytes(), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(cfm.tableName).getColumnFamilyStore(cfm.cfName);
        assert store != null;
        store.forceBlockingFlush();
        assert DefsTable.getFiles(cfm.tableName, cfm.cfName).size() > 0;
        
        new DropKeyspace(ks.name, true).apply();
        
        assert DatabaseDescriptor.getTableDefinition(ks.name) == null;
        
        // write should fail.
        rm = new RowMutation(ks.name, dk.key);
        try
        {
            rm.add(new QueryPath("Standard1", null, "col0".getBytes()), "value0".getBytes(), 1L);
            rm.apply();
            throw new AssertionError("This mutation should have failed since the CF no longer exists.");
        }
        catch (Throwable th)
        {
            assert th instanceof IllegalArgumentException;
        }
        
        // reads should fail too.
        try
        {
            Table.open(ks.name);
        }
        catch (Throwable th)
        {
            // this is what has historically happened when you try to open a table that doesn't exist.
            assert th instanceof NullPointerException;
        }
    }
    
    @Test
    public void renameKs() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        DecoratedKey dk = Util.dk("renameKs");
        final KSMetaData oldKs = DatabaseDescriptor.getTableDefinition("Keyspace2");
        assert oldKs != null;
        final String cfName = "Standard3";
        assert oldKs.cfMetaData().containsKey(cfName);
        assert oldKs.cfMetaData().get(cfName).tableName.equals(oldKs.name);
        
        // write some data that we hope to read back later.
        RowMutation rm = new RowMutation(oldKs.name, dk.key);
        for (int i = 0; i < 10; i++)
            rm.add(new QueryPath(cfName, null, ("col" + i).getBytes()), "value".getBytes(), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(oldKs.name).getColumnFamilyStore(cfName);
        assert store != null;
        store.forceBlockingFlush();
        assert DefsTable.getFiles(oldKs.name, cfName).size() > 0;
        
        final String newKsName = "RenamedKeyspace2";
        new RenameKeyspace(oldKs.name, newKsName).apply();
        KSMetaData newKs = DatabaseDescriptor.getTableDefinition(newKsName);
        
        assert DatabaseDescriptor.getTableDefinition(oldKs.name) == null;
        assert newKs != null;
        assert newKs.name.equals(newKsName);
        assert newKs.cfMetaData().containsKey(cfName);
        assert newKs.cfMetaData().get(cfName).tableName.equals(newKsName);
        assert DefsTable.getFiles(newKs.name, cfName).size() > 0;
        assert DefsTable.getFiles(oldKs.name, cfName).size() == 0;
        
        // read on old should fail.
        try
        {
            Table.open(oldKs.name);
        }
        catch (Throwable th)
        {
            assert th instanceof NullPointerException;
        }
        
        // write on old should fail.
        rm = new RowMutation(oldKs.name, "any key will do".getBytes());
        try
        {
            rm.add(new QueryPath(cfName, null, "col0".getBytes()), "value0".getBytes(), 1L);
            rm.apply();
            throw new AssertionError("This mutation should have failed since the CF/Table no longer exists.");
        }
        catch (Throwable th)
        {
            assert th instanceof IllegalArgumentException;
        }
        
        // write on new should work.
        rm = new RowMutation(newKsName, dk.key);
        rm.add(new QueryPath(cfName, null, "col0".getBytes()), "newvalue".getBytes(), 2L);
        rm.apply();
        store = Table.open(newKs.name).getColumnFamilyStore(cfName);
        assert store != null;
        store.forceBlockingFlush();
        
        // read on new should work.
        SortedSet<byte[]> cols = new TreeSet<byte[]>(new BytesType());
        cols.add("col0".getBytes());
        cols.add("col1".getBytes());
        ColumnFamily cfam = store.getColumnFamily(QueryFilter.getNamesFilter(dk, new QueryPath(cfName), cols));
        assert cfam.getColumnCount() == cols.size();
        // tests new write.
        assert Arrays.equals(cfam.getColumn("col0".getBytes()).value(), "newvalue".getBytes());
        // tests old write.
        assert Arrays.equals(cfam.getColumn("col1".getBytes()).value(), "value".getBytes());
    }
}
