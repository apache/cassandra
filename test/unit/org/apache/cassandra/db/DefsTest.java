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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.avro.util.Utf8;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.migration.AddColumnFamily;
import org.apache.cassandra.db.migration.AddKeyspace;
import org.apache.cassandra.db.migration.DropColumnFamily;
import org.apache.cassandra.db.migration.DropKeyspace;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.db.migration.RenameColumnFamily;
import org.apache.cassandra.db.migration.RenameKeyspace;
import org.apache.cassandra.db.migration.UpdateColumnFamily;
import org.apache.cassandra.db.migration.UpdateKeyspace;
import org.apache.cassandra.io.SerDeUtils;
import org.apache.cassandra.locator.OldNetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Test;
import org.apache.cassandra.utils.ByteBufferUtil;


public class DefsTest extends CleanupHelper
{   
    @Test
    public void testZeroInjection() throws IOException
    {
        org.apache.cassandra.db.migration.avro.CfDef cd = new org.apache.cassandra.db.migration.avro.CfDef();
        // populate only fields that must be non-null.
        cd.keyspace = new Utf8("Lest Ks");
        cd.name = new Utf8("Mest Cf");
        
        org.apache.cassandra.db.migration.avro.CfDef cd2 = SerDeUtils.deserializeWithSchema(SerDeUtils.serializeWithSchema(cd), new org.apache.cassandra.db.migration.avro.CfDef());
        assert cd.equals(cd2);
        // make sure some of the fields didn't get unexpected zeros put in during [de]serialize operations.
        assert cd.min_compaction_threshold == null;
        assert cd2.min_compaction_threshold == null;
        assert cd.row_cache_save_period_in_seconds == null;
        assert cd2.row_cache_save_period_in_seconds == null;
        
    }
    
    @Test
    public void ensureStaticCFMIdsAreLessThan1000()
    {
        assert CFMetaData.StatusCf.cfId == 0;    
        assert CFMetaData.HintsCf.cfId == 1;    
        assert CFMetaData.MigrationsCf.cfId == 2;    
        assert CFMetaData.SchemaCf.cfId == 3;    
    }
    
    @Test
    public void testCFMetaDataApply() throws ConfigurationException
    {
        Map<ByteBuffer, ColumnDefinition> indexes = new HashMap<ByteBuffer, ColumnDefinition>();
        for (int i = 0; i < 5; i++) 
        {
            ByteBuffer name = ByteBuffer.wrap(new byte[] { (byte)i });
            indexes.put(name, new ColumnDefinition(name, null, IndexType.KEYS, Integer.toString(i)));
        }
        CFMetaData cfm = new CFMetaData("Keyspace1",
                "TestApplyCFM_CF",
                ColumnFamilyType.Standard,
                BytesType.instance,
                null,
                "No comment",
                1.0,
                1.0,
                0.5,
                false,
                100000,
                null,
                500,
                500,
                500,
                500,
                500,
                500,
                500.0,
                0,
                indexes);
        
        // we'll be adding this one later. make sure it's not already there.
        assert cfm.getColumn_metadata().get(ByteBuffer.wrap(new byte[] { 5 })) == null;
        org.apache.cassandra.db.migration.avro.CfDef cfDef = CFMetaData.convertToAvro(cfm);
        
        // add one.
        org.apache.cassandra.db.migration.avro.ColumnDef addIndexDef = new org.apache.cassandra.db.migration.avro.ColumnDef();
        addIndexDef.index_name = "5";
        addIndexDef.index_type = org.apache.cassandra.db.migration.avro.IndexType.KEYS;
        addIndexDef.name = ByteBuffer.wrap(new byte[] { 5 });
        addIndexDef.validation_class = BytesType.class.getName();
        cfDef.column_metadata.add(addIndexDef);
        
        // remove one.
        org.apache.cassandra.db.migration.avro.ColumnDef removeIndexDef = new org.apache.cassandra.db.migration.avro.ColumnDef();
        removeIndexDef.index_name = "0";
        removeIndexDef.index_type = org.apache.cassandra.db.migration.avro.IndexType.KEYS;
        removeIndexDef.name = ByteBuffer.wrap(new byte[] { 0 });
        removeIndexDef.validation_class = BytesType.class.getName();
        assert cfDef.column_metadata.remove(removeIndexDef);
        
        cfm.apply(cfDef);
        
        for (int i = 1; i < indexes.size(); i++)
            assert cfm.getColumn_metadata().get(ByteBuffer.wrap(new byte[] { 1 })) != null;
        assert cfm.getColumn_metadata().get(ByteBuffer.wrap(new byte[] { 0 })) == null;
        assert cfm.getColumn_metadata().get(ByteBuffer.wrap(new byte[] { 5 })) != null;
    }
    
    @Test
    public void testInvalidNames() throws IOException
    {
        String[] valid = {"1", "a", "_1", "b_", "__", "1_a"};
        for (String s : valid)
            assert Migration.isLegalName(s);
        
        String[] invalid = {"b@t", "dash-y", "", " ", "dot.s", ".hidden"};
        for (String s : invalid)
            assert !Migration.isLegalName(s);
    }
    
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
        CFMetaData newCf = addTestCF("MadeUpKeyspace", "NewCF", "new cf");
        try
        {
            new AddColumnFamily(newCf).apply();
            throw new AssertionError("You shouldn't be able to do anything to a keyspace that doesn't exist.");
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
        CFMetaData newCf1 = addTestCF("Keyspace1", "MigrationCf_1", "Migration CF");

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
        Migration m3 = new DropColumnFamily("Keyspace1", "MigrationCf_2");
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
            UUID version = UUIDGen.getUUID(col.name());
            reconstituded[i] = Migration.deserialize(col.value(), MessagingService.version_);
            assert version.equals(reconstituded[i].getVersion());
            i++;
        }
        
        assert m1.getClass().equals(reconstituded[0].getClass());
        assert m2.getClass().equals(reconstituded[1].getClass());
        assert m3.getClass().equals(reconstituded[2].getClass());
        
        // verify that the row mutations are the same. rather than exposing the private fields, serialize and verify.
        assert m1.serialize().equals(reconstituded[0].serialize());
        assert m2.serialize().equals(reconstituded[1].serialize());
        assert m3.serialize().equals(reconstituded[2].serialize());
    }

    @Test
    public void addNewCF() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        final String ks = "Keyspace1";
        final String cf = "BrandNewCf";
        KSMetaData original = DatabaseDescriptor.getTableDefinition(ks);

        CFMetaData newCf = addTestCF(original.name, cf, "A New Column Family");

        assert !DatabaseDescriptor.getTableDefinition(ks).cfMetaData().containsKey(newCf.cfName);
        new AddColumnFamily(newCf).apply();

        assert DatabaseDescriptor.getTableDefinition(ks).cfMetaData().containsKey(newCf.cfName);
        assert DatabaseDescriptor.getTableDefinition(ks).cfMetaData().get(newCf.cfName).equals(newCf);

        // now read and write to it.
        DecoratedKey dk = Util.dk("key0");
        RowMutation rm = new RowMutation(ks, dk.key);
        rm.add(new QueryPath(cf, null, ByteBufferUtil.bytes("col0")), ByteBufferUtil.bytes("value0"), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(ks).getColumnFamilyStore(cf);
        assert store != null;
        store.forceBlockingFlush();
        
        ColumnFamily cfam = store.getColumnFamily(QueryFilter.getNamesFilter(dk, new QueryPath(cf), ByteBufferUtil.bytes("col0")));
        assert cfam.getColumn(ByteBufferUtil.bytes("col0")) != null;
        IColumn col = cfam.getColumn(ByteBufferUtil.bytes("col0"));
        assert ByteBufferUtil.bytes("value0").equals(col.value());
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
            rm.add(new QueryPath(cfm.cfName, null, ByteBuffer.wrap(("col" + i).getBytes())), ByteBufferUtil.bytes("anyvalue"), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(cfm.tableName).getColumnFamilyStore(cfm.cfName);
        assert store != null;
        store.forceBlockingFlush();
        store.getFlushPath();
        assert DefsTable.getFiles(cfm.tableName, cfm.cfName).size() > 0;
        
        new DropColumnFamily(ks.name, cfm.cfName).apply();
        
        assert !DatabaseDescriptor.getTableDefinition(ks.name).cfMetaData().containsKey(cfm.cfName);
        
        // any write should fail.
        rm = new RowMutation(ks.name, dk.key);
        boolean success = true;
        try
        {
            rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("col0")), ByteBufferUtil.bytes("value0"), 1L);
            rm.apply();
        }
        catch (Throwable th)
        {
            success = false;
        }
        assert !success : "This mutation should have failed since the CF no longer exists.";

        // verify that the files are gone.
        for (File file : DefsTable.getFiles(cfm.tableName, cfm.cfName))
        {
            if (file.getPath().endsWith("Data.db") && !new File(file.getPath().replace("Data.db", "Compacted")).exists())
                throw new AssertionError("undeleted file " + file);
        }
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
            rm.add(new QueryPath(oldCfm.cfName, null, ByteBuffer.wrap(("col" + i).getBytes())), ByteBufferUtil.bytes("anyvalue"), 1L);
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
        ColumnFamily cfam = store.getColumnFamily(QueryFilter.getSliceFilter(dk, new QueryPath(cfName), ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1000));
        assert cfam.getSortedColumns().size() == 100; // should be good enough?
        
        // do some writes
        rm = new RowMutation(ks.name, dk.key);
        rm.add(new QueryPath(cfName, null, ByteBufferUtil.bytes("col5")), ByteBufferUtil.bytes("updated"), 2L);
        rm.apply();
        store.forceBlockingFlush();
        
        cfam = store.getColumnFamily(QueryFilter.getNamesFilter(dk, new QueryPath(cfName), ByteBufferUtil.bytes("col5")));
        assert cfam.getColumnCount() == 1;
        assert cfam.getColumn(ByteBufferUtil.bytes("col5")).value().equals( ByteBufferUtil.bytes("updated"));
    }
    
    @Test
    public void addNewKS() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        DecoratedKey dk = Util.dk("key0");
        CFMetaData newCf = addTestCF("NewKeyspace1", "AddedStandard1", "A new cf for a new ks");

        KSMetaData newKs = new KSMetaData(newCf.tableName, SimpleStrategy.class, null, 5, newCf);
        
        new AddKeyspace(newKs).apply();
        
        assert DatabaseDescriptor.getTableDefinition(newCf.tableName) != null;
        assert DatabaseDescriptor.getTableDefinition(newCf.tableName) == newKs;

        // test reads and writes.
        RowMutation rm = new RowMutation(newCf.tableName, dk.key);
        rm.add(new QueryPath(newCf.cfName, null, ByteBufferUtil.bytes("col0")), ByteBufferUtil.bytes("value0"), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(newCf.tableName).getColumnFamilyStore(newCf.cfName);
        assert store != null;
        store.forceBlockingFlush();
        
        ColumnFamily cfam = store.getColumnFamily(QueryFilter.getNamesFilter(dk, new QueryPath(newCf.cfName), ByteBufferUtil.bytes("col0")));
        assert cfam.getColumn(ByteBufferUtil.bytes("col0")) != null;
        IColumn col = cfam.getColumn(ByteBufferUtil.bytes("col0"));
        assert ByteBufferUtil.bytes("value0").equals(col.value());
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
            rm.add(new QueryPath(cfm.cfName, null, ByteBuffer.wrap(("col" + i).getBytes())), ByteBufferUtil.bytes("anyvalue"), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(cfm.tableName).getColumnFamilyStore(cfm.cfName);
        assert store != null;
        store.forceBlockingFlush();
        assert DefsTable.getFiles(cfm.tableName, cfm.cfName).size() > 0;
        
        new DropKeyspace(ks.name).apply();
        
        assert DatabaseDescriptor.getTableDefinition(ks.name) == null;
        
        // write should fail.
        rm = new RowMutation(ks.name, dk.key);
        boolean success = true;
        try
        {
            rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("col0")), ByteBufferUtil.bytes("value0"), 1L);
            rm.apply();
        }
        catch (Throwable th)
        {
            success = false;
        }
        assert !success : "This mutation should have failed since the CF no longer exists.";

        // reads should fail too.
        boolean threw = false;
        try
        {
            Table.open(ks.name);
        }
        catch (Throwable th)
        {
            threw = true;
        }
        assert threw;
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
            rm.add(new QueryPath(cfName, null, ByteBuffer.wrap(("col" + i).getBytes())), ByteBufferUtil.bytes("value"), 1L);
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
        
        // read on old should fail.
        // reads should fail too.
        boolean threw = false;
        try
        {
            Table.open(oldKs.name);
        }
        catch (Throwable th)
        {
            threw = true;
        }
        assert threw;
        
        // write on old should fail.
        rm = new RowMutation(oldKs.name, ByteBufferUtil.bytes("any key will do"));
        boolean success = true;
        try
        {
            rm.add(new QueryPath(cfName, null, ByteBufferUtil.bytes("col0")), ByteBufferUtil.bytes("value0"), 1L);
            rm.apply();
        }
        catch (Throwable th)
        {
            success = false;
        }
        assert !success : "This mutation should have failed since the CF/Table no longer exists.";
        
        // write on new should work.
        rm = new RowMutation(newKsName, dk.key);
        rm.add(new QueryPath(cfName, null, ByteBufferUtil.bytes("col0")), ByteBufferUtil.bytes("newvalue"), 2L);
        rm.apply();
        store = Table.open(newKs.name).getColumnFamilyStore(cfName);
        assert store != null;
        store.forceBlockingFlush();
        
        // read on new should work.
        SortedSet<ByteBuffer> cols = new TreeSet<ByteBuffer>(BytesType.instance);
        cols.add(ByteBufferUtil.bytes("col0"));
        cols.add(ByteBufferUtil.bytes("col1"));
        ColumnFamily cfam = store.getColumnFamily(QueryFilter.getNamesFilter(dk, new QueryPath(cfName), cols));
        assert cfam.getColumnCount() == cols.size();
        // tests new write.
        
        ByteBuffer val = cfam.getColumn(ByteBufferUtil.bytes("col0")).value();
        assertEquals(ByteBufferUtil.string(val), "newvalue");
        // tests old write.
         val = cfam.getColumn(ByteBufferUtil.bytes("col1")).value();
        assertEquals(ByteBufferUtil.string(val), "value");
    }

    @Test
    public void createEmptyKsAddNewCf() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        assert DatabaseDescriptor.getTableDefinition("EmptyKeyspace") == null;
        
        KSMetaData newKs = new KSMetaData("EmptyKeyspace", SimpleStrategy.class, null, 5);

        new AddKeyspace(newKs).apply();
        assert DatabaseDescriptor.getTableDefinition("EmptyKeyspace") != null;

        CFMetaData newCf = addTestCF("EmptyKeyspace", "AddedLater", "A new CF to add to an empty KS");

        //should not exist until apply
        assert !DatabaseDescriptor.getTableDefinition(newKs.name).cfMetaData().containsKey(newCf.cfName);

        //add the new CF to the empty space
        new AddColumnFamily(newCf).apply();

        assert DatabaseDescriptor.getTableDefinition(newKs.name).cfMetaData().containsKey(newCf.cfName);
        assert DatabaseDescriptor.getTableDefinition(newKs.name).cfMetaData().get(newCf.cfName).equals(newCf);

        // now read and write to it.
        DecoratedKey dk = Util.dk("key0");
        RowMutation rm = new RowMutation(newKs.name, dk.key);
        rm.add(new QueryPath(newCf.cfName, null, ByteBufferUtil.bytes("col0")), ByteBufferUtil.bytes("value0"), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(newKs.name).getColumnFamilyStore(newCf.cfName);
        assert store != null;
        store.forceBlockingFlush();

        ColumnFamily cfam = store.getColumnFamily(QueryFilter.getNamesFilter(dk, new QueryPath(newCf.cfName), ByteBufferUtil.bytes("col0")));
        assert cfam.getColumn(ByteBufferUtil.bytes("col0")) != null;
        IColumn col = cfam.getColumn(ByteBufferUtil.bytes("col0"));
        assert ByteBufferUtil.bytes("value0").equals(col.value());
    }
    
    @Test
    public void testUpdateKeyspace() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        // create a keyspace to serve as existing.
        CFMetaData cf = addTestCF("UpdatedKeyspace", "AddedStandard1", "A new cf for a new ks");
        KSMetaData oldKs = new KSMetaData(cf.tableName, SimpleStrategy.class, null, 5, cf);
        
        new AddKeyspace(oldKs).apply();
        
        assert DatabaseDescriptor.getTableDefinition(cf.tableName) != null;
        assert DatabaseDescriptor.getTableDefinition(cf.tableName) == oldKs;
        
        // anything with cf defs should fail.
        CFMetaData cf2 = addTestCF(cf.tableName, "AddedStandard2", "A new cf for a new ks");
        KSMetaData newBadKs = new KSMetaData(cf.tableName, SimpleStrategy.class, null, 4, cf2);
        try
        {
            new UpdateKeyspace(newBadKs).apply();
            throw new AssertionError("Should not have been able to update a KS with a KS that described column families.");
        }
        catch (ConfigurationException ex)
        {
            // expected.
        }
        
        // names should match.
        KSMetaData newBadKs2 = new KSMetaData(cf.tableName + "trash", SimpleStrategy.class, null, 4);
        try
        {
            new UpdateKeyspace(newBadKs2).apply();
            throw new AssertionError("Should not have been able to update a KS with an invalid KS name.");
        }
        catch (ConfigurationException ex)
        {
            // expected.
        }
        
        KSMetaData newKs = new KSMetaData(cf.tableName, OldNetworkTopologyStrategy.class, null, 1);
        new UpdateKeyspace(newKs).apply();
        
        KSMetaData newFetchedKs = DatabaseDescriptor.getKSMetaData(newKs.name);
        assert newFetchedKs.replicationFactor == newKs.replicationFactor;
        assert newFetchedKs.replicationFactor != oldKs.replicationFactor;
        assert newFetchedKs.strategyClass.equals(newKs.strategyClass);
        assert !newFetchedKs.strategyClass.equals(oldKs.strategyClass);
    }

    @Test
    public void testUpdateColumnFamilyNoIndexes() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        // create a keyspace with a cf to update.
        CFMetaData cf = addTestCF("UpdatedCfKs", "Standard1added", "A new cf that will be updated");
        KSMetaData ksm = new KSMetaData(cf.tableName, SimpleStrategy.class, null, 1, cf);
        new AddKeyspace(ksm).apply();
        
        assert DatabaseDescriptor.getTableDefinition(cf.tableName) != null;
        assert DatabaseDescriptor.getTableDefinition(cf.tableName) == ksm;
        assert DatabaseDescriptor.getCFMetaData(cf.tableName, cf.cfName) != null;
        
        // updating certain fields should fail.
        org.apache.cassandra.db.migration.avro.CfDef cf_def = CFMetaData.convertToAvro(cf);
        cf_def.row_cache_size = 43.3;
        cf_def.column_metadata = new ArrayList<org.apache.cassandra.db.migration.avro.ColumnDef>();
        cf_def.default_validation_class ="BytesType";
        cf_def.min_compaction_threshold = 5;
        cf_def.max_compaction_threshold = 31;
        
        // test valid operations.
        cf_def.comment = "Modified comment";
        new UpdateColumnFamily(cf_def).apply(); // doesn't get set back here.
        
        cf_def.row_cache_size = 2d;
        new UpdateColumnFamily(cf_def).apply();
        
        cf_def.key_cache_size = 3d;
        new UpdateColumnFamily(cf_def).apply();
        
        cf_def.read_repair_chance = 0.23;
        new UpdateColumnFamily(cf_def).apply();
        
        cf_def.gc_grace_seconds = 12;
        new UpdateColumnFamily(cf_def).apply();
        
        cf_def.default_validation_class = "UTF8Type";
        new UpdateColumnFamily(cf_def).apply();

        cf_def.min_compaction_threshold = 3;
        new UpdateColumnFamily(cf_def).apply();

        cf_def.max_compaction_threshold = 33;
        new UpdateColumnFamily(cf_def).apply();

        // can't test changing the reconciler because there is only one impl.
        
        // check the cumulative affect.
        assert DatabaseDescriptor.getCFMetaData(cf.tableName, cf.cfName).getComment().equals(cf_def.comment);
        assert DatabaseDescriptor.getCFMetaData(cf.tableName, cf.cfName).getRowCacheSize() == cf_def.row_cache_size;
        assert DatabaseDescriptor.getCFMetaData(cf.tableName, cf.cfName).getKeyCacheSize() == cf_def.key_cache_size;
        assert DatabaseDescriptor.getCFMetaData(cf.tableName, cf.cfName).getReadRepairChance() == cf_def.read_repair_chance;
        assert DatabaseDescriptor.getCFMetaData(cf.tableName, cf.cfName).getGcGraceSeconds() == cf_def.gc_grace_seconds;
        assert DatabaseDescriptor.getCFMetaData(cf.tableName, cf.cfName).getDefaultValidator() == UTF8Type.instance;
        
        // todo: we probably don't need to reset old values in the catches anymore.
        // make sure some invalid operations fail.
        int oldId = cf_def.id;
        try
        {
            cf_def.id++;
            cf.apply(cf_def);
            throw new AssertionError("Should have blown up when you used a different id.");
        }
        catch (ConfigurationException expected) 
        {
            cf_def.id = oldId;    
        }
        
        CharSequence oldStr = cf_def.name;
        try
        {
            cf_def.name = cf_def.name + "_renamed";
            cf.apply(cf_def);
            throw new AssertionError("Should have blown up when you used a different name.");
        }
        catch (ConfigurationException expected)
        {
            cf_def.name = oldStr;
        }
        
        oldStr = cf_def.keyspace;
        try
        {
            cf_def.keyspace = oldStr + "_renamed";
            cf.apply(cf_def);
            throw new AssertionError("Should have blown up when you used a different keyspace.");
        }
        catch (ConfigurationException expected)
        {
            cf_def.keyspace = oldStr;
        }
        
        try
        {
            cf_def.column_type = ColumnFamilyType.Super.name();
            cf.apply(cf_def);
            throw new AssertionError("Should have blwon up when you used a different cf type.");
        }
        catch (ConfigurationException expected)
        {
            cf_def.column_type = ColumnFamilyType.Standard.name();
        }
        
        oldStr = cf_def.comparator_type;
        try 
        {
            cf_def.comparator_type = BytesType.class.getSimpleName();
            cf.apply(cf_def);
            throw new AssertionError("Should have blown up when you used a different comparator.");
        }
        catch (ConfigurationException expected)
        {
            cf_def.comparator_type = UTF8Type.class.getSimpleName();
        }

        try
        {
            cf_def.min_compaction_threshold = 34;
            cf.apply(cf_def);
            throw new AssertionError("Should have blown up when min > max.");
        }
        catch (ConfigurationException expected)
        {
            cf_def.min_compaction_threshold = 3;
        }

        try
        {
            cf_def.max_compaction_threshold = 2;
            cf.apply(cf_def);
            throw new AssertionError("Should have blown up when max > min.");
        }
        catch (ConfigurationException expected)
        {
            cf_def.max_compaction_threshold = 33;
        }
    }

    private CFMetaData addTestCF(String ks, String cf, String comment)
    {
        return new CFMetaData(ks,
                              cf,
                              ColumnFamilyType.Standard,
                              UTF8Type.instance,
                              null,
                              comment,
                              0,
                              1.0,
                              0,
							  false,
                              CFMetaData.DEFAULT_GC_GRACE_SECONDS,
                              BytesType.instance,
                              CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD,
                              CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD,
                              CFMetaData.DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS,
                              CFMetaData.DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS,
                              CFMetaData.DEFAULT_MEMTABLE_LIFETIME_IN_MINS,
                              CFMetaData.DEFAULT_MEMTABLE_THROUGHPUT_IN_MB,
                              CFMetaData.DEFAULT_MEMTABLE_OPERATIONS_IN_MILLIONS,
                              0,
                              Collections.<ByteBuffer, ColumnDefinition>emptyMap());
    }
}
