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
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.avro.util.Utf8;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.migration.AddColumnFamily;
import org.apache.cassandra.db.migration.AddKeyspace;
import org.apache.cassandra.db.migration.DropColumnFamily;
import org.apache.cassandra.db.migration.DropKeyspace;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.db.migration.UpdateColumnFamily;
import org.apache.cassandra.db.migration.UpdateKeyspace;
import org.apache.cassandra.io.SerDeUtils;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableDeletingTask;
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
        assert cd.compaction_strategy == null;
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
            indexes.put(name, new ColumnDefinition(name, BytesType.instance, IndexType.KEYS, null, Integer.toString(i)));
        }
        CFMetaData cfm = new CFMetaData("Keyspace1",
                                        "TestApplyCFM_CF",
                                        ColumnFamilyType.Standard,
                                        BytesType.instance,
                                        null);

        cfm.comment("No comment")
           .rowCacheSize(1.0)
           .keyCacheSize(1.0)
           .readRepairChance(0.5)
           .replicateOnWrite(false)
           .gcGraceSeconds(100000)
           .defaultValidator(null)
           .minCompactionThreshold(500)
           .maxCompactionThreshold(500)
           .rowCacheSavePeriod(500)
           .keyCacheSavePeriod(500)
           .mergeShardsChance(0.0)
           .columnMetadata(indexes);

        // we'll be adding this one later. make sure it's not already there.
        assert cfm.getColumn_metadata().get(ByteBuffer.wrap(new byte[] { 5 })) == null;
        org.apache.cassandra.db.migration.avro.CfDef cfDef = cfm.toAvro();
        
        // add one.
        org.apache.cassandra.db.migration.avro.ColumnDef addIndexDef = new org.apache.cassandra.db.migration.avro.ColumnDef();
        addIndexDef.index_name = "5";
        addIndexDef.index_type = org.apache.cassandra.db.migration.avro.IndexType.KEYS;
        addIndexDef.name = ByteBuffer.wrap(new byte[] { 5 });
        addIndexDef.validation_class = BytesType.class.getName();
        cfDef.column_metadata.add(addIndexDef);
        
        // remove one.
        org.apache.cassandra.db.migration.avro.ColumnDef removeIndexDef = new org.apache.cassandra.db.migration.avro.ColumnDef();
        removeIndexDef.index_name = new Utf8("0");
        removeIndexDef.index_type = org.apache.cassandra.db.migration.avro.IndexType.KEYS;
        removeIndexDef.name = ByteBuffer.wrap(new byte[] { 0 });
        removeIndexDef.validation_class = new Utf8(BytesType.class.getName());
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
        UUID first = UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress());
        DefsTable.dumpToStorage(first);
        List<KSMetaData> defs = new ArrayList<KSMetaData>(DefsTable.loadFromStorage(first));

        assert defs.size() > 0;
        assert defs.size() == Schema.instance.getNonSystemTables().size();
        for (KSMetaData loaded : defs)
        {
            KSMetaData defined = Schema.instance.getTableDefinition(loaded.name);
            assert defined.equals(loaded) : String.format("%s != %s", loaded, defined);
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
        UUID prior = Schema.instance.getVersion();
        UUID ver0 = UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress());
        DefsTable.dumpToStorage(ver0);
        assert Schema.instance.getVersion().equals(prior);

        // add a cf.
        CFMetaData newCf1 = addTestCF("Keyspace1", "MigrationCf_1", "Migration CF");

        Migration m1 = new AddColumnFamily(newCf1);
        m1.apply();
        UUID ver1 = m1.getVersion();
        assert Schema.instance.getVersion().equals(ver1);
        
        // drop it.
        Migration m3 = new DropColumnFamily("Keyspace1", "MigrationCf_1");
        m3.apply();
        UUID ver3 = m3.getVersion();
        assert Schema.instance.getVersion().equals(ver3);
        
        // now lets load the older migrations to see if that code works.
        Collection<IColumn> serializedMigrations = Migration.getLocalMigrations(ver1, ver3);
        assert serializedMigrations.size() == 2;
        
        // test deserialization of the migrations.
        Migration[] reconstituded = new Migration[2];
        int i = 0;
        for (IColumn col : serializedMigrations)
        {
            UUID version = UUIDGen.getUUID(col.name());
            reconstituded[i] = Migration.deserialize(col.value(), MessagingService.version_);
            assert version.equals(reconstituded[i].getVersion());
            i++;
        }
        
        assert m1.getClass().equals(reconstituded[0].getClass());
        assert m3.getClass().equals(reconstituded[1].getClass());
        
        // verify that the row mutations are the same. rather than exposing the private fields, serialize and verify.
        assert m1.serialize().equals(reconstituded[0].serialize());
        assert m3.serialize().equals(reconstituded[1].serialize());
    }
    
    @Test
    public void addNewCfWithNullComment() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        final String ks = "Keyspace1";
        final String cf = "BrandNewCfWithNull";
        KSMetaData original = Schema.instance.getTableDefinition(ks);

        CFMetaData newCf = addTestCF(original.name, cf, null);

        assert !Schema.instance.getTableDefinition(ks).cfMetaData().containsKey(newCf.cfName);
        new AddColumnFamily(newCf).apply();

        assert Schema.instance.getTableDefinition(ks).cfMetaData().containsKey(newCf.cfName);
        assert Schema.instance.getTableDefinition(ks).cfMetaData().get(newCf.cfName).equals(newCf);
    }

    @Test
    public void addNewCF() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        final String ks = "Keyspace1";
        final String cf = "BrandNewCf";
        KSMetaData original = Schema.instance.getTableDefinition(ks);

        CFMetaData newCf = addTestCF(original.name, cf, "A New Column Family");

        assert !Schema.instance.getTableDefinition(ks).cfMetaData().containsKey(newCf.cfName);
        new AddColumnFamily(newCf).apply();

        assert Schema.instance.getTableDefinition(ks).cfMetaData().containsKey(newCf.cfName);
        assert Schema.instance.getTableDefinition(ks).cfMetaData().get(newCf.cfName).equals(newCf);

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
        final KSMetaData ks = Schema.instance.getTableDefinition("Keyspace1");
        assert ks != null;
        final CFMetaData cfm = ks.cfMetaData().get("Standard1");
        assert cfm != null;
        
        // write some data, force a flush, then verify that files exist on disk.
        RowMutation rm = new RowMutation(ks.name, dk.key);
        for (int i = 0; i < 100; i++)
            rm.add(new QueryPath(cfm.cfName, null, ByteBufferUtil.bytes(("col" + i))), ByteBufferUtil.bytes("anyvalue"), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(cfm.ksName).getColumnFamilyStore(cfm.cfName);
        assert store != null;
        store.forceBlockingFlush();
        store.getFlushPath(1024, Descriptor.CURRENT_VERSION);
        assert DefsTable.getFiles(cfm.ksName, cfm.cfName).size() > 0;
        
        new DropColumnFamily(ks.name, cfm.cfName).apply();

        assert !Schema.instance.getTableDefinition(ks.name).cfMetaData().containsKey(cfm.cfName);
        
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
        for (File file : DefsTable.getFiles(cfm.ksName, cfm.cfName))
        {
            if (file.getPath().endsWith("Data.db") && !new File(file.getPath().replace("Data.db", "Compacted")).exists())
                throw new AssertionError("undeleted file " + file);
        }
    }

    @Test
    public void addNewKS() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        DecoratedKey dk = Util.dk("key0");
        CFMetaData newCf = addTestCF("NewKeyspace1", "AddedStandard1", "A new cf for a new ks");

        KSMetaData newKs = KSMetaData.testMetadata(newCf.ksName, SimpleStrategy.class, KSMetaData.optsWithRF(5), newCf);
        
        new AddKeyspace(newKs).apply();

        assert Schema.instance.getTableDefinition(newCf.ksName) != null;
        assert Schema.instance.getTableDefinition(newCf.ksName) == newKs;

        // test reads and writes.
        RowMutation rm = new RowMutation(newCf.ksName, dk.key);
        rm.add(new QueryPath(newCf.cfName, null, ByteBufferUtil.bytes("col0")), ByteBufferUtil.bytes("value0"), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(newCf.ksName).getColumnFamilyStore(newCf.cfName);
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
        final KSMetaData ks = Schema.instance.getTableDefinition("Keyspace1");
        assert ks != null;
        final CFMetaData cfm = ks.cfMetaData().get("Standard2");
        assert cfm != null;

        // write some data, force a flush, then verify that files exist on disk.
        RowMutation rm = new RowMutation(ks.name, dk.key);
        for (int i = 0; i < 100; i++)
            rm.add(new QueryPath(cfm.cfName, null, ByteBufferUtil.bytes(("col" + i))), ByteBufferUtil.bytes("anyvalue"), 1L);
        rm.apply();
        ColumnFamilyStore store = Table.open(cfm.ksName).getColumnFamilyStore(cfm.cfName);
        assert store != null;
        store.forceBlockingFlush();
        assert DefsTable.getFiles(cfm.ksName, cfm.cfName).size() > 0;
        
        new DropKeyspace(ks.name).apply();

        assert Schema.instance.getTableDefinition(ks.name) == null;
        
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
    public void dropKSUnflushed() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        DecoratedKey dk = Util.dk("dropKs");
        // sanity
        final KSMetaData ks = Schema.instance.getTableDefinition("Keyspace3");
        assert ks != null;
        final CFMetaData cfm = ks.cfMetaData().get("Standard1");
        assert cfm != null;

        // write some data
        RowMutation rm = new RowMutation(ks.name, dk.key);
        for (int i = 0; i < 100; i++)
            rm.add(new QueryPath(cfm.cfName, null, ByteBufferUtil.bytes(("col" + i))), ByteBufferUtil.bytes("anyvalue"), 1L);
        rm.apply();

        new DropKeyspace(ks.name).apply();

        assert Schema.instance.getTableDefinition(ks.name) == null;
    }

    @Test
    public void createEmptyKsAddNewCf() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        assert Schema.instance.getTableDefinition("EmptyKeyspace") == null;
        
        KSMetaData newKs = KSMetaData.testMetadata("EmptyKeyspace", SimpleStrategy.class, KSMetaData.optsWithRF(5));

        new AddKeyspace(newKs).apply();
        assert Schema.instance.getTableDefinition("EmptyKeyspace") != null;

        CFMetaData newCf = addTestCF("EmptyKeyspace", "AddedLater", "A new CF to add to an empty KS");

        //should not exist until apply
        assert !Schema.instance.getTableDefinition(newKs.name).cfMetaData().containsKey(newCf.cfName);

        //add the new CF to the empty space
        new AddColumnFamily(newCf).apply();

        assert Schema.instance.getTableDefinition(newKs.name).cfMetaData().containsKey(newCf.cfName);
        assert Schema.instance.getTableDefinition(newKs.name).cfMetaData().get(newCf.cfName).equals(newCf);

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
        KSMetaData oldKs = KSMetaData.testMetadata(cf.ksName, SimpleStrategy.class, KSMetaData.optsWithRF(5), cf);
        
        new AddKeyspace(oldKs).apply();

        assert Schema.instance.getTableDefinition(cf.ksName) != null;
        assert Schema.instance.getTableDefinition(cf.ksName) == oldKs;
        
        // anything with cf defs should fail.
        CFMetaData cf2 = addTestCF(cf.ksName, "AddedStandard2", "A new cf for a new ks");
        KSMetaData newBadKs = KSMetaData.testMetadata(cf.ksName, SimpleStrategy.class, KSMetaData.optsWithRF(4), cf2);
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
        KSMetaData newBadKs2 = KSMetaData.testMetadata(cf.ksName + "trash", SimpleStrategy.class, KSMetaData.optsWithRF(4));
        try
        {
            new UpdateKeyspace(newBadKs2).apply();
            throw new AssertionError("Should not have been able to update a KS with an invalid KS name.");
        }
        catch (ConfigurationException ex)
        {
            // expected.
        }
        
        KSMetaData newKs = KSMetaData.testMetadata(cf.ksName, OldNetworkTopologyStrategy.class, KSMetaData.optsWithRF(1));
        new UpdateKeyspace(newKs).apply();

        KSMetaData newFetchedKs = Schema.instance.getKSMetaData(newKs.name);
        assert newFetchedKs.strategyClass.equals(newKs.strategyClass);
        assert !newFetchedKs.strategyClass.equals(oldKs.strategyClass);
    }

    @Test
    public void testUpdateColumnFamilyNoIndexes() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        // create a keyspace with a cf to update.
        CFMetaData cf = addTestCF("UpdatedCfKs", "Standard1added", "A new cf that will be updated");
        KSMetaData ksm = KSMetaData.testMetadata(cf.ksName, SimpleStrategy.class, KSMetaData.optsWithRF(1), cf);
        new AddKeyspace(ksm).apply();

        assert Schema.instance.getTableDefinition(cf.ksName) != null;
        assert Schema.instance.getTableDefinition(cf.ksName) == ksm;
        assert Schema.instance.getCFMetaData(cf.ksName, cf.cfName) != null;
        
        // updating certain fields should fail.
        org.apache.cassandra.db.migration.avro.CfDef cf_def = cf.toAvro();
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
        assert Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getComment().equals(cf_def.comment);
        assert Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getRowCacheSize() == cf_def.row_cache_size;
        assert Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getKeyCacheSize() == cf_def.key_cache_size;
        assert Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getReadRepairChance() == cf_def.read_repair_chance;
        assert Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getGcGraceSeconds() == cf_def.gc_grace_seconds;
        assert Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getDefaultValidator() == UTF8Type.instance;
        
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

    @Test
    public void testDropIndex() throws IOException, ExecutionException, InterruptedException, ConfigurationException
    {
        // insert some data.  save the sstable descriptor so we can make sure it's marked for delete after the drop
        RowMutation rm = new RowMutation("Keyspace6", ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("notbirthdate")), ByteBufferUtil.bytes(1L), 0);
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 0);
        rm.apply();
        ColumnFamilyStore cfs = Table.open("Keyspace6").getColumnFamilyStore("Indexed1");
        cfs.forceBlockingFlush();
        ColumnFamilyStore indexedCfs = cfs.indexManager.getIndexForColumn(cfs.indexManager.getIndexedColumns().iterator().next()).getIndexCfs();
        Descriptor desc = indexedCfs.getSSTables().iterator().next().descriptor;

        // drop the index
        CFMetaData meta = CFMetaData.rename(cfs.metadata, cfs.metadata.cfName); // abusing rename to clone
        ColumnDefinition cdOld = meta.getColumn_metadata().values().iterator().next();
        ColumnDefinition cdNew = new ColumnDefinition(cdOld.name, cdOld.getValidator(), null, null, null);
        meta.columnMetadata(Collections.singletonMap(cdOld.name, cdNew));
        UpdateColumnFamily update = new UpdateColumnFamily(meta.toAvro());
        update.apply();

        // check
        assert cfs.indexManager.getIndexedColumns().isEmpty();
        SSTableDeletingTask.waitForDeletions();
        assert !new File(desc.filenameFor(Component.DATA)).exists();
    }

    private CFMetaData addTestCF(String ks, String cf, String comment)
    {
        CFMetaData newCFMD = new CFMetaData(ks, cf, ColumnFamilyType.Standard, UTF8Type.instance, null);
        newCFMD.comment(comment)
               .keyCacheSize(1.0)
               .readRepairChance(0.0)
               .mergeShardsChance(0.0);

        return newCFMD;
    }
}
