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

package org.apache.cassandra.schema;

import java.io.File;
import java.nio.ByteBuffer;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableDeletingTask;
import org.apache.cassandra.locator.OldNetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.schema.LegacySchemaTables;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.cellname;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.base.Supplier;

@RunWith(OrderedJUnit4ClassRunner.class)
public class DefsTest
{
    private static final String KEYSPACE1 = "Keyspace1";
    private static final String KEYSPACE3 = "Keyspace3";
    private static final String KEYSPACE6 = "Keyspace6";
    private static final String EMPTYKEYSPACE = "DefsTestEmptyKeyspace";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_STANDARD2 = "Standard2";
    private static final String CF_INDEXED = "Indexed1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.startGossiper();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2));
        SchemaLoader.createKeyspace(KEYSPACE3, true, false,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(5),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.indexCFMD(KEYSPACE3, CF_INDEXED, true));
        SchemaLoader.createKeyspace(KEYSPACE6,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.indexCFMD(KEYSPACE6, CF_INDEXED, true));
    }

    @Test
    public void testCFMetaDataApply() throws ConfigurationException
    {
        CFMetaData cfm = new CFMetaData(KEYSPACE1,
                                        "TestApplyCFM_CF",
                                        ColumnFamilyType.Standard,
                                        new SimpleDenseCellNameType(BytesType.instance));

        for (int i = 0; i < 5; i++)
        {
            ByteBuffer name = ByteBuffer.wrap(new byte[] { (byte)i });
            cfm.addColumnDefinition(ColumnDefinition.regularDef(cfm, name, BytesType.instance, null).setIndex(Integer.toString(i), IndexType.KEYS, null));
        }

        cfm.comment("No comment")
           .readRepairChance(0.5)
           .gcGraceSeconds(100000)
           .minCompactionThreshold(500)
           .maxCompactionThreshold(500);

        // we'll be adding this one later. make sure it's not already there.
        Assert.assertNull(cfm.getColumnDefinition(ByteBuffer.wrap(new byte[] { 5 })));

        CFMetaData cfNew = cfm.copy();

        // add one.
        ColumnDefinition addIndexDef = ColumnDefinition.regularDef(cfm, ByteBuffer.wrap(new byte[] { 5 }), BytesType.instance, null)
                                                       .setIndex("5", IndexType.KEYS, null);
        cfNew.addColumnDefinition(addIndexDef);

        // remove one.
        ColumnDefinition removeIndexDef = ColumnDefinition.regularDef(cfm, ByteBuffer.wrap(new byte[] { 0 }), BytesType.instance, null)
                                                          .setIndex("0", IndexType.KEYS, null);
        Assert.assertTrue(cfNew.removeColumnDefinition(removeIndexDef));

        cfm.apply(cfNew);

        for (int i = 1; i < cfm.allColumns().size(); i++)
            Assert.assertNotNull(cfm.getColumnDefinition(ByteBuffer.wrap(new byte[] { 1 })));
        Assert.assertNull(cfm.getColumnDefinition(ByteBuffer.wrap(new byte[] { 0 })));
        Assert.assertNotNull(cfm.getColumnDefinition(ByteBuffer.wrap(new byte[] { 5 })));
    }

    @Test
    public void testInvalidNames()
    {
        String[] valid = {"1", "a", "_1", "b_", "__", "1_a"};
        for (String s : valid)
            Assert.assertTrue(CFMetaData.isNameValid(s));

        String[] invalid = {"b@t", "dash-y", "", " ", "dot.s", ".hidden"};
        for (String s : invalid)
            Assert.assertFalse(CFMetaData.isNameValid(s));
    }

    @Ignore
    @Test
    public void saveAndRestore()
    {
        /*
        // verify dump and reload.
        UUID first = UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress());
        DefsTables.dumpToStorage(first);
        List<KSMetaData> defs = new ArrayList<KSMetaData>(DefsTables.loadFromStorage(first));

        Assert.assertTrue(defs.size() > 0);
        Assert.assertEquals(defs.size(), Schema.instance.getNonSystemKeyspaces().size());
        for (KSMetaData loaded : defs)
        {
            KSMetaData defined = Schema.instance.getKeyspaceDefinition(loaded.name);
            Assert.assertTrue(String.format("%s != %s", loaded, defined), defined.equals(loaded));
        }
        */
    }

    @Test
    public void addNewCfToBogusKeyspace()
    {
        CFMetaData newCf = addTestCF("MadeUpKeyspace", "NewCF", "new cf");
        try
        {
            MigrationManager.announceNewColumnFamily(newCf);
            throw new AssertionError("You shouldn't be able to do anything to a keyspace that doesn't exist.");
        }
        catch (ConfigurationException expected)
        {
        }
    }

    @Test
    public void addNewCfWithNullComment() throws ConfigurationException
    {
        final String ks = KEYSPACE1;
        final String cf = "BrandNewCfWithNull";
        KSMetaData original = Schema.instance.getKSMetaData(ks);

        CFMetaData newCf = addTestCF(original.name, cf, null);

        Assert.assertFalse(Schema.instance.getKSMetaData(ks).cfMetaData().containsKey(newCf.cfName));
        MigrationManager.announceNewColumnFamily(newCf);

        Assert.assertTrue(Schema.instance.getKSMetaData(ks).cfMetaData().containsKey(newCf.cfName));
        Assert.assertEquals(newCf, Schema.instance.getKSMetaData(ks).cfMetaData().get(newCf.cfName));
    }

    @Test
    public void addNewCF() throws ConfigurationException
    {
        final String ks = KEYSPACE1;
        final String cf = "BrandNewCf";
        KSMetaData original = Schema.instance.getKSMetaData(ks);

        CFMetaData newCf = addTestCF(original.name, cf, "A New Table");

        Assert.assertFalse(Schema.instance.getKSMetaData(ks).cfMetaData().containsKey(newCf.cfName));
        MigrationManager.announceNewColumnFamily(newCf);

        Assert.assertTrue(Schema.instance.getKSMetaData(ks).cfMetaData().containsKey(newCf.cfName));
        Assert.assertEquals(newCf, Schema.instance.getKSMetaData(ks).cfMetaData().get(newCf.cfName));

        // now read and write to it.
        CellName col0 = cellname("col0");
        DecoratedKey dk = Util.dk("key0");
        Mutation rm = new Mutation(ks, dk.getKey());
        rm.add(cf, col0, ByteBufferUtil.bytes("value0"), 1L);
        rm.applyUnsafe();
        ColumnFamilyStore store = Keyspace.open(ks).getColumnFamilyStore(cf);
        Assert.assertNotNull(store);
        store.forceBlockingFlush();

        ColumnFamily cfam = store.getColumnFamily(Util.namesQueryFilter(store, dk, col0));
        Assert.assertNotNull(cfam.getColumn(col0));
        Cell col = cfam.getColumn(col0);
        Assert.assertEquals(ByteBufferUtil.bytes("value0"), col.value());
    }

    @Test
    public void dropCf() throws ConfigurationException
    {
        DecoratedKey dk = Util.dk("dropCf");
        // sanity
        final KSMetaData ks = Schema.instance.getKSMetaData(KEYSPACE1);
        Assert.assertNotNull(ks);
        final CFMetaData cfm = ks.cfMetaData().get("Standard1");
        Assert.assertNotNull(cfm);

        // write some data, force a flush, then verify that files exist on disk.
        Mutation rm = new Mutation(ks.name, dk.getKey());
        for (int i = 0; i < 100; i++)
            rm.add(cfm.cfName, cellname("col" + i), ByteBufferUtil.bytes("anyvalue"), 1L);
        rm.applyUnsafe();
        final ColumnFamilyStore store = Keyspace.open(cfm.ksName).getColumnFamilyStore(cfm.cfName);
        Assert.assertNotNull(store);
        store.forceBlockingFlush();
        Assert.assertTrue(store.directories.sstableLister().list().size() > 0);

        MigrationManager.announceColumnFamilyDrop(ks.name, cfm.cfName);

        Assert.assertFalse(Schema.instance.getKSMetaData(ks.name).cfMetaData().containsKey(cfm.cfName));

        // any write should fail.
        rm = new Mutation(ks.name, dk.getKey());
        boolean success = true;
        try
        {
            rm.add("Standard1", cellname("col0"), ByteBufferUtil.bytes("value0"), 1L);
            rm.applyUnsafe();
        }
        catch (Throwable th)
        {
            success = false;
        }
        Assert.assertFalse("This mutation should have failed since the CF no longer exists.", success);

        // verify that the files are gone.
        Supplier<Object> lambda = new Supplier<Object>() {
            @Override
            public Boolean get() {
                for (File file : store.directories.sstableLister().listFiles())
                {
                    if (file.getPath().endsWith("Data.db") && !new File(file.getPath().replace("Data.db", "Compacted")).exists())
                        return false;
                }
                return true;
            }
        };
        Util.spinAssertEquals(true, lambda, 30);

    }

    @Test
    public void addNewKS() throws ConfigurationException
    {
        DecoratedKey dk = Util.dk("key0");
        CFMetaData newCf = addTestCF("NewKeyspace1", "AddedStandard1", "A new cf for a new ks");

        KSMetaData newKs = KSMetaData.testMetadata(newCf.ksName, SimpleStrategy.class, KSMetaData.optsWithRF(5), newCf);

        MigrationManager.announceNewKeyspace(newKs);

        Assert.assertNotNull(Schema.instance.getKSMetaData(newCf.ksName));
        Assert.assertEquals(Schema.instance.getKSMetaData(newCf.ksName), newKs);

        // test reads and writes.
        CellName col0 = cellname("col0");
        Mutation rm = new Mutation(newCf.ksName, dk.getKey());
        rm.add(newCf.cfName, col0, ByteBufferUtil.bytes("value0"), 1L);
        rm.applyUnsafe();
        ColumnFamilyStore store = Keyspace.open(newCf.ksName).getColumnFamilyStore(newCf.cfName);
        Assert.assertNotNull(store);
        store.forceBlockingFlush();

        ColumnFamily cfam = store.getColumnFamily(Util.namesQueryFilter(store, dk, col0));
        Assert.assertNotNull(cfam.getColumn(col0));
        Cell col = cfam.getColumn(col0);
        Assert.assertEquals(ByteBufferUtil.bytes("value0"), col.value());
    }

    @Test
    public void dropKS() throws ConfigurationException
    {
        DecoratedKey dk = Util.dk("dropKs");
        // sanity
        final KSMetaData ks = Schema.instance.getKSMetaData(KEYSPACE1);
        Assert.assertNotNull(ks);
        final CFMetaData cfm = ks.cfMetaData().get("Standard2");
        Assert.assertNotNull(cfm);

        // write some data, force a flush, then verify that files exist on disk.
        Mutation rm = new Mutation(ks.name, dk.getKey());
        for (int i = 0; i < 100; i++)
            rm.add(cfm.cfName, cellname("col" + i), ByteBufferUtil.bytes("anyvalue"), 1L);
        rm.applyUnsafe();
        ColumnFamilyStore store = Keyspace.open(cfm.ksName).getColumnFamilyStore(cfm.cfName);
        Assert.assertNotNull(store);
        store.forceBlockingFlush();
        Assert.assertTrue(store.directories.sstableLister().list().size() > 0);

        MigrationManager.announceKeyspaceDrop(ks.name);

        Assert.assertNull(Schema.instance.getKSMetaData(ks.name));

        // write should fail.
        rm = new Mutation(ks.name, dk.getKey());
        boolean success = true;
        try
        {
            rm.add("Standard1", cellname("col0"), ByteBufferUtil.bytes("value0"), 1L);
            rm.applyUnsafe();
        }
        catch (Throwable th)
        {
            success = false;
        }
        Assert.assertFalse("This mutation should have failed since the CF no longer exists.", success);

        // reads should fail too.
        boolean threw = false;
        try
        {
            Keyspace.open(ks.name);
        }
        catch (Throwable th)
        {
            threw = true;
        }
        Assert.assertTrue(threw);
    }

    @Test
    public void dropKSUnflushed() throws ConfigurationException
    {
        DecoratedKey dk = Util.dk("dropKs");
        // sanity
        final KSMetaData ks = Schema.instance.getKSMetaData(KEYSPACE3);
        Assert.assertNotNull(ks);
        final CFMetaData cfm = ks.cfMetaData().get("Standard1");
        Assert.assertNotNull(cfm);

        // write some data
        Mutation rm = new Mutation(ks.name, dk.getKey());
        for (int i = 0; i < 100; i++)
            rm.add(cfm.cfName, cellname("col" + i), ByteBufferUtil.bytes("anyvalue"), 1L);
        rm.applyUnsafe();

        MigrationManager.announceKeyspaceDrop(ks.name);

        Assert.assertNull(Schema.instance.getKSMetaData(ks.name));
    }

    @Test
    public void createEmptyKsAddNewCf() throws ConfigurationException
    {
        Assert.assertNull(Schema.instance.getKSMetaData(EMPTYKEYSPACE));

        KSMetaData newKs = KSMetaData.testMetadata(EMPTYKEYSPACE, SimpleStrategy.class, KSMetaData.optsWithRF(5));

        MigrationManager.announceNewKeyspace(newKs);
        Assert.assertNotNull(Schema.instance.getKSMetaData(EMPTYKEYSPACE));

        CFMetaData newCf = addTestCF(EMPTYKEYSPACE, "AddedLater", "A new CF to add to an empty KS");

        //should not exist until apply
        Assert.assertFalse(Schema.instance.getKSMetaData(newKs.name).cfMetaData().containsKey(newCf.cfName));

        //add the new CF to the empty space
        MigrationManager.announceNewColumnFamily(newCf);

        Assert.assertTrue(Schema.instance.getKSMetaData(newKs.name).cfMetaData().containsKey(newCf.cfName));
        Assert.assertEquals(Schema.instance.getKSMetaData(newKs.name).cfMetaData().get(newCf.cfName), newCf);

        // now read and write to it.
        CellName col0 = cellname("col0");
        DecoratedKey dk = Util.dk("key0");
        Mutation rm = new Mutation(newKs.name, dk.getKey());
        rm.add(newCf.cfName, col0, ByteBufferUtil.bytes("value0"), 1L);
        rm.applyUnsafe();
        ColumnFamilyStore store = Keyspace.open(newKs.name).getColumnFamilyStore(newCf.cfName);
        Assert.assertNotNull(store);
        store.forceBlockingFlush();

        ColumnFamily cfam = store.getColumnFamily(Util.namesQueryFilter(store, dk, col0));
        Assert.assertNotNull(cfam.getColumn(col0));
        Cell col = cfam.getColumn(col0);
        Assert.assertEquals(ByteBufferUtil.bytes("value0"), col.value());
    }

    @Test
    public void testUpdateKeyspace() throws ConfigurationException
    {
        // create a keyspace to serve as existing.
        CFMetaData cf = addTestCF("UpdatedKeyspace", "AddedStandard1", "A new cf for a new ks");
        KSMetaData oldKs = KSMetaData.testMetadata(cf.ksName, SimpleStrategy.class, KSMetaData.optsWithRF(5), cf);

        MigrationManager.announceNewKeyspace(oldKs);

        Assert.assertNotNull(Schema.instance.getKSMetaData(cf.ksName));
        Assert.assertEquals(Schema.instance.getKSMetaData(cf.ksName), oldKs);

        // names should match.
        KSMetaData newBadKs2 = KSMetaData.testMetadata(cf.ksName + "trash", SimpleStrategy.class, KSMetaData.optsWithRF(4));
        try
        {
            MigrationManager.announceKeyspaceUpdate(newBadKs2);
            throw new AssertionError("Should not have been able to update a KS with an invalid KS name.");
        }
        catch (ConfigurationException ex)
        {
            // expected.
        }

        KSMetaData newKs = KSMetaData.testMetadata(cf.ksName, OldNetworkTopologyStrategy.class, KSMetaData.optsWithRF(1));
        MigrationManager.announceKeyspaceUpdate(newKs);

        KSMetaData newFetchedKs = Schema.instance.getKSMetaData(newKs.name);
        Assert.assertEquals(newFetchedKs.strategyClass, newKs.strategyClass);
        Assert.assertFalse(newFetchedKs.strategyClass.equals(oldKs.strategyClass));
    }

    @Test
    public void testUpdateColumnFamilyNoIndexes() throws ConfigurationException
    {
        // create a keyspace with a cf to update.
        CFMetaData cf = addTestCF("UpdatedCfKs", "Standard1added", "A new cf that will be updated");
        KSMetaData ksm = KSMetaData.testMetadata(cf.ksName, SimpleStrategy.class, KSMetaData.optsWithRF(1), cf);
        MigrationManager.announceNewKeyspace(ksm);

        Assert.assertNotNull(Schema.instance.getKSMetaData(cf.ksName));
        Assert.assertEquals(Schema.instance.getKSMetaData(cf.ksName), ksm);
        Assert.assertNotNull(Schema.instance.getCFMetaData(cf.ksName, cf.cfName));

        // updating certain fields should fail.
        CFMetaData newCfm = cf.copy();
        newCfm.defaultValidator(BytesType.instance);
        newCfm.minCompactionThreshold(5);
        newCfm.maxCompactionThreshold(31);

        // test valid operations.
        newCfm.comment("Modified comment");
        MigrationManager.announceColumnFamilyUpdate(newCfm); // doesn't get set back here.

        newCfm.readRepairChance(0.23);
        MigrationManager.announceColumnFamilyUpdate(newCfm);

        newCfm.gcGraceSeconds(12);
        MigrationManager.announceColumnFamilyUpdate(newCfm);

        newCfm.defaultValidator(UTF8Type.instance);
        MigrationManager.announceColumnFamilyUpdate(newCfm);

        newCfm.minCompactionThreshold(3);
        MigrationManager.announceColumnFamilyUpdate(newCfm);

        newCfm.maxCompactionThreshold(33);
        MigrationManager.announceColumnFamilyUpdate(newCfm);

        // can't test changing the reconciler because there is only one impl.

        // check the cumulative affect.
        Assert.assertEquals(Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getComment(), newCfm.getComment());
        Assert.assertEquals(Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getReadRepairChance(), newCfm.getReadRepairChance(), 0.0001);
        Assert.assertEquals(Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getGcGraceSeconds(), newCfm.getGcGraceSeconds());
        Assert.assertEquals(UTF8Type.instance, Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getDefaultValidator());

        // Change cfId
        newCfm = new CFMetaData(cf.ksName, cf.cfName, cf.cfType, cf.comparator);
        CFMetaData.copyOpts(newCfm, cf);
        try
        {
            cf.apply(newCfm);
            throw new AssertionError("Should have blown up when you used a different id.");
        }
        catch (ConfigurationException expected) {}

        // Change cfName
        newCfm = new CFMetaData(cf.ksName, cf.cfName + "_renamed", cf.cfType, cf.comparator);
        CFMetaData.copyOpts(newCfm, cf);
        try
        {
            cf.apply(newCfm);
            throw new AssertionError("Should have blown up when you used a different name.");
        }
        catch (ConfigurationException expected) {}

        // Change ksName
        newCfm = new CFMetaData(cf.ksName + "_renamed", cf.cfName, cf.cfType, cf.comparator);
        CFMetaData.copyOpts(newCfm, cf);
        try
        {
            cf.apply(newCfm);
            throw new AssertionError("Should have blown up when you used a different keyspace.");
        }
        catch (ConfigurationException expected) {}

        // Change cf type
        newCfm = new CFMetaData(cf.ksName, cf.cfName, ColumnFamilyType.Super, cf.comparator);
        CFMetaData.copyOpts(newCfm, cf);
        try
        {
            cf.apply(newCfm);
            throw new AssertionError("Should have blwon up when you used a different cf type.");
        }
        catch (ConfigurationException expected) {}

        // Change comparator
        newCfm = new CFMetaData(cf.ksName, cf.cfName, cf.cfType, new SimpleDenseCellNameType(TimeUUIDType.instance));
        CFMetaData.copyOpts(newCfm, cf);
        try
        {
            cf.apply(newCfm);
            throw new AssertionError("Should have blown up when you used a different comparator.");
        }
        catch (ConfigurationException expected) {}
    }

    @Test
    public void testDropIndex() throws ConfigurationException
    {
        // persist keyspace definition in the system keyspace
        LegacySchemaTables.makeCreateKeyspaceMutation(Schema.instance.getKSMetaData(KEYSPACE6), FBUtilities.timestampMicros()).applyUnsafe();
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE6).getColumnFamilyStore("Indexed1");

        // insert some data.  save the sstable descriptor so we can make sure it's marked for delete after the drop
        Mutation rm = new Mutation(KEYSPACE6, ByteBufferUtil.bytes("k1"));
        rm.add("Indexed1", cellname("notbirthdate"), ByteBufferUtil.bytes(1L), 0);
        rm.add("Indexed1", cellname("birthdate"), ByteBufferUtil.bytes(1L), 0);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();
        ColumnFamilyStore indexedCfs = cfs.indexManager.getIndexForColumn(ByteBufferUtil.bytes("birthdate")).getIndexCfs();
        Descriptor desc = indexedCfs.getSSTables().iterator().next().descriptor;

        // drop the index
        CFMetaData meta = cfs.metadata.copy();
        ColumnDefinition cdOld = meta.regularColumns().iterator().next();
        ColumnDefinition cdNew = ColumnDefinition.regularDef(meta, cdOld.name.bytes, cdOld.type, null);
        meta.addOrReplaceColumnDefinition(cdNew);
        MigrationManager.announceColumnFamilyUpdate(meta);

        // check
        Assert.assertTrue(cfs.indexManager.getIndexes().isEmpty());
        SSTableDeletingTask.waitForDeletions();
        Assert.assertFalse(new File(desc.filenameFor(Component.DATA)).exists());
    }

    private CFMetaData addTestCF(String ks, String cf, String comment)
    {
        CFMetaData newCFMD = new CFMetaData(ks, cf, ColumnFamilyType.Standard, new SimpleDenseCellNameType(UTF8Type.instance));
        newCFMD.comment(comment)
               .readRepairChance(0.0);

        return newCFMD;
    }
}
