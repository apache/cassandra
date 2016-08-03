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

package org.apache.cassandra.schema;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.locator.OldNetworkTopologyStrategy;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.throwAssert;
import static org.apache.cassandra.cql3.CQLTester.assertRows;
import static org.apache.cassandra.cql3.CQLTester.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


@RunWith(OrderedJUnit4ClassRunner.class)
public class DefsTest
{
    private static final String KEYSPACE1 = "keyspace1";
    private static final String KEYSPACE3 = "keyspace3";
    private static final String KEYSPACE6 = "keyspace6";
    private static final String EMPTY_KEYSPACE = "test_empty_keyspace";
    private static final String TABLE1 = "standard1";
    private static final String TABLE2 = "standard2";
    private static final String TABLE1i = "indexed1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.startGossiper();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, TABLE1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, TABLE2));
        SchemaLoader.createKeyspace(KEYSPACE3,
                                    KeyspaceParams.simple(5),
                                    SchemaLoader.standardCFMD(KEYSPACE1, TABLE1),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE3, TABLE1i, true));
        SchemaLoader.createKeyspace(KEYSPACE6,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE6, TABLE1i, true));
    }

    @Test
    public void testCFMetaDataApply() throws ConfigurationException
    {
        CFMetaData cfm = CFMetaData.Builder.create(KEYSPACE1, "TestApplyCFM_CF")
                                           .addPartitionKey("keys", BytesType.instance)
                                           .addClusteringColumn("col", BytesType.instance).build();


        for (int i = 0; i < 5; i++)
        {
            ByteBuffer name = ByteBuffer.wrap(new byte[] { (byte)i });
            cfm.addColumnDefinition(ColumnDefinition.regularDef(cfm, name, BytesType.instance));
        }

        cfm.comment("No comment")
           .readRepairChance(0.5)
           .gcGraceSeconds(100000)
           .compaction(CompactionParams.scts(ImmutableMap.of("min_threshold", "500",
                                                             "max_threshold", "500")));

        // we'll be adding this one later. make sure it's not already there.
        assertNull(cfm.getColumnDefinition(ByteBuffer.wrap(new byte[]{ 5 })));

        CFMetaData cfNew = cfm.copy();

        // add one.
        ColumnDefinition addIndexDef = ColumnDefinition.regularDef(cfm, ByteBuffer.wrap(new byte[] { 5 }), BytesType.instance);
        cfNew.addColumnDefinition(addIndexDef);

        // remove one.
        ColumnDefinition removeIndexDef = ColumnDefinition.regularDef(cfm, ByteBuffer.wrap(new byte[] { 0 }), BytesType.instance);
        assertTrue(cfNew.removeColumnDefinition(removeIndexDef));

        cfm.apply(cfNew);

        for (int i = 1; i < cfm.allColumns().size(); i++)
            assertNotNull(cfm.getColumnDefinition(ByteBuffer.wrap(new byte[]{ 1 })));
        assertNull(cfm.getColumnDefinition(ByteBuffer.wrap(new byte[]{ 0 })));
        assertNotNull(cfm.getColumnDefinition(ByteBuffer.wrap(new byte[]{ 5 })));
    }

    @Test
    public void testInvalidNames()
    {
        String[] valid = {"1", "a", "_1", "b_", "__", "1_a"};
        for (String s : valid)
            assertTrue(CFMetaData.isNameValid(s));

        String[] invalid = {"b@t", "dash-y", "", " ", "dot.s", ".hidden"};
        for (String s : invalid)
            assertFalse(CFMetaData.isNameValid(s));
    }

    @Test
    public void addNewCfToBogusKeyspace()
    {
        CFMetaData newCf = addTestTable("MadeUpKeyspace", "NewCF", "new cf");
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
    public void addNewTable() throws ConfigurationException
    {
        final String ksName = KEYSPACE1;
        final String tableName = "anewtable";
        KeyspaceMetadata original = Schema.instance.getKSMetaData(ksName);

        CFMetaData cfm = addTestTable(original.name, tableName, "A New Table");

        assertFalse(Schema.instance.getKSMetaData(ksName).tables.get(cfm.cfName).isPresent());
        MigrationManager.announceNewColumnFamily(cfm);

        assertTrue(Schema.instance.getKSMetaData(ksName).tables.get(cfm.cfName).isPresent());
        assertEquals(cfm, Schema.instance.getKSMetaData(ksName).tables.get(cfm.cfName).get());

        // now read and write to it.
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (key, col, val) VALUES (?, ?, ?)",
                                                     ksName, tableName),
                                       "key0", "col0", "val0");

        // flush to exercise more than just hitting the memtable
        ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(tableName);
        assertNotNull(cfs);
        cfs.forceBlockingFlush();

        // and make sure we get out what we put in
        UntypedResultSet rows = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s", ksName, tableName));
        assertRows(rows, row("key0", "col0", "val0"));
    }

    @Test
    public void dropCf() throws ConfigurationException
    {
        // sanity
        final KeyspaceMetadata ks = Schema.instance.getKSMetaData(KEYSPACE1);
        assertNotNull(ks);
        final CFMetaData cfm = ks.tables.getNullable(TABLE1);
        assertNotNull(cfm);

        // write some data, force a flush, then verify that files exist on disk.
        for (int i = 0; i < 100; i++)
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (key, name, val) VALUES (?, ?, ?)",
                                                         KEYSPACE1, TABLE1),
                                           "dropCf", "col" + i, "anyvalue");
        ColumnFamilyStore store = Keyspace.open(cfm.ksName).getColumnFamilyStore(cfm.cfName);
        assertNotNull(store);
        store.forceBlockingFlush();
        assertTrue(store.getDirectories().sstableLister(Directories.OnTxnErr.THROW).list().size() > 0);

        MigrationManager.announceColumnFamilyDrop(ks.name, cfm.cfName);

        assertFalse(Schema.instance.getKSMetaData(ks.name).tables.get(cfm.cfName).isPresent());

        // any write should fail.
        boolean success = true;
        try
        {
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (key, name, val) VALUES (?, ?, ?)",
                                                         KEYSPACE1, TABLE1),
                                           "dropCf", "col0", "anyvalue");
        }
        catch (Throwable th)
        {
            success = false;
        }
        assertFalse("This mutation should have failed since the CF no longer exists.", success);

        // verify that the files are gone.
        Supplier<Object> lambda = () -> {
            for (File file : store.getDirectories().sstableLister(Directories.OnTxnErr.THROW).listFiles())
            {
                if (file.getPath().endsWith("Data.db") && !new File(file.getPath().replace("Data.db", "Compacted")).exists())
                    return false;
            }
            return true;
        };
        Util.spinAssertEquals(true, lambda, 30);

    }

    @Test
    public void addNewKS() throws ConfigurationException
    {
        CFMetaData cfm = addTestTable("newkeyspace1", "newstandard1", "A new cf for a new ks");
        KeyspaceMetadata newKs = KeyspaceMetadata.create(cfm.ksName, KeyspaceParams.simple(5), Tables.of(cfm));
        MigrationManager.announceNewKeyspace(newKs);

        assertNotNull(Schema.instance.getKSMetaData(cfm.ksName));
        assertEquals(Schema.instance.getKSMetaData(cfm.ksName), newKs);

        // test reads and writes.
        QueryProcessor.executeInternal("INSERT INTO newkeyspace1.newstandard1 (key, col, val) VALUES (?, ?, ?)",
                                       "key0", "col0", "val0");
        ColumnFamilyStore store = Keyspace.open(cfm.ksName).getColumnFamilyStore(cfm.cfName);
        assertNotNull(store);
        store.forceBlockingFlush();

        UntypedResultSet rows = QueryProcessor.executeInternal("SELECT * FROM newkeyspace1.newstandard1");
        assertRows(rows, row("key0", "col0", "val0"));
    }

    @Test
    public void dropKS() throws ConfigurationException
    {
        // sanity
        final KeyspaceMetadata ks = Schema.instance.getKSMetaData(KEYSPACE1);
        assertNotNull(ks);
        final CFMetaData cfm = ks.tables.getNullable(TABLE2);
        assertNotNull(cfm);

        // write some data, force a flush, then verify that files exist on disk.
        for (int i = 0; i < 100; i++)
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (key, name, val) VALUES (?, ?, ?)",
                                                         KEYSPACE1, TABLE2),
                                           "dropKs", "col" + i, "anyvalue");
        ColumnFamilyStore cfs = Keyspace.open(cfm.ksName).getColumnFamilyStore(cfm.cfName);
        assertNotNull(cfs);
        cfs.forceBlockingFlush();
        assertTrue(!cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).list().isEmpty());

        MigrationManager.announceKeyspaceDrop(ks.name);

        assertNull(Schema.instance.getKSMetaData(ks.name));

        // write should fail.
        boolean success = true;
        try
        {
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (key, name, val) VALUES (?, ?, ?)",
                                                         KEYSPACE1, TABLE2),
                                           "dropKs", "col0", "anyvalue");
        }
        catch (Throwable th)
        {
            success = false;
        }
        assertFalse("This mutation should have failed since the KS no longer exists.", success);

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
        assertTrue(threw);
    }

    @Test
    public void dropKSUnflushed() throws ConfigurationException
    {
        // sanity
        final KeyspaceMetadata ks = Schema.instance.getKSMetaData(KEYSPACE3);
        assertNotNull(ks);
        final CFMetaData cfm = ks.tables.getNullable(TABLE1);
        assertNotNull(cfm);

        // write some data
        for (int i = 0; i < 100; i++)
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (key, name, val) VALUES (?, ?, ?)",
                                                         KEYSPACE3, TABLE1),
                                           "dropKs", "col" + i, "anyvalue");

        MigrationManager.announceKeyspaceDrop(ks.name);

        assertNull(Schema.instance.getKSMetaData(ks.name));
    }

    @Test
    public void createEmptyKsAddNewCf() throws ConfigurationException
    {
        assertNull(Schema.instance.getKSMetaData(EMPTY_KEYSPACE));
        KeyspaceMetadata newKs = KeyspaceMetadata.create(EMPTY_KEYSPACE, KeyspaceParams.simple(5));
        MigrationManager.announceNewKeyspace(newKs);
        assertNotNull(Schema.instance.getKSMetaData(EMPTY_KEYSPACE));

        String tableName = "added_later";
        CFMetaData newCf = addTestTable(EMPTY_KEYSPACE, tableName, "A new CF to add to an empty KS");

        //should not exist until apply
        assertFalse(Schema.instance.getKSMetaData(newKs.name).tables.get(newCf.cfName).isPresent());

        //add the new CF to the empty space
        MigrationManager.announceNewColumnFamily(newCf);

        assertTrue(Schema.instance.getKSMetaData(newKs.name).tables.get(newCf.cfName).isPresent());
        assertEquals(Schema.instance.getKSMetaData(newKs.name).tables.get(newCf.cfName).get(), newCf);

        // now read and write to it.
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (key, col, val) VALUES (?, ?, ?)",
                                                     EMPTY_KEYSPACE, tableName),
                                       "key0", "col0", "val0");

        ColumnFamilyStore cfs = Keyspace.open(newKs.name).getColumnFamilyStore(newCf.cfName);
        assertNotNull(cfs);
        cfs.forceBlockingFlush();

        UntypedResultSet rows = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s", EMPTY_KEYSPACE, tableName));
        assertRows(rows, row("key0", "col0", "val0"));
    }

    @Test
    public void testUpdateKeyspace() throws ConfigurationException
    {
        // create a keyspace to serve as existing.
        CFMetaData cf = addTestTable("UpdatedKeyspace", "AddedStandard1", "A new cf for a new ks");
        KeyspaceMetadata oldKs = KeyspaceMetadata.create(cf.ksName, KeyspaceParams.simple(5), Tables.of(cf));

        MigrationManager.announceNewKeyspace(oldKs);

        assertNotNull(Schema.instance.getKSMetaData(cf.ksName));
        assertEquals(Schema.instance.getKSMetaData(cf.ksName), oldKs);

        // names should match.
        KeyspaceMetadata newBadKs2 = KeyspaceMetadata.create(cf.ksName + "trash", KeyspaceParams.simple(4));
        try
        {
            MigrationManager.announceKeyspaceUpdate(newBadKs2);
            throw new AssertionError("Should not have been able to update a KS with an invalid KS name.");
        }
        catch (ConfigurationException ex)
        {
            // expected.
        }

        Map<String, String> replicationMap = new HashMap<>();
        replicationMap.put(ReplicationParams.CLASS, OldNetworkTopologyStrategy.class.getName());
        replicationMap.put("replication_factor", "1");

        KeyspaceMetadata newKs = KeyspaceMetadata.create(cf.ksName, KeyspaceParams.create(true, replicationMap));
        MigrationManager.announceKeyspaceUpdate(newKs);

        KeyspaceMetadata newFetchedKs = Schema.instance.getKSMetaData(newKs.name);
        assertEquals(newFetchedKs.params.replication.klass, newKs.params.replication.klass);
        assertFalse(newFetchedKs.params.replication.klass.equals(oldKs.params.replication.klass));
    }

    /*
    @Test
    public void testUpdateColumnFamilyNoIndexes() throws ConfigurationException
    {
        // create a keyspace with a cf to update.
        CFMetaData cf = addTestTable("UpdatedCfKs", "Standard1added", "A new cf that will be updated");
        KSMetaData ksm = KSMetaData.testMetadata(cf.ksName, SimpleStrategy.class, KSMetaData.optsWithRF(1), cf);
        MigrationManager.announceNewKeyspace(ksm);

        assertNotNull(Schema.instance.getKSMetaData(cf.ksName));
        assertEquals(Schema.instance.getKSMetaData(cf.ksName), ksm);
        assertNotNull(Schema.instance.getCFMetaData(cf.ksName, cf.cfName));

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
        assertEquals(Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getComment(), newCfm.getComment());
        assertEquals(Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getReadRepairChance(), newCfm.getReadRepairChance(), 0.0001);
        assertEquals(Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getGcGraceSeconds(), newCfm.getGcGraceSeconds());
        assertEquals(UTF8Type.instance, Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getDefaultValidator());

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
    */

    @Test
    public void testDropIndex() throws ConfigurationException
    {
        // persist keyspace definition in the system keyspace
        SchemaKeyspace.makeCreateKeyspaceMutation(Schema.instance.getKSMetaData(KEYSPACE6), FBUtilities.timestampMicros()).build().applyUnsafe();
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE6).getColumnFamilyStore(TABLE1i);
        String indexName = "birthdate_key_index";

        // insert some data.  save the sstable descriptor so we can make sure it's marked for delete after the drop
        QueryProcessor.executeInternal(String.format(
                                                    "INSERT INTO %s.%s (key, c1, birthdate, notbirthdate) VALUES (?, ?, ?, ?)",
                                                    KEYSPACE6,
                                                    TABLE1i),
                                       "key0", "col0", 1L, 1L);

        cfs.forceBlockingFlush();
        ColumnFamilyStore indexCfs = cfs.indexManager.getIndexByName(indexName)
                                                     .getBackingTable()
                                                     .orElseThrow(throwAssert("Cannot access index cfs"));
        Descriptor desc = indexCfs.getLiveSSTables().iterator().next().descriptor;

        // drop the index
        CFMetaData meta = cfs.metadata.copy();
        IndexMetadata existing = cfs.metadata.getIndexes()
                                             .get(indexName)
                                             .orElseThrow(throwAssert("Index not found"));

        meta.indexes(meta.getIndexes().without(existing.name));
        MigrationManager.announceColumnFamilyUpdate(meta);

        // check
        assertTrue(cfs.indexManager.listIndexes().isEmpty());
        LifecycleTransaction.waitForDeletions();
        assertFalse(new File(desc.filenameFor(Component.DATA)).exists());
    }

    private CFMetaData addTestTable(String ks, String cf, String comment)
    {
        CFMetaData newCFMD = CFMetaData.Builder.create(ks, cf)
                                               .addPartitionKey("key", UTF8Type.instance)
                                               .addClusteringColumn("col", UTF8Type.instance)
                                               .addRegularColumn("val", UTF8Type.instance).build();

        newCFMD.comment(comment)
               .readRepairChance(0.0);

        return newCFMD;
    }
}
