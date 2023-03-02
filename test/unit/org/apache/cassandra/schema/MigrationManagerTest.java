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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.throwAssert;
import static org.apache.cassandra.cql3.CQLTester.assertRows;
import static org.apache.cassandra.cql3.CQLTester.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class MigrationManagerTest
{
    private static final String KEYSPACE1 = "keyspace1";
    private static final String KEYSPACE3 = "keyspace3";
    private static final String KEYSPACE6 = "keyspace6";
    private static final String EMPTY_KEYSPACE = "test_empty_keyspace";
    private static final String TABLE1 = "standard1";
    private static final String TABLE2 = "standard2";
    private static final String TABLE1i = "indexed1";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
                                    SchemaLoader.standardCFMD(KEYSPACE3, TABLE1),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE3, TABLE1i, true));
        SchemaLoader.createKeyspace(KEYSPACE6,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE6, TABLE1i, true));
    }

    @Test
    public void testTableMetadataBuilder() throws ConfigurationException
    {
        TableMetadata.Builder builder =
            TableMetadata.builder(KEYSPACE1, "TestApplyCFM_CF")
                         .addPartitionKeyColumn("keys", BytesType.instance)
                         .addClusteringColumn("col", BytesType.instance)
                         .comment("No comment")
                         .gcGraceSeconds(100000)
                         .compaction(CompactionParams.stcs(ImmutableMap.of("min_threshold", "500", "max_threshold", "500")));

        for (int i = 0; i < 5; i++)
        {
            ByteBuffer name = ByteBuffer.wrap(new byte[] { (byte)i });
            builder.addRegularColumn(ColumnIdentifier.getInterned(name, BytesType.instance), ByteType.instance);
        }


        TableMetadata table = builder.build();
        // we'll be adding this one later. make sure it's not already there.
        assertNull(table.getColumn(ByteBuffer.wrap(new byte[]{ 5 })));

        // add one.
        ColumnMetadata addIndexDef = ColumnMetadata.regularColumn(table, ByteBuffer.wrap(new byte[] { 5 }), BytesType.instance);
        builder.addColumn(addIndexDef);

        // remove one.
        ColumnMetadata removeIndexDef = ColumnMetadata.regularColumn(table, ByteBuffer.wrap(new byte[] { 0 }), BytesType.instance);
        builder.removeRegularOrStaticColumn(removeIndexDef.name);

        TableMetadata table2 = builder.build();

        for (int i = 1; i < table2.columns().size(); i++)
            assertNotNull(table2.getColumn(ByteBuffer.wrap(new byte[]{ 1 })));
        assertNull(table2.getColumn(ByteBuffer.wrap(new byte[]{ 0 })));
        assertNotNull(table2.getColumn(ByteBuffer.wrap(new byte[]{ 5 })));
    }

    @Test
    public void testInvalidNames()
    {
        String[] valid = {"1", "a", "_1", "b_", "__", "1_a"};
        for (String s : valid)
            assertTrue(SchemaConstants.isValidName(s));

        String[] invalid = {"b@t", "dash-y", "", " ", "dot.s", ".hidden"};
        for (String s : invalid)
            assertFalse(SchemaConstants.isValidName(s));
    }

    @Test
    public void addNewCfToBogusKeyspace()
    {
        TableMetadata newCf = addTestTable("MadeUpKeyspace", "NewCF", "new cf");
        try
        {
            SchemaTestUtil.announceNewTable(newCf);
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
        KeyspaceMetadata original = Schema.instance.getKeyspaceMetadata(ksName);

        TableMetadata cfm = addTestTable(original.name, tableName, "A New Table");

        assertFalse(Schema.instance.getKeyspaceMetadata(ksName).tables.get(cfm.name).isPresent());
        SchemaTestUtil.announceNewTable(cfm);

        assertTrue(Schema.instance.getKeyspaceMetadata(ksName).tables.get(cfm.name).isPresent());
        assertEquals(cfm, Schema.instance.getKeyspaceMetadata(ksName).tables.get(cfm.name).get());

        // now read and write to it.
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (key, col, val) VALUES (?, ?, ?)",
                                                     ksName, tableName),
                                       "key0", "col0", "val0");

        // flush to exercise more than just hitting the memtable
        ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(tableName);
        assertNotNull(cfs);
        Util.flush(cfs);

        // and make sure we get out what we put in
        UntypedResultSet rows = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s", ksName, tableName));
        assertRows(rows, row("key0", "col0", "val0"));
    }

    @Test
    public void dropCf() throws ConfigurationException
    {
        // sanity
        final KeyspaceMetadata ks = Schema.instance.getKeyspaceMetadata(KEYSPACE1);
        assertNotNull(ks);
        final TableMetadata cfm = ks.tables.getNullable(TABLE1);
        assertNotNull(cfm);

        // write some data, force a flush, then verify that files exist on disk.
        for (int i = 0; i < 100; i++)
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (key, name, val) VALUES (?, ?, ?)",
                                                         KEYSPACE1, TABLE1),
                                           "dropCf", "col" + i, "anyvalue");
        ColumnFamilyStore store = Keyspace.open(cfm.keyspace).getColumnFamilyStore(cfm.name);
        assertNotNull(store);
        Util.flush(store);
        assertTrue(store.getDirectories().sstableLister(Directories.OnTxnErr.THROW).list().size() > 0);

        SchemaTestUtil.announceTableDrop(ks.name, cfm.name);

        assertFalse(Schema.instance.getKeyspaceMetadata(ks.name).tables.get(cfm.name).isPresent());

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
                if (file.path().endsWith("Data.db") && !new File(file.path().replace("Data.db", "Compacted")).exists())
                    return false;
            }
            return true;
        };
        Util.spinAssertEquals(true, lambda, 30);

    }

    @Test
    public void addNewKS() throws ConfigurationException
    {
        TableMetadata cfm = addTestTable("newkeyspace1", "newstandard1", "A new cf for a new ks");
        KeyspaceMetadata newKs = KeyspaceMetadata.create(cfm.keyspace, KeyspaceParams.simple(5), Tables.of(cfm));
        SchemaTestUtil.announceNewKeyspace(newKs);

        assertNotNull(Schema.instance.getKeyspaceMetadata(cfm.keyspace));
        assertEquals(Schema.instance.getKeyspaceMetadata(cfm.keyspace), newKs);

        // test reads and writes.
        QueryProcessor.executeInternal("INSERT INTO newkeyspace1.newstandard1 (key, col, val) VALUES (?, ?, ?)",
                                       "key0", "col0", "val0");
        ColumnFamilyStore store = Keyspace.open(cfm.keyspace).getColumnFamilyStore(cfm.name);
        assertNotNull(store);
        Util.flush(store);

        UntypedResultSet rows = QueryProcessor.executeInternal("SELECT * FROM newkeyspace1.newstandard1");
        assertRows(rows, row("key0", "col0", "val0"));
    }

    @Test
    public void dropKSUnflushed() throws ConfigurationException
    {
        // sanity
        final KeyspaceMetadata ks = Schema.instance.getKeyspaceMetadata(KEYSPACE3);
        assertNotNull(ks);
        final TableMetadata cfm = ks.tables.getNullable(TABLE1);
        assertNotNull(cfm);

        // write some data
        for (int i = 0; i < 100; i++)
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (key, name, val) VALUES (?, ?, ?)",
                                                         KEYSPACE3, TABLE1),
                                           "dropKs", "col" + i, "anyvalue");

        SchemaTestUtil.announceKeyspaceDrop(ks.name);

        assertNull(Schema.instance.getKeyspaceMetadata(ks.name));
    }

    @Test
    public void createEmptyKsAddNewCf() throws ConfigurationException
    {
        assertNull(Schema.instance.getKeyspaceMetadata(EMPTY_KEYSPACE));
        KeyspaceMetadata newKs = KeyspaceMetadata.create(EMPTY_KEYSPACE, KeyspaceParams.simple(5));
        SchemaTestUtil.announceNewKeyspace(newKs);
        assertNotNull(Schema.instance.getKeyspaceMetadata(EMPTY_KEYSPACE));

        String tableName = "added_later";
        TableMetadata newCf = addTestTable(EMPTY_KEYSPACE, tableName, "A new CF to add to an empty KS");

        //should not exist until apply
        assertFalse(Schema.instance.getKeyspaceMetadata(newKs.name).tables.get(newCf.name).isPresent());

        //add the new CF to the empty space
        SchemaTestUtil.announceNewTable(newCf);

        assertTrue(Schema.instance.getKeyspaceMetadata(newKs.name).tables.get(newCf.name).isPresent());
        assertEquals(Schema.instance.getKeyspaceMetadata(newKs.name).tables.get(newCf.name).get(), newCf);

        // now read and write to it.
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (key, col, val) VALUES (?, ?, ?)",
                                                     EMPTY_KEYSPACE, tableName),
                                       "key0", "col0", "val0");

        ColumnFamilyStore cfs = Keyspace.open(newKs.name).getColumnFamilyStore(newCf.name);
        assertNotNull(cfs);
        Util.flush(cfs);

        UntypedResultSet rows = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s", EMPTY_KEYSPACE, tableName));
        assertRows(rows, row("key0", "col0", "val0"));
    }

    @Test
    public void testUpdateKeyspace() throws ConfigurationException
    {
        // create a keyspace to serve as existing.
        TableMetadata cf = addTestTable("UpdatedKeyspace", "AddedStandard1", "A new cf for a new ks");
        KeyspaceMetadata oldKs = KeyspaceMetadata.create(cf.keyspace, KeyspaceParams.simple(5), Tables.of(cf));

        SchemaTestUtil.announceNewKeyspace(oldKs);

        assertNotNull(Schema.instance.getKeyspaceMetadata(cf.keyspace));
        assertEquals(Schema.instance.getKeyspaceMetadata(cf.keyspace), oldKs);

        // names should match.
        KeyspaceMetadata newBadKs2 = KeyspaceMetadata.create(cf.keyspace + "trash", KeyspaceParams.simple(4));
        try
        {
            SchemaTestUtil.announceKeyspaceUpdate(newBadKs2);
            throw new AssertionError("Should not have been able to update a KS with an invalid KS name.");
        }
        catch (ConfigurationException ex)
        {
            // expected.
        }

        Map<String, String> replicationMap = new HashMap<>();
        replicationMap.put(ReplicationParams.CLASS, NetworkTopologyStrategy.class.getName());
        replicationMap.put("replication_factor", "1");

        KeyspaceMetadata newKs = KeyspaceMetadata.create(cf.keyspace, KeyspaceParams.create(true, replicationMap));
        SchemaTestUtil.announceKeyspaceUpdate(newKs);

        KeyspaceMetadata newFetchedKs = Schema.instance.getKeyspaceMetadata(newKs.name);
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
        assertNotNull(Schema.instance.getTableMetadataRef(cf.ksName, cf.cfName));

        // updating certain fields should fail.
        CFMetaData newCfm = cf.copy();
        newCfm.defaultValidator(BytesType.instance);
        newCfm.minCompactionThreshold(5);
        newCfm.maxCompactionThreshold(31);

        // test valid operations.
        newCfm.comment("Modified comment");
        MigrationManager.announceTableUpdate(newCfm); // doesn't get set back here.

        newCfm.readRepairChance(0.23);
        MigrationManager.announceTableUpdate(newCfm);

        newCfm.gcGraceSeconds(12);
        MigrationManager.announceTableUpdate(newCfm);

        newCfm.defaultValidator(UTF8Type.instance);
        MigrationManager.announceTableUpdate(newCfm);

        newCfm.minCompactionThreshold(3);
        MigrationManager.announceTableUpdate(newCfm);

        newCfm.maxCompactionThreshold(33);
        MigrationManager.announceTableUpdate(newCfm);

        // can't test changing the reconciler because there is only one impl.

        // check the cumulative affect.
        assertEquals(Schema.instance.getTableMetadataRef(cf.ksName, cf.cfName).getComment(), newCfm.getComment());
        assertEquals(Schema.instance.getTableMetadataRef(cf.ksName, cf.cfName).getReadRepairChance(), newCfm.getReadRepairChance(), 0.0001);
        assertEquals(Schema.instance.getTableMetadataRef(cf.ksName, cf.cfName).getGcGraceSeconds(), newCfm.getGcGraceSeconds());
        assertEquals(UTF8Type.instance, Schema.instance.getTableMetadataRef(cf.ksName, cf.cfName).getDefaultValidator());

        // Change tableId
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
        SchemaKeyspace.makeCreateKeyspaceMutation(Schema.instance.getKeyspaceMetadata(KEYSPACE6), FBUtilities.timestampMicros()).build().applyUnsafe();
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE6).getColumnFamilyStore(TABLE1i);
        String indexName = TABLE1i + "_birthdate_key_index";

        // insert some data.  save the sstable descriptor so we can make sure it's marked for delete after the drop
        QueryProcessor.executeInternal(String.format(
                                                    "INSERT INTO %s.%s (key, c1, birthdate, notbirthdate) VALUES (?, ?, ?, ?)",
                                                    KEYSPACE6,
                                                    TABLE1i),
                                       "key0", "col0", 1L, 1L);

        Util.flush(cfs);
        ColumnFamilyStore indexCfs = cfs.indexManager.getIndexByName(indexName)
                                                     .getBackingTable()
                                                     .orElseThrow(throwAssert("Cannot access index cfs"));
        Descriptor desc = indexCfs.getLiveSSTables().iterator().next().descriptor;

        // drop the index
        TableMetadata meta = cfs.metadata();
        IndexMetadata existing = meta.indexes
                                     .get(indexName)
                                     .orElseThrow(throwAssert("Index not found"));

        SchemaTestUtil.announceTableUpdate(meta.unbuild().indexes(meta.indexes.without(existing.name)).build());

        // check
        assertTrue(cfs.indexManager.listIndexes().isEmpty());
        LifecycleTransaction.waitForDeletions();
        assertFalse(desc.fileFor(Components.DATA).exists());
    }

    @Test
    public void testValidateNullKeyspace() throws Exception
    {
        TableMetadata.Builder builder = TableMetadata.builder(null, TABLE1).addPartitionKeyColumn("partitionKey", BytesType.instance);

        TableMetadata table1 = builder.build();
        thrown.expect(ConfigurationException.class);
        thrown.expectMessage(null + "." + TABLE1 + ": Keyspace name must not be empty");
        table1.validate();
    }

    @Test
    public void testValidateCompatibilityIDMismatch() throws Exception
    {
        TableMetadata.Builder builder = TableMetadata.builder(KEYSPACE1, TABLE1).addPartitionKeyColumn("partitionKey", BytesType.instance);

        TableMetadata table1 = builder.build();
        TableMetadata table2 = table1.unbuild().id(TableId.generate()).build();
        thrown.expect(ConfigurationException.class);
        thrown.expectMessage(KEYSPACE1 + "." + TABLE1 + ": Table ID mismatch");
        table1.validateCompatibility(table2);
    }

    @Test
    public void testValidateCompatibilityNameMismatch() throws Exception
    {
        TableMetadata.Builder builder1 = TableMetadata.builder(KEYSPACE1, TABLE1).addPartitionKeyColumn("partitionKey", BytesType.instance);
        TableMetadata.Builder builder2 = TableMetadata.builder(KEYSPACE1, TABLE2).addPartitionKeyColumn("partitionKey", BytesType.instance);
        TableMetadata table1 = builder1.build();
        TableMetadata table2 = builder2.build();
        thrown.expect(ConfigurationException.class);
        thrown.expectMessage(KEYSPACE1 + "." + TABLE1 + ": Table mismatch");
        table1.validateCompatibility(table2);
    }

    @Test
    public void testEvolveSystemKeyspaceNew()
    {
        TableMetadata table = addTestTable("ks0", "t", "");
        KeyspaceMetadata keyspace = KeyspaceMetadata.create("ks0", KeyspaceParams.simple(1), Tables.of(table));

        SchemaTransformation transformation = SchemaTransformations.updateSystemKeyspace(keyspace, 0);
        Keyspaces before = Keyspaces.none();
        Keyspaces after = transformation.apply(before);
        Keyspaces.KeyspacesDiff diff = Keyspaces.diff(before, after);

        assertTrue(diff.altered.isEmpty());
        assertTrue(diff.dropped.isEmpty());
        assertEquals(keyspace, diff.created.getNullable("ks0"));
    }

    @Test
    public void testEvolveSystemKeyspaceExistsUpToDate()
    {
        TableMetadata table = addTestTable("ks1", "t", "");
        KeyspaceMetadata keyspace = KeyspaceMetadata.create("ks1", KeyspaceParams.simple(1), Tables.of(table));

        SchemaTransformation transformation = SchemaTransformations.updateSystemKeyspace(keyspace, 0);
        Keyspaces before = Keyspaces.of(keyspace);
        Keyspaces after = transformation.apply(before);
        Keyspaces.KeyspacesDiff diff = Keyspaces.diff(before, after);

        assertTrue(diff.isEmpty());
    }

    @Test
    public void testEvolveSystemKeyspaceChanged()
    {
        TableMetadata table0 = addTestTable("ks2", "t", "");
        KeyspaceMetadata keyspace0 = KeyspaceMetadata.create("ks2", KeyspaceParams.simple(1), Tables.of(table0));

        TableMetadata table1 = table0.unbuild().comment("comment").build();
        KeyspaceMetadata keyspace1 = KeyspaceMetadata.create("ks2", KeyspaceParams.simple(1), Tables.of(table1));

        SchemaTransformation transformation = SchemaTransformations.updateSystemKeyspace(keyspace1, 1);
        Keyspaces before = Keyspaces.of(keyspace0);
        Keyspaces after = transformation.apply(before);
        Keyspaces.KeyspacesDiff diff = Keyspaces.diff(before, after);

        assertTrue(diff.created.isEmpty());
        assertTrue(diff.dropped.isEmpty());
        assertEquals(1, diff.altered.size());
        assertEquals(keyspace1, diff.altered.get(0).after);
    }

    private TableMetadata addTestTable(String ks, String cf, String comment)
    {
        return
            TableMetadata.builder(ks, cf)
                         .addPartitionKeyColumn("key", UTF8Type.instance)
                         .addClusteringColumn("col", UTF8Type.instance)
                         .addRegularColumn("val", UTF8Type.instance)
                         .comment(comment)
                         .build();
    }
}
