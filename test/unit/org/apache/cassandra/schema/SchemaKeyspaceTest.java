/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.schema;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableMap;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.ThriftConversion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(BMUnitRunner.class)
public class SchemaKeyspaceTest
{
    private static final String KEYSPACE1 = "CFMetaDataTest1";
    private static final String CF_STANDARD1 = "Standard1";

    private static final List<ColumnDef> columnDefs = new ArrayList<>();

    static
    {
        columnDefs.add(new ColumnDef(ByteBufferUtil.bytes("col1"), AsciiType.class.getCanonicalName())
                                    .setIndex_name("col1Index")
                                    .setIndex_type(IndexType.KEYS));

        columnDefs.add(new ColumnDef(ByteBufferUtil.bytes("col2"), UTF8Type.class.getCanonicalName())
                                    .setIndex_name("col2Index")
                                    .setIndex_type(IndexType.KEYS));
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    /** See CASSANDRA-16856. Make sure schema pulls are synchronized to prevent concurrent schema pull/writes
     *
     * @throws Exception
     */
    @Test
    public void testSchemaPullSynchoricity() throws Exception
    {
        for (String methodName : Arrays.asList("convertSchemaToMutations",
                                               "truncate",
                                               "saveSystemKeyspacesSchema"))
        {
            Method method = SchemaKeyspace.class.getDeclaredMethod(methodName);
            assertTrue(methodName + " is not thread-safe", Modifier.isSynchronized(method.getModifiers()));
        }

        Method method = SchemaKeyspace.class.getDeclaredMethod("calculateSchemaDigest", Set.class);
        assertTrue(Modifier.isSynchronized(method.getModifiers()));
        method = SchemaKeyspace.class.getDeclaredMethod("mergeSchemaAndAnnounceVersion", Collection.class);
        assertTrue(Modifier.isSynchronized(method.getModifiers()));
        method = SchemaKeyspace.class.getDeclaredMethod("mergeSchema", Collection.class);
        assertTrue(Modifier.isSynchronized(method.getModifiers()));
        method = SchemaKeyspace.class.getDeclaredMethod("mergeSchema", Keyspaces.class, Keyspaces.class);
        assertTrue(Modifier.isSynchronized(method.getModifiers()));        
    }

    /** See CASSANDRA-16856/16996. Make sure schema pulls are synchronized to prevent concurrent schema pull/writes */
    @Test
    @BMRule(name = "delay partition updates to schema tables",
            targetClass = "ColumnFamilyStore",
            targetMethod = "apply",
            action = "Thread.sleep(5000);",
            targetLocation = "AT EXIT")
    public void testNoVisiblePartialSchemaUpdates() throws Exception
    {
        String keyspace = "sandbox";
        ExecutorService pool = Executors.newFixedThreadPool(2);

        SchemaKeyspace.truncate(); // Make sure there's nothing but the create we're about to do
        CyclicBarrier barrier = new CyclicBarrier(2);

        Future<Void> creation = pool.submit(() -> {
            barrier.await();
            createTable(keyspace, "CREATE TABLE test (a text primary key, b int, c int)");
            return null;
        });

        Future<Collection<Mutation>> mutationsFromThread = pool.submit(() -> {
            barrier.await();

            // Make sure we actually have a mutation to check for partial modification.
            Collection<Mutation> mutations = SchemaKeyspace.convertSchemaToMutations();
            while (mutations.size() == 0)
                mutations = SchemaKeyspace.convertSchemaToMutations();

            return mutations;
        });

        creation.get(); // make sure the creation is finished

        Collection<Mutation> mutationsFromConcurrentAccess = mutationsFromThread.get();
        Collection<Mutation> settledMutations = SchemaKeyspace.convertSchemaToMutations();

        // If the worker thread picked up the creation at all, it should have the same modifications.
        // In other words, we should see all modifications or none.
        if (mutationsFromConcurrentAccess.size() == settledMutations.size())
        {
            assertEquals(1, settledMutations.size());
            Mutation mutationFromConcurrentAccess = mutationsFromConcurrentAccess.iterator().next();
            Mutation settledMutation = settledMutations.iterator().next();

            assertEquals("Read partial schema change!",
                         settledMutation.getColumnFamilyIds(), mutationFromConcurrentAccess.getColumnFamilyIds());
        }

        pool.shutdownNow();
    }

    @Test
    public void testThriftConversion() throws Exception
    {
        CfDef cfDef = new CfDef().setDefault_validation_class(AsciiType.class.getCanonicalName())
                                 .setComment("Test comment")
                                 .setColumn_metadata(columnDefs)
                                 .setKeyspace(KEYSPACE1)
                                 .setName(CF_STANDARD1);

        // convert Thrift to CFMetaData
        CFMetaData cfMetaData = ThriftConversion.fromThrift(cfDef);

        CfDef thriftCfDef = new CfDef();
        thriftCfDef.keyspace = KEYSPACE1;
        thriftCfDef.name = CF_STANDARD1;
        thriftCfDef.default_validation_class = cfDef.default_validation_class;
        thriftCfDef.comment = cfDef.comment;
        thriftCfDef.column_metadata = new ArrayList<>();
        for (ColumnDef columnDef : columnDefs)
        {
            ColumnDef c = new ColumnDef();
            c.name = ByteBufferUtil.clone(columnDef.name);
            c.validation_class = columnDef.getValidation_class();
            c.index_name = columnDef.getIndex_name();
            c.index_type = IndexType.KEYS;
            thriftCfDef.column_metadata.add(c);
        }

        CfDef converted = ThriftConversion.toThrift(cfMetaData);

        assertEquals(thriftCfDef.keyspace, converted.keyspace);
        assertEquals(thriftCfDef.name, converted.name);
        assertEquals(thriftCfDef.default_validation_class, converted.default_validation_class);
        assertEquals(thriftCfDef.comment, converted.comment);
        assertEquals(new HashSet<>(thriftCfDef.column_metadata), new HashSet<>(converted.column_metadata));
    }

    @Test
    public void testConversionsInverses() throws Exception
    {
        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
            {
                CFMetaData cfm = cfs.metadata;
                if (!cfm.isThriftCompatible())
                    continue;

                checkInverses(cfm);

                // Testing with compression to catch #3558
                CFMetaData withCompression = cfm.copy();
                withCompression.compression(CompressionParams.snappy(32768));
                checkInverses(withCompression);
            }
        }
    }

    @Test
    public void testExtensions() throws IOException
    {
        String keyspace = "SandBox";

        createTable(keyspace, "CREATE TABLE test (a text primary key, b int, c int)");

        CFMetaData metadata = Schema.instance.getCFMetaData(keyspace, "test");
        assertTrue("extensions should be empty", metadata.params.extensions.isEmpty());

        ImmutableMap<String, ByteBuffer> extensions = ImmutableMap.of("From ... with Love",
                                                                      ByteBuffer.wrap(new byte[]{0, 0, 7}));

        CFMetaData copy = metadata.copy().extensions(extensions);

        updateTable(keyspace, metadata, copy);

        metadata = Schema.instance.getCFMetaData(keyspace, "test");
        assertEquals(extensions, metadata.params.extensions);
    }

    private static void updateTable(String keyspace, CFMetaData oldTable, CFMetaData newTable)
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceInstance(keyspace).getMetadata();
        Mutation mutation = SchemaKeyspace.makeUpdateTableMutation(ksm, oldTable, newTable, FBUtilities.timestampMicros()).build();
        SchemaKeyspace.mergeSchema(Collections.singleton(mutation));
    }

    private static void createTable(String keyspace, String cql)
    {
        CFMetaData table = CFMetaData.compile(cql, keyspace);

        KeyspaceMetadata ksm = KeyspaceMetadata.create(keyspace, KeyspaceParams.simple(1), Tables.of(table));
        Mutation mutation = SchemaKeyspace.makeCreateTableMutation(ksm, table, FBUtilities.timestampMicros()).build();
        SchemaKeyspace.mergeSchema(Collections.singleton(mutation));
    }

    private static void checkInverses(CFMetaData cfm) throws Exception
    {
        KeyspaceMetadata keyspace = Schema.instance.getKSMetaData(cfm.ksName);

        // Test thrift conversion
        CFMetaData before = cfm;
        CFMetaData after = ThriftConversion.fromThriftForUpdate(ThriftConversion.toThrift(before), before);
        assert before.equals(after) : String.format("%n%s%n!=%n%s", before, after);

        // Test schema conversion
        Mutation rm = SchemaKeyspace.makeCreateTableMutation(keyspace, cfm, FBUtilities.timestampMicros()).build();
        PartitionUpdate serializedCf = rm.getPartitionUpdate(Schema.instance.getId(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.TABLES));
        PartitionUpdate serializedCD = rm.getPartitionUpdate(Schema.instance.getId(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMNS));

        UntypedResultSet.Row tableRow = QueryProcessor.resultify(String.format("SELECT * FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.TABLES),
                                                                 UnfilteredRowIterators.filter(serializedCf.unfilteredIterator(), FBUtilities.nowInSeconds()))
                                                      .one();
        TableParams params = SchemaKeyspace.createTableParamsFromRow(tableRow);

        UntypedResultSet columnsRows = QueryProcessor.resultify(String.format("SELECT * FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMNS),
                                                                UnfilteredRowIterators.filter(serializedCD.unfilteredIterator(), FBUtilities.nowInSeconds()));
        Set<ColumnDefinition> columns = new HashSet<>();
        for (UntypedResultSet.Row row : columnsRows)
            columns.add(SchemaKeyspace.createColumnFromRow(row, Types.none()));

        assertEquals(cfm.params, params);
        assertEquals(new HashSet<>(cfm.allColumns()), columns);
    }

    private static boolean hasCDC(Mutation m)
    {
        for (PartitionUpdate p : m.getPartitionUpdates())
        {
            for (ColumnDefinition cd : p.columns())
            {
                if (cd.name.toString().equals("cdc"))
                    return true;
            }
        }
        return false;
    }

    private static boolean hasSchemaTables(Mutation m)
    {
        for (PartitionUpdate p : m.getPartitionUpdates())
        {
            if (p.metadata().cfName.equals(SchemaKeyspaceTables.TABLES))
                return true;
        }
        return false;
    }

    @Test
    public void testConvertSchemaToMutationsWithoutCDC() throws IOException
    {
        boolean oldCDCOption = DatabaseDescriptor.isCDCEnabled();
        try
        {
            DatabaseDescriptor.setCDCEnabled(false);
            Collection<Mutation> mutations = SchemaKeyspace.convertSchemaToMutations();
            boolean foundTables = false;
            for (Mutation m : mutations)
            {
                if (hasSchemaTables(m))
                {
                    foundTables = true;
                    assertFalse(hasCDC(m));
                    try (DataOutputBuffer output = new DataOutputBuffer())
                    {
                        Mutation.serializer.serialize(m, output, MessagingService.current_version);
                        try (DataInputBuffer input = new DataInputBuffer(output.getData()))
                        {
                            Mutation out = Mutation.serializer.deserialize(input, MessagingService.current_version);
                            assertFalse(hasCDC(out));
                        }
                    }
                }
            }
            assertTrue(foundTables);
        }
        finally
        {
            DatabaseDescriptor.setCDCEnabled(oldCDCOption);
        }
    }

    @Test
    public void testConvertSchemaToMutationsWithCDC()
    {
        boolean oldCDCOption = DatabaseDescriptor.isCDCEnabled();
        try
        {
            DatabaseDescriptor.setCDCEnabled(true);
            Collection<Mutation> mutations = SchemaKeyspace.convertSchemaToMutations();
            boolean foundTables = false;
            for (Mutation m : mutations)
            {
                if (hasSchemaTables(m))
                {
                    foundTables = true;
                    assertTrue(hasCDC(m));
                }
            }
            assertTrue(foundTables);
        }
        finally
        {
            DatabaseDescriptor.setCDCEnabled(oldCDCOption);
        }
    }

    @Test
    public void testSchemaDigest()
    {
        Set<ByteBuffer> abc = Collections.singleton(ByteBufferUtil.bytes("abc"));
        Pair<UUID, UUID> versions = SchemaKeyspace.calculateSchemaDigest(abc);
        assertTrue(versions.left.equals(versions.right));

        Set<ByteBuffer> cdc = Collections.singleton(ByteBufferUtil.bytes("cdc"));
        versions = SchemaKeyspace.calculateSchemaDigest(cdc);
        assertFalse(versions.left.equals(versions.right));
    }

    @Test(expected = SchemaKeyspace.MissingColumns.class)
    public void testSchemaNoPartition()
    {
        String testKS = "test_schema_no_partition";
        String testTable = "invalid_table";
        SchemaLoader.createKeyspace(testKS,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(testKS, testTable));
        // Delete partition column in the schema
        String query = String.format("DELETE FROM %s.%s WHERE keyspace_name=? and table_name=? and column_name=?", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMNS);
        executeOnceInternal(query, testKS, testTable, "key");
        SchemaKeyspace.fetchNonSystemKeyspaces();
    }

    @Test(expected = SchemaKeyspace.MissingColumns.class)
    public void testSchemaNoColumn()
    {
        String testKS = "test_schema_no_Column";
        String testTable = "invalid_table";
        SchemaLoader.createKeyspace(testKS,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(testKS, testTable));
        // Delete all colmns in the schema
        String query = String.format("DELETE FROM %s.%s WHERE keyspace_name=? and table_name=?", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMNS);
        executeOnceInternal(query, testKS, testTable);
        SchemaKeyspace.fetchNonSystemKeyspaces();
    }
}
