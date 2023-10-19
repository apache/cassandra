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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

@RunWith(BMUnitRunner.class)
public class SchemaKeyspaceTest
{
    private static final String KEYSPACE1 = "CFMetaDataTest1";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));

        MessagingService.instance().listen();
    }

    /** See CASSANDRA-16856/16996. Make sure schema pulls are synchronized to prevent concurrent schema pull/writes */
    @Test
    @BMRule(name = "delay partition updates to schema tables",
            targetClass = "CassandraTableWriteHandler",
            targetMethod = "write",
            action = "Thread.sleep(1000);",
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
            return Stream.generate(this::getSchemaMutations).filter(m -> !m.isEmpty()).findFirst().get();
        });

        creation.get(); // make sure the creation is finished

        Collection<Mutation> mutationsFromConcurrentAccess = mutationsFromThread.get();
        Collection<Mutation> settledMutations = getSchemaMutations();

        // If the worker thread picked up the creation at all, it should have the same modifications.
        // In other words, we should see all modifications or none.
        if (mutationsFromConcurrentAccess.size() == settledMutations.size())
        {
            assertEquals(1, settledMutations.size());
            Mutation mutationFromConcurrentAccess = mutationsFromConcurrentAccess.iterator().next();
            Mutation settledMutation = settledMutations.iterator().next();

            assertEquals("Read partial schema change!",
                         settledMutation.getTableIds(), mutationFromConcurrentAccess.getTableIds());
        }

        pool.shutdownNow();
    }

    private Collection<Mutation> getSchemaMutations()
    {
        AsyncPromise<Collection<Mutation>> p = new AsyncPromise<>();
        MessagingService.instance().sendWithCallback(Message.out(Verb.SCHEMA_PULL_REQ, NoPayload.noPayload),
                                                     FBUtilities.getBroadcastAddressAndPort(),
                                                     (RequestCallback<Collection<Mutation>>) msg -> p.setSuccess(msg.payload));
        p.syncUninterruptibly();
        return p.getNow();
    }

    @Test
    public void testConversionsInverses() throws Exception
    {
        for (String keyspaceName : Schema.instance.distributedKeyspaces().names())
        {
            for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
            {
                checkInverses(cfs.metadata());

                // Testing with compression to catch #3558
                TableMetadata withCompression = cfs.metadata().unbuild().compression(CompressionParams.snappy(32768)).build();
                checkInverses(withCompression);
            }
        }
    }

    @Test
    public void testExtensions() throws IOException
    {
        String keyspace = "SandBox";

        createTable(keyspace, "CREATE TABLE test (a text primary key, b int, c int)");

        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, "test");
        assertTrue("extensions should be empty", metadata.params.extensions.isEmpty());

        ImmutableMap<String, ByteBuffer> extensions = ImmutableMap.of("From ... with Love",
                                                                      ByteBuffer.wrap(new byte[]{0, 0, 7}));

        TableMetadata copy = metadata.unbuild().extensions(extensions).build();

        updateTable(keyspace, metadata, copy);

        metadata = Schema.instance.getTableMetadata(keyspace, "test");
        assertEquals(extensions, metadata.params.extensions);
    }

    @Test
    public void testReadRepair()
    {
        createTable("ks", "CREATE TABLE tbl (a text primary key, b int, c int) WITH read_repair='none'");
        TableMetadata metadata = Schema.instance.getTableMetadata("ks", "tbl");
        Assert.assertEquals(ReadRepairStrategy.NONE, metadata.params.readRepair);

    }

    @Test
    public void testAutoSnapshotEnabledOnTable()
    {
        Assume.assumeTrue(DatabaseDescriptor.isAutoSnapshot());
        String keyspaceName = "AutoSnapshot";
        String tableName = "table1";

        createTable(keyspaceName, "CREATE TABLE " + tableName + " (a text primary key, b int) WITH allow_auto_snapshot = true");

        ColumnFamilyStore cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(tableName);

        assertTrue(cfs.isAutoSnapshotEnabled());

        SchemaTestUtil.announceTableDrop(keyspaceName, tableName);

        assertFalse(cfs.listSnapshots().isEmpty());
    }

    @Test
    public void testAutoSnapshotDisabledOnTable()
    {
        Assume.assumeTrue(DatabaseDescriptor.isAutoSnapshot());
        String keyspaceName = "AutoSnapshot";
        String tableName = "table2";

        createTable(keyspaceName, "CREATE TABLE " + tableName + " (a text primary key, b int) WITH allow_auto_snapshot = false");

        ColumnFamilyStore cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(tableName);

        assertFalse(cfs.isAutoSnapshotEnabled());

        SchemaTestUtil.announceTableDrop(keyspaceName, tableName);

        assertTrue(cfs.listSnapshots().isEmpty());
    }

    private static void updateTable(String keyspace, TableMetadata oldTable, TableMetadata newTable)
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceInstance(keyspace).getMetadata();
        Mutation mutation = SchemaKeyspace.makeUpdateTableMutation(ksm, oldTable, newTable, FBUtilities.timestampMicros()).build();
        SchemaTestUtil.mergeAndAnnounceLocally(Collections.singleton(mutation));
    }

    private static void createTable(String keyspace, String cql)
    {
        TableMetadata table = CreateTableStatement.parse(cql, keyspace).build();

        KeyspaceMetadata ksm = KeyspaceMetadata.create(keyspace, KeyspaceParams.simple(1), Tables.of(table));
        Mutation mutation = SchemaKeyspace.makeCreateTableMutation(ksm, table, FBUtilities.timestampMicros()).build();
        SchemaTestUtil.mergeAndAnnounceLocally(Collections.singleton(mutation));
    }

    private static void checkInverses(TableMetadata metadata) throws Exception
    {
        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(metadata.keyspace);

        // Test schema conversion
        Mutation rm = SchemaKeyspace.makeCreateTableMutation(keyspace, metadata, FBUtilities.timestampMicros()).build();
        PartitionUpdate serializedCf = rm.getPartitionUpdate(Schema.instance.getTableMetadata(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.TABLES));
        PartitionUpdate serializedCD = rm.getPartitionUpdate(Schema.instance.getTableMetadata(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMNS));

        UntypedResultSet.Row tableRow = QueryProcessor.resultify(String.format("SELECT * FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.TABLES),
                                                                 UnfilteredRowIterators.filter(serializedCf.unfilteredIterator(), FBUtilities.nowInSeconds()))
                                                      .one();
        TableParams params = SchemaKeyspace.createTableParamsFromRow(tableRow);

        UntypedResultSet columnsRows = QueryProcessor.resultify(String.format("SELECT * FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMNS),
                                                                UnfilteredRowIterators.filter(serializedCD.unfilteredIterator(), FBUtilities.nowInSeconds()));
        Set<ColumnMetadata> columns = new HashSet<>();
        for (UntypedResultSet.Row row : columnsRows)
            columns.add(SchemaKeyspace.createColumnFromRow(row, Types.none(), UserFunctions.none()));

        assertEquals(metadata.params, params);
        assertEquals(new HashSet<>(metadata.columns()), columns);
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
