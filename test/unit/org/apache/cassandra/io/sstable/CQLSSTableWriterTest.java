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
package org.apache.cassandra.io.sstable;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.File;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.types.*;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CQLSSTableWriterTest
{
    private static final AtomicInteger idGen = new AtomicInteger(0);
    private String keyspace;
    private String table;
    private String qualifiedTable;
    private File dataDir;

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void setup() throws Exception
    {
        CommitLog.instance.start();
        SchemaLoader.cleanupAndLeaveDirs();
        Keyspace.setInitialized();
        StorageService.instance.initServer();
    }

    @Before
    public void perTestSetup() throws IOException
    {
        keyspace = "cql_keyspace" + idGen.incrementAndGet();
        table = "table" + idGen.incrementAndGet();
        qualifiedTable = keyspace + '.' + table;
        dataDir = new File(tempFolder.newFolder().getAbsolutePath() + File.pathSeparator() + keyspace + File.pathSeparator() + table);
        assert dataDir.tryCreateDirectories();
    }

    @Test
    public void testUnsortedWriter() throws Exception
    {
        try (AutoCloseable switcher = Util.switchPartitioner(ByteOrderedPartitioner.instance))
        {
            String schema = "CREATE TABLE " + qualifiedTable + " ("
                          + "  k int PRIMARY KEY,"
                          + "  v1 text,"
                          + "  v2 int"
                          + ")";
            String insert = "INSERT INTO " + qualifiedTable + " (k, v1, v2) VALUES (?, ?, ?)";
            CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                      .inDirectory(dataDir)
                                                      .forTable(schema)
                                                      .using(insert).build();

            writer.addRow(0, "test1", 24);
            writer.addRow(1, "test2", 44);
            writer.addRow(2, "test3", 42);
            writer.addRow(ImmutableMap.<String, Object>of("k", 3, "v2", 12));

            writer.close();

            loadSSTables(dataDir, keyspace);

            UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
            assertEquals(4, rs.size());

            Iterator<UntypedResultSet.Row> iter = rs.iterator();
            UntypedResultSet.Row row;

            row = iter.next();
            assertEquals(0, row.getInt("k"));
            assertEquals("test1", row.getString("v1"));
            assertEquals(24, row.getInt("v2"));

            row = iter.next();
            assertEquals(1, row.getInt("k"));
            assertEquals("test2", row.getString("v1"));
            //assertFalse(row.has("v2"));
            assertEquals(44, row.getInt("v2"));

            row = iter.next();
            assertEquals(2, row.getInt("k"));
            assertEquals("test3", row.getString("v1"));
            assertEquals(42, row.getInt("v2"));

            row = iter.next();
            assertEquals(3, row.getInt("k"));
            assertEquals(null, row.getBytes("v1")); // Using getBytes because we know it won't NPE
            assertEquals(12, row.getInt("v2"));
        }
    }

    @Test
    public void testForbidCounterUpdates() throws Exception
    {
        String schema = "CREATE TABLE " + qualifiedTable + " (" +
                        "  my_id int, " +
                        "  my_counter counter, " +
                        "  PRIMARY KEY (my_id)" +
                        ")";
        String insert = String.format("UPDATE " + qualifiedTable + " SET my_counter = my_counter - ? WHERE my_id = ?");
        try
        {
            CQLSSTableWriter.builder().inDirectory(dataDir)
                            .forTable(schema)
                            .withPartitioner(Murmur3Partitioner.instance)
                            .using(insert).build();
            fail("Counter update statements should not be supported");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(e.getMessage(), "Counter modification statements are not supported");
        }
    }

    @Test
    public void testSyncWithinPartition() throws Exception
    {
        // Check that the write respect the buffer size even if we only insert rows withing the same partition (#7360)
        // To do that simply, we use a writer with a buffer of 1MiB, and write 2 rows in the same partition with a value
        // > 1MiB and validate that this created more than 1 sstable.
        String schema = "CREATE TABLE " + qualifiedTable + " ("
                      + "  k int PRIMARY KEY,"
                      + "  v blob"
                      + ")";
        String insert = "INSERT INTO " + qualifiedTable + " (k, v) VALUES (?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .using(insert)
                                                  .forTable(schema)
                                                  .withBufferSizeInMB(1)
                                                  .build();

        ByteBuffer val = ByteBuffer.allocate(1024 * 1050);

        writer.addRow(0, val);
        writer.addRow(1, val);
        writer.close();

        BiPredicate<File, String> filterDataFiles = (dir, name) -> name.endsWith("-Data.db");
        assert dataDir.tryListNames(filterDataFiles).length > 1 : Arrays.toString(dataDir.tryListNames(filterDataFiles));
    }


    @Test
    public void testSyncNoEmptyRows() throws Exception
    {
        // Check that the write does not throw an empty partition error (#9071)
        String schema = "CREATE TABLE " + qualifiedTable + " ("
                        + "  k UUID,"
                        + "  c int,"
                        + "  PRIMARY KEY (k)"
                        + ")";
        String insert = "INSERT INTO " + qualifiedTable + " (k, c) VALUES (?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using(insert)
                                                  .withBufferSizeInMB(1)
                                                  .build();

        for (int i = 0 ; i < 50000 ; i++) {
            writer.addRow(UUID.randomUUID(), 0);
        }
        writer.close();

    }

    @Test
    public void testDeleteStatement() throws Exception
    {

        final String schema = "CREATE TABLE " + qualifiedTable + " ("
                              + "  k int,"
                              + "  c1 int,"
                              + "  c2 int,"
                              + "  v text,"
                              + "  PRIMARY KEY (k, c1, c2)"
                              + ")";

        testUpdateStatement(); // start by adding some data
        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        assertEquals(2, resultSet.size());

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using("DELETE FROM " + qualifiedTable +
                                                         " WHERE k = ? AND c1 = ? AND c2 = ?")
                                                  .build();

        writer.addRow(1, 2, 3);
        writer.addRow(4, 5, 6);
        writer.close();
        loadSSTables(dataDir, keyspace);

        resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        assertEquals(0, resultSet.size());
        Iterator<UntypedResultSet.Row> iter = resultSet.iterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testDeletePartition() throws Exception
    {

        final String schema = "CREATE TABLE " + qualifiedTable + " ("
                              + "  k int,"
                              + "  c1 int,"
                              + "  c2 int,"
                              + "  v text,"
                              + "  PRIMARY KEY (k, c1, c2)"
                              + ")";

        // First, write some rows
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using("INSERT INTO " + qualifiedTable + " (k, c1, c2, v) " +
                                                         "VALUES (?, ?, ?, ?)")
                                                  .build();

        writer.addRow(1, 2, 3, "a");
        writer.addRow(1, 4, 5, "b");
        writer.addRow(1, 6, 7, "c");
        writer.addRow(2, 8, 9, "d");

        writer.close();
        loadSSTables(dataDir, keyspace);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        assertEquals(4, resultSet.size());
        Iterator<UntypedResultSet.Row> iter = resultSet.iterator();
        UntypedResultSet.Row r1 = iter.next();
        assertEquals(1, r1.getInt("k"));
        assertEquals(2, r1.getInt("c1"));
        assertEquals(3, r1.getInt("c2"));
        assertEquals("a", r1.getString("v"));
        UntypedResultSet.Row r2 = iter.next();
        assertEquals(1, r2.getInt("k"));
        assertEquals(4, r2.getInt("c1"));
        assertEquals(5, r2.getInt("c2"));
        assertEquals("b", r2.getString("v"));
        UntypedResultSet.Row r3 = iter.next();
        assertEquals(1, r3.getInt("k"));
        assertEquals(6, r3.getInt("c1"));
        assertEquals(7, r3.getInt("c2"));
        assertEquals("c", r3.getString("v"));
        UntypedResultSet.Row r4 = iter.next();
        assertEquals(2, r4.getInt("k"));
        assertEquals(8, r4.getInt("c1"));
        assertEquals(9, r4.getInt("c2"));
        assertEquals("d", r4.getString("v"));
        assertFalse(iter.hasNext());

        writer = CQLSSTableWriter.builder()
                                 .inDirectory(dataDir)
                                 .forTable(schema)
                                 .using("DELETE FROM " + qualifiedTable +
                                        " WHERE k = ?")
                                 .build();

        writer.addRow(1);
        writer.close();
        loadSSTables(dataDir, keyspace);

        resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        assertEquals(1, resultSet.size());
        iter = resultSet.iterator();
        UntypedResultSet.Row r5 = iter.next();
        assertEquals(2, r5.getInt("k"));
        assertEquals(8, r5.getInt("c1"));
        assertEquals(9, r5.getInt("c2"));
        assertEquals("d", r5.getString("v"));
        assertFalse(iter.hasNext());
    }

    @Test
    public void testDeleteRange() throws Exception
    {

        final String schema = "CREATE TABLE " + qualifiedTable + " ("
                              + "  k text,"
                              + "  c1 int,"
                              + "  c2 int,"
                              + "  v text,"
                              + "  PRIMARY KEY (k, c1, c2)"
                              + ")";

        CQLSSTableWriter updateWriter = CQLSSTableWriter.builder()
                                                        .inDirectory(dataDir)
                                                        .forTable(schema)
                                                        .using(String.format("UPDATE %s SET v=? WHERE k=? AND c1=? AND c2=?", qualifiedTable))
                                                        .build();
        CQLSSTableWriter deleteWriter = CQLSSTableWriter.builder()
                                                        .inDirectory(dataDir)
                                                        .forTable(schema)
                                                        .using(String.format("DELETE FROM %s WHERE k=? AND c1=? and c2>=?", qualifiedTable))
                                                        .build();

        updateWriter.addRow("v0.0", "a", 0, 0);
        updateWriter.addRow("v0.1", "a", 0, 1);
        updateWriter.addRow("v0.2", "a", 0, 2);
        updateWriter.addRow("v0.0", "b", 0, 0);
        updateWriter.addRow("v0.1", "b", 0, 1);
        updateWriter.addRow("v0.2", "b", 0, 2);
        updateWriter.close();
        deleteWriter.addRow("a", 0, 1);
        deleteWriter.addRow("b", 0, 2);
        deleteWriter.close();
        loadSSTables(dataDir, keyspace);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        assertEquals(3, resultSet.size());

        Iterator<UntypedResultSet.Row> iter = resultSet.iterator();
        UntypedResultSet.Row r1 = iter.next();
        assertEquals("a", r1.getString("k"));
        assertEquals(0, r1.getInt("c1"));
        assertEquals(0, r1.getInt("c2"));
        UntypedResultSet.Row r2 = iter.next();
        assertEquals("b", r2.getString("k"));
        assertEquals(0, r2.getInt("c1"));
        assertEquals(0, r2.getInt("c2"));
        UntypedResultSet.Row r3 = iter.next();
        assertEquals("b", r3.getString("k"));
        assertEquals(0, r3.getInt("c1"));
        assertEquals(1, r3.getInt("c2"));
    }

    @Test
    public void testDeleteRangeEmptyKeyComponent() throws Exception
    {


        final String schema = "CREATE TABLE " + qualifiedTable + " ("
                              + "  k text,"
                              + "  c1 int,"
                              + "  c2 int,"
                              + "  v text,"
                              + "  PRIMARY KEY (k, c1, c2)"
                              + ")";

        CQLSSTableWriter updateWriter = CQLSSTableWriter.builder()
                                                        .inDirectory(dataDir)
                                                        .forTable(schema)
                                                        .using(String.format("UPDATE %s SET v=? WHERE k=? AND c1=? AND c2=?", qualifiedTable))
                                                        .build();
        CQLSSTableWriter deleteWriter = CQLSSTableWriter.builder()
                                                        .inDirectory(dataDir)
                                                        .forTable(schema)
                                                        .using(String.format("DELETE FROM %s WHERE k=? AND c1=?", qualifiedTable))
                                                        .build();

        updateWriter.addRow("v0.0", "a", 0, 0);
        updateWriter.addRow("v0.1", "a", 0, 1);
        updateWriter.addRow("v0.2", "a", 1, 2);
        updateWriter.addRow("v0.0", "b", 0, 0);
        updateWriter.addRow("v0.1", "b", 0, 1);
        updateWriter.addRow("v0.2", "b", 1, 2);
        updateWriter.close();
        deleteWriter.addRow("a", 0);
        deleteWriter.addRow("b", 0);
        deleteWriter.close();
        loadSSTables(dataDir, keyspace);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        assertEquals(2, resultSet.size());

        Iterator<UntypedResultSet.Row> iter = resultSet.iterator();
        UntypedResultSet.Row r1 = iter.next();
        assertEquals("a", r1.getString("k"));
        assertEquals(1, r1.getInt("c1"));
        assertEquals(2, r1.getInt("c2"));
        UntypedResultSet.Row r2 = iter.next();
        assertEquals("b", r2.getString("k"));
        assertEquals(1, r2.getInt("c1"));
        assertEquals(2, r2.getInt("c2"));
    }

    @Test
    public void testDeleteValue() throws Exception
    {
        final String schema = "CREATE TABLE " + qualifiedTable + " ("
                              + "  k text,"
                              + "  c1 int,"
                              + "  c2 int,"
                              + "  v text,"
                              + "  PRIMARY KEY (k, c1, c2)"
                              + ")";

        CQLSSTableWriter insertWriter = CQLSSTableWriter.builder()
                                                        .inDirectory(dataDir)
                                                        .forTable(schema)
                                                        .using(String.format("INSERT INTO %s (v, k, c1, c2) values (?, ?, ?, ?)", qualifiedTable))
                                                        .build();

        // UPDATE does not set the row's liveness information, just the cells'. So when we delete the value from rows
        // added with the updateWriter, the entire row will no longer exist, not just the value.
        CQLSSTableWriter updateWriter = CQLSSTableWriter.builder()
                                                        .inDirectory(dataDir)
                                                        .forTable(schema)
                                                        .using(String.format("UPDATE %s SET v=? WHERE k=? AND c1=? AND c2=?", qualifiedTable))
                                                        .build();

        CQLSSTableWriter deleteWriter = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using("DELETE v FROM " + qualifiedTable +
                                                         " WHERE k = ? AND c1 = ? AND c2 = ?")
                                                  .build();

        insertWriter.addRow("v0.2", "a", 1, 2);
        insertWriter.close();

        updateWriter.addRow("v0.3", "b", 3, 4);
        updateWriter.close();

        loadSSTables(dataDir, keyspace);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        assertEquals(2, resultSet.size());
        Iterator<UntypedResultSet.Row> iter = resultSet.iterator();
        UntypedResultSet.Row insertedRow = iter.next();
        assertEquals("v0.2", insertedRow.getString("v"));
        assertEquals("a", insertedRow.getString("k"));
        assertEquals(1, insertedRow.getInt("c1"));
        assertEquals(2, insertedRow.getInt("c2"));
        UntypedResultSet.Row updatedRow = iter.next();
        assertEquals("v0.3", updatedRow.getString("v"));
        assertEquals("b", updatedRow.getString("k"));
        assertEquals(3, updatedRow.getInt("c1"));
        assertEquals(4, updatedRow.getInt("c2"));

        deleteWriter.addRow("a", 1, 2);
        deleteWriter.addRow("b", 3, 4);
        deleteWriter.close();
        loadSSTables(dataDir, keyspace);

        resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        assertEquals(1, resultSet.size());
        iter = resultSet.iterator();
        UntypedResultSet.Row modifiedRow = iter.next();
        assertFalse(modifiedRow.has("v"));
        assertEquals("a", modifiedRow.getString("k"));
        assertEquals(1, modifiedRow.getInt("c1"));
        assertEquals(2, modifiedRow.getInt("c2"));
    }

    private static final int NUMBER_WRITES_IN_RUNNABLE = 10;
    private class WriterThread extends Thread
    {
        private final File dataDir;
        private final int id;
        private final String qualifiedTable;
        public volatile Exception exception;

        public WriterThread(File dataDir, int id, String qualifiedTable)
        {
            this.dataDir = dataDir;
            this.id = id;
            this.qualifiedTable = qualifiedTable;
        }

        @Override
        public void run()
        {
            String schema = "CREATE TABLE " + qualifiedTable + " ("
                    + "  k int,"
                    + "  v int,"
                    + "  PRIMARY KEY (k, v)"
                    + ")";
            String insert = "INSERT INTO " + qualifiedTable + " (k, v) VALUES (?, ?)";
            CQLSSTableWriter writer = CQLSSTableWriter.builder()
                    .inDirectory(dataDir)
                    .forTable(schema)
                    .using(insert).build();

            try
            {
                for (int i = 0; i < NUMBER_WRITES_IN_RUNNABLE; i++)
                {
                    writer.addRow(id, i);
                }
                writer.close();
            }
            catch (Exception e)
            {
                exception = e;
            }
        }
    }

    @Test
    public void testConcurrentWriters() throws Exception
    {
        WriterThread[] threads = new WriterThread[5];
        for (int i = 0; i < threads.length; i++)
        {
            WriterThread thread = new WriterThread(dataDir, i, qualifiedTable);
            threads[i] = thread;
            thread.start();
        }

        for (WriterThread thread : threads)
        {
            thread.join();
            assert !thread.isAlive() : "Thread should be dead by now";
            if (thread.exception != null)
            {
                throw thread.exception;
            }
        }

        loadSSTables(dataDir, keyspace);

        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + ";");
        assertEquals(threads.length * NUMBER_WRITES_IN_RUNNABLE, rs.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWritesWithUdts() throws Exception
    {
        final String schema = "CREATE TABLE " + qualifiedTable + " ("
                              + "  k int,"
                              + "  v1 list<frozen<tuple2>>,"
                              + "  v2 frozen<tuple3>,"
                              + "  PRIMARY KEY (k)"
                              + ")";

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .withType("CREATE TYPE " + keyspace + ".tuple2 (a int, b int)")
                                                  .withType("CREATE TYPE " + keyspace + ".tuple3 (a int, b int, c int)")
                                                  .forTable(schema)
                                                  .using("INSERT INTO " + keyspace + "." + table + " (k, v1, v2) " +
                                                         "VALUES (?, ?, ?)").build();

        UserType tuple2Type = writer.getUDType("tuple2");
        UserType tuple3Type = writer.getUDType("tuple3");
        for (int i = 0; i < 100; i++)
        {
            writer.addRow(i,
                          ImmutableList.builder()
                                       .add(tuple2Type.newValue()
                                                      .setInt("a", i * 10)
                                                      .setInt("b", i * 20))
                                       .add(tuple2Type.newValue()
                                                      .setInt("a", i * 30)
                                                      .setInt("b", i * 40))
                                       .build(),
                          tuple3Type.newValue()
                                    .setInt("a", i * 100)
                                    .setInt("b", i * 200)
                                    .setInt("c", i * 300));
        }

        writer.close();
        loadSSTables(dataDir, keyspace);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + keyspace + "." + table);
        TypeCodec collectionCodec = JavaDriverUtils.codecFor(DataType.CollectionType.list(tuple2Type));
        TypeCodec tuple3Codec = JavaDriverUtils.codecFor(tuple3Type);

        assertEquals(resultSet.size(), 100);
        int cnt = 0;
        for (UntypedResultSet.Row row: resultSet) {
            assertEquals(cnt,
                         row.getInt("k"));
            List<UDTValue> values = (List<UDTValue>) collectionCodec.deserialize(row.getBytes("v1"),
                                                                                 ProtocolVersion.CURRENT);
            assertEquals(values.get(0).getInt("a"), cnt * 10);
            assertEquals(values.get(0).getInt("b"), cnt * 20);
            assertEquals(values.get(1).getInt("a"), cnt * 30);
            assertEquals(values.get(1).getInt("b"), cnt * 40);

            UDTValue v2 = (UDTValue) tuple3Codec.deserialize(row.getBytes("v2"), ProtocolVersion.CURRENT);

            assertEquals(v2.getInt("a"), cnt * 100);
            assertEquals(v2.getInt("b"), cnt * 200);
            assertEquals(v2.getInt("c"), cnt * 300);
            cnt++;
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWritesWithDependentUdts() throws Exception
    {
        final String schema = "CREATE TABLE " + qualifiedTable + " ("
                              + "  k int,"
                              + "  v1 frozen<nested_tuple>,"
                              + "  PRIMARY KEY (k)"
                              + ")";

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .withType("CREATE TYPE " + keyspace + ".nested_tuple (c int, tpl frozen<tuple2>)")
                                                  .withType("CREATE TYPE " + keyspace + ".tuple2 (a int, b int)")
                                                  .forTable(schema)
                                                  .using("INSERT INTO " + keyspace + "." + table + " (k, v1) " +
                                                         "VALUES (?, ?)")
                                                  .build();

        UserType tuple2Type = writer.getUDType("tuple2");
        UserType nestedTuple = writer.getUDType("nested_tuple");
        TypeCodec tuple2Codec = JavaDriverUtils.codecFor(tuple2Type);
        TypeCodec nestedTupleCodec = JavaDriverUtils.codecFor(nestedTuple);

        for (int i = 0; i < 100; i++)
        {
            writer.addRow(i,
                          nestedTuple.newValue()
                                     .setInt("c", i * 100)
                                     .set("tpl",
                                          tuple2Type.newValue()
                                                    .setInt("a", i * 200)
                                                    .setInt("b", i * 300),
                                          tuple2Codec));
        }

        writer.close();
        loadSSTables(dataDir, keyspace);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + keyspace + "." + table);

        assertEquals(resultSet.size(), 100);
        int cnt = 0;
        for (UntypedResultSet.Row row: resultSet) {
            assertEquals(cnt,
                         row.getInt("k"));
            UDTValue nestedTpl = (UDTValue) nestedTupleCodec.deserialize(row.getBytes("v1"),
                                                                         ProtocolVersion.CURRENT);
            assertEquals(nestedTpl.getInt("c"), cnt * 100);
            UDTValue tpl = nestedTpl.getUDTValue("tpl");
            assertEquals(tpl.getInt("a"), cnt * 200);
            assertEquals(tpl.getInt("b"), cnt * 300);

            cnt++;
        }
    }

    @Test
    public void testUnsetValues() throws Exception
    {
        final String schema = "CREATE TABLE " + qualifiedTable + " ("
                              + "  k int,"
                              + "  c1 int,"
                              + "  c2 int,"
                              + "  v text,"
                              + "  PRIMARY KEY (k, c1, c2)"
                              + ")";

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using("INSERT INTO " + qualifiedTable + " (k, c1, c2, v) " +
                                                         "VALUES (?, ?, ?, ?)")
                                                  .build();

        try
        {
            writer.addRow(1, 1, 1);
            fail("Passing less arguments then expected in prepared statement should not work.");
        }
        catch (InvalidRequestException e)
        {
            assertEquals("Invalid number of arguments, expecting 4 values but got 3",
                         e.getMessage());
        }

        try
        {
            writer.addRow(1, 1, CQLSSTableWriter.UNSET_VALUE, "1");
            fail("Unset values should not work with clustering columns.");
        }
        catch (InvalidRequestException e)
        {
            assertEquals("Invalid unset value for column c2",
                         e.getMessage());
        }

        try
        {
            writer.addRow(ImmutableMap.<String, Object>builder().put("k", 1).put("c1", 1).put("v", CQLSSTableWriter.UNSET_VALUE).build());
            fail("Unset or null clustering columns should not be allowed.");
        }
        catch (InvalidRequestException e)
        {
            assertEquals("Invalid null value in condition for column c2",
                         e.getMessage());
        }

        writer.addRow(1, 1, 1, CQLSSTableWriter.UNSET_VALUE);
        writer.addRow(2, 2, 2, null);
        writer.addRow(Arrays.asList(3, 3, 3, CQLSSTableWriter.UNSET_VALUE));
        writer.addRow(ImmutableMap.<String, Object>builder()
                                  .put("k", 4)
                                  .put("c1", 4)
                                  .put("c2", 4)
                                  .put("v", CQLSSTableWriter.UNSET_VALUE)
                                  .build());
        writer.addRow(Arrays.asList(3, 3, 3, CQLSSTableWriter.UNSET_VALUE));
        writer.addRow(5, 5, 5, "5");

        writer.close();
        loadSSTables(dataDir, keyspace);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        Iterator<UntypedResultSet.Row> iter = resultSet.iterator();
        UntypedResultSet.Row r1 = iter.next();
        assertEquals(1, r1.getInt("k"));
        assertEquals(1, r1.getInt("c1"));
        assertEquals(1, r1.getInt("c2"));
        assertEquals(false, r1.has("v"));
        UntypedResultSet.Row r2 = iter.next();
        assertEquals(2, r2.getInt("k"));
        assertEquals(2, r2.getInt("c1"));
        assertEquals(2, r2.getInt("c2"));
        assertEquals(false, r2.has("v"));
        UntypedResultSet.Row r3 = iter.next();
        assertEquals(3, r3.getInt("k"));
        assertEquals(3, r3.getInt("c1"));
        assertEquals(3, r3.getInt("c2"));
        assertEquals(false, r3.has("v"));
        UntypedResultSet.Row r4 = iter.next();
        assertEquals(4, r4.getInt("k"));
        assertEquals(4, r4.getInt("c1"));
        assertEquals(4, r4.getInt("c2"));
        assertEquals(false, r3.has("v"));
        UntypedResultSet.Row r5 = iter.next();
        assertEquals(5, r5.getInt("k"));
        assertEquals(5, r5.getInt("c1"));
        assertEquals(5, r5.getInt("c2"));
        assertEquals(true, r5.has("v"));
        assertEquals("5", r5.getString("v"));
    }

    @Test
    public void testUpdateStatement() throws Exception
    {
        final String schema = "CREATE TABLE " + qualifiedTable + " ("
                              + "  k int,"
                              + "  c1 int,"
                              + "  c2 int,"
                              + "  v text,"
                              + "  PRIMARY KEY (k, c1, c2)"
                              + ")";

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using("UPDATE " + qualifiedTable + " SET v = ? " +
                                                         "WHERE k = ? AND c1 = ? AND c2 = ?")
                                                  .build();

        writer.addRow("a", 1, 2, 3);
        writer.addRow("b", 4, 5, 6);
        writer.addRow(null, 7, 8, 9);
        writer.addRow(CQLSSTableWriter.UNSET_VALUE, 10, 11, 12);
        writer.close();
        loadSSTables(dataDir, keyspace);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        assertEquals(2, resultSet.size());

        Iterator<UntypedResultSet.Row> iter = resultSet.iterator();
        UntypedResultSet.Row r1 = iter.next();
        assertEquals(1, r1.getInt("k"));
        assertEquals(2, r1.getInt("c1"));
        assertEquals(3, r1.getInt("c2"));
        assertEquals("a", r1.getString("v"));
        UntypedResultSet.Row r2 = iter.next();
        assertEquals(4, r2.getInt("k"));
        assertEquals(5, r2.getInt("c1"));
        assertEquals(6, r2.getInt("c2"));
        assertEquals("b", r2.getString("v"));
        assertFalse(iter.hasNext());
    }

    @Test
    public void testNativeFunctions() throws Exception
    {
        final String schema = "CREATE TABLE " + qualifiedTable + " ("
                              + "  k int,"
                              + "  c1 int,"
                              + "  c2 int,"
                              + "  v blob,"
                              + "  PRIMARY KEY (k, c1, c2)"
                              + ")";

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using("INSERT INTO " + qualifiedTable + " (k, c1, c2, v) VALUES (?, ?, ?, text_as_blob(?))")
                                                  .build();

        writer.addRow(1, 2, 3, "abc");
        writer.addRow(4, 5, 6, "efg");

        writer.close();
        loadSSTables(dataDir, keyspace);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        assertEquals(2, resultSet.size());

        Iterator<UntypedResultSet.Row> iter = resultSet.iterator();
        UntypedResultSet.Row r1 = iter.next();
        assertEquals(1, r1.getInt("k"));
        assertEquals(2, r1.getInt("c1"));
        assertEquals(3, r1.getInt("c2"));
        assertEquals(ByteBufferUtil.bytes("abc"), r1.getBytes("v"));

        UntypedResultSet.Row r2 = iter.next();
        assertEquals(4, r2.getInt("k"));
        assertEquals(5, r2.getInt("c1"));
        assertEquals(6, r2.getInt("c2"));
        assertEquals(ByteBufferUtil.bytes("efg"), r2.getBytes("v"));

        assertFalse(iter.hasNext());
    }

    @Test
    public void testWriteWithNestedTupleUdt() throws Exception
    {
        // Check the writer does not throw "InvalidRequestException: Non-frozen tuples are not allowed inside collections: list<tuple<int, int>>"
        // See CASSANDRA-15857
        final String schema = "CREATE TABLE " + qualifiedTable + " ("
                              + "  k int,"
                              + "  v1 frozen<nested_type>,"
                              + "  PRIMARY KEY (k)"
                              + ")";

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .withType("CREATE TYPE " + keyspace + ".nested_type (a list<tuple<int, int>>)")
                                                  .forTable(schema)
                                                  .using("INSERT INTO " + qualifiedTable + " (k, v1) " +
                                                         "VALUES (?, ?)").build();

        UserType nestedType = writer.getUDType("nested_type");
        for (int i = 0; i < 100; i++)
        {
            writer.addRow(i, nestedType.newValue()
                                       .setList("a", Collections.emptyList()));
        }

        writer.close();
        loadSSTables(dataDir, keyspace);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        assertEquals(100, resultSet.size());
    }

    @Test
    public void testDateType() throws Exception
    {
        // Test to make sure we can write to `date` fields in both old and new formats
        String schema = "CREATE TABLE " + qualifiedTable + " ("
                        + "  k int,"
                        + "  c date,"
                        + "  PRIMARY KEY (k)"
                        + ")";
        String insert = "INSERT INTO " + qualifiedTable + " (k, c) VALUES (?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using(insert)
                                                  .withBufferSizeInMB(1)
                                                  .build();

        final int ID_OFFSET = 1000;
        for (int i = 0; i < 100 ; i++) {
            // Use old-style integer as date to test backwards-compatibility
            writer.addRow(i, i - Integer.MIN_VALUE); // old-style raw integer needs to be offset
            // Use new-style `LocalDate` for date value.
            writer.addRow(i + ID_OFFSET, LocalDate.fromDaysSinceEpoch(i));
        }
        writer.close();
        loadSSTables(dataDir, keyspace);

        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + ";");
        assertEquals(200, rs.size());
        Map<Integer, LocalDate> map = StreamSupport.stream(rs.spliterator(), false)
                                                   .collect(Collectors.toMap( r -> r.getInt("k"), r -> r.getDate("c")));
        for (int i = 0; i < 100; i++) {
            final LocalDate expected = LocalDate.fromDaysSinceEpoch(i);
            assertEquals(expected, map.get(i + ID_OFFSET));
            assertEquals(expected, map.get(i));
        }
    }

    @Test
    public void testFrozenMapType() throws Exception
    {
        // Test to make sure we can write to `date` fields in both old and new formats
        String schema = "CREATE TABLE " + qualifiedTable + " ("
                        + "  k text,"
                        + "  c frozen<map<text, text>>,"
                        + "  PRIMARY KEY (k, c)"
                        + ")";
        String insert = "INSERT INTO " + qualifiedTable + " (k, c) VALUES (?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using(insert)
                                                  .withBufferSizeInMB(1)
                                                  .build();
        for (int i = 0; i < 100; i++)
        {
            LinkedHashMap<String, String> map = new LinkedHashMap<>();
            map.put("a_key", "av" + i);
            map.put("b_key", "zv" + i);
            writer.addRow(String.valueOf(i), map);
        }
        for (int i = 100; i < 200; i++)
        {
            LinkedHashMap<String, String> map = new LinkedHashMap<>();
            map.put("b_key", "zv" + i);
            map.put("a_key", "av" + i);
            writer.addRow(String.valueOf(i), map);
        }
        writer.close();
        loadSSTables(dataDir, keyspace);

        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + ";");
        assertEquals(200, rs.size());
        Map<String, Map<String, String>> map = StreamSupport.stream(rs.spliterator(), false)
                                                            .collect(Collectors.toMap(r -> r.getString("k"), r -> r.getFrozenMap("c", UTF8Type.instance, UTF8Type.instance)));
        for (int i = 0; i < 200; i++)
        {
            final String expectedKey = String.valueOf(i);
            assertTrue(map.containsKey(expectedKey));
            Map<String, String> innerMap = map.get(expectedKey);
            assertTrue(innerMap.containsKey("a_key"));
            assertEquals(innerMap.get("a_key"), "av" + i);
            assertTrue(innerMap.containsKey("b_key"));
            assertEquals(innerMap.get("b_key"), "zv" + i);
        }

        // Make sure we can filter with map values regardless of which order we put the keys in
        UntypedResultSet filtered;
        filtered = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + " where k='0' and c={'a_key': 'av0', 'b_key': 'zv0'};");
        assertEquals(1, filtered.size());
        filtered = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + " where k='0' and c={'b_key': 'zv0', 'a_key': 'av0'};");
        assertEquals(1, filtered.size());
        filtered = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + " where k='100' and c={'b_key': 'zv100', 'a_key': 'av100'};");
        assertEquals(1, filtered.size());
        filtered = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + " where k='100' and c={'a_key': 'av100', 'b_key': 'zv100'};");
        assertEquals(1, filtered.size());
    }

    @Test
    public void testFrozenMapTypeCustomOrdered() throws Exception
    {
        // Test to make sure we can write to `date` fields in both old and new formats
        String schema = "CREATE TABLE " + qualifiedTable + " ("
                        + "  k text,"
                        + "  c frozen<map<timeuuid, int>>,"
                        + "  PRIMARY KEY (k, c)"
                        + ")";
        String insert = "INSERT INTO " + qualifiedTable + " (k, c) VALUES (?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using(insert)
                                                  .withBufferSizeInMB(1)
                                                  .build();
        UUID uuid1 = UUIDs.timeBased();
        UUID uuid2 = UUIDs.timeBased();
        UUID uuid3 = UUIDs.timeBased();
        UUID uuid4 = UUIDs.timeBased();
        Map<UUID, Integer> map = new LinkedHashMap<>();
        // NOTE: if these two `put` calls are switched, the test passes
        map.put(uuid2, 2);
        map.put(uuid1, 1);
        writer.addRow(String.valueOf(1), map);

        Map<UUID, Integer> map2 = new LinkedHashMap<>();
        map2.put(uuid3, 1);
        map2.put(uuid4, 2);
        writer.addRow(String.valueOf(2), map2);

        writer.close();
        loadSSTables(dataDir, keyspace);

        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + ";");
        assertEquals(2, rs.size());

        // Make sure we can filter with map values regardless of which order we put the keys in
        UntypedResultSet filtered;
        filtered = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + " where k='1' and c={" + uuid1 + ": 1, " + uuid2 + ": 2};");
        assertEquals(1, filtered.size());
        filtered = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + " where k='1' and c={" + uuid2 + ": 2, " + uuid1 + ": 1};");
        assertEquals(1, filtered.size());
        filtered = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + " where k='2' and c={" + uuid3 + ": 1, " + uuid4 + ": 2};");
        assertEquals(1, filtered.size());
        filtered = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + " where k='2' and c={" + uuid4 + ": 2, " + uuid3 + ": 1};");
        assertEquals(1, filtered.size());
        UUID other = UUIDs.startOf(1234L); // Just some other TimeUUID
        filtered = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + " where k='2' and c={" + uuid3 + ": 1, " + other + ": 2};");
        assertEquals(0, filtered.size());
        filtered = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + " where k='2' and c={" + uuid4 + ": 2, " + other + ": 1};");
        assertEquals(0, filtered.size());
    }

    @Test
    public void testFrozenSetTypeCustomOrdered() throws Exception
    {
        // Test to make sure we can write to `date` fields in both old and new formats
        String schema = "CREATE TABLE " + qualifiedTable + " ("
                        + "  k text,"
                        + "  c frozen<set<timeuuid>>,"
                        + "  PRIMARY KEY (k, c)"
                        + ")";
        String insert = "INSERT INTO " + qualifiedTable + " (k, c) VALUES (?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using(insert)
                                                  .withBufferSizeInMB(1)
                                                  .build();
        UUID uuid1 = UUIDs.startOf(0L);
        UUID uuid2 = UUIDs.startOf(10000000L);

        LinkedHashSet<UUID> set = new LinkedHashSet<>();
        set.add(uuid1);
        set.add(uuid2);
        writer.addRow(String.valueOf(1), set);

        LinkedHashSet<UUID> set2 = new LinkedHashSet<>();
        set2.add(uuid2);
        set2.add(uuid1);
        writer.addRow(String.valueOf(2), set2);

        writer.close();
        loadSSTables(dataDir, keyspace);

        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + ";");
        assertEquals(2, rs.size());

        // Make sure we can filter with map values regardless of which order we put the keys in
        UntypedResultSet filtered;
        filtered = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + " where k='1' and c={" + uuid1 + ", " + uuid2 + "};");
        assertEquals(1, filtered.size());
        filtered = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + " where k='1' and c={" + uuid2 + ", " + uuid1 + "};");
        assertEquals(1, filtered.size());
        filtered = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + " where k='2' and c={" + uuid1 + ", " + uuid2 + "};");
        assertEquals(1, filtered.size());
        filtered = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + " where k='2' and c={" + uuid2 + ", " + uuid1 + "};");
        assertEquals(1, filtered.size());
        UUID other = UUIDs.startOf(10000000L + 1L); // Pick one that's really close just to make sure clustering filters are working
        filtered = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + " where k='2' and c={" + uuid1 + ", " + other + "};");
        assertEquals(0, filtered.size());
        filtered = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable + " where k='2' and c={" + other + ", " + uuid1 + "};");
        assertEquals(0, filtered.size());
    }

    @Test
    public void testWriteWithTimestamps() throws Exception
    {
        long now = currentTimeMillis();
        long then = now - 1000;
        final String schema = "CREATE TABLE " + qualifiedTable + " ("
                              + "  k int,"
                              + "  v1 int,"
                              + "  v2 int,"
                              + "  v3 text,"
                              + "  PRIMARY KEY (k)"
                              + ")";

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using("INSERT INTO " + qualifiedTable +
                                                         " (k, v1, v2, v3) VALUES (?,?,?,?) using timestamp ?" )
                                                  .build();

        // Note that, all other things being equal, Cassandra will sort these rows lexicographically, so we use "higher" values in the
        // row we expect to "win" so that we're sure that it isn't just accidentally picked due to the row sorting.
        writer.addRow( 1, 4, 5, "b", now); // This write should be the one found at the end because it has a higher timestamp
        writer.addRow( 1, 2, 3, "a", then);
        writer.close();
        loadSSTables(dataDir, keyspace);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        assertEquals(1, resultSet.size());

        Iterator<UntypedResultSet.Row> iter = resultSet.iterator();
        UntypedResultSet.Row r1 = iter.next();
        assertEquals(1, r1.getInt("k"));
        assertEquals(4, r1.getInt("v1"));
        assertEquals(5, r1.getInt("v2"));
        assertEquals("b", r1.getString("v3"));
        assertFalse(iter.hasNext());
    }
    @Test
    public void testWriteWithTtl() throws Exception
    {
        final String schema = "CREATE TABLE " + qualifiedTable + " ("
                              + "  k int,"
                              + "  v1 int,"
                              + "  v2 int,"
                              + "  v3 text,"
                              + "  PRIMARY KEY (k)"
                              + ")";

        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                         .inDirectory(dataDir)
                                                         .forTable(schema)
                                                         .using("INSERT INTO " + qualifiedTable +
                                                                " (k, v1, v2, v3) VALUES (?,?,?,?) using TTL ?");
        CQLSSTableWriter writer = builder.build();
        // add a row that _should_ show up - 1 hour TTL
        writer.addRow( 1, 2, 3, "a", 3600);
        // Insert a row with a TTL of 1 second - should not appear in results once we sleep
        writer.addRow( 2, 4, 5, "b", 1);
        writer.close();
        Thread.sleep(1200); // Slightly over 1 second, just to make sure
        loadSSTables(dataDir, keyspace);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        assertEquals(1, resultSet.size());

        Iterator<UntypedResultSet.Row> iter = resultSet.iterator();
        UntypedResultSet.Row r1 = iter.next();
        assertEquals(1, r1.getInt("k"));
        assertEquals(2, r1.getInt("v1"));
        assertEquals(3, r1.getInt("v2"));
        assertEquals("a", r1.getString("v3"));
        assertFalse(iter.hasNext());
    }
    @Test
    public void testWriteWithTimestampsAndTtl() throws Exception
    {
        final String schema = "CREATE TABLE " + qualifiedTable + " ("
                              + "  k int,"
                              + "  v1 int,"
                              + "  v2 int,"
                              + "  v3 text,"
                              + "  PRIMARY KEY (k)"
                              + ")";

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using("INSERT INTO " + qualifiedTable +
                                                         " (k, v1, v2, v3) VALUES (?,?,?,?) using timestamp ? AND TTL ?" )
                                                  .build();
        // NOTE: It would be easier to make this a timestamp in the past, but Cassandra also has a _local_ deletion time
        // which is based on the server's timestamp, so simply setting the timestamp to some time in the past
        // doesn't actually do what you'd think it would do.
        long oneSecondFromNow = TimeUnit.MILLISECONDS.toMicros(currentTimeMillis() + 1000);
        // Insert some rows with a timestamp of 1 second from now, and different TTLs
        // add a row that _should_ show up - 1 hour TTL
        writer.addRow( 1, 2, 3, "a", oneSecondFromNow, 3600);
        // Insert a row "two seconds ago" with a TTL of 1 second - should not appear in results
        writer.addRow( 2, 4, 5, "b", oneSecondFromNow, 1);
        writer.close();
        loadSSTables(dataDir, keyspace);
        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        Thread.sleep(1200);
        resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        assertEquals(1, resultSet.size());

        Iterator<UntypedResultSet.Row> iter = resultSet.iterator();
        UntypedResultSet.Row r1 = iter.next();
        assertEquals(1, r1.getInt("k"));
        assertEquals(2, r1.getInt("v1"));
        assertEquals(3, r1.getInt("v2"));
        assertEquals("a", r1.getString("v3"));
        assertFalse(iter.hasNext());
    }

    @Test
    public void testWriteWithSorted() throws Exception
    {
        String schema = "CREATE TABLE " + qualifiedTable + " ("
                        + "  k int PRIMARY KEY,"
                        + "  v blob )";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using("INSERT INTO " + qualifiedTable +
                                                         " (k, v) VALUES (?, text_as_blob(?))" )
                                                  .sorted()
                                                  .build();
        int rowCount = 10_000;
        for (int i = 0; i < rowCount; i++)
        {
            writer.addRow(i, UUID.randomUUID().toString());
        }
        writer.close();
        loadSSTables(dataDir, keyspace);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        assertEquals(rowCount, resultSet.size());
        Iterator<UntypedResultSet.Row> iter = resultSet.iterator();
        for (int i = 0; i < rowCount; i++)
        {
            UntypedResultSet.Row row = iter.next();
            assertEquals(i, row.getInt("k"));
        }
    }

    @Test
    public void testWriteWithSortedAndMaxSize() throws Exception
    {
        String schema = "CREATE TABLE " + qualifiedTable + " ("
                        + "  k int PRIMARY KEY,"
                        + "  v blob )";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using("INSERT INTO " + qualifiedTable +
                                                         " (k, v) VALUES (?, text_as_blob(?))" )
                                                  .sorted()
                                                  .withMaxSSTableSizeInMiB(1)
                                                  .build();
        int rowCount = 30_000;
        // Max SSTable size is 1 MiB
        // 30_000 rows should take 30_000 * (4 + 37) = 1.17 MiB > 1 MiB
        for (int i = 0; i < rowCount; i++)
        {
            writer.addRow(i, UUID.randomUUID().toString());
        }
        writer.close();

        File[] dataFiles = dataDir.list(f -> f.name().endsWith(BigFormat.Components.DATA.type.repr));
        assertNotNull(dataFiles);
        assertEquals("The sorted writer should produce 2 sstables when max sstable size is configured",
                     2, dataFiles.length);
        long closeTo1MiBFileSize = Math.max(dataFiles[0].length(), dataFiles[1].length());
        assertTrue("The file size should be close to 1MiB (with at most 50KiB error rate for the test)",
                   Math.abs(1024 * 1024 - closeTo1MiBFileSize) < 50 * 1024);

        loadSSTables(dataDir, keyspace);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + qualifiedTable);
        assertEquals(rowCount, resultSet.size());
        Iterator<UntypedResultSet.Row> iter = resultSet.iterator();
        for (int i = 0; i < rowCount; i++)
        {
            UntypedResultSet.Row row = iter.next();
            assertEquals(i, row.getInt("k"));
        }
    }

    private static void loadSSTables(File dataDir, String ks) throws ExecutionException, InterruptedException
    {
        SSTableLoader loader = new SSTableLoader(dataDir, new SSTableLoader.Client()
        {
            private String keyspace;

            public void init(String keyspace)
            {
                this.keyspace = keyspace;
                for (Range<Token> range : StorageService.instance.getLocalReplicas(ks).ranges())
                    addRangeForEndpoint(range, FBUtilities.getBroadcastAddressAndPort());
            }

            public TableMetadataRef getTableMetadata(String cfName)
            {
                return Schema.instance.getTableMetadataRef(keyspace, cfName);
            }
        }, new OutputHandler.SystemOutput(false, false));

        loader.stream().get();
    }
}
