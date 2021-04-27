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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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
import org.apache.cassandra.cql3.functions.UDHelper;
import org.apache.cassandra.cql3.functions.types.*;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link CQLSSTableWriter}.
 *
 * Please note: most tests here both create sstables and try to load them, so for the last part, we need to make sure
 * we have properly "loaded" the table (which we do with {@link SchemaLoader#load(String, String, String...)}). But
 * a small subtlety is that this <b>must</b> be called before we call {@link CQLSSTableWriter#builder} because
 * otherwise the guardrail validation in {@link CreateTableStatement#validate(QueryState)} ends up breaking because
 * the {@link ColumnFamilyStore} is not loaded yet. This would not be a problem in real usage of
 * {@link CQLSSTableWriter} because the later only calls {@link DatabaseDescriptor#clientInitialization}, not
 * {@link DatabaseDescriptor#daemonInitialization}, so said guardrail validation don't execute, but this test does
 * manually call {@link DatabaseDescriptor#daemonInitialization} so...
 */
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
        dataDir = new File(tempFolder.newFolder().getAbsolutePath() + File.separator + keyspace + File.separator + table);
        assert dataDir.mkdirs();
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

            SchemaLoader.load(keyspace, schema);

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
            SchemaLoader.load(keyspace, schema);
            CQLSSTableWriter.builder().inDirectory(dataDir)
                            .forTable(schema)
                            .withPartitioner(Murmur3Partitioner.instance)
                            .using(insert).build();
            fail("Counter update statements should not be supported");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(e.getMessage(), "Counter update statements are not supported");
        }
    }

    @Test
    public void testSyncWithinPartition() throws Exception
    {
        // Check that the write respect the buffer size even if we only insert rows withing the same partition (#7360)
        // To do that simply, we use a writer with a buffer of 1MB, and write 2 rows in the same partition with a value
        // > 1MB and validate that this created more than 1 sstable.
        String schema = "CREATE TABLE " + qualifiedTable + " ("
                      + "  k int PRIMARY KEY,"
                      + "  v blob"
                      + ")";
        String insert = "INSERT INTO " + qualifiedTable + " (k, v) VALUES (?, ?)";
        SchemaLoader.load(keyspace, schema);
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

        FilenameFilter filterDataFiles = new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.endsWith("-Data.db");
            }
        };
        assert dataDir.list(filterDataFiles).length > 1 : Arrays.toString(dataDir.list(filterDataFiles));
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
        SchemaLoader.load(keyspace, schema);
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
            SchemaLoader.load(keyspace, schema);
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

        String type1 = "CREATE TYPE " + keyspace + ".tuple2 (a int, b int)";
        String type2 = "CREATE TYPE " + keyspace + ".tuple3 (a int, b int, c int)";
        SchemaLoader.load(keyspace, schema, type1, type2);
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .withType(type1)
                                                  .withType(type2)
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
        TypeCodec collectionCodec = UDHelper.codecFor(DataType.CollectionType.list(tuple2Type));
        TypeCodec tuple3Codec = UDHelper.codecFor(tuple3Type);

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

        String type1 = "CREATE TYPE " + keyspace + ".tuple2 (a int, b int)";
        String type2 = "CREATE TYPE " + keyspace + ".nested_tuple (c int, tpl frozen<tuple2>)";
        SchemaLoader.load(keyspace, schema, type1, type2);
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .withType(type2)
                                                  .withType(type1)
                                                  .forTable(schema)
                                                  .using("INSERT INTO " + keyspace + "." + table + " (k, v1) " +
                                                         "VALUES (?, ?)")
                                                  .build();

        UserType tuple2Type = writer.getUDType("tuple2");
        UserType nestedTuple = writer.getUDType("nested_tuple");
        TypeCodec tuple2Codec = UDHelper.codecFor(tuple2Type);
        TypeCodec nestedTupleCodec = UDHelper.codecFor(nestedTuple);

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

        SchemaLoader.load(keyspace, schema);
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

        SchemaLoader.load(keyspace, schema);
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

        SchemaLoader.load(keyspace, schema);
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using("INSERT INTO " + qualifiedTable + " (k, c1, c2, v) VALUES (?, ?, ?, textAsBlob(?))")
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
