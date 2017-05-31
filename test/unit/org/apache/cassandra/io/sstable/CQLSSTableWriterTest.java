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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.UDHelper;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class CQLSSTableWriterTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.cleanupAndLeaveDirs();
        Keyspace.setInitialized();
        StorageService.instance.initServer();
    }

    @Test
    public void testUnsortedWriter() throws Exception
    {
        try (AutoCloseable switcher = Util.switchPartitioner(ByteOrderedPartitioner.instance))
        {
            String KS = "cql_keyspace";
            String TABLE = "table1";

            File tempdir = Files.createTempDir();
            File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
            assert dataDir.mkdirs();

            String schema = "CREATE TABLE cql_keyspace.table1 ("
                          + "  k int PRIMARY KEY,"
                          + "  v1 text,"
                          + "  v2 int"
                          + ")";
            String insert = "INSERT INTO cql_keyspace.table1 (k, v1, v2) VALUES (?, ?, ?)";
            CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                      .inDirectory(dataDir)
                                                      .forTable(schema)
                                                      .using(insert).build();

            writer.addRow(0, "test1", 24);
            writer.addRow(1, "test2", 44);
            writer.addRow(2, "test3", 42);
            writer.addRow(ImmutableMap.<String, Object>of("k", 3, "v2", 12));

            writer.close();

            loadSSTables(dataDir, KS);

            UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM cql_keyspace.table1;");
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
        String KS = "cql_keyspace";
        String TABLE = "counter1";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        String schema = "CREATE TABLE cql_keyspace.counter1 (" +
                        "  my_id int, " +
                        "  my_counter counter, " +
                        "  PRIMARY KEY (my_id)" +
                        ")";
        String insert = String.format("UPDATE cql_keyspace.counter1 SET my_counter = my_counter - ? WHERE my_id = ?");
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
            assertEquals(e.getMessage(), "Counter update statements are not supported");
        }
    }

    @Test
    public void testSyncWithinPartition() throws Exception
    {
        // Check that the write respect the buffer size even if we only insert rows withing the same partition (#7360)
        // To do that simply, we use a writer with a buffer of 1MB, and write 2 rows in the same partition with a value
        // > 1MB and validate that this created more than 1 sstable.
        String KS = "ks";
        String TABLE = "test";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();
        String schema = "CREATE TABLE ks.test ("
                      + "  k int PRIMARY KEY,"
                      + "  v blob"
                      + ")";
        String insert = "INSERT INTO ks.test (k, v) VALUES (?, ?)";
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
        File tempdir = Files.createTempDir();
        String schema = "CREATE TABLE ks.test2 ("
                        + "  k UUID,"
                        + "  c int,"
                        + "  PRIMARY KEY (k)"
                        + ")";
        String insert = "INSERT INTO ks.test2 (k, c) VALUES (?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(tempdir)
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
        public volatile Exception exception;

        public WriterThread(File dataDir, int id)
        {
            this.dataDir = dataDir;
            this.id = id;
        }

        @Override
        public void run()
        {
            String schema = "CREATE TABLE cql_keyspace2.table2 ("
                    + "  k int,"
                    + "  v int,"
                    + "  PRIMARY KEY (k, v)"
                    + ")";
            String insert = "INSERT INTO cql_keyspace2.table2 (k, v) VALUES (?, ?)";
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
        final String KS = "cql_keyspace2";
        final String TABLE = "table2";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        WriterThread[] threads = new WriterThread[5];
        for (int i = 0; i < threads.length; i++)
        {
            WriterThread thread = new WriterThread(dataDir, i);
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

        loadSSTables(dataDir, KS);

        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM cql_keyspace2.table2;");
        assertEquals(threads.length * NUMBER_WRITES_IN_RUNNABLE, rs.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWritesWithUdts() throws Exception
    {
        final String KS = "cql_keyspace3";
        final String TABLE = "table3";

        final String schema = "CREATE TABLE " + KS + "." + TABLE + " ("
                              + "  k int,"
                              + "  v1 list<frozen<tuple2>>,"
                              + "  v2 frozen<tuple3>,"
                              + "  PRIMARY KEY (k)"
                              + ")";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .withType("CREATE TYPE " + KS + ".tuple2 (a int, b int)")
                                                  .withType("CREATE TYPE " + KS + ".tuple3 (a int, b int, c int)")
                                                  .forTable(schema)
                                                  .using("INSERT INTO " + KS + "." + TABLE + " (k, v1, v2) " +
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
        loadSSTables(dataDir, KS);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + KS + "." + TABLE);
        TypeCodec collectionCodec = UDHelper.codecFor(DataType.CollectionType.frozenList(tuple2Type));
        TypeCodec tuple3Codec = UDHelper.codecFor(tuple3Type);

        assertEquals(resultSet.size(), 100);
        int cnt = 0;
        for (UntypedResultSet.Row row: resultSet) {
            assertEquals(cnt,
                         row.getInt("k"));
            List<UDTValue> values = (List<UDTValue>) collectionCodec.deserialize(row.getBytes("v1"),
                                                                                 ProtocolVersion.NEWEST_SUPPORTED);
            assertEquals(values.get(0).getInt("a"), cnt * 10);
            assertEquals(values.get(0).getInt("b"), cnt * 20);
            assertEquals(values.get(1).getInt("a"), cnt * 30);
            assertEquals(values.get(1).getInt("b"), cnt * 40);

            UDTValue v2 = (UDTValue) tuple3Codec.deserialize(row.getBytes("v2"), ProtocolVersion.NEWEST_SUPPORTED);

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
        final String KS = "cql_keyspace4";
        final String TABLE = "table4";

        final String schema = "CREATE TABLE " + KS + "." + TABLE + " ("
                              + "  k int,"
                              + "  v1 frozen<nested_tuple>,"
                              + "  PRIMARY KEY (k)"
                              + ")";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .withType("CREATE TYPE " + KS + ".nested_tuple (c int, tpl frozen<tuple2>)")
                                                  .withType("CREATE TYPE " + KS + ".tuple2 (a int, b int)")
                                                  .forTable(schema)
                                                  .using("INSERT INTO " + KS + "." + TABLE + " (k, v1) " +
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
        loadSSTables(dataDir, KS);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + KS + "." + TABLE);

        assertEquals(resultSet.size(), 100);
        int cnt = 0;
        for (UntypedResultSet.Row row: resultSet) {
            assertEquals(cnt,
                         row.getInt("k"));
            UDTValue nestedTpl = (UDTValue) nestedTupleCodec.deserialize(row.getBytes("v1"),
                                                                         ProtocolVersion.NEWEST_SUPPORTED);
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
        final String KS = "cql_keyspace5";
        final String TABLE = "table5";

        final String schema = "CREATE TABLE " + KS + "." + TABLE + " ("
                              + "  k int,"
                              + "  c1 int,"
                              + "  c2 int,"
                              + "  v text,"
                              + "  PRIMARY KEY (k, c1, c2)"
                              + ")";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using("INSERT INTO " + KS + "." + TABLE + " (k, c1, c2, v) " +
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
        loadSSTables(dataDir, KS);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + KS + "." + TABLE);
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
    public void testUpdateSatement() throws Exception
    {
        final String KS = "cql_keyspace6";
        final String TABLE = "table6";

        final String schema = "CREATE TABLE " + KS + "." + TABLE + " ("
                              + "  k int,"
                              + "  c1 int,"
                              + "  c2 int,"
                              + "  v text,"
                              + "  PRIMARY KEY (k, c1, c2)"
                              + ")";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using("UPDATE " + KS + "." + TABLE + " SET v = ? " +
                                                         "WHERE k = ? AND c1 = ? AND c2 = ?")
                                                  .build();

        writer.addRow("a", 1, 2, 3);
        writer.addRow("b", 4, 5, 6);
        writer.addRow(null, 7, 8, 9);
        writer.addRow(CQLSSTableWriter.UNSET_VALUE, 10, 11, 12);
        writer.close();
        loadSSTables(dataDir, KS);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + KS + "." + TABLE);
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

    private static void loadSSTables(File dataDir, String ks) throws ExecutionException, InterruptedException
    {
        SSTableLoader loader = new SSTableLoader(dataDir, new SSTableLoader.Client()
        {
            private String keyspace;

            public void init(String keyspace)
            {
                this.keyspace = keyspace;
                for (Range<Token> range : StorageService.instance.getLocalRanges(ks))
                    addRangeForEndpoint(range, FBUtilities.getBroadcastAddress());
            }

            public TableMetadataRef getTableMetadata(String cfName)
            {
                return Schema.instance.getTableMetadataRef(keyspace, cfName);
            }
        }, new OutputHandler.SystemOutput(false, false));

        loader.stream().get();
    }
}
