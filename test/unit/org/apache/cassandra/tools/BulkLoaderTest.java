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

package org.apache.cassandra.tools;


import java.io.File;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.TFramedTransportFactory;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;

public class BulkLoaderTest
{

    static EmbeddedCassandraService embeddedCassandraService = new EmbeddedCassandraService();

    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.cleanupAndLeaveDirs();
        embeddedCassandraService.start();


        QueryProcessor.executeInternal("CREATE KEYSPACE cql_keyspace WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
    }


    @Test
    public void testClientWriter() throws Exception
    {
        String KS = "cql_keyspace";
        String TABLE = "table2";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        String schema = "CREATE TABLE cql_keyspace.table2 ("
                        + "  k int PRIMARY KEY,"
                        + "  v1 text,"
                        + "  v2 int"
                        + ")";

        QueryProcessor.executeInternal(schema);

        String insert = "INSERT INTO cql_keyspace.table2 (k, v1, v2) VALUES (?, ?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .withPartitioner(StorageService.instance.getPartitioner())
                                                  .using(insert).build();

        writer.addRow(0, "test1", 24);
        writer.addRow(1, "test2", null);
        writer.addRow(2, "test3", 42);
        writer.addRow(ImmutableMap.<String, Object>of("k", 3, "v2", 12));
        writer.close();

        BulkLoader.ExternalClient client = new BulkLoader.ExternalClient(Sets.newHashSet(FBUtilities.getLocalAddress()),
                                                                         DatabaseDescriptor.getRpcPort(),
                                                                         null, null, new TFramedTransportFactory(),
                                                                         DatabaseDescriptor.getStoragePort(),
                                                                         DatabaseDescriptor.getSSLStoragePort(), null);


        SSTableLoader loader = new SSTableLoader(dataDir, client, new OutputHandler.SystemOutput(false, false));



        loader.stream().get();

        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM cql_keyspace.table2;");
        assertEquals(4, rs.size());
    }


    @Test
    public void testClientWriterWithDroppedColumn() throws Exception
    {
        String KS = "cql_keyspace";
        String TABLE = "table3";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        String schemaToDrop = "CREATE TABLE cql_keyspace.table3 ("
                            + "  k int PRIMARY KEY,"
                            + "  v1 text,"
                            + "  v2 int,"
                            + "  v3 list<int>,"
                            + "  v4 text"
                            + ")";

        QueryProcessor.executeInternal(schemaToDrop);
        QueryProcessor.executeInternal("ALTER TABLE cql_keyspace.table3 DROP v4");


        String schema = "CREATE TABLE cql_keyspace.table3 ("
                        + "  k int PRIMARY KEY,"
                        + "  v1 text,"
                        + "  v2 int,"
                        + "  v3 list<int>"
                        + ")";


        String insert = "INSERT INTO cql_keyspace.table3 (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .withPartitioner(StorageService.instance.getPartitioner())
                                                  .using(insert).build();

        writer.addRow(0, "test1", 24, Lists.newArrayList(4));
        writer.addRow(1, "test2", null, Lists.newArrayList(4,4,5));
        writer.addRow(2, "test3", 42, null);
        writer.close();

        BulkLoader.ExternalClient client = new BulkLoader.ExternalClient(Sets.newHashSet(FBUtilities.getLocalAddress()),
                                                                         DatabaseDescriptor.getRpcPort(),
                                                                         null, null, new TFramedTransportFactory(),
                                                                         DatabaseDescriptor.getStoragePort(),
                                                                         DatabaseDescriptor.getSSLStoragePort(), null);

        SSTableLoader loader = new SSTableLoader(dataDir, client, new OutputHandler.SystemOutput(false, false));
        

        loader.stream().get();


        CFMetaData cfMetaData = client.getCFMetaData(KS, TABLE);
        assert cfMetaData != null;

        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM cql_keyspace.table3;");
        assertEquals(3, rs.size());
    }

}
