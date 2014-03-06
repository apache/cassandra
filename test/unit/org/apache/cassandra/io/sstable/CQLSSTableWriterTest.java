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
import java.util.Iterator;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

public class CQLSSTableWriterTest
{
    @BeforeClass
    public static void setup() throws Exception
    {
        StorageService.instance.initServer();
    }

    @Test
    public void testUnsortedWriter() throws Exception
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
                                                  .withPartitioner(StorageService.instance.getPartitioner())
                                                  .using(insert).build();

        writer.addRow(0, "test1", 24);
        writer.addRow(1, "test2", null);
        writer.addRow(2, "test3", 42);
        writer.addRow(ImmutableMap.<String, Object>of("k", 3, "v2", 12));
        writer.close();

        SSTableLoader loader = new SSTableLoader(dataDir, new SSTableLoader.Client()
        {
            public void init(String keyspace)
            {
                for (Range<Token> range : StorageService.instance.getLocalRanges("cql_keyspace"))
                    addRangeForEndpoint(range, FBUtilities.getBroadcastAddress());
                setPartitioner(StorageService.getPartitioner());
            }

            public CFMetaData getCFMetaData(String keyspace, String cfName)
            {
                return Schema.instance.getCFMetaData(keyspace, cfName);
            }
        }, new OutputHandler.SystemOutput(false, false));

        loader.stream().get();

        UntypedResultSet rs = QueryProcessor.processInternal("SELECT * FROM cql_keyspace.table1;");
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
        assertFalse(row.has("v2"));

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
