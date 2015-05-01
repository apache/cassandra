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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.google.common.io.Files;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.service.StorageService;

public class CQLSSTableWriterLongTest
{
    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.cleanupAndLeaveDirs();
        Keyspace.setInitialized();
        StorageService.instance.initServer();
    }

    @AfterClass
    public static void tearDown()
    {
        Config.setClientMode(false);
    }

    @Test
    public void testWideRow() throws Exception
    {
        String KS = "cql_keyspace";
        String TABLE = "table1";
        int size = 30000;

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        StringBuilder schemaColumns = new StringBuilder();
        StringBuilder queryColumns = new StringBuilder();
        StringBuilder queryValues = new StringBuilder();
        for (int i = 0; i < size; i++)
        {
            schemaColumns.append("v");
            schemaColumns.append(i);
            schemaColumns.append(" int,");

            queryColumns.append(", v");
            queryColumns.append(i);

            queryValues.append(", ?");
        }
        String schema = "CREATE TABLE cql_keyspace.table1 ("
                      + "  k int,"
                      + "  c int,"
                      + schemaColumns.toString()
                      + "  PRIMARY KEY (k, c)"
                      + ")";
        String insert = "INSERT INTO cql_keyspace.table1 (k, c" + queryColumns.toString() + ") VALUES (?, ?" + queryValues.toString() + ")";

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .withPartitioner(StorageService.instance.getPartitioner())
                                                  .using(insert)
                                                  .withBufferSizeInMB(1)
                                                  .build();

        long high = 100;
        Random r = new Random(0);
        for (int i = 0; i < high; i++)
        {
            List<Object> values = new ArrayList<>(size + 2);
            values.add(0);
            values.add(r.nextInt());
            for (int j = 0; j < size; j++)
            {
                values.add(i);
            }
            writer.addRow(values);
        }
        writer.close();
    }
}
