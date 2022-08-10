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
import java.util.function.BiPredicate;

import com.google.common.io.Files;
import org.apache.cassandra.io.util.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.FileUtils;

import static org.junit.Assert.assertEquals;

public class CQLSSTableWriterClientTest
{
    private File testDirectory;

    @Before
    public void setUp()
    {
        this.testDirectory = new File(Files.createTempDir());
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void testWriterInClientMode() throws IOException, InvalidRequestException
    {
        final String TABLE1 = "table1";
        final String TABLE2 = "table2";

        String schema = "CREATE TABLE client_test.%s ("
                            + "  k int PRIMARY KEY,"
                            + "  v1 text,"
                            + "  v2 int"
                            + ")";
        String insert = "INSERT INTO client_test.%s (k, v1, v2) VALUES (?, ?, ?)";

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(this.testDirectory)
                                                  .forTable(String.format(schema, TABLE1))
                                                  .using(String.format(insert, TABLE1)).build();

        CQLSSTableWriter writer2 = CQLSSTableWriter.builder()
                                                   .inDirectory(this.testDirectory)
                                                   .forTable(String.format(schema, TABLE2))
                                                   .using(String.format(insert, TABLE2)).build();

        writer.addRow(0, "A", 0);
        writer2.addRow(0, "A", 0);
        writer.addRow(1, "B", 1);
        writer2.addRow(1, "B", 1);
        writer.close();
        writer2.close();

        BiPredicate<File, String> filter = (dir, name) -> name.endsWith("-Data.db");

        File[] dataFiles = this.testDirectory.tryList(filter);
        assertEquals(2, dataFiles.length);
    }

    @After
    public void tearDown()
    {
        FileUtils.deleteRecursive(this.testDirectory);
    }
}
