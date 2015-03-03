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
import java.util.Arrays;

import com.google.common.io.Files;

import org.junit.*;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.FileUtils;

import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertTrue;

public class CQLSSTableWriterClientTest
{
    private File testDirectory;

    @Before
    public void setUp()
    {
        this.testDirectory = Files.createTempDir();
    }

    @After
    public void tearDown()
    {
        FileUtils.deleteRecursive(this.testDirectory);
    }

    @AfterClass
    public static void cleanup() throws Exception
    {
        Config.setClientMode(false);
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

        assertContainsDataFiles(this.testDirectory, "client_test-table1", "client_test-table2");
    }

    /**
     * Checks that the specified directory contains the files with the specified prefixes.
     *
     * @param directory the directory containing the data files
     * @param prefixes the file prefixes
     */
    private static void assertContainsDataFiles(File directory, String... prefixes)
    {
        FilenameFilter filter = new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String name)
            {
                return name.endsWith("-Data.db");
            }
        };

        File[] dataFiles = directory.listFiles(filter);
        Arrays.sort(dataFiles);

        assertEquals(dataFiles.length, prefixes.length);
        for (int i = 0; i < dataFiles.length; i++)
            assertTrue(dataFiles[i].toString().contains(prefixes[i]));
    }
}
