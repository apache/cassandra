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

package org.apache.cassandra.tools.cqlsh;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CqlshTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        requireNetwork();
        addVirtualKeyspace();
    }

    @Test
    public void testKeyspaceRequired()
    {
        ToolResult tool = ToolRunner.invokeCqlsh("SELECT * FROM test");
        tool.asserts().errorContains("No keyspace has been specified");
        assertEquals(2, tool.getExitCode());
    }

    @Test
    public void testCopyFloatVector() throws IOException
    {
        assertCopyOfVectorTypeSucceeds("float", 6, new Object[][] {
            row(1, vector(0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f)),
            row(2, vector(-0.1f, -0.2f, -0.3f, -0.4f, -0.5f, -0.6f)),
            row(3, vector(0.9f, 0.8f, 0.7f, 0.6f, 0.5f, 0.4f))
        });

        assertCopyOfVectorTypeSucceeds("float", 3, new Object[][] {
            row(1, vector(0.1f, 0.2f, 0.3f)),
            row(2, vector(-0.4f, -0.5f, -0.6f)),
            row(3, vector(0.7f, 0.8f, 0.9f))
        });
    }

    @Test
    public void testCopyIntVector() throws IOException
    {
        assertCopyOfVectorTypeSucceeds("int", 6, new Object[][] {
            row(1, vector(1, 2, 3, 4, 5, 6)),
            row(2, vector(-1, -2, -3, -4, -5, -6)),
            row(3, vector(9, 8, 7, 6, 5, 4))
        });

        assertCopyOfVectorTypeSucceeds("int", 3, new Object[][] {
            row(1, vector(1, 2, 3)),
            row(2, vector(-4, -5, -6)),
            row(3, vector(7, 8, 9))
        });
    }

    private void assertCopyOfVectorTypeSucceeds(String vectorType, int vectorSize, Object[][] rows) throws IOException
    {
        // given a table with a vector column
        createTable(KEYSPACE, format("CREATE TABLE %%s (id int PRIMARY KEY, embedding_vector vector<%s, %d>)", vectorType, vectorSize));
        assertTrue("table should be initially empty", execute("SELECT * FROM %s").isEmpty());

        // write the rows into the table
        for (Object[] row : rows)
            execute("INSERT INTO %s (id, embedding_vector) VALUES (?, ?)", row);

        // when running COPY TO CSV via cqlsh
        Path csv = createTempFile("test_copy_to_vector");
        ToolRunner.ToolResult copyToResult = ToolRunner.invokeCqlsh(format("COPY %s.%s TO '%s'", KEYSPACE, currentTable(), csv.toAbsolutePath()));

        // then all rows should be exported
        copyToResult.asserts().success();
        // verify that the exported CSV contains the expected rows
        assertThat(csv).hasSameTextualContentAs(prepareCSVFile(rows));

        // truncate the table
        execute("TRUNCATE %s");
        assertTrue("table should be empty", execute("SELECT * FROM %s").isEmpty());

        // when running COPY FROM via cqlsh
        ToolRunner.ToolResult copyFromResult = ToolRunner.invokeCqlsh(format("COPY %s.%s FROM '%s'", KEYSPACE, currentTable(), csv.toAbsolutePath()));

        // then all rows should be imported
        copyFromResult.asserts().success();
        UntypedResultSet importedRows = execute("SELECT * FROM %s");
        assertRowsIgnoringOrder(importedRows, rows);
    }

    @Test
    public void testCopyOnlyThoseRowsThatMatchVectorTypeSize() throws IOException
    {
        // given a table with a vector column and a file containing vector literals
        createTable(KEYSPACE, "CREATE TABLE %s (id int PRIMARY KEY, embedding_vector vector<int, 6>)");
        assertTrue("table should be initially empty", execute("SELECT * FROM %s").isEmpty());

        Object[][] rows = {
            row(1, vector(1, 2, 3, 4, 5, 6)),
            row(2, vector(1, 2, 3, 4, 5)),
            row(3, vector(1, 2, 3, 4, 6, 7))
        };

        Path csv = prepareCSVFile(rows);

        // when running COPY via cqlsh
        Path tmpDir = Files.createTempDirectory("CqlshTest");
        File tempFile = FileUtils.createTempFile("testCopyOnlyThoseRowsThatMatchVectorTypeSize", "", new File(tmpDir));
        // Since this test has failure, with ERRFILE option of COPY command, we can put the err file to tmp directory
        ToolRunner.ToolResult result = ToolRunner.invokeCqlsh(format("COPY %s.%s FROM '%s' WITH ERRFILE = '%s'", KEYSPACE, currentTable(), csv.toAbsolutePath(), tempFile));

        // then only rows that match type size should be imported
        result.asserts().failure();
        result.asserts().errorContains("The length of given vector value '5' is not equal to the vector size from the type definition '6'");
        UntypedResultSet importedRows = execute("SELECT * FROM %s");
        assertRowsIgnoringOrder(importedRows, row(1, vector(1, 2, 3, 4, 5, 6)),
                                row(3, vector(1, 2, 3, 4, 6, 7)));
    }

    @Test
    public void testManagementPortCommandStatus()
    {
        ToolResult tool = ToolRunner.invokeCqlshManagement(Collections.singletonList("INVOKE COMMAND status;"));
        tool.asserts().success();
        assertThat(tool.getStdout()).contains("execution_id");
        assertThat(tool.getStdout()).contains("output");
    }

    @Test
    public void testManagementPortSelectSystemLocal()
    {
        ToolResult tool = ToolRunner.invokeCqlshManagement(
            Collections.singletonList("SELECT cluster_name, release_version FROM system.local;"));
        tool.asserts().success();
        assertThat(tool.getStdout()).contains("cluster_name");
        assertThat(tool.getStdout()).contains("release_version");
    }

    @Test
    public void testManagementPortSelectSystemPeers()
    {
        ToolResult tool = ToolRunner.invokeCqlshManagement(Collections.singletonList("SELECT * FROM system.peers;"));
        tool.asserts().success();
        assertThat(tool.getStdout()).contains("peer");
    }

    @Test
    public void testManagementPortSelectSystemSchemaKeyspaces()
    {
        ToolResult tool = ToolRunner.invokeCqlshManagement(
            Collections.singletonList("SELECT keyspace_name FROM system_schema.keyspaces;"));
        tool.asserts().success();
        assertThat(tool.getStdout()).contains("keyspace_name");
        assertThat(tool.getStdout()).contains("system");
    }

    @Test
    public void testManagementPortSelectVirtualTableSettings()
    {
        ToolResult tool = ToolRunner.invokeCqlshManagement(
            Collections.singletonList("SELECT name FROM system_views.settings LIMIT 1;"));
        tool.asserts().success();
        assertThat(tool.getStdout()).contains("name");
    }

    @Test
    public void testManagementPortSelectVirtualTableClients()
    {
        ToolResult tool = ToolRunner.invokeCqlshManagement(
            Collections.singletonList("SELECT * FROM system_views.clients;"));
        tool.asserts().success();
        assertThat(tool.getStdout()).contains("address");
    }

    @Test
    public void testManagementPortRejectsNonSystemSelect()
    {
        String keyspaceName = createKeyspace(
            "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String tableName = createTable(keyspaceName, "CREATE TABLE %s (k text PRIMARY KEY, v int)");
        ToolResult tool = ToolRunner.invokeCqlshManagement(
            Collections.singletonList(format("SELECT * FROM %s.%s;", keyspaceName, tableName)));
        tool.asserts().failure();
    }

    @Test
    public void testManagementPortRejectsInsert()
    {
        String keyspaceName = createKeyspace(
            "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String tableName = createTable(keyspaceName, "CREATE TABLE %s (k text PRIMARY KEY, v int)");

        ToolResult tool = ToolRunner.invokeCqlshManagement(
            Collections.singletonList(format("INSERT INTO %s.%s (k, v) VALUES ('a', 1);", keyspaceName, tableName)));
        tool.asserts().failure();
    }

    private static Path prepareCSVFile(Object[][] rows) throws IOException
    {
        Path csv = createTempFile("test_copy_from_vector");

        try (Writer out = Files.newBufferedWriter(csv, StandardCharsets.UTF_8))
        {
            for (Object[] row : rows)
            {
                out.write(String.format("%s,\"%s\"\n", row[0], row[1]));
            }
        }

        return csv;
    }

    private static Path createTempFile(String prefix) throws IOException
    {
        Path csv = Files.createTempFile(prefix, ".csv");
        csv.toFile().deleteOnExit();
        return csv;
    }
}
