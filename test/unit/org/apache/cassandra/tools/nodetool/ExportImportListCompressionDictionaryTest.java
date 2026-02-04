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

package org.apache.cassandra.tools.nodetool;

import java.nio.file.Files;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.compression.CompressionDictionaryDetailsTabularData.CompressionDictionaryDataObject;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.apache.cassandra.utils.JsonUtils.serializeToJsonFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class ExportImportListCompressionDictionaryTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Throwable
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testExportImportListCompressionDictionary() throws Throwable
    {
        // create table, train dictionary for it, export it to json file
        String firstTable = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");
        Pair<CompressionDictionaryDataObject, File> pair = trainAndExport(firstTable);

        String secondTable = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");
        rewriteTable(secondTable, pair);

        importDictionary(pair.right);

        list(pair.left, firstTable);
        list(export(secondTable, null).left, secondTable);
    }

    @Test
    public void testExportingSpecificDictionary() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");
        Pair<CompressionDictionaryDataObject, File> pair = trainAndExport(table);
        Pair<CompressionDictionaryDataObject, File> pair2 = trainAndExport(table);

        export(table, pair.left.dictId);
        export(table, pair2.left.dictId);
    }

    @Test
    public void testInvalidKeyspaceTable() throws Throwable
    {
        // create table, train dictionary for it, export it to json file
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");
        Pair<CompressionDictionaryDataObject, File> pair = trainAndExport(table);

        // test non-existing keyspace / table
        serializeToJsonFile(new CompressionDictionaryDataObject("abc",
                                                                "def",
                                                                "123",
                                                                pair.left.dictId,
                                                                pair.left.dict,
                                                                pair.left.kind,
                                                                pair.left.dictChecksum,
                                                                pair.left.dictLength), pair.right);

        ToolResult result = invokeNodetool("compressiondictionary", "import", pair.right.absolutePath());
        assertTrue(result.getStderr().contains("Unable to import dictionary JSON: Table abc.def does not exist or does not support dictionary compression"));
    }

    @Test
    public void testValidationOnClient() throws Throwable
    {
        // create table, train dictionary for it, export it to json file
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");
        Pair<CompressionDictionaryDataObject, File> pair = trainAndExport(table);

        // remove table on purpose  will trigger client-side validation
        // and it fails to even reach Cassandra
        JsonNode jsonNode = JsonUtils.JSON_OBJECT_MAPPER.readTree(Files.readString(pair.right.toPath()));
        ObjectNode node = (ObjectNode) jsonNode;
        node.remove("table");

        File jsonWithoutTable = FileUtils.createTempFile("zstd-dictionary-", ".dict");
        JsonUtils.JSON_OBJECT_MAPPER.writeValue(jsonWithoutTable.toJavaIOFile(), node);

        ToolResult result = invokeNodetool("compressiondictionary", "import", jsonWithoutTable.absolutePath());
        assertTrue(result.getStderr().contains("Unable to import dictionary JSON: Table not specified."));
    }

    @Test
    public void testNotImportingOlderThanLatest() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");
        Pair<CompressionDictionaryDataObject, File> pair = trainAndExport(table);
        Pair<CompressionDictionaryDataObject, File> pair2 = trainAndExport(table);

        String newTable = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");
        rewriteTable(newTable, pair);
        rewriteTable(newTable, pair2);

        // import newer first
        ToolResult result = invokeNodetool("compressiondictionary", "import", pair2.right.absolutePath());
        result.assertOnCleanExit();
        // older will not be possible to import
        result = invokeNodetool("compressiondictionary", "import", pair.right.absolutePath());
        assertTrue(result.getStderr().contains(format("Unable to import dictionary JSON: Dictionary to import has older dictionary id " +
                                                      "(%s) than the latest compression dictionary (%s) " +
                                                      "for table %s.%s",
                                                      pair.left.dictId,
                                                      pair2.left.dictId,
                                                      keyspace(), newTable)));
    }

    @Test
    public void testImportingIntoTableWithDisabledCompression() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");
        Pair<CompressionDictionaryDataObject, File> pair = trainAndExport(table);

        alterTable("ALTER TABLE %s WITH compression = {'enabled': false}");

        ToolResult result = invokeNodetool("compressiondictionary", "import", pair.right.absolutePath());
        Assert.assertEquals(1, result.getExitCode());
        assertTrue(result.getStderr().contains(format("Unable to import dictionary JSON: Table %s.%s does not exist or does not support dictionary compression",
                                                      keyspace(),
                                                      table)));
    }

    private void importDictionary(File dictFile)
    {
        ToolResult result = invokeNodetool("compressiondictionary", "import", dictFile.absolutePath());
        result.assertOnCleanExit();
    }

    private void list(CompressionDictionaryDataObject dataObject, String table)
    {
        ToolResult result = invokeNodetool("compressiondictionary", "list", keyspace(), table);
        result.assertOnExitCode();
        assertTrue(result.getStdout()
                         .contains(format("%s %s %s %s %s %s",
                                          keyspace(), table, dataObject.dictId,
                                          dataObject.kind, dataObject.dictChecksum,
                                          dataObject.dictLength)));
    }

    private void rewriteTable(String table, Pair<CompressionDictionaryDataObject, File> pair) throws Throwable
    {
        serializeToJsonFile(new CompressionDictionaryDataObject(pair.left.keyspace,
                                                                table,
                                                                pair.left.tableId,
                                                                pair.left.dictId,
                                                                pair.left.dict,
                                                                pair.left.kind,
                                                                pair.left.dictChecksum,
                                                                pair.left.dictLength),
                            pair.right);
    }

    private Pair<CompressionDictionaryDataObject, File> trainAndExport(String table) throws Throwable
    {
        trainDictionary(table);
        return export(table, null);
    }

    private Pair<CompressionDictionaryDataObject, File> export(String table, Long id) throws Throwable
    {
        File dictionaryFile = FileUtils.createTempFile("zstd-dictionary-", ".dict");
        ToolResult result;
        if (id != null)
        {
            result = invokeNodetool("compressiondictionary", "export", keyspace(), table, dictionaryFile.absolutePath(), "--id", id.toString());
            result.assertOnCleanExit();
        }
        else
        {
            result = invokeNodetool("compressiondictionary", "export", keyspace(), table, dictionaryFile.absolutePath());
            result.assertOnCleanExit();
        }
        CompressionDictionaryDataObject dataObject = JsonUtils.deserializeFromJsonFile(CompressionDictionaryDataObject.class, dictionaryFile);

        assertTrue(dictionaryFile.exists());
        assertTrue(dictionaryFile.length() > 0);

        return Pair.create(dataObject, dictionaryFile);
    }

    private void trainDictionary(String table)
    {
        createSSTables();

        // Test training command with --force since we have limited test data
        ToolResult result = invokeNodetool("compressiondictionary", "train", "--force", keyspace(), table);
        result.assertOnCleanExit();

        assertThat(result.getStdout())
        .as("Should indicate training completed")
        .contains("Training completed successfully")
        .contains(keyspace())
        .contains(table);
    }

    private void createSSTables()
    {
        for (int file = 0; file < 10; file++)
        {
            int batchSize = 1000;
            for (int i = 0; i < batchSize; i++)
            {
                int index = i + file * batchSize;
                execute("INSERT INTO %s (id, data) VALUES (?, ?)", index, "test data " + index);
            }

            flush();
        }
    }
}
