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

package org.apache.cassandra.db.compression;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compression.CompressionDictionaryDetailsTabularData.CompressionDictionaryDataObject;
import org.apache.cassandra.io.compress.IDictionaryCompressor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CompressionDictionaryTrainingFrequencyTest extends CQLTester
{
    private static final String tableName = "mytable";

    @BeforeClass
    public static void setup() throws Throwable
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testTrainingFrequency() throws Throwable
    {
        // we can train twice when no limit is imposed
        String tableId = createDictTable(IDictionaryCompressor.DEFAULT_TRAINING_MIN_FREQUENCY);
        trainDict();
        trainDict();

        assertDicts(2, tableId);

        alterDictTable("5m");

        assertDictTrainingFails("5m");

        alterDictTable("1m");

        // we can train again as 1 minute from the last training has passed
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MINUTES);
        trainDict();
        assertDicts(3, tableId);

        // resetting back to 0, so we can train whenever we want
        alterDictTable("0m");
        trainDict();
        assertDicts(4, tableId);

        alterDictTable("10m");

        Pair<CompressionDictionaryDataObject, File> export = export();
        assertFailingImport(export.right);

        alterDictTable("0m");
        assertSuccessfulImport(export.right);
    }

    private String getTableId()
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace(), tableName);
        Assert.assertNotNull(cfs);
        return cfs.metadata.id.toLongString();
    }

    private String createDictTable(String frequency)
    {
        schemaChange(format("CREATE TABLE %s.%s (id int PRIMARY KEY, data text)" +
                            " WITH compression = {'class': 'ZstdDictionaryCompressor', '%s': %s}",
                            keyspace(),
                            tableName,
                            IDictionaryCompressor.TRAINING_MIN_FREQUENCY_PARAMETER_NAME,
                            frequency));

        return getTableId();
    }

    private void alterDictTable(String trainingFrequency)
    {
        schemaChange(format("ALTER TABLE %s.%s WITH compression = {'class': 'ZstdDictionaryCompressor', '%s': %s}",
                            keyspace(),
                            tableName,
                            IDictionaryCompressor.TRAINING_MIN_FREQUENCY_PARAMETER_NAME,
                            trainingFrequency));
    }

    private void assertDicts(int expectedDicts, String tableId)
    {
        List<CompressionDictionary.LightweightCompressionDictionary> dicts = SystemDistributedKeyspace.retrieveLightweightCompressionDictionaries();
        assertNotNull(dicts);
        assertEquals(expectedDicts, dicts.size());
        for (int i = 0; i < expectedDicts; i++)
            assertEquals(tableId, dicts.get(i).tableId);
    }

    private void trainDict()
    {
        createSSTables();

        // Test training command with --force since we have limited test data
        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary", "train", "--force", keyspace(), tableName);
        result.assertOnCleanExit();

        assertThat(result.getStdout())
        .as("Should indicate training completed")
        .contains("Training completed successfully")
        .contains(keyspace())
        .contains(tableName);
    }

    private void assertDictTrainingFails(String frequency)
    {
        createSSTables();

        // Test training command with --force since we have limited test data
        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary", "train", "--force", keyspace(), tableName);
        Assert.assertEquals(1, result.getExitCode());

        assertThat(result.getStderr())
        .as("Should indicate training can not be triggered")
        .contains("The next training or importing can occur only at least after " + frequency + " from the last training which happened");

        String failingMessage = Arrays.stream(result.getStderr()
                                                    .split(System.lineSeparator()))
                                      .filter(p -> p.contains("The next training or importing can occur only at least"))
                                      .findFirst()
                                      .orElseThrow(() -> new RuntimeException("Unable to find failing message"));

        String pattern = "Failed to trigger training: The next training or importing can occur only at least after " +
                         "(.*) from the last training which happened at (.*). " +
                         "You can train again no earlier than at (.*).";
        Matcher matcher = Pattern.compile(pattern).matcher(failingMessage);

        assertTrue(matcher.matches());

        DurationSpec.IntMinutesBound frequencySpec = new DurationSpec.IntMinutesBound(matcher.group(1));
        Instant lastTraining = Instant.parse(matcher.group(2));
        Instant earliestNextTraining = Instant.parse(matcher.group(3));

        assertTrue(earliestNextTraining.isAfter(lastTraining));
        assertFalse(earliestNextTraining.minus(frequencySpec.toMinutes(), ChronoUnit.MINUTES).isBefore(lastTraining));
    }

    private void assertFailingImport(File file)
    {
        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary", "import", file.absolutePath());
        Assert.assertEquals(1, result.getExitCode());
    }

    private void assertSuccessfulImport(File file)
    {
        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary", "import", file.absolutePath());
        result.assertOnCleanExit();
    }

    private void createSSTables()
    {
        for (int file = 0; file < 10; file++)
        {
            int batchSize = 1000;
            for (int i = 0; i < batchSize; i++)
            {
                int index = i + file * batchSize;
                executeFormattedQuery(format("INSERT INTO %s.%s (id, data) VALUES (?, ?)", keyspace(), tableName),
                                      index, "test data " + index);
            }

            flush();
        }
    }

    private Pair<CompressionDictionaryDataObject, File> export() throws Throwable
    {
        File dictionaryFile = FileUtils.createTempFile("zstd-dictionary-", ".dict");
        ToolRunner.ToolResult result;

        result = invokeNodetool("compressiondictionary", "export", keyspace(), tableName, dictionaryFile.absolutePath());
        result.assertOnCleanExit();

        CompressionDictionaryDataObject dataObject = JsonUtils.deserializeFromJsonFile(CompressionDictionaryDataObject.class, dictionaryFile);

        assertTrue(dictionaryFile.exists());
        assertTrue(dictionaryFile.length() > 0);

        return Pair.create(dataObject, dictionaryFile);
    }
}
