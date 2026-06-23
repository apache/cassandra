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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLNodetoolProtocolTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compression.CompressionDictionary.LightweightCompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionaryDetailsTabularData.CompressionDictionaryDataObject;
import org.apache.cassandra.io.compress.IDictionaryCompressor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class CompressionDictionaryTrainingFrequencyTest extends CQLNodetoolProtocolTester
{
    private static final String tableName = "mytable";

    @BeforeClass
    public static void setup() throws Throwable
    {
        requireNetwork();
        startJMXServer();
    }

    @After
    public void afterCompressionDictionaryTest() throws Throwable
    {
        execute(format("TRUNCATE %s.%s",
                       SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                       SystemDistributedKeyspace.COMPRESSION_DICTIONARIES));
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
        backdateLastDictionaryCreatedAt(tableId);
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
        assertThat(cfs).isNotNull();
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
        List<LightweightCompressionDictionary> dicts = SystemDistributedKeyspace.retrieveLightweightCompressionDictionaries();
        assertThat(dicts).isNotNull().hasSize(expectedDicts);
        for (int i = 0; i < expectedDicts; i++)
            assertThat(dicts.get(i).tableId).isEqualTo(tableId);
    }

    // instead of explicit waiting, just overwrite created_at directly in the table
    private void backdateLastDictionaryCreatedAt(String tableId)
    {
        List<LightweightCompressionDictionary> dicts = SystemDistributedKeyspace.retrieveLightweightCompressionDictionaries();
        assertThat(dicts).isNotNull();
        assertThat(dicts).isNotEmpty();

        LightweightCompressionDictionary latest = dicts.get(0);
        long pastTimeMillis = Instant.now().minus(2, ChronoUnit.MINUTES).toEpochMilli();

        execute(format("UPDATE %s.%s SET created_at = %d WHERE keyspace_name = '%s' AND table_name = '%s' AND table_id = '%s' AND dict_id = %d",
                       SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                       SystemDistributedKeyspace.COMPRESSION_DICTIONARIES,
                       pastTimeMillis,
                       keyspace(),
                       tableName,
                       tableId,
                       latest.dictId.id));
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
        assertThat(result.getExitCode()).isEqualTo(2);

        assertThat(result.getStderr())
        .as("Should indicate training can not be triggered")
        .contains("The next training or importing can occur only at least after " + frequency + " from the last training which happened");

        String failingMessage = Arrays.stream(result.getStderr()
                                                    .split(System.lineSeparator()))
                                      .filter(p -> p.contains("The next training or importing can occur only at least"))
                                      .findFirst()
                                      .orElseThrow(() -> new RuntimeException("Unable to find failing message"));

        String pattern = "The next training or importing can occur only at least after " +
                         "(.*) from the last training which happened at (.*). " +
                         "You can train again no earlier than at (.*).";
        Matcher matcher = Pattern.compile(pattern).matcher(failingMessage);

        assertThat(matcher.find()).isTrue();

        DurationSpec.IntMinutesBound frequencySpec = new DurationSpec.IntMinutesBound(matcher.group(1));
        Instant lastTraining = Instant.parse(matcher.group(2));
        Instant earliestNextTraining = Instant.parse(matcher.group(3));

        assertThat(earliestNextTraining).isAfter(lastTraining);
        assertThat(earliestNextTraining.minus(frequencySpec.toMinutes(), ChronoUnit.MINUTES)).isAfterOrEqualTo(lastTraining);
    }

    private void assertFailingImport(File file)
    {
        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary", "import", file.absolutePath());
        assertThat(result.getExitCode()).isEqualTo(2);
    }

    private void assertSuccessfulImport(File file)
    {
        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary", "import", file.absolutePath());
        result.assertOnCleanExit();
    }

    private static int batch = 1;

    private void createSSTables()
    {
        for (int file = 0; file < 10; file++)
        {
            int batchSize = 1000;
            for (int i = 0; i < batchSize; i++)
            {
                int index = batch * (i + file * batchSize);
                executeFormattedQuery(format("INSERT INTO %s.%s (id, data) VALUES (?, ?)", keyspace(), tableName),
                                      index, "test data " + index);
            }

            flush();
        }

        batch++;
    }

    private Pair<CompressionDictionaryDataObject, File> export() throws Throwable
    {
        File dictionaryFile = FileUtils.createTempFile("zstd-dictionary-", ".dict");
        ToolRunner.ToolResult result;

        result = invokeNodetool("compressiondictionary", "export", keyspace(), tableName, dictionaryFile.absolutePath());
        result.assertOnCleanExit();

        CompressionDictionaryDataObject dataObject = JsonUtils.deserializeFromJsonFile(CompressionDictionaryDataObject.class, dictionaryFile);

        assertThat(dictionaryFile.exists()).isTrue();
        assertThat(dictionaryFile.length()).isGreaterThan(0);

        return Pair.create(dataObject, dictionaryFile);
    }
}
