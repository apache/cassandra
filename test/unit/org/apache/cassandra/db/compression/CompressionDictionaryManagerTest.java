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

import java.nio.ByteBuffer;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompressionDictionaryManagerTest
{
    private static final String KEYSPACE_WITH_DICT = "keyspace_with_dict";
    private static final String KEYSPACE_WITHOUT_DICT = "keyspace_without_dict";
    private static final String TABLE = "test_table";

    private static ColumnFamilyStore cfsWithDict;
    private static ColumnFamilyStore cfsWithoutDict;

    private CompressionDictionaryManager managerWithDict;
    private CompressionDictionaryManager managerWithoutDict;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(true);
        ServerTestUtils.prepareServerNoRegister();

        // Create table with dictionary compression enabled
        CompressionParams compressionParamsWithDict = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true,
                                                                             Map.of("compression_level", "3"));

        TableMetadata.Builder tableBuilderWithDict = TableMetadata.builder(KEYSPACE_WITH_DICT, TABLE)
                                                                  .addPartitionKeyColumn("pk", org.apache.cassandra.db.marshal.UTF8Type.instance)
                                                                  .addRegularColumn("data", org.apache.cassandra.db.marshal.UTF8Type.instance)
                                                                  .compression(compressionParamsWithDict);

        // Create table without dictionary compression
        CompressionParams compressionParamsWithoutDict = CompressionParams.lz4();

        TableMetadata.Builder tableBuilderWithoutDict = TableMetadata.builder(KEYSPACE_WITHOUT_DICT, TABLE)
                                                                     .addPartitionKeyColumn("pk", org.apache.cassandra.db.marshal.UTF8Type.instance)
                                                                     .addRegularColumn("data", org.apache.cassandra.db.marshal.UTF8Type.instance)
                                                                     .compression(compressionParamsWithoutDict);

        SchemaLoader.createKeyspace(KEYSPACE_WITH_DICT,
                                    KeyspaceParams.simple(1),
                                    tableBuilderWithDict);

        SchemaLoader.createKeyspace(KEYSPACE_WITHOUT_DICT,
                                    KeyspaceParams.simple(1),
                                    tableBuilderWithoutDict);

        cfsWithDict = Keyspace.open(KEYSPACE_WITH_DICT).getColumnFamilyStore(TABLE);
        cfsWithoutDict = Keyspace.open(KEYSPACE_WITHOUT_DICT).getColumnFamilyStore(TABLE);
    }

    @Before
    public void setUp()
    {
        managerWithDict = new CompressionDictionaryManager(cfsWithDict, true);
        managerWithoutDict = new CompressionDictionaryManager(cfsWithoutDict, true);
    }

    @After
    public void tearDown() throws Exception
    {
        if (managerWithDict != null)
        {
            managerWithDict.close();
        }
        if (managerWithoutDict != null)
        {
            managerWithoutDict.close();
        }
    }

    @Test
    public void testManagerInitializationWithDictionaryCompression()
    {
        assertThat(managerWithDict)
        .as("Manager should be created successfully for dictionary-enabled table")
        .isNotNull();

        // Manager should start in a valid state
        String status = managerWithDict.getTrainingStatus();
        assertThat(status)
        .as("Training status should be valid")
        .isEqualTo(TrainingStatus.NOT_STARTED.toString());
    }

    @Test
    public void testManagerInitializationWithoutDictionaryCompression()
    {
        assertThat(managerWithoutDict)
        .as("Manager should be created successfully for non-dictionary table")
        .isNotNull();

        // Should report NOT_STARTED since no trainer is created
        String status = managerWithoutDict.getTrainingStatus();
        assertThat(status)
        .as("Should report NOT_STARTED for non-dictionary tables")
        .isEqualTo(TrainingStatus.NOT_STARTED.toString());
    }

    @Test
    public void testMaybeReloadFromSchemaEnableDictionaryCompression()
    {
        // Start with manager for non-dictionary table
        String initialStatus = managerWithoutDict.getTrainingStatus();
        assertThat(initialStatus)
        .as("Initially should not be training")
        .isEqualTo(TrainingStatus.NOT_STARTED.toString());

        // Enable dictionary compression by switching to dict params
        CompressionParams dictParams = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true,
                                                              Map.of("compression_level", "3"));

        managerWithoutDict.maybeReloadFromSchema(dictParams);

        managerWithoutDict.train(Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "600"));
        // Should now have training capability
        String newStatus = managerWithoutDict.getTrainingStatus();
        assertThat(newStatus)
        .as("Should now support training")
        .isEqualTo(TrainingStatus.SAMPLING.toString());
    }

    @Test
    public void testMaybeReloadFromSchemaDisableDictionaryCompression()
    {
        managerWithDict.train(Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "600"));
        String status = managerWithDict.getTrainingStatus();
        assertThat(status)
        .as("Should be sampling")
        .isEqualTo(TrainingStatus.SAMPLING.toString());

        // Disable dictionary compression
        CompressionParams nonDictParams = CompressionParams.lz4();
        managerWithDict.maybeReloadFromSchema(nonDictParams);

        // Should disable training
        String newStatus = managerWithDict.getTrainingStatus();
        assertThat(newStatus)
        .as("Should disable training when dictionary compression is disabled")
        .isEqualTo(TrainingStatus.NOT_STARTED.toString());
    }

    @Test
    public void testTrainerCompatibilityCheck()
    {
        managerWithDict.train(Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "600"));
        String initialStatus = managerWithDict.getTrainingStatus();
        assertThat(initialStatus)
        .as("Should be sampling")
        .isEqualTo(TrainingStatus.SAMPLING.toString());

        // Change compression level - should create new trainer
        CompressionParams differentLevelParams = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true,
                                                                        Map.of("compression_level", "5"));
        managerWithDict.maybeReloadFromSchema(differentLevelParams);
        String newStatus = managerWithDict.getTrainingStatus();

        // Status should reset due to trainer replacement
        assertThat(newStatus)
        .as("Should reset status when creating new trainer")
        .isEqualTo(TrainingStatus.NOT_STARTED.toString());
    }

    @Test
    public void testAddSample()
    {
        ByteBuffer sample = ByteBuffer.wrap("test sample data".getBytes());
        ByteBuffer emptyBuffer = ByteBuffer.allocate(0);

        // Should not throw for dictionary-enabled table
        assertThatNoException().isThrownBy(() -> managerWithDict.addSample(sample));
        assertThatNoException().isThrownBy(() -> managerWithDict.addSample(null));
        assertThatNoException().isThrownBy(() -> managerWithDict.addSample(emptyBuffer));
        // Should not throw for non-dictionary table (graceful handling)
        assertThatNoException().isThrownBy(() -> managerWithoutDict.addSample(sample));
        assertThatNoException().isThrownBy(() -> managerWithoutDict.addSample(null));
        assertThatNoException().isThrownBy(() -> managerWithoutDict.addSample(emptyBuffer));
    }

    @Test
    public void testTrainManualWithNonDictionaryTable()
    {
        assertThatThrownBy(() -> managerWithoutDict.train(Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "600")))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not support dictionary compression");
    }

    @Test
    public void testTrainManualWithMissingParameters()
    {
        assertThatThrownBy(() -> managerWithDict.train(Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxSamplingDurationSeconds parameter is required");

        assertThatThrownBy(() -> managerWithDict.train(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxSamplingDurationSeconds parameter is required");
    }

    @Test
    public void testTrainManualWithInvalidParameters()
    {
        assertThatThrownBy(() -> managerWithDict.train(Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "invalid")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid maxSamplingDurationSeconds value: invalid")
        .hasCauseInstanceOf(NumberFormatException.class);

        assertThatThrownBy(() -> managerWithDict.train(Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "-1")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxSamplingDurationSeconds must be positive, got: -1");
    }

    @Test
    public void testTrainManualWithOptions()
    {
        // Should accept custom options
        managerWithDict.train(Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "30"));

        String status = managerWithDict.getTrainingStatus();
        assertThat(status)
        .as("Training with options should work")
        .isEqualTo(TrainingStatus.SAMPLING.toString());
    }

    @Test
    public void testSchemaChangeWorkflow()
    {
        // Start with non-dictionary table
        String initialStatus = managerWithoutDict.getTrainingStatus();
        assertThat(initialStatus).isEqualTo(TrainingStatus.NOT_STARTED.toString());

        // Enable dictionary compression
        CompressionParams dictParams = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true,
                                                              Map.of("compression_level", "3"));
        managerWithoutDict.maybeReloadFromSchema(dictParams);
        managerWithoutDict.train(Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "600"));
        // Should now support training
        String enabledStatus = managerWithoutDict.getTrainingStatus();
        assertThat(enabledStatus).isEqualTo(TrainingStatus.SAMPLING.toString());

        // Change compression level
        CompressionParams newDictParams = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true,
                                                                 Map.of("compression_level", "5"));
        managerWithoutDict.maybeReloadFromSchema(newDictParams);

        // Should still support training with new parameters
        String updatedStatus = managerWithoutDict.getTrainingStatus();
        assertThat(updatedStatus).isEqualTo(TrainingStatus.NOT_STARTED.toString());
        managerWithoutDict.train(Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "600"));
        assertThat(enabledStatus).isEqualTo(TrainingStatus.SAMPLING.toString());

        // Disable dictionary compression
        CompressionParams nonDictParams = CompressionParams.lz4();
        managerWithoutDict.maybeReloadFromSchema(nonDictParams);

        // Should disable training
        String disabledStatus = managerWithoutDict.getTrainingStatus();
        assertThat(disabledStatus).isEqualTo(TrainingStatus.NOT_STARTED.toString());
    }

    @Test
    public void testUpdateSamplingRate()
    {
        // Test with enabled dictionary manager
        managerWithDict.train(Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "600"));

        // Should be able to update sampling rate
        assertThatNoException().isThrownBy(() -> managerWithDict.updateSamplingRate(5));
        assertThatNoException().isThrownBy(() -> managerWithDict.updateSamplingRate(1));
        assertThatNoException().isThrownBy(() -> managerWithDict.updateSamplingRate(100));
    }

    @Test
    public void testUpdateSamplingRateWithoutTrainer()
    {
        // Test with disabled dictionary manager (no trainer)
        assertThatThrownBy(() -> managerWithoutDict.updateSamplingRate(5))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Dictionary trainer is not available");
    }

    @Test
    public void testUpdateSamplingRateValidation()
    {
        // Test with enabled dictionary manager
        managerWithDict.train(Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "600"));

        // Test invalid sampling rates are rejected by the trainer
        assertThatThrownBy(() -> managerWithDict.updateSamplingRate(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Sampling rate must be positive");

        assertThatThrownBy(() -> managerWithDict.updateSamplingRate(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Sampling rate must be positive");
    }
}
