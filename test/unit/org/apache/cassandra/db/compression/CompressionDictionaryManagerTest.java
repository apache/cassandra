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
        TrainingState trainingState = TrainingState.fromCompositeData(managerWithDict.getTrainingState());
        assertThat(trainingState.getStatus())
        .as("Training status should be valid")
        .isEqualTo(TrainingStatus.NOT_STARTED);
    }

    @Test
    public void testManagerInitializationWithoutDictionaryCompression()
    {
        assertThat(managerWithoutDict)
        .as("Manager should be created successfully for non-dictionary table")
        .isNotNull();

        // Should report NOT_STARTED since no trainer is created
        TrainingState trainingState = TrainingState.fromCompositeData(managerWithoutDict.getTrainingState());
        assertThat(trainingState.getStatus())
        .as("Should report NOT_STARTED for non-dictionary tables")
        .isEqualTo(TrainingStatus.NOT_STARTED);
    }

    @Test
    public void testMaybeReloadFromSchemaEnableDictionaryCompression()
    {
        // Start with manager for non-dictionary table
        TrainingState initialTrainingState = TrainingState.fromCompositeData(managerWithoutDict.getTrainingState());
        assertThat(initialTrainingState.getStatus())
        .as("Initially should not be training")
        .isEqualTo(TrainingStatus.NOT_STARTED);

        // Enable dictionary compression by switching to dict params
        CompressionParams dictParams = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true,
                                                              Map.of("compression_level", "3"));

        managerWithoutDict.maybeReloadFromSchema(dictParams);

        // Should be now enabled
        assertThat(managerWithoutDict.isEnabled())
        .as("manager should have enabled dictionary compression")
        .isTrue();
    }

    @Test
    public void testMaybeReloadFromSchemaDisableDictionaryCompression()
    {
        // Verify we have a trainer initially
        assertThat(managerWithDict.isEnabled())
        .as("manager with a dictionary support should be enabled")
        .isTrue();

        // Disable dictionary compression
        CompressionParams nonDictParams = CompressionParams.lz4();
        managerWithDict.maybeReloadFromSchema(nonDictParams);

        // Should disable training
        assertThat(managerWithDict.isEnabled())
        .as("manager without a dictionary support should be disabled")
        .isFalse();
    }

    @Test
    public void testTrainManualWithNonDictionaryTable()
    {
        assertThatThrownBy(() -> managerWithoutDict.train(false))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not support dictionary compression");
    }

    @Test
    public void testTrainManualWithDictionaryTable()
    {
        // Should throw because no SSTables exist
        assertThatThrownBy(() -> managerWithDict.train(false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No SSTables available for training");
    }

    @Test
    public void testSchemaChangeWorkflow()
    {
        // Start with non-dictionary table
        TrainingState initialTrainingState = TrainingState.fromCompositeData(managerWithoutDict.getTrainingState());
        assertThat(initialTrainingState.getStatus()).isEqualTo(TrainingStatus.NOT_STARTED);
        assertThat(managerWithoutDict.isEnabled()).isFalse();

        // Enable dictionary compression
        CompressionParams dictParams = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true,
                                                              Map.of("compression_level", "3"));
        managerWithoutDict.maybeReloadFromSchema(dictParams);

        // Should now support training
        assertThat(managerWithoutDict.isEnabled()).isTrue();

        // Change compression level
        CompressionParams newDictParams = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true,
                                                                 Map.of("compression_level", "5"));
        managerWithoutDict.maybeReloadFromSchema(newDictParams);

        // Should still support training with new parameters
        assertThat(managerWithoutDict.isEnabled()).isTrue();

        // Disable dictionary compression
        CompressionParams nonDictParams = CompressionParams.lz4();
        managerWithoutDict.maybeReloadFromSchema(nonDictParams);

        // Should disable training
        assertThat(managerWithoutDict.isEnabled()).isFalse();
    }
}
