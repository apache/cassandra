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

package org.apache.cassandra.schema;

import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.db.compression.ZstdCompressionDictionary;

import static org.assertj.core.api.Assertions.assertThat;

public class SystemDistributedKeyspaceCompressionDictionaryTest extends CQLTester
{
    private static final String TEST_KEYSPACE = "test_keyspace";
    private static final String TEST_TABLE = "test_table";
    private static final String OTHER_TABLE = "other_table";

    private CompressionDictionary testDictionary1;
    private CompressionDictionary testDictionary2;

    @Before
    public void setUp()
    {
        DictId dictId1 = new DictId(Kind.ZSTD, 100L);
        DictId dictId2 = new DictId(Kind.ZSTD, 200L);

        byte[] dictData1 = "test dictionary data 1".getBytes();
        byte[] dictData2 = "test dictionary data 2".getBytes();

        testDictionary1 = new ZstdCompressionDictionary(dictId1, dictData1);
        testDictionary2 = new ZstdCompressionDictionary(dictId2, dictData2);

        clearCompressionDictionaries();
    }

    @Test
    public void testCompressionDictionariesTableExists()
    {
        Set<String> tableNames = SystemDistributedKeyspace.TABLE_NAMES;

        assertThat(tableNames)
        .as("TABLE_NAMES should contain compression_dictionaries")
        .contains(SystemDistributedKeyspace.COMPRESSION_DICTIONARIES);

        // Verify the table exists in the schema
        KeyspaceMetadata systemDistributedKs = SystemDistributedKeyspace.metadata();
        TableMetadata compressionDictTable = systemDistributedKs
                                             .getTableOrViewNullable(SystemDistributedKeyspace.COMPRESSION_DICTIONARIES);

        assertThat(compressionDictTable)
        .as("compression_dictionaries table should exist in schema")
        .isNotNull();
    }

    @Test
    public void testStoreCompressionDictionary() throws Exception
    {
        // Store a dictionary
        SystemDistributedKeyspace.storeCompressionDictionary(TEST_KEYSPACE, TEST_TABLE, testDictionary1);

        // Verify it was stored
        CompressionDictionary retrieved = SystemDistributedKeyspace.retrieveLatestCompressionDictionary(
        TEST_KEYSPACE, TEST_TABLE);

        assertThat(retrieved)
        .as("Retrieved dictionary should not be null")
        .isNotNull();

        assertThat(retrieved.dictId())
        .as("Retrieved dictionary ID should match stored")
        .isEqualTo(testDictionary1.dictId());

        assertThat(retrieved.kind())
        .as("Retrieved dictionary kind should match stored")
        .isEqualTo(testDictionary1.kind());

        assertThat(retrieved.rawDictionary())
        .as("Retrieved dictionary data should match stored")
        .isEqualTo(testDictionary1.rawDictionary());

        retrieved.selfRef().close();
    }

    @Test
    public void testStoreMultipleDictionaries() throws Exception
    {
        // Store multiple dictionaries for the same table
        SystemDistributedKeyspace.storeCompressionDictionary(TEST_KEYSPACE, TEST_TABLE, testDictionary1);
        SystemDistributedKeyspace.storeCompressionDictionary(TEST_KEYSPACE, TEST_TABLE, testDictionary2);

        // Should retrieve the latest one (higher ID due to clustering order)
        CompressionDictionary latest = SystemDistributedKeyspace.retrieveLatestCompressionDictionary(
        TEST_KEYSPACE, TEST_TABLE);

        assertThat(latest)
        .as("Should retrieve the latest dictionary")
        .isNotNull();

        assertThat(latest.dictId())
        .as("Should retrieve dictionary with higher ID")
        .isEqualTo(testDictionary2.dictId());

        latest.selfRef().close();
    }

    @Test
    public void testRetrieveSpecificDictionary() throws Exception
    {
        // Store both dictionaries
        SystemDistributedKeyspace.storeCompressionDictionary(TEST_KEYSPACE, TEST_TABLE, testDictionary1);
        SystemDistributedKeyspace.storeCompressionDictionary(TEST_KEYSPACE, TEST_TABLE, testDictionary2);

        // Retrieve specific dictionary by ID
        CompressionDictionary dict1 = SystemDistributedKeyspace.retrieveCompressionDictionary(
        TEST_KEYSPACE, TEST_TABLE, 100L);
        CompressionDictionary dict2 = SystemDistributedKeyspace.retrieveCompressionDictionary(
        TEST_KEYSPACE, TEST_TABLE, 200L);

        assertThat(dict1)
        .as("Should retrieve dictionary 1")
        .isNotNull();

        assertThat(dict1.dictId())
        .as("Should retrieve correct dictionary by ID")
        .isEqualTo(testDictionary1.dictId());

        assertThat(dict2)
        .as("Should retrieve dictionary 2")
        .isNotNull();

        assertThat(dict2.dictId())
        .as("Should retrieve correct dictionary by ID")
        .isEqualTo(testDictionary2.dictId());

        dict1.selfRef().close();
        dict2.selfRef().close();
    }

    @Test
    public void testRetrieveNonExistentDictionary()
    {
        // Try to retrieve dictionary that doesn't exist
        CompressionDictionary nonExistent = SystemDistributedKeyspace.retrieveLatestCompressionDictionary(
        "nonexistent_keyspace", "nonexistent_table");

        assertThat(nonExistent)
        .as("Should return null for non-existent dictionary")
        .isNull();

        // Try to retrieve specific dictionary that doesn't exist
        CompressionDictionary nonExistentById = SystemDistributedKeyspace.retrieveCompressionDictionary(
        TEST_KEYSPACE, TEST_TABLE, 999L);

        assertThat(nonExistentById)
        .as("Should return null for non-existent dictionary ID")
        .isNull();
    }

    @Test
    public void testStoredDictionaryIncludesLengthAndChecksum() throws Exception
    {
        // Store a dictionary
        SystemDistributedKeyspace.storeCompressionDictionary(TEST_KEYSPACE, TEST_TABLE, testDictionary1);

        // Query the table directly to verify dict_length and dict_checksum are stored
        String query = String.format("SELECT dict_length, dict_checksum FROM %s.%s WHERE keyspace_name = '%s' AND table_name = '%s' AND dict_id = %d",
                                     SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                     SystemDistributedKeyspace.COMPRESSION_DICTIONARIES,
                                     TEST_KEYSPACE,
                                     TEST_TABLE,
                                     testDictionary1.dictId().id);

        var resultSet = QueryProcessor.executeInternal(query);

        assertThat(resultSet.isEmpty())
        .as("Should have at least one result")
        .isFalse();

        var row = resultSet.one();
        int storedLength = row.getInt("dict_length");
        int storedChecksum = row.getInt("dict_checksum");

        // Verify the stored length matches the actual dictionary length
        assertThat(storedLength)
        .as("Stored dict_length should match actual dictionary length")
        .isEqualTo(testDictionary1.rawDictionary().length);

        // Verify the stored checksum matches the calculated checksum
        byte[] dict = testDictionary1.rawDictionary();
        int expectedChecksum = CompressionDictionary.calculateChecksum(
            (byte) testDictionary1.kind().ordinal(),
            testDictionary1.dictId().id,
            dict);

        assertThat(storedChecksum)
        .as("Stored dict_checksum should match calculated checksum")
        .isEqualTo(expectedChecksum);
    }

    private void clearCompressionDictionaries()
    {
        for (String table : List.of(TEST_TABLE, OTHER_TABLE))
        {
            String deleteQuery = String.format("DELETE FROM %s.%s WHERE keyspace_name = '%s' AND table_name = '%s'",
                                               SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                               SystemDistributedKeyspace.COMPRESSION_DICTIONARIES,
                                               TEST_KEYSPACE,
                                               table);
            QueryProcessor.executeInternal(deleteQuery);
        }
    }
}
