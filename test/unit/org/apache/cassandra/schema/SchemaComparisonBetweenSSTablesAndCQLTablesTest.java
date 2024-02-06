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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.tools.Util;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class SchemaComparisonBetweenSSTablesAndCQLTablesTest
{
    private static final String ksName = "74656e616e742d616161_schema_validation_tests";
    private static final String ssTableResourceRootPath = "test/resources/schema/schema_validation_tests/";

    /**
     * Table schema:
     * CREATE TABLE test_1 (
     * col_txt text PRIMARY KEY,
     * col_int int,
     * col_uuid uuid,
     * col_bool boolean,
     * col_dec decimal
     * );
     */
    private static final String TABLE_1_NAME = "test_1";
    private static final UUID TABLE_1_UUID = UUID.fromString("7ab21491-8482-11ee-825e-31532694b0d3");

    /**
     * Table schema:
     * CREATE TABLE test_2 (
     * col_txt text,
     * col_int int,
     * col_uuid uuid,
     * col_bool boolean,
     * col_dec decimal,
     * PRIMARY KEY ((col_txt), col_uuid)
     * );
     */
    private static final String TABLE_2_NAME = "test_2";
    private static final UUID TABLE_2_UUID = UUID.fromString("83ac0dd1-8482-11ee-825e-31532694b0d3");

    /**
     * Table schema:
     * CREATE TABLE test_3 (
     * col_txt text,
     * col_int int,
     * col_uuid uuid,
     * col_bool boolean,
     * col_dec decimal,
     * PRIMARY KEY ((col_txt, col_int), col_uuid)
     * ) WITH CLUSTERING ORDER BY (col_uuid DESC);
     */
    private static final String TABLE_3_NAME = "test_3";
    private static final UUID TABLE_3_UUID = UUID.fromString("88b6cb80-8482-11ee-825e-31532694b0d3");

    /**
     * Table schema:
     * CREATE TABLE test_4(
     * col_txt text,
     * col_int int,
     * col_uuid uuid,
     * col_bool boolean,
     * col_dec decimal,
     * PRIMARY KEY ((col_txt), col_int, col_uuid)
     * ) WITH CLUSTERING ORDER BY (col_int DESC, col_uuid ASC);
     */
    private static final String TABLE_4_NAME = "test_4";
    private static final UUID TABLE_4_UUID = UUID.fromString("8bf57bc0-8482-11ee-825e-31532694b0d3");

    /**
     * Table schema:
     * CREATE TABLE test_5(
     * col_txt text primary key,
     * col_ascii ascii,
     * col_bigint bigint,
     * col_blob blob,
     * col_bool boolean,
     * col_date date,
     * col_dec decimal,
     * col_dbl double,
     * col_float float,
     * col_inet inet,
     * col_small smallint,
     * col_time time,
     * col_timestamp timestamp,
     * col_timeuuid timeuuid,
     * col_tinyint tinyint,
     * col_varchar varchar,
     * col_varint varint
     * );
     */
    private static final String TABLE_5_NAME = "test_5";
    private static final UUID TABLE_5_UUID = UUID.fromString("9db47b90-8482-11ee-825e-31532694b0d3");

    private static final Map<String, TableMetadata> ssTableMetadataForTableMap = new HashMap<>();

    @BeforeClass
    public static void init() throws Exception
    {
        DatabaseDescriptor.toolInitialization();
        initializeSSTableMetadataForTableMap();
    }

    private static void initializeSSTableMetadataForTableMap() throws Exception
    {
        ssTableMetadataForTableMap.put(TABLE_1_NAME,
                                       Util.metadataFromSSTable(Descriptor.fromFilename(ssTableResourceRootPath + "test_1-7ab21491848211ee825e31532694b0d3/bb-2-bti-Data.db"),
                                                                ksName, TABLE_1_NAME));

        ssTableMetadataForTableMap.put(TABLE_2_NAME,
                                       Util.metadataFromSSTable(Descriptor.fromFilename(ssTableResourceRootPath + "test_2-83ac0dd1848211ee825e31532694b0d3/bb-1-bti-Data.db"),
                                                                ksName, TABLE_2_NAME));

        ssTableMetadataForTableMap.put(TABLE_3_NAME,
                                       Util.metadataFromSSTable(Descriptor.fromFilename(ssTableResourceRootPath + "test_3-88b6cb80848211ee825e31532694b0d3/bb-1-bti-Data.db"),
                                                                ksName, TABLE_3_NAME));

        ssTableMetadataForTableMap.put(TABLE_4_NAME,
                                       Util.metadataFromSSTable(Descriptor.fromFilename(ssTableResourceRootPath + "test_4-8bf57bc0848211ee825e31532694b0d3/bb-1-bti-Data.db"),
                                                                ksName, TABLE_4_NAME));

        ssTableMetadataForTableMap.put(TABLE_5_NAME,
                                       Util.metadataFromSSTable(Descriptor.fromFilename(ssTableResourceRootPath + "test_5-9db47b90848211ee825e31532694b0d3/bb-1-bti-Data.db"),
                                                                ksName, TABLE_5_NAME));
    }

    // *** GENERAL VALIDATION SUCCESS TESTS *** //

    @Test
    public void testIdenticalSchemaPassesValidation_SinglePartKeyNoClustering()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_1_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_1_NAME, TableId.fromUUID(TABLE_1_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addRegularColumn("col_uuiid", UUIDType.instance)
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatCode(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata)).doesNotThrowAnyException();
    }

    @Test
    public void testIdenticalSchemaPassesValidation_SinglePartKeySingleClusteringAsc()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_2_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_2_NAME, TableId.fromUUID(TABLE_2_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addClusteringColumn("col_uuiid", UUIDType.instance)
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatCode(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata)).doesNotThrowAnyException();
    }

    @Test
    public void testIdenticalSchemaPassesValidation_CompoundPartKeySingleClusteringDesc()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_3_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_3_NAME, TableId.fromUUID(TABLE_3_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addPartitionKeyColumn("col_int", Int32Type.instance)
                                                      .addClusteringColumn("col_uuid", ReversedType.getInstance(UUIDType.instance))
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();

        assertThatCode(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata)).doesNotThrowAnyException();
    }

    @Test
    public void testIdenticalSchemaPassesValidation_SinglePartKeyTwoClusteringAscDesc()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_4_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_4_NAME, TableId.fromUUID(TABLE_4_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addClusteringColumn("col_int", ReversedType.getInstance(Int32Type.instance))
                                                      .addClusteringColumn("col_uuid", UUIDType.instance)
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatCode(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata)).doesNotThrowAnyException();
    }

    @Test
    public void testIdenticalSchemaPassesValidation_ManyDataTypes()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_5_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_5_NAME, TableId.fromUUID(TABLE_5_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addRegularColumn("col_ascii", AsciiType.instance)
                                                      .addRegularColumn("col_bigint", LongType.instance)
                                                      .addRegularColumn("col_blob", BytesType.instance)
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_date", SimpleDateType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .addRegularColumn("col_dbl", DoubleType.instance)
                                                      .addRegularColumn("col_float", FloatType.instance)
                                                      .addRegularColumn("col_inet", InetAddressType.instance)
                                                      .addRegularColumn("col_small", ShortType.instance)
                                                      .addRegularColumn("col_time", TimeType.instance)
                                                      .addRegularColumn("col_timestamp", TimestampType.instance)
                                                      .addRegularColumn("col_timeuuid", TimeUUIDType.instance)
                                                      .addRegularColumn("col_tinyint", ByteType.instance)
                                                      .addRegularColumn("col_varchar", UTF8Type.instance)
                                                      .addRegularColumn("col_varint", IntegerType.instance)
                                                      .build();
        assertThatCode(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata)).doesNotThrowAnyException();
    }

    // *** KEYSPACE NAME, TABLE NAME AND TABLE ID ADDITIONAL TESTS *** //

    @Test
    public void testTableNameMismatchFailsValidation()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_1_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, "different_table_name", TableId.fromUUID(TABLE_1_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addRegularColumn("col_uuiid", UUIDType.instance)
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatExceptionOfType(org.apache.cassandra.exceptions.ConfigurationException.class)
        .isThrownBy(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata));
    }

    @Test
    public void testKeyspaceNameMismatchPassesValidation()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_1_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder("different_keyspace_name", TABLE_1_NAME, TableId.fromUUID(TABLE_1_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addRegularColumn("col_uuiid", UUIDType.instance)
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatCode(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata)).doesNotThrowAnyException();
    }

    @Test
    public void testTableIdMismatchPassesValidation()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_1_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_1_NAME, TableId.fromUUID(UUID.randomUUID())) // different UUID
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addRegularColumn("col_uuiid", UUIDType.instance)
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatCode(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata)).doesNotThrowAnyException();
    }

    // *** PARTITION KEY VALIDATION ADDITIONAL TESTS *** //

    @Test
    public void testPartitionKeyMismatchFailsValidation_WrongColumn()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_1_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_1_NAME, TableId.fromUUID(TABLE_1_UUID))
                                                      .addRegularColumn("col_txt", UTF8Type.instance) // should be single part key
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addPartitionKeyColumn("col_uuiid", UUIDType.instance) // is not part key
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();

        assertThatExceptionOfType(org.apache.cassandra.exceptions.ConfigurationException.class)
        .isThrownBy(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata));
    }

    @Test
    public void testPartitionKeyMismatchFailsValidation_WrongAdditionalColumn()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_1_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_1_NAME, TableId.fromUUID(TABLE_1_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance) // is single part key
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addPartitionKeyColumn("col_uuiid", UUIDType.instance) // is not part key
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance).build();
        assertThatExceptionOfType(org.apache.cassandra.exceptions.ConfigurationException.class)
        .isThrownBy(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata));
    }

    @Test
    public void testPartitionKeyMismatchFailsValidation_WrongColumnType()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_1_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_1_NAME, TableId.fromUUID(TABLE_1_UUID))
                                                      .addPartitionKeyColumn("col_txt", Int32Type.instance) // is single part key but wrong type
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addRegularColumn("col_uuiid", UUIDType.instance)
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatExceptionOfType(org.apache.cassandra.exceptions.ConfigurationException.class)
        .isThrownBy(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata));
    }

    /**
     * Different column name for partition key column passes validation
     * This is the expected behaviour as the partition key column names are not contained in the sstable metadata
     */
    @Test
    public void testPartitionKeyColumnNameMismatchPassesValidation()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_1_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_1_NAME, TableId.fromUUID(TABLE_1_UUID))
                                                      .addPartitionKeyColumn("wrong_name", UTF8Type.instance) // correct type but wrong type
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addRegularColumn("col_uuiid", UUIDType.instance)
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance).build();
        assertThatCode(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata)).doesNotThrowAnyException();
    }

    @Test
    public void testPartitionKeyMismatchFailsValidation_CompoundPartKeyWrongColumnOrder()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_3_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_3_NAME, TableId.fromUUID(TABLE_3_UUID))
                                                      .addPartitionKeyColumn("col_int", Int32Type.instance)   // is part key column but should be second
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)   // is part key column but should be first
                                                      .addClusteringColumn("col_uuid", ReversedType.getInstance(UUIDType.instance))
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatExceptionOfType(org.apache.cassandra.exceptions.ConfigurationException.class)
        .isThrownBy(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata));
    }

    // *** CLUSTERING COLUMN VALIDATION ADDITIONAL TESTS *** //

    @Test
    public void testClusteringColumnsMismatchFailsValidation_MissingClusteringColumn()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_2_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_2_NAME, TableId.fromUUID(TABLE_2_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addRegularColumn("col_uuiid", UUIDType.instance)   // should be clustering column
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatExceptionOfType(org.apache.cassandra.exceptions.ConfigurationException.class)
        .isThrownBy(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata));
    }

    @Test
    public void testClusteringColumnsMismatchFailsValidation_WrongClusteringColumn()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_2_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_2_NAME, TableId.fromUUID(TABLE_2_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addClusteringColumn("col_int", Int32Type.instance) // is not clustering column
                                                      .addRegularColumn("col_uuiid", UUIDType.instance)   // should be clustering column
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatExceptionOfType(org.apache.cassandra.exceptions.ConfigurationException.class)
        .isThrownBy(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata));
    }

    @Test
    public void testClusteringColumnsMismatchFailsValidation_CorrectClusteringColumnButWrongOrderingClause()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_2_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_2_NAME, TableId.fromUUID(TABLE_2_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addClusteringColumn("col_uuiid", ReversedType.getInstance(UUIDType.instance)) // is clustering column but should be ascending
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatExceptionOfType(org.apache.cassandra.exceptions.ConfigurationException.class)
        .isThrownBy(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata));
    }

    @Test
    public void testClusteringColumnsMismatchFailsValidation_WrongAdditionalClusteringColumn()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_2_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_2_NAME, TableId.fromUUID(TABLE_2_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addClusteringColumn("col_int", Int32Type.instance)// is not clustering column
                                                      .addClusteringColumn("col_uuiid", UUIDType.instance) // is clustering column
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatExceptionOfType(org.apache.cassandra.exceptions.ConfigurationException.class)
        .isThrownBy(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata));
    }

    @Test
    public void testClusteringColumnsMismatchFailsValidation_WrongClusteringColumnType()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_2_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_2_NAME, TableId.fromUUID(TABLE_2_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addClusteringColumn("col_uuiid", DecimalType.instance) // is clustering column but wrong type
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatExceptionOfType(org.apache.cassandra.exceptions.ConfigurationException.class)
        .isThrownBy(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata));
    }

    @Test
    public void testClusteringColumnsMismatchFailsValidation_WrongOrderOfClusteringColumns()
    {

        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_4_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_4_NAME, TableId.fromUUID(TABLE_4_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addClusteringColumn("col_uuid", UUIDType.instance) // is clustering column but should be second
                                                      .addClusteringColumn("col_int", ReversedType.getInstance(Int32Type.instance))   // is clustering column but should be first
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatExceptionOfType(org.apache.cassandra.exceptions.ConfigurationException.class)
        .isThrownBy(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata));
    }

    /**
     * Different column name for clustering column passes validation
     * This is the expected behaviour as the clustering column names are not contained in the sstable metadata
     */
    @Test
    public void testClusteringColumnNameMismatchPassesValidation()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_2_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_2_NAME, TableId.fromUUID(TABLE_2_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addClusteringColumn("wrong_name", UUIDType.instance)   // is clustering column but wrong name
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance).build();
        assertThatCode(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata)).doesNotThrowAnyException();
    }

    // *** REGULAR COLUMN VALIDATION ADDITIONAL TESTS *** //

    @Test
    public void testRegularColumnMismatchFailsValidation_WrongColumnType()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_1_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_1_NAME, TableId.fromUUID(TABLE_1_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addRegularColumn("col_uuid", UTF8Type.instance)  // is regular column but wrong type
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatExceptionOfType(org.apache.cassandra.exceptions.ConfigurationException.class)
        .isThrownBy(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata));
    }

    @Test
    public void testRegularColumnNameMismatchPassesValidation()
    {
        // TODO this may have to be expected to pass validation legitimately
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_1_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_1_NAME, TableId.fromUUID(TABLE_1_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addRegularColumn("xvfdsfsdfsd", UUIDType.instance)  // is regular column but wrong name
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatCode(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata)).doesNotThrowAnyException();
    }

    @Test
    public void testAdditionalRegularColumnPassesValidation()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_1_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_1_NAME, TableId.fromUUID(TABLE_1_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addRegularColumn("col_uuid", UUIDType.instance)
                                                      .addRegularColumn("col_bool", BooleanType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .addRegularColumn("additional_column", FloatType.instance) // column not present in sstable schema
                                                      .build();
        assertThatCode(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata)).doesNotThrowAnyException();
    }

    @Test
    public void testMissingRegularColumnPassesValidation()
    {
        TableMetadata ssTableMetadata = ssTableMetadataForTableMap.get(TABLE_1_NAME);
        TableMetadata cqlTableMetadata = TableMetadata.builder(ksName, TABLE_1_NAME, TableId.fromUUID(TABLE_1_UUID))
                                                      .addPartitionKeyColumn("col_txt", UTF8Type.instance)
                                                      .addRegularColumn("col_int", Int32Type.instance)
                                                      .addRegularColumn("col_uuid", UUIDType.instance)
                                                      .addRegularColumn("col_dec", DecimalType.instance)
                                                      .build();
        assertThatCode(() -> ssTableMetadata.validateTableNameAndStructureCompatibility(cqlTableMetadata)).doesNotThrowAnyException();
    }
}