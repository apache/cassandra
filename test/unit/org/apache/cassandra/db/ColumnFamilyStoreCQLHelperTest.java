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

package org.apache.cassandra.db;

import java.io.FileReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.index.sasi.*;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.utils.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ColumnFamilyStoreCQLHelperTest extends CQLTester
{
    @Before
    public void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void testUserTypesCQL()
    {
        String keyspace = "cql_test_keyspace_user_types";
        String table = "test_table_user_types";

        UserType typeA = new UserType(keyspace, ByteBufferUtil.bytes("a"),
                                      Arrays.asList(FieldIdentifier.forUnquoted("a1"),
                                                    FieldIdentifier.forUnquoted("a2"),
                                                    FieldIdentifier.forUnquoted("a3")),
                                      Arrays.asList(IntegerType.instance,
                                                    IntegerType.instance,
                                                    IntegerType.instance),
                                      true);

        UserType typeB = new UserType(keyspace, ByteBufferUtil.bytes("b"),
                                      Arrays.asList(FieldIdentifier.forUnquoted("b1"),
                                                    FieldIdentifier.forUnquoted("b2"),
                                                    FieldIdentifier.forUnquoted("b3")),
                                      Arrays.asList(typeA,
                                                    typeA,
                                                    typeA),
                                      true);

        UserType typeC = new UserType(keyspace, ByteBufferUtil.bytes("c"),
                                      Arrays.asList(FieldIdentifier.forUnquoted("c1"),
                                                    FieldIdentifier.forUnquoted("c2"),
                                                    FieldIdentifier.forUnquoted("c3")),
                                      Arrays.asList(typeB,
                                                    typeB,
                                                    typeB),
                                      true);

        TableMetadata cfm =
            TableMetadata.builder(keyspace, table)
                         .addPartitionKeyColumn("pk1", IntegerType.instance)
                         .addClusteringColumn("ck1", IntegerType.instance)
                         .addRegularColumn("reg1", typeC)
                         .addRegularColumn("reg2", ListType.getInstance(IntegerType.instance, false))
                         .addRegularColumn("reg3", MapType.getInstance(AsciiType.instance, IntegerType.instance, true))
                         .build();

        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), Tables.of(cfm), Types.of(typeA, typeB, typeC));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        assertEquals(ImmutableList.of("CREATE TYPE cql_test_keyspace_user_types.a (a1 varint, a2 varint, a3 varint);",
                                      "CREATE TYPE cql_test_keyspace_user_types.b (b1 a, b2 a, b3 a);",
                                      "CREATE TYPE cql_test_keyspace_user_types.c (c1 b, c2 b, c3 b);"),
                     ColumnFamilyStoreCQLHelper.getUserTypesAsCQL(cfs.metadata()));
    }

    @Test
    public void testDroppedColumnsCQL()
    {
        String keyspace = "cql_test_keyspace_dropped_columns";
        String table = "test_table_dropped_columns";

        TableMetadata.Builder builder =
            TableMetadata.builder(keyspace, table)
                         .addPartitionKeyColumn("pk1", IntegerType.instance)
                         .addClusteringColumn("ck1", IntegerType.instance)
                         .addRegularColumn("reg1", IntegerType.instance)
                         .addRegularColumn("reg2", IntegerType.instance)
                         .addRegularColumn("reg3", IntegerType.instance);

        ColumnMetadata reg1 = builder.getColumn(ByteBufferUtil.bytes("reg1"));
        ColumnMetadata reg2 = builder.getColumn(ByteBufferUtil.bytes("reg2"));
        ColumnMetadata reg3 = builder.getColumn(ByteBufferUtil.bytes("reg3"));

        builder.removeRegularOrStaticColumn(reg1.name)
               .removeRegularOrStaticColumn(reg2.name)
               .removeRegularOrStaticColumn(reg3.name);

        builder.recordColumnDrop(reg1, 10000)
               .recordColumnDrop(reg2, 20000)
               .recordColumnDrop(reg3, 30000);

        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), builder);

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        assertEquals(ImmutableList.of("ALTER TABLE cql_test_keyspace_dropped_columns.test_table_dropped_columns DROP reg1 USING TIMESTAMP 10000;",
                                      "ALTER TABLE cql_test_keyspace_dropped_columns.test_table_dropped_columns DROP reg3 USING TIMESTAMP 30000;",
                                      "ALTER TABLE cql_test_keyspace_dropped_columns.test_table_dropped_columns DROP reg2 USING TIMESTAMP 20000;"),
                     ColumnFamilyStoreCQLHelper.getDroppedColumnsAsCQL(cfs.metadata()));

        assertTrue(ColumnFamilyStoreCQLHelper.getTableMetadataAsCQL(cfs.metadata(), true).startsWith(
        "CREATE TABLE IF NOT EXISTS cql_test_keyspace_dropped_columns.test_table_dropped_columns (\n" +
        "\tpk1 varint,\n" +
        "\tck1 varint,\n" +
        "\treg1 varint,\n" +
        "\treg3 varint,\n" +
        "\treg2 varint,\n" +
        "\tPRIMARY KEY (pk1, ck1))"));
    }

    @Test
    public void testReaddedColumns()
    {
        String keyspace = "cql_test_keyspace_readded_columns";
        String table = "test_table_readded_columns";

        TableMetadata.Builder builder =
            TableMetadata.builder(keyspace, table)
                         .addPartitionKeyColumn("pk1", IntegerType.instance)
                         .addClusteringColumn("ck1", IntegerType.instance)
                         .addRegularColumn("reg1", IntegerType.instance)
                         .addStaticColumn("reg2", IntegerType.instance)
                         .addRegularColumn("reg3", IntegerType.instance);

        ColumnMetadata reg1 = builder.getColumn(ByteBufferUtil.bytes("reg1"));
        ColumnMetadata reg2 = builder.getColumn(ByteBufferUtil.bytes("reg2"));

        builder.removeRegularOrStaticColumn(reg1.name);
        builder.removeRegularOrStaticColumn(reg2.name);

        builder.recordColumnDrop(reg1, 10000);
        builder.recordColumnDrop(reg2, 20000);

        builder.addColumn(reg1);
        builder.addColumn(reg2);

        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), builder);

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        // when re-adding, column is present in CREATE, then in DROP and then in ADD again, to record DROP with a proper timestamp
        assertTrue(ColumnFamilyStoreCQLHelper.getTableMetadataAsCQL(cfs.metadata(), true).startsWith(
        "CREATE TABLE IF NOT EXISTS cql_test_keyspace_readded_columns.test_table_readded_columns (\n" +
        "\tpk1 varint,\n" +
        "\tck1 varint,\n" +
        "\treg2 varint static,\n" +
        "\treg1 varint,\n" +
        "\treg3 varint,\n" +
        "\tPRIMARY KEY (pk1, ck1))"));

        assertEquals(ImmutableList.of("ALTER TABLE cql_test_keyspace_readded_columns.test_table_readded_columns DROP reg1 USING TIMESTAMP 10000;",
                                      "ALTER TABLE cql_test_keyspace_readded_columns.test_table_readded_columns ADD reg1 varint;",
                                      "ALTER TABLE cql_test_keyspace_readded_columns.test_table_readded_columns DROP reg2 USING TIMESTAMP 20000;",
                                      "ALTER TABLE cql_test_keyspace_readded_columns.test_table_readded_columns ADD reg2 varint static;"),
                     ColumnFamilyStoreCQLHelper.getDroppedColumnsAsCQL(cfs.metadata()));
    }

    @Test
    public void testCfmColumnsCQL()
    {
        String keyspace = "cql_test_keyspace_create_table";
        String table = "test_table_create_table";

        TableMetadata.Builder metadata =
            TableMetadata.builder(keyspace, table)
                         .addPartitionKeyColumn("pk1", IntegerType.instance)
                         .addPartitionKeyColumn("pk2", AsciiType.instance)
                         .addClusteringColumn("ck1", ReversedType.getInstance(IntegerType.instance))
                         .addClusteringColumn("ck2", IntegerType.instance)
                         .addStaticColumn("st1", AsciiType.instance)
                         .addRegularColumn("reg1", AsciiType.instance)
                         .addRegularColumn("reg2", ListType.getInstance(IntegerType.instance, false))
                         .addRegularColumn("reg3", MapType.getInstance(AsciiType.instance, IntegerType.instance, true));

        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), metadata);

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        assertTrue(ColumnFamilyStoreCQLHelper.getTableMetadataAsCQL(cfs.metadata(), true).startsWith(
        "CREATE TABLE IF NOT EXISTS cql_test_keyspace_create_table.test_table_create_table (\n" +
        "\tpk1 varint,\n" +
        "\tpk2 ascii,\n" +
        "\tck1 varint,\n" +
        "\tck2 varint,\n" +
        "\tst1 ascii static,\n" +
        "\treg1 ascii,\n" +
        "\treg2 frozen<list<varint>>,\n" +
        "\treg3 map<ascii, varint>,\n" +
        "\tPRIMARY KEY ((pk1, pk2), ck1, ck2))\n" +
        "\tWITH ID = " + cfs.metadata.id + "\n" +
        "\tAND CLUSTERING ORDER BY (ck1 DESC, ck2 ASC)"));
    }

    @Test
    public void testCfmOptionsCQL()
    {
        String keyspace = "cql_test_keyspace_options";
        String table = "test_table_options";

        TableMetadata.Builder builder = TableMetadata.builder(keyspace, table);
        builder.addPartitionKeyColumn("pk1", IntegerType.instance)
               .addClusteringColumn("cl1", IntegerType.instance)
               .addRegularColumn("reg1", AsciiType.instance)
               .bloomFilterFpChance(1.0)
               .comment("comment")
               .compaction(CompactionParams.lcs(Collections.singletonMap("sstable_size_in_mb", "1")))
               .compression(CompressionParams.lz4(1 << 16, 1 << 15))
               .dcLocalReadRepairChance(0.2)
               .crcCheckChance(0.3)
               .defaultTimeToLive(4)
               .gcGraceSeconds(5)
               .minIndexInterval(6)
               .maxIndexInterval(7)
               .memtableFlushPeriod(8)
               .readRepairChance(0.9)
               .speculativeRetry(SpeculativeRetryParam.always())
               .extensions(ImmutableMap.of("ext1", ByteBuffer.wrap("val1".getBytes())))
               .recordColumnDrop(ColumnMetadata.regularColumn(keyspace, table, "reg1", AsciiType.instance),
                                 FBUtilities.timestampMicros());

        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), builder);

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        assertTrue(ColumnFamilyStoreCQLHelper.getTableMetadataAsCQL(cfs.metadata(), true).endsWith(
        "AND bloom_filter_fp_chance = 1.0\n" +
        "\tAND dclocal_read_repair_chance = 0.2\n" +
        "\tAND crc_check_chance = 0.3\n" +
        "\tAND default_time_to_live = 4\n" +
        "\tAND gc_grace_seconds = 5\n" +
        "\tAND min_index_interval = 6\n" +
        "\tAND max_index_interval = 7\n" +
        "\tAND memtable_flush_period_in_ms = 8\n" +
        "\tAND read_repair_chance = 0.9\n" +
        "\tAND speculative_retry = 'ALWAYS'\n" +
        "\tAND comment = 'comment'\n" +
        "\tAND caching = { 'keys': 'ALL', 'rows_per_partition': 'NONE' }\n" +
        "\tAND compaction = { 'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'sstable_size_in_mb': '1' }\n" +
        "\tAND compression = { 'chunk_length_in_kb': '64', 'min_compress_ratio': '2.0', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor' }\n" +
        "\tAND cdc = false\n" +
        "\tAND extensions = { 'ext1': 0x76616c31 };"
        ));
    }

    @Test
    public void testCfmIndexJson()
    {
        String keyspace = "cql_test_keyspace_3";
        String table = "test_table_3";

        TableMetadata.Builder builder =
            TableMetadata.builder(keyspace, table)
                         .addPartitionKeyColumn("pk1", IntegerType.instance)
                         .addClusteringColumn("cl1", IntegerType.instance)
                         .addRegularColumn("reg1", AsciiType.instance);

        ColumnIdentifier reg1 = ColumnIdentifier.getInterned("reg1", true);

        builder.indexes(
            Indexes.of(IndexMetadata.fromIndexTargets(
            Collections.singletonList(new IndexTarget(reg1, IndexTarget.Type.VALUES)),
                                                      "indexName",
                                                      IndexMetadata.Kind.COMPOSITES,
                                                      Collections.emptyMap()),
                       IndexMetadata.fromIndexTargets(
                       Collections.singletonList(new IndexTarget(reg1, IndexTarget.Type.KEYS)),
                                                      "indexName2",
                                                      IndexMetadata.Kind.COMPOSITES,
                                                      Collections.emptyMap()),
                       IndexMetadata.fromIndexTargets(
                       Collections.singletonList(new IndexTarget(reg1, IndexTarget.Type.KEYS_AND_VALUES)),
                                                      "indexName3",
                                                      IndexMetadata.Kind.COMPOSITES,
                                                      Collections.emptyMap()),
                       IndexMetadata.fromIndexTargets(
                       Collections.singletonList(new IndexTarget(reg1, IndexTarget.Type.KEYS_AND_VALUES)),
                                                      "indexName4",
                                                      IndexMetadata.Kind.CUSTOM,
                                                      Collections.singletonMap(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName()))));


        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), builder);

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        assertEquals(ImmutableList.of("CREATE INDEX \"indexName\" ON cql_test_keyspace_3.test_table_3 (values(reg1));",
                                      "CREATE INDEX \"indexName2\" ON cql_test_keyspace_3.test_table_3 (keys(reg1));",
                                      "CREATE INDEX \"indexName3\" ON cql_test_keyspace_3.test_table_3 (entries(reg1));",
                                      "CREATE CUSTOM INDEX \"indexName4\" ON cql_test_keyspace_3.test_table_3 (entries(reg1)) USING 'org.apache.cassandra.index.sasi.SASIIndex';"),
                     ColumnFamilyStoreCQLHelper.getIndexesAsCQL(cfs.metadata()));
    }

    private final static String SNAPSHOT = "testsnapshot";

    @Test
    public void testSnapshot() throws Throwable
    {
        String typeA = createType("CREATE TYPE %s (a1 varint, a2 varint, a3 varint);");
        String typeB = createType("CREATE TYPE %s (b1 frozen<" + typeA + ">, b2 frozen<" + typeA + ">, b3 frozen<" + typeA + ">);");
        String typeC = createType("CREATE TYPE %s (c1 frozen<" + typeB + ">, c2 frozen<" + typeB + ">, c3 frozen<" + typeB + ">);");

        String tableName = createTable("CREATE TABLE IF NOT EXISTS %s (" +
                                       "pk1 varint," +
                                       "pk2 ascii," +
                                       "ck1 varint," +
                                       "ck2 varint," +
                                       "reg1 " + typeC + "," +
                                       "reg2 int," +
                                       "reg3 int," +
                                       "PRIMARY KEY ((pk1, pk2), ck1, ck2)) WITH " +
                                       "CLUSTERING ORDER BY (ck1 ASC, ck2 DESC);");

        alterTable("ALTER TABLE %s DROP reg3 USING TIMESTAMP 10000;");
        alterTable("ALTER TABLE %s ADD reg3 int;");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (pk1, pk2, ck1, ck2, reg1, reg2) VALUES (?, ?, ?, ?, ?, ?)", i, i + 1, i + 2, i + 3, null, i + 5);

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(tableName);
        cfs.snapshot(SNAPSHOT);

        String schema = Files.toString(cfs.getDirectories().getSnapshotSchemaFile(SNAPSHOT), Charset.defaultCharset());
        assertTrue(schema.contains(String.format("CREATE TYPE %s.%s (a1 varint, a2 varint, a3 varint);", keyspace(), typeA)));
        assertTrue(schema.contains(String.format("CREATE TYPE %s.%s (a1 varint, a2 varint, a3 varint);", keyspace(), typeA)));
        assertTrue(schema.contains(String.format("CREATE TYPE %s.%s (b1 frozen<%s>, b2 frozen<%s>, b3 frozen<%s>);", keyspace(), typeB, typeA, typeA, typeA)));
        assertTrue(schema.contains(String.format("CREATE TYPE %s.%s (c1 frozen<%s>, c2 frozen<%s>, c3 frozen<%s>);", keyspace(), typeC, typeB, typeB, typeB)));

        schema = schema.substring(schema.indexOf("CREATE TABLE")); // trim to ensure order

        assertTrue(schema.startsWith("CREATE TABLE IF NOT EXISTS " + keyspace() + "." + tableName + " (\n" +
                                     "\tpk1 varint,\n" +
                                     "\tpk2 ascii,\n" +
                                     "\tck1 varint,\n" +
                                     "\tck2 varint,\n" +
                                     "\treg2 int,\n" +
                                     "\treg3 int,\n" +
                                     "\treg1 " + typeC + ",\n" +
                                     "\tPRIMARY KEY ((pk1, pk2), ck1, ck2))\n" +
                                     "\tWITH ID = " + cfs.metadata.id + "\n" +
                                     "\tAND CLUSTERING ORDER BY (ck1 ASC, ck2 DESC)"));

        schema = schema.substring(schema.indexOf("ALTER"));
        assertTrue(schema.startsWith(String.format("ALTER TABLE %s.%s DROP reg3 USING TIMESTAMP 10000;", keyspace(), tableName)));
        assertTrue(schema.contains(String.format("ALTER TABLE %s.%s ADD reg3 int;", keyspace(), tableName)));

        JSONObject manifest = (JSONObject) new JSONParser().parse(new FileReader(cfs.getDirectories().getSnapshotManifestFile(SNAPSHOT)));
        JSONArray files = (JSONArray) manifest.get("files");
        Assert.assertEquals(1, files.size());
    }

    @Test
    public void testSystemKsSnapshot() throws Throwable
    {
        ColumnFamilyStore cfs = Keyspace.open("system").getColumnFamilyStore("peers");
        cfs.snapshot(SNAPSHOT);

        Assert.assertTrue(cfs.getDirectories().getSnapshotManifestFile(SNAPSHOT).exists());
        Assert.assertFalse(cfs.getDirectories().getSnapshotSchemaFile(SNAPSHOT).exists());
    }

    @Test
    public void testDroppedType() throws Throwable
    {
        String typeA = createType("CREATE TYPE %s (a1 varint, a2 varint, a3 varint);");
        String typeB = createType("CREATE TYPE %s (b1 frozen<" + typeA + ">, b2 frozen<" + typeA + ">, b3 frozen<" + typeA + ">);");

        String tableName = createTable("CREATE TABLE IF NOT EXISTS %s (" +
                                       "pk1 varint," +
                                       "ck1 varint," +
                                       "reg1 " + typeB + "," +
                                       "reg2 varint," +
                                       "PRIMARY KEY (pk1, ck1));");

        alterTable("ALTER TABLE %s DROP reg1 USING TIMESTAMP 10000;");

        Runnable validate = () -> {
            try
            {
                ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(tableName);
                cfs.snapshot(SNAPSHOT);
                String schema = Files.toString(cfs.getDirectories().getSnapshotSchemaFile(SNAPSHOT), Charset.defaultCharset());

                // When both column and it's type are dropped, the type in column definition gets substituted with a tuple
                assertTrue(schema.startsWith("CREATE TABLE IF NOT EXISTS " + keyspace() + "." + tableName + " (\n" +
                                             "\tpk1 varint,\n" +
                                             "\tck1 varint,\n" +
                                             "\treg2 varint,\n" +
                                             "\treg1 frozen<tuple<frozen<tuple<varint, varint, varint>>, frozen<tuple<varint, varint, varint>>, frozen<tuple<varint, varint, varint>>>>,\n" +
                                             "\tPRIMARY KEY (pk1, ck1))"));
                assertTrue(schema.contains("ALTER TABLE " + keyspace() + "." + tableName + " DROP reg1 USING TIMESTAMP 10000;"));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        };

        // Validate before and after the type drop
        validate.run();
        schemaChange("DROP TYPE " + keyspace() + "." + typeB);
        schemaChange("DROP TYPE " + keyspace() + "." + typeA);
        validate.run();
    }
}
