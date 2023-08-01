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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.cassandra.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class SchemaCQLHelperTest extends CQLTester
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
                     .addRegularColumn("reg1", typeC.freeze())
                     .addRegularColumn("reg2", ListType.getInstance(IntegerType.instance, false))
                     .addRegularColumn("reg3", MapType.getInstance(AsciiType.instance, IntegerType.instance, true))
                     .build();

        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), Tables.of(cfm), Types.of(typeA, typeB, typeC));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        assertEquals(ImmutableList.of("CREATE TYPE cql_test_keyspace_user_types.a (\n" +
                                      "    a1 varint,\n" +
                                      "    a2 varint,\n" +
                                      "    a3 varint\n" +
                                      ");",
                                      "CREATE TYPE cql_test_keyspace_user_types.b (\n" +
                                      "    b1 a,\n" +
                                      "    b2 a,\n" +
                                      "    b3 a\n" +
                                      ");",
                                      "CREATE TYPE cql_test_keyspace_user_types.c (\n" +
                                      "    c1 b,\n" +
                                      "    c2 b,\n" +
                                      "    c3 b\n" +
                                      ");"),
                     SchemaCQLHelper.getUserTypesAsCQL(cfs.metadata(), cfs.keyspace.getMetadata().types, false).collect(Collectors.toList()));
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
                     .addStaticColumn("st1", IntegerType.instance)
                     .addRegularColumn("reg1", IntegerType.instance)
                     .addRegularColumn("reg2", IntegerType.instance)
                     .addRegularColumn("reg3", IntegerType.instance);

        ColumnMetadata st1 = builder.getColumn(ByteBufferUtil.bytes("st1"));
        ColumnMetadata reg1 = builder.getColumn(ByteBufferUtil.bytes("reg1"));
        ColumnMetadata reg2 = builder.getColumn(ByteBufferUtil.bytes("reg2"));
        ColumnMetadata reg3 = builder.getColumn(ByteBufferUtil.bytes("reg3"));

        builder.removeRegularOrStaticColumn(st1.name)
               .removeRegularOrStaticColumn(reg1.name)
               .removeRegularOrStaticColumn(reg2.name)
               .removeRegularOrStaticColumn(reg3.name);

        builder.recordColumnDrop(st1, 5000)
               .recordColumnDrop(reg1, 10000)
               .recordColumnDrop(reg2, 20000)
               .recordColumnDrop(reg3, 30000);

        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), builder);

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        String expected = "CREATE TABLE IF NOT EXISTS cql_test_keyspace_dropped_columns.test_table_dropped_columns (\n" +
                          "    pk1 varint,\n" +
                          "    ck1 varint,\n" +
                          "    reg1 varint,\n" +
                          "    reg3 varint,\n" +
                          "    reg2 varint,\n" +
                          "    st1 varint static,\n" +
                          "    PRIMARY KEY (pk1, ck1)\n) WITH ID =";
        String actual = SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), cfs.keyspace.getMetadata());

        assertThat(actual,
                   allOf(startsWith(expected),
                         containsString("ALTER TABLE cql_test_keyspace_dropped_columns.test_table_dropped_columns DROP reg1 USING TIMESTAMP 10000;"),
                         containsString("ALTER TABLE cql_test_keyspace_dropped_columns.test_table_dropped_columns DROP reg3 USING TIMESTAMP 30000;"),
                         containsString("ALTER TABLE cql_test_keyspace_dropped_columns.test_table_dropped_columns DROP reg2 USING TIMESTAMP 20000;"),
                         containsString("ALTER TABLE cql_test_keyspace_dropped_columns.test_table_dropped_columns DROP st1 USING TIMESTAMP 5000;")));
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
                     .addStaticColumn("st1", IntegerType.instance)
                     .addRegularColumn("reg2", IntegerType.instance);

        ColumnMetadata reg1 = builder.getColumn(ByteBufferUtil.bytes("reg1"));
        ColumnMetadata st1 = builder.getColumn(ByteBufferUtil.bytes("st1"));

        builder.removeRegularOrStaticColumn(reg1.name);
        builder.removeRegularOrStaticColumn(st1.name);

        builder.recordColumnDrop(reg1, 10000);
        builder.recordColumnDrop(st1, 20000);

        builder.addColumn(reg1);
        builder.addColumn(st1);

        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), builder);

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        // when re-adding, column is present as both column and as dropped column record.
        String actual = SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), cfs.keyspace.getMetadata());
        String expected = "CREATE TABLE IF NOT EXISTS cql_test_keyspace_readded_columns.test_table_readded_columns (\n" +
                          "    pk1 varint,\n" +
                          "    ck1 varint,\n" +
                          "    reg2 varint,\n" +
                          "    reg1 varint,\n" +
                          "    st1 varint static,\n" +
                          "    PRIMARY KEY (pk1, ck1)\n" +
                          ") WITH ID";

        assertThat(actual,
                   allOf(startsWith(expected),
                         containsString("ALTER TABLE cql_test_keyspace_readded_columns.test_table_readded_columns DROP reg1 USING TIMESTAMP 10000;"),
                         containsString("ALTER TABLE cql_test_keyspace_readded_columns.test_table_readded_columns ADD reg1 varint;"),
                         containsString("ALTER TABLE cql_test_keyspace_readded_columns.test_table_readded_columns DROP st1 USING TIMESTAMP 20000;"),
                         containsString("ALTER TABLE cql_test_keyspace_readded_columns.test_table_readded_columns ADD st1 varint static;")));
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

        assertThat(SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), cfs.keyspace.getMetadata()),
                   startsWith(
                   "CREATE TABLE IF NOT EXISTS cql_test_keyspace_create_table.test_table_create_table (\n" +
                   "    pk1 varint,\n" +
                   "    pk2 ascii,\n" +
                   "    ck1 varint,\n" +
                   "    ck2 varint,\n" +
                   "    st1 ascii static,\n" +
                   "    reg1 ascii,\n" +
                   "    reg2 frozen<list<varint>>,\n" +
                   "    reg3 map<ascii, varint>,\n" +
                   "    PRIMARY KEY ((pk1, pk2), ck1, ck2)\n" +
                   ") WITH ID = " + cfs.metadata.id + "\n" +
                   "    AND CLUSTERING ORDER BY (ck1 DESC, ck2 ASC)"));
    }

    @Test
    public void testCfmOptionsCQL()
    {
        String keyspace = "cql_test_keyspace_options";
        String table = "test_table_options";

        TableMetadata.Builder builder = TableMetadata.builder(keyspace, table);
        long droppedTimestamp = FBUtilities.timestampMicros();
        builder.addPartitionKeyColumn("pk1", IntegerType.instance)
               .addClusteringColumn("cl1", IntegerType.instance)
               .addRegularColumn("reg1", AsciiType.instance)
               .bloomFilterFpChance(1.0)
               .comment("comment")
               .compaction(CompactionParams.lcs(Collections.singletonMap("sstable_size_in_mb", "1")))
               .compression(CompressionParams.lz4(1 << 16, 1 << 15))
               .crcCheckChance(0.3)
               .defaultTimeToLive(4)
               .gcGraceSeconds(5)
               .minIndexInterval(6)
               .maxIndexInterval(7)
               .memtableFlushPeriod(8)
               .speculativeRetry(SpeculativeRetryPolicy.fromString("always"))
               .additionalWritePolicy(SpeculativeRetryPolicy.fromString("always"))
               .extensions(ImmutableMap.of("ext1", ByteBuffer.wrap("val1".getBytes())))
               .recordColumnDrop(ColumnMetadata.regularColumn(keyspace, table, "reg1", AsciiType.instance),
                                 droppedTimestamp);

        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), builder);

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        assertThat(SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), cfs.keyspace.getMetadata()),
                   containsString("CLUSTERING ORDER BY (cl1 ASC)\n" +
                            "    AND additional_write_policy = 'ALWAYS'\n" +
                            "    AND allow_auto_snapshot = true\n" +
                            "    AND bloom_filter_fp_chance = 1.0\n" +
                            "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n" +
                            "    AND cdc = false\n" +
                            "    AND comment = 'comment'\n" +
                            "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4', 'sstable_size_in_mb': '1'}\n" +
                            "    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor', 'min_compress_ratio': '2.0'}\n" +
                            "    AND memtable = 'default'\n" +
                            "    AND crc_check_chance = 0.3\n" +
                            "    AND default_time_to_live = 4\n" +
                            "    AND extensions = {'ext1': 0x76616c31}\n" +
                            "    AND gc_grace_seconds = 5\n" +
                            "    AND incremental_backups = true\n" +
                            "    AND max_index_interval = 7\n" +
                            "    AND memtable_flush_period_in_ms = 8\n" +
                            "    AND min_index_interval = 6\n" +
                            "    AND read_repair = 'BLOCKING'\n" +
                            "    AND speculative_retry = 'ALWAYS';"
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
                   Collections.singletonMap(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName())),
                   IndexMetadata.fromIndexTargets(
                   Collections.singletonList(new IndexTarget(reg1, IndexTarget.Type.KEYS_AND_VALUES)),
                   "indexName5",
                   IndexMetadata.Kind.CUSTOM,
                   ImmutableMap.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME,SASIIndex.class.getName(),
                                   "is_literal", "false"))
                   ));


        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), builder);

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        assertEquals(ImmutableList.of("CREATE INDEX \"indexName\" ON cql_test_keyspace_3.test_table_3 (values(reg1));",
                                      "CREATE INDEX \"indexName2\" ON cql_test_keyspace_3.test_table_3 (keys(reg1));",
                                      "CREATE INDEX \"indexName3\" ON cql_test_keyspace_3.test_table_3 (entries(reg1));",
                                      "CREATE CUSTOM INDEX \"indexName4\" ON cql_test_keyspace_3.test_table_3 (entries(reg1)) USING 'org.apache.cassandra.index.sasi.SASIIndex';",
                                      "CREATE CUSTOM INDEX \"indexName5\" ON cql_test_keyspace_3.test_table_3 (entries(reg1)) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'is_literal': 'false'};"),
                     SchemaCQLHelper.getIndexesAsCQL(cfs.metadata(), false).collect(Collectors.toList()));
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
        // CREATE INDEX def_name_idx ON abc.def (name);
        createIndex("CREATE INDEX ON %s(reg2)");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (pk1, pk2, ck1, ck2, reg1, reg2) VALUES (?, ?, ?, ?, ?, ?)", i, i + 1, i + 2, i + 3, null, i + 5);

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(tableName);
        cfs.snapshot(SNAPSHOT);

        String schema = Files.toString(cfs.getDirectories().getSnapshotSchemaFile(SNAPSHOT).toJavaIOFile(), Charset.defaultCharset());
        assertThat(schema,
                   allOf(containsString(String.format("CREATE TYPE IF NOT EXISTS %s.%s (\n" +
                                                      "    a1 varint,\n" +
                                                      "    a2 varint,\n" +
                                                      "    a3 varint\n" +
                                                      ");", keyspace(), typeA)),
                         containsString(String.format("CREATE TYPE IF NOT EXISTS %s.%s (\n" +
                                                      "    a1 varint,\n" +
                                                      "    a2 varint,\n" +
                                                      "    a3 varint\n" +
                                                      ");", keyspace(), typeA)),
                         containsString(String.format("CREATE TYPE IF NOT EXISTS %s.%s (\n" +
                                                      "    b1 frozen<%s>,\n" +
                                                      "    b2 frozen<%s>,\n" +
                                                      "    b3 frozen<%s>\n" +
                                                      ");", keyspace(), typeB, typeA, typeA, typeA)),
                         containsString(String.format("CREATE TYPE IF NOT EXISTS %s.%s (\n" +
                                                      "    c1 frozen<%s>,\n" +
                                                      "    c2 frozen<%s>,\n" +
                                                      "    c3 frozen<%s>\n" +
                                                      ");", keyspace(), typeC, typeB, typeB, typeB))));

        schema = schema.substring(schema.indexOf("CREATE TABLE")); // trim to ensure order
        String expected = "CREATE TABLE IF NOT EXISTS " + keyspace() + "." + tableName + " (\n" +
                          "    pk1 varint,\n" +
                          "    pk2 ascii,\n" +
                          "    ck1 varint,\n" +
                          "    ck2 varint,\n" +
                          "    reg2 int,\n" +
                          "    reg1 " + typeC+ ",\n" +
                          "    reg3 int,\n" +
                          "    PRIMARY KEY ((pk1, pk2), ck1, ck2)\n" +
                          ") WITH ID = " + cfs.metadata.id + "\n" +
                          "    AND CLUSTERING ORDER BY (ck1 ASC, ck2 DESC)";

        assertThat(schema,
                   allOf(startsWith(expected),
                         containsString("ALTER TABLE " + keyspace() + "." + tableName + " DROP reg3 USING TIMESTAMP 10000;"),
                         containsString("ALTER TABLE " + keyspace() + "." + tableName + " ADD reg3 int;")));

        assertThat(schema, containsString("CREATE INDEX IF NOT EXISTS " + tableName + "_reg2_idx ON " + keyspace() + '.' + tableName + " (reg2);"));

        JsonNode manifest = JsonUtils.JSON_OBJECT_MAPPER.readTree(cfs.getDirectories().getSnapshotManifestFile(SNAPSHOT).toJavaIOFile());
        JsonNode files = manifest.get("files");
        // two files, the second is index
        Assert.assertTrue(files.isArray());
        Assert.assertEquals(2, files.size());
    }

    @Test
    public void testSystemKsSnapshot()
    {
        ColumnFamilyStore cfs = Keyspace.open("system").getColumnFamilyStore("peers");
        cfs.snapshot(SNAPSHOT);

        Assert.assertTrue(cfs.getDirectories().getSnapshotManifestFile(SNAPSHOT).exists());
        Assert.assertFalse(cfs.getDirectories().getSnapshotSchemaFile(SNAPSHOT).exists());
    }

    @Test
    public void testBooleanCompositeKey() throws Throwable
    {
        createTable("CREATE TABLE %s (t_id boolean, id boolean, ck boolean, nk boolean, PRIMARY KEY ((t_id, id), ck))");

        execute("insert into %s (t_id, id, ck, nk) VALUES (true, false, false, true)");
        assertRows(execute("select * from %s"), row(true, false, false, true));

        // CASSANDRA-14752 -
        // a problem with composite boolean types meant that calling this would
        // prevent any boolean values to be inserted afterwards
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.getSSTablesForKey("false:true");

        execute("insert into %s (t_id, id, ck, nk) VALUES (true, true, false, true)");
        assertRows(execute("select t_id, id, ck, nk from %s"), row(true, true, false, true), row(true, false, false, true));
    }
}
