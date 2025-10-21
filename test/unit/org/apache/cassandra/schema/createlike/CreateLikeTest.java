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

package org.apache.cassandra.schema.createlike;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.validation.operations.CreateTest;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CreateLikeTest extends CQLTester
{
    @Parameterized.Parameter
    public boolean differentKs;

    @Parameterized.Parameters(name = "{index}: differentKs={0}")
    public static Collection<Object[]> data()
    {
        List<Object[]> result = new ArrayList<>();
        result.add(new Object[]{ false });
        result.add(new Object[]{ true });
        return result;
    }

    private final UUID uuid1 = UUID.fromString("62c3e96f-55cd-493b-8c8e-5a18883a1698");
    private final UUID uuid2 = UUID.fromString("52c3e96f-55cd-493b-8c8e-5a18883a1698");
    private final TimeUUID timeUuid1 = TimeUUID.fromString("00346642-2d2f-11ed-a261-0242ac120002");
    private final TimeUUID timeUuid2 = TimeUUID.fromString("10346642-2d2f-11ed-a261-0242ac120002");
    private final Duration duration1 = Duration.newInstance(1, 2, 3);
    private final Duration duration2 = Duration.newInstance(1, 2, 4);
    private final Double d1 = Double.valueOf("1.1");
    private final Double d2 = Double.valueOf("2.2");
    private final Float f1 = Float.valueOf("3.33");
    private final Float f2 = Float.valueOf("4.44");
    private final BigDecimal decimal1 = BigDecimal.valueOf(1.1);
    private final BigDecimal decimal2 = BigDecimal.valueOf(2.2);
    private final Vector<Integer> vector1 = vector(1, 2);
    private final Vector<Integer> vector2 = vector(3, 4);
    private final String keyspace1 = "keyspace1";
    private final String keyspace2 = "keyspace2";
    private String sourceKs;
    private String targetKs;

    @Before
    public void before()
    {
        createKeyspace("CREATE KEYSPACE " + keyspace1 + " WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        createKeyspace("CREATE KEYSPACE " + keyspace2 + " WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        sourceKs = keyspace1;
        targetKs = differentKs ? keyspace2 : keyspace1;
    }

    @Test
    public void testTableSchemaCopy()
    {
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b duration, c text);");
        String targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);
        execute("INSERT INTO " + sourceKs + "." + sourceTb + " (a, b, c) VALUES (?, ?, ?)", 1, duration1, "1");
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a, b, c) VALUES (?, ?, ?)", 2, duration2, "2");
        assertRows(execute("SELECT * FROM " + sourceKs + "." + sourceTb),
                   row(1, duration1, "1"));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb),
                   row(2, duration2, "2"));

        sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY);");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);
        execute("INSERT INTO " + sourceKs + "." + sourceTb + " (a) VALUES (1)");
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a) VALUES (2)");
        assertRows(execute("SELECT * FROM " + sourceKs + "." + sourceTb),
                   row(1));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb),
                   row(2));

        sourceTb = createTable(sourceKs, "CREATE TABLE %s (a frozen<map<text, text>> PRIMARY KEY);");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);
        execute("INSERT INTO " + sourceKs + "." + sourceTb + " (a) VALUES (?)", map("k", "v"));
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a) VALUES (?)", map("nk", "nv"));
        assertRows(execute("SELECT * FROM " + sourceKs + "." + sourceTb),
                   row(map("k", "v")));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb),
                   row(map("nk", "nv")));

        sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b set<frozen<list<text>>>, c map<text, int>, d smallint, e duration, f tinyint);");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);
        execute("INSERT INTO " + sourceKs + "." + sourceTb + " (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)",
                1, set(list("1", "2"), list("3", "4")), map("k", 1), (short) 2, duration1, (byte) 4);
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)",
                2, set(list("5", "6"), list("7", "8")), map("nk", 2), (short) 3, duration2, (byte) 5);
        assertRows(execute("SELECT * FROM " + sourceKs + "." + sourceTb),
                   row(1, set(list("1", "2"), list("3", "4")), map("k", 1), (short) 2, duration1, (byte) 4));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb),
                   row(2, set(list("5", "6"), list("7", "8")), map("nk", 2), (short) 3, duration2, (byte) 5));

        sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int , b double, c tinyint, d float, e list<text>, f map<text, int>, g duration, PRIMARY KEY((a, b, c), d));");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);
        execute("INSERT INTO " + sourceKs + "." + sourceTb + " (a, b, c, d, e, f, g) VALUES (?, ?, ?, ?, ?, ?, ?) ",
                1, d1, (byte) 4, f1, list("a", "b"), map("k", 1), duration1);
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a, b, c, d, e, f, g) VALUES (?, ?, ?, ?, ?, ?, ?) ",
                2, d2, (byte) 5, f2, list("c", "d"), map("nk", 2), duration2);
        assertRows(execute("SELECT * FROM " + sourceKs + "." + sourceTb),
                   row(1, d1, (byte) 4, f1, list("a", "b"), map("k", 1), duration1));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb),
                   row(2, d2, (byte) 5, f2, list("c", "d"), map("nk", 2), duration2));

        sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int , " +
                                         "b text, " +
                                         "c bigint, " +
                                         "d decimal, " +
                                         "e set<text>, " +
                                         "f uuid, " +
                                         "g vector<int, 2>, " +
                                         "h list<float>, " +
                                         "i timeuuid, " +
                                         "j map<text, frozen<set<int>>>, " +
                                         "PRIMARY KEY((a, b), c, d)) " +
                                         "WITH CLUSTERING ORDER BY (c DESC, d ASC);");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);
        execute("INSERT INTO " + sourceKs + "." + sourceTb + " (a, b, c, d, e, f, g, h, i, j)  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                1, "b", 100L, decimal1, set("1", "2"), uuid1, vector1, list(1.1F, 2.2F), timeUuid1, map("k", set(1, 2)));
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a, b, c, d, e, f, g, h, i, j) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                2, "nb", 200L, decimal2, set("3", "4"), uuid2, vector2, list(3.3F, 4.4F), timeUuid2, map("nk", set(3, 4)));
        assertRows(execute("SELECT * FROM " + sourceKs + "." + sourceTb),
                   row(1, "b", 100L, decimal1, set("1", "2"), uuid1, vector1, list(1.1F, 2.2F), timeUuid1, map("k", set(1, 2))));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb),
                   row(2, "nb", 200L, decimal2, set("3", "4"), uuid2, vector2, list(3.3F, 4.4F), timeUuid2, map("nk", set(3, 4))));

        // test that can create a copy of a copied table
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b duration, c text);");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        createTableLike("CREATE TABLE %s LIKE %s", targetTb, targetKs, "newtargettb", sourceKs);
    }

    @Test
    public void testIfNotExists() throws Throwable
    {
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int, b text, c duration, d float, PRIMARY KEY(a, b));");
        String targetTb = createTableLike("CREATE TABLE IF NOT EXISTS %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);

        createTableLike("CREATE TABLE IF NOT EXISTS %s LIKE %s", sourceTb, sourceKs, targetTb, targetKs);
        assertInvalidThrowMessage("Cannot add already existing table \"" + targetTb + "\" to keyspace \"" + targetKs + "\"", AlreadyExistsException.class,
                                  "CREATE TABLE " + targetKs + "." + targetTb + " LIKE " + sourceKs + "." + sourceTb);
    }

    @Test
    public void testCopyAfterAlterTable()
    {
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int, b text, c duration, d float, PRIMARY KEY(a, b));");
        String targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);

        alterTable("ALTER TABLE " + sourceKs + "." + sourceTb + " DROP d");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);

        alterTable("ALTER TABLE " + sourceKs + "." + sourceTb + " ADD e uuid");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);

        alterTable("ALTER TABLE " + sourceKs + "." + sourceTb + " ADD f float");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);

        execute("INSERT INTO " + sourceKs + "." + sourceTb + " (a, b, c, e, f) VALUES (?, ?, ?, ?, ?)", 1, "1", duration1, uuid1, f1);
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a, b, c, e, f) VALUES (?, ?, ?, ?, ?)", 2, "2", duration2, uuid2, f2);
        assertRows(execute("SELECT * FROM " + sourceKs + "." + sourceTb),
                   row(1, "1", duration1, uuid1, f1));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb),
                   row(2, "2", duration2, uuid2, f2));

        alterTable("ALTER TABLE " + sourceKs + "." + sourceTb + " DROP f USING TIMESTAMP 20000");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);

        alterTable("ALTER TABLE " + sourceKs + "." + sourceTb + " RENAME b TO bb ");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);

        alterTable("ALTER TABLE " + sourceKs + "." + sourceTb + " WITH compaction = {'class':'LeveledCompactionStrategy', 'sstable_size_in_mb' : 10, 'fanout_size' : 16} ");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);

        execute("INSERT INTO " + sourceKs + "." + sourceTb + " (a, bb, c, e) VALUES (?, ?, ?, ?)", 1, "1", duration1, uuid1);
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a, bb, c, e) VALUES (?, ?, ?, ?)", 2, "2", duration2, uuid2);
        assertRows(execute("SELECT * FROM " + sourceKs + "." + sourceTb),
                   row(1, "1", duration1, uuid1));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb),
                   row(2, "2", duration2, uuid2));
    }

    @Test
    public void testTableOptionsCopy() throws Throwable
    {
        // compression
        String tbCompressionDefault1 = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))");
        String tbCompressionDefault2 = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                             " WITH compression = { 'enabled' : 'false'};");
        String tbCompressionSnappy1 = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                            " WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 };");
        String tbCompressionSnappy2 = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                            " WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32, 'enabled' : true };");
        String tbCompressionSnappy3 = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                            " WITH compression = { 'class' : 'SnappyCompressor', 'min_compress_ratio' : 2 };");
        String tbCompressionSnappy4 = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                            " WITH compression = { 'class' : 'SnappyCompressor', 'min_compress_ratio' : 1 };");
        String tbCompressionSnappy5 = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                            " WITH compression = { 'class' : 'SnappyCompressor', 'min_compress_ratio' : 0 };");

        // memtable
        String tableMemtableSkipList = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                             " WITH memtable = 'skiplist';");
        String tableMemtableTrie = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                         " WITH memtable = 'trie';");
        String tableMemtableDefault = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                            " WITH memtable = 'default';");

        // compaction
        String tableCompactionStcs = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH compaction = {'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 2, 'enabled' : false};");
        String tableCompactionLcs = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH compaction = {'class' : 'LeveledCompactionStrategy', 'sstable_size_in_mb' : 1, 'fanout_size' : 5};");
        String tableCompactionTwcs = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH compaction = {'class' : 'TimeWindowCompactionStrategy', 'min_threshold' : 2};");
        String tableCompactionUcs = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH compaction = {'class' : 'UnifiedCompactionStrategy'};");

        // other options are all different from default
        String tableOtherOptions = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH" +
                                                         " additional_write_policy = '95p' " +
                                                         " AND bloom_filter_fp_chance = 0.1 " +
                                                         " AND caching = {'keys' : 'ALL', 'rows_per_partition' : '100'}" +
                                                         " AND cdc = true " +
                                                         " AND comment = 'test for create like'" +
                                                         " AND crc_check_chance = 0.1" +
                                                         " AND default_time_to_live = 10" +
                                                         " AND compaction = {'class' : 'UnifiedCompactionStrategy'} " +
                                                         " AND compression = {'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 }" +
                                                         " AND gc_grace_seconds = 100" +
                                                         " AND incremental_backups = false" +
                                                         " AND max_index_interval = 1024" +
                                                         " AND min_index_interval = 64" +
                                                         " AND speculative_retry = '95p'" +
                                                         " AND read_repair = 'NONE'" +
                                                         " AND memtable_flush_period_in_ms = 360000" +
                                                         " AND memtable = 'default';");

        String tbLikeCompressionDefault1 = createTableLike("CREATE TABLE %s LIKE %s", tbCompressionDefault1, sourceKs, targetKs);
        String tbLikeCompressionDefault2 = createTableLike("CREATE TABLE %s LIKE %s", tbCompressionDefault2, sourceKs, targetKs);
        String tbLikeCompressionSp1 = createTableLike("CREATE TABLE %s LIKE %s", tbCompressionSnappy1, sourceKs, targetKs);
        String tbLikeCompressionSp2 = createTableLike("CREATE TABLE %s LIKE %s", tbCompressionSnappy2, sourceKs, targetKs);
        String tbLikeCompressionSp3 = createTableLike("CREATE TABLE %s LIKE %s", tbCompressionSnappy3, sourceKs, targetKs);
        String tbLikeCompressionSp4 = createTableLike("CREATE TABLE %s LIKE %s", tbCompressionSnappy4, sourceKs, targetKs);
        String tbLikeCompressionSp5 = createTableLike("CREATE TABLE %s LIKE %s", tbCompressionSnappy5, sourceKs, targetKs);
        String tbLikeMemtableSkipList = createTableLike("CREATE TABLE %s LIKE %s", tableMemtableSkipList, sourceKs, targetKs);
        String tbLikeMemtableTrie = createTableLike("CREATE TABLE %s LIKE %s", tableMemtableTrie, sourceKs, targetKs);
        String tbLikeMemtableDefault = createTableLike("CREATE TABLE %s LIKE %s", tableMemtableDefault, sourceKs, targetKs);
        String tbLikeCompactionStcs = createTableLike("CREATE TABLE %s LIKE %s", tableCompactionStcs, sourceKs, targetKs);
        String tbLikeCompactionLcs = createTableLike("CREATE TABLE %s LIKE %s", tableCompactionLcs, sourceKs, targetKs);
        String tbLikeCompactionTwcs = createTableLike("CREATE TABLE %s LIKE %s", tableCompactionTwcs, sourceKs, targetKs);
        String tbLikeCompactionUcs = createTableLike("CREATE TABLE %s LIKE %s", tableCompactionUcs, sourceKs, targetKs);
        String tbLikeCompactionOthers = createTableLike("CREATE TABLE %s LIKE %s", tableOtherOptions, sourceKs, targetKs);

        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tbCompressionDefault1, tbLikeCompressionDefault1);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tbCompressionDefault2, tbLikeCompressionDefault2);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tbCompressionSnappy1, tbLikeCompressionSp1);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tbCompressionSnappy2, tbLikeCompressionSp2);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tbCompressionSnappy3, tbLikeCompressionSp3);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tbCompressionSnappy4, tbLikeCompressionSp4);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tbCompressionSnappy5, tbLikeCompressionSp5);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tableMemtableSkipList, tbLikeMemtableSkipList);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tableMemtableTrie, tbLikeMemtableTrie);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tableMemtableDefault, tbLikeMemtableDefault);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tableCompactionStcs, tbLikeCompactionStcs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tableCompactionLcs, tbLikeCompactionLcs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tableCompactionTwcs, tbLikeCompactionTwcs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tableCompactionUcs, tbLikeCompactionUcs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tableOtherOptions, tbLikeCompactionOthers);

        // a copy of the table with the table parameters set
        String tableCopyAndSetCompression = createTableLike("CREATE TABLE %s LIKE %s WITH compression = {'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 64 };",
                                                            tbCompressionSnappy1, sourceKs, targetKs);
        String tableCopyAndSetLCSCompaction = createTableLike("CREATE TABLE %s LIKE %s WITH compaction = {'class' : 'LeveledCompactionStrategy', 'sstable_size_in_mb' : 10, 'fanout_size' : 16};",
                                                              tableCompactionLcs, sourceKs, targetKs);
        String tableCopyAndSetAllParams = createTableLike("CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH" +
                                                          " bloom_filter_fp_chance = 0.75 " +
                                                          " AND caching = {'keys' : 'NONE', 'rows_per_partition' : '10'}" +
                                                          " AND cdc = true " +
                                                          " AND comment = 'test for create like and set params'" +
                                                          " AND crc_check_chance = 0.8" +
                                                          " AND default_time_to_live = 100" +
                                                          " AND compaction = {'class' : 'SizeTieredCompactionStrategy'} " +
                                                          " AND compression = {'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 64}" +
                                                          " AND gc_grace_seconds = 1000" +
                                                          " AND incremental_backups = true" +
                                                          " AND max_index_interval = 128" +
                                                          " AND min_index_interval = 16" +
                                                          " AND speculative_retry = '96p'" +
                                                          " AND read_repair = 'NONE'" +
                                                          " AND memtable_flush_period_in_ms = 3600;",
                                                          tableOtherOptions, sourceKs, targetKs);

        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tbCompressionDefault1, tableCopyAndSetCompression, false, false, false);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tableCompactionLcs, tableCopyAndSetLCSCompaction, false, false, false);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, tableOtherOptions, tableCopyAndSetAllParams, false, false, false);
        TableParams paramsSetCompression = getTableMetadata(targetKs, tableCopyAndSetCompression).params;
        TableParams paramsSetLCSCompaction = getTableMetadata(targetKs, tableCopyAndSetLCSCompaction).params;
        TableParams paramsSetAllParams = getTableMetadata(targetKs, tableCopyAndSetAllParams).params;

        assertEquals(paramsSetCompression, TableParams.builder().compression(CompressionParams.snappy(64 * 1024, 0.0)).build());
        assertEquals(paramsSetLCSCompaction, TableParams.builder().compaction(CompactionParams.create(LeveledCompactionStrategy.class,
                                                                                                      Map.of("sstable_size_in_mb", "10",
                                                                                                             "fanout_size", "16")))
                                                        .build());
        assertEquals(paramsSetAllParams, TableParams.builder().bloomFilterFpChance(0.75)
                                                    .caching(new CachingParams(false, 10))
                                                    .cdc(true)
                                                    .comment("test for create like and set params")
                                                    .crcCheckChance(0.8)
                                                    .defaultTimeToLive(100)
                                                    .compaction(CompactionParams.stcs(Collections.emptyMap()))
                                                    .compression(CompressionParams.snappy(64 * 1024, 0.0))
                                                    .gcGraceSeconds(1000)
                                                    .incrementalBackups(true)
                                                    .maxIndexInterval(128)
                                                    .minIndexInterval(16)
                                                    .speculativeRetry(SpeculativeRetryPolicy.fromString("96PERCENTILE"))
                                                    .readRepair(ReadRepairStrategy.NONE)
                                                    .memtableFlushPeriodInMs(3600)
                                                    .build());

        // table id
        TableId id = TableId.generate();
        String tbNormal = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))");
        assertInvalidThrowMessage("Cannot alter table id.", ConfigurationException.class,
                                  "CREATE TABLE " + targetKs + ".targetnormal LIKE " + sourceKs + "." + tbNormal + " WITH ID = " + id);
    }

    @Test
    public void testStaticColumnCopy()
    {
        // create with static column
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int , b int , c int static, d int, e list<text>, PRIMARY KEY(a, b));", "tb1");
        String targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a, b, c, d, e) VALUES (0, 1, 2, 3, ?)", list("1", "2", "3", "4"));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb), row(0, 1, 2, 3, list("1", "2", "3", "4")));

        // add static column
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int, b int, c text, PRIMARY KEY (a, b))");
        alterTable("ALTER TABLE " + sourceKs + "." + sourceTb + " ADD d int static");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);
    }

    @Test
    public void testColumnMaskTableCopy()
    {
        DatabaseDescriptor.setDynamicDataMaskingEnabled(true);
        // masked partition key
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (k int MASKED WITH mask_default() PRIMARY KEY, r int)");
        String targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);

        // masked partition key component
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (k1 int, k2 text MASKED WITH DEFAULT, r int, PRIMARY KEY(k1, k2))");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);

        // masked clustering key
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (k int, c int MASKED WITH mask_default(), r int, PRIMARY KEY (k, c))");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);

        // masked clustering key with reverse order
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (k int, c text MASKED WITH mask_default(), r int, PRIMARY KEY (k, c)) " +
                                         "WITH CLUSTERING ORDER BY (c DESC)");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);

        // masked clustering key component
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (k int, c1 int, c2 text MASKED WITH DEFAULT, r int, PRIMARY KEY (k, c1, c2))");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);

        // masked regular column
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (k int PRIMARY KEY, r1 text MASKED WITH DEFAULT, r2 int)");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);

        // masked static column
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (k int, c int, r int, s int STATIC MASKED WITH DEFAULT, PRIMARY KEY (k, c))");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);

        // multiple masked columns
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (" +
                                         "k1 int, k2 int MASKED WITH DEFAULT, " +
                                         "c1 int, c2 text MASKED WITH DEFAULT, " +
                                         "r1 int, r2 int MASKED WITH DEFAULT, " +
                                         "s1 int static, s2 int static MASKED WITH DEFAULT, " +
                                         "PRIMARY KEY((k1, k2), c1, c2))");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);

        sourceTb = createTable(sourceKs, "CREATE TABLE %s (k int PRIMARY KEY, " +
                                         "s set<int> MASKED WITH DEFAULT, " +
                                         "l list<int> MASKED WITH DEFAULT, " +
                                         "m map<int, int> MASKED WITH DEFAULT, " +
                                         "fs frozen<set<int>> MASKED WITH DEFAULT, " +
                                         "fl frozen<list<int>> MASKED WITH DEFAULT, " +
                                         "fm frozen<map<int, int>> MASKED WITH DEFAULT)");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb);
    }

    @Test
    public void testUDTTableCopy() throws Throwable
    {
        //normal udt
        String udt = createType(sourceKs, "CREATE TYPE %s (a int, b uuid, c text)");
        String udtNew = createType(sourceKs, "CREATE TYPE %s (a int, b text)");
        //collection udt
        String udtSet = createType(sourceKs, "CREATE TYPE %s (a int, c frozen <set<text>>)");
        //frozen udt
        String udtFrozen = createType(sourceKs, "CREATE TYPE %s (a int, c frozen<" + udt + ">)");
        String udtFrozenNotExist = createType(sourceKs, "CREATE TYPE %s (a int, c frozen<" + udtNew + ">)");

        // source table's column's data type is udt, and its subtypes are all native type
        String sourceTbUdt = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b duration, c " + udt + ");");
        // source table's column's data type is udt, and its subtypes are native type and collection type
        String sourceTbUdtSet = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b duration, c " + udtSet + ");");
        // source table's column's data type is udt, and its subtypes are native type and udt
        String sourceTbUdtFrozen = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b duration, c " + udtFrozen + ");");
        // source table's column's data type is udt, and its subtypes are native type and  more than one udt
        String sourceTbUdtComb = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b duration, c " + udtFrozen + ", d " + udt + ");");
        // source table's column's data type is udt, and its subtypes are native type and  more than one udt
        String sourceTbUdtCombNotExist = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b duration, c " + udtFrozen + ", d " + udtFrozenNotExist + ");");

        if (differentKs)
        {
            assertInvalidThrowMessage("UDTs " + udt + " do not exist in target keyspace '" + targetKs + "'.",
                                      InvalidRequestException.class,
                                      "CREATE TABLE " + targetKs + ".tbudt LIKE " + sourceKs + "." + sourceTbUdt);
            assertInvalidThrowMessage("UDTs " + udtSet + " do not exist in target keyspace '" + targetKs + "'.",
                                      InvalidRequestException.class,
                                      "CREATE TABLE " + targetKs + ".tbdtset LIKE " + sourceKs + "." + sourceTbUdtSet);
            assertInvalidThrowMessage(String.format("UDTs %s do not exist in target keyspace '%s'.", Sets.newHashSet(udt, udtFrozen).stream().sorted().collect(Collectors.joining(", ")), targetKs),
                                      InvalidRequestException.class,
                                      "CREATE TABLE " + targetKs + ".tbudtfrozen LIKE " + sourceKs + "." + sourceTbUdtFrozen);
            assertInvalidThrowMessage(String.format("UDTs %s do not exist in target keyspace '%s'.", Sets.newHashSet(udt, udtFrozen).stream().sorted().collect(Collectors.joining(", ")), targetKs),
                                      InvalidRequestException.class,
                                      "CREATE TABLE " + targetKs + ".tbudtfrozen LIKE " + sourceKs + "." + sourceTbUdtFrozen);
            assertInvalidThrowMessage(String.format("UDTs %s do not exist in target keyspace '%s'.", Sets.newHashSet(udt, udtFrozen).stream().sorted().collect(Collectors.joining(", ")), targetKs),
                                      InvalidRequestException.class,
                                      "CREATE TABLE " + targetKs + ".tbudtcomb LIKE " + sourceKs + "." + sourceTbUdtComb);
            assertInvalidThrowMessage(String.format("UDTs %s do not exist in target keyspace '%s'.", Sets.newHashSet(udtNew, udt, udtFrozenNotExist, udtFrozen).stream().sorted().collect(Collectors.joining(", ")), targetKs),
                                      InvalidRequestException.class,
                                      "CREATE TABLE " + targetKs + ".tbudtcomb LIKE " + sourceKs + "." + sourceTbUdtCombNotExist);
            // different keyspaces with udts that have same udt name, different fields
            String udtWithDifferentField = createType(sourceKs, "CREATE TYPE %s (aa int, bb text)");
            createType("CREATE TYPE IF NOT EXISTS " + targetKs + "." + udtWithDifferentField + " (aa int, cc text)");
            String sourceTbDiffUdt = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b duration, c " + udtWithDifferentField + ");");
            assertInvalidThrowMessage("Target keyspace '" + targetKs + "' has same UDT name '" + udtWithDifferentField + "' as source keyspace '" + sourceKs + "' but with different structure.",
                                      InvalidRequestException.class,
                                      "CREATE TABLE " + targetKs + ".tbdiffudt LIKE " + sourceKs + "." + sourceTbDiffUdt);
        }
        else
        {
            // copy table that have udt, and udt's subtype are all native type, target table will create this udt
            String targetTbUdt = createTableLike("CREATE TABLE %s LIKE %s", sourceTbUdt, sourceKs, "tbudt", targetKs);
            // copy table that have udt, and udt's subtype are all native type and collection typ, target table will create this udt
            String targetTbUdtSet = createTableLike("CREATE TABLE %s LIKE %s", sourceTbUdtSet, sourceKs, "tbdtset", targetKs);
            // copy table that have udt, and udt's subtype are all native type and udt, target table will create udt in order
            String targetTbUdtFrozen = createTableLike("CREATE TABLE %s LIKE %s", sourceTbUdtFrozen, sourceKs, "tbudtfrozen", targetKs);
            // copy table that have udt, and udt's subtype are all native type and more than one udt, target table will create udt in order
            String targetTbUdtComb = createTableLike("CREATE TABLE %s LIKE %s", sourceTbUdtComb, sourceKs, "tbudtcomb", targetKs);
            // copy table that have udt, and udt's subtype are all native type and udt, target table will create udt in order
            String targetTbUdtCombNotExist = createTableLike("CREATE TABLE %s LIKE %s", sourceTbUdtCombNotExist, sourceKs, "tbudtcombnotexist", targetKs);

            assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTbUdt, targetTbUdt);
            assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTbUdtSet, targetTbUdtSet);
            assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTbUdtFrozen, targetTbUdtFrozen);
            assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTbUdtComb, targetTbUdtComb);
            assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTbUdtCombNotExist, targetTbUdtCombNotExist);

            // same udt already exist in target ks, the existed udt will be used
            String udtWithSameField = createType(sourceKs, "CREATE TYPE %s (a int, b text)");
            createType("CREATE TYPE IF NOT EXISTS " + targetKs + "." + udtWithSameField + " (a int, b text)");
            String sourceTbSameUdt = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b duration, c " + udtWithSameField + ");");
            String targetTbSameUdt = createTableLike("CREATE TABLE %s LIKE %s", sourceTbSameUdt, sourceKs, "tbsameudt", targetKs);
            assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTbSameUdt, targetTbSameUdt);
        }
    }

    @Test
    public void testIndexOperationOnCopiedTable()
    {
        // copied table can do index creation
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (id text PRIMARY KEY, val text, num int);");
        String targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        String saiIndex = createIndex(targetKs, "CREATE INDEX ON %s(val) USING 'sai'");
        execute("INSERT INTO " + targetKs + "." + targetTb + " (id, val, num) VALUES ('1', 'value', 1)");
        assertEquals(1, execute("SELECT id FROM " + targetKs + "." + targetTb + " WHERE val = 'value'").size());
        String normalIndex = createIndex(targetKs, "CREATE INDEX ON %s(num)");
        Indexes targetIndexes = getTableMetadata(targetKs, targetTb).indexes;
        assertEquals(targetIndexes.size(), 2);
        assertNotNull(targetIndexes.get(saiIndex));
        assertNotNull(targetIndexes.get(normalIndex));
    }

    @Test
    public void testTriggerOperationOnCopiedTable()
    {
        String triggerName = "trigger_1";
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a))");
        String targetTb =  createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        execute("CREATE TRIGGER " + triggerName + " ON " + targetKs + "." + targetTb + " USING '" + CreateTest.TestTrigger.class.getName() + "'");
        assertNotNull(getTableMetadata(targetKs, targetTb).triggers.get(triggerName));
    }

    @Test
    public void testUnSupportedSchema() throws Throwable
    {
        createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b int, c text)", "tb");
        String index = createIndex("CREATE INDEX ON " + sourceKs + ".tb (c)");
        assertInvalidThrowMessage("Souce Table '" + targetKs + "." + index + "' doesn't exist", InvalidRequestException.class,
                                  "CREATE TABLE " + sourceKs + ".newtb LIKE  " + targetKs + "." + index + ";");
        assertInvalidThrowMessage("System keyspace 'system' is not user-modifiable", InvalidRequestException.class,
                                  "CREATE TABLE system.local_clone LIKE system.local ;");
        assertInvalidThrowMessage("System keyspace 'system_views' is not user-modifiable", InvalidRequestException.class,
                                  "CREATE TABLE system_views.newtb LIKE system_views.snapshots ;");
    }

    @Test
    public void testTableCopyWithIndexes()
    {
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b int, c text, d int, e text, f int, g text)", "sourcetb");
        createIndex(sourceKs, "CREATE INDEX ON %s (d)");
        createIndex(sourceKs, "CREATE INDEX ON %s (c)");
        createIndex(sourceKs, "CREATE INDEX ON %s (b) USING 'sai'");
        createIndex(sourceKs, "CREATE CUSTOM INDEX ON %s (e) USING 'storageattachedindex'");
        createIndex(sourceKs, "CREATE CUSTOM INDEX ON %s (f) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        String targetTb = createTableLike("CREATE TABLE %s LIKE %s WITH INDEXES", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb, true, true, true);
    }

    @Test
    public void testTableCopyWithMultiIndexOnSameColumn()
    {
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b int, c text, d int, e text, f int, g text)", "sourcetb");
        createIndex(sourceKs, "CREATE INDEX " + sourceTb + "_b_idx1 ON %s (b) USING 'legacy_local_table'");
        createIndex(sourceKs, "CREATE INDEX " + sourceTb + "_b_idx2 ON %s (b) USING 'sai'");
        String targetTb = createTableLike("CREATE TABLE %s LIKE %s WITH INDEXES", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb, true, true, true);
    }

    @Test
    public void testTableCopyWithOutIndexes()
    {
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b int)", "sourcetb");
        // not copied
        createIndex(sourceKs, "CREATE CUSTOM INDEX ON %s (b) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
        createIndex(sourceKs, "CREATE CUSTOM INDEX testidx ON %s (b) USING '" + StubIndex.class.getName() + "'");
        String targetTb = createTableLike("CREATE TABLE %s LIKE %s WITH indexes", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb, true, false, false);
        assertEquals(2, getTableMetadata(sourceKs, sourceTb).indexes.size());
        assertEquals(0, getTableMetadata(targetKs, targetTb).indexes.size());
    }

    @Test
    public void testManyTableCopyWithIndex()
    {
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b int, c int)", "sourcetb");
        createIndex(sourceKs, "CREATE INDEX myindex ON %s (b) USING 'legacy_local_table'");
        createIndex(sourceKs, "CREATE INDEX myindex_1 ON %s (b) USING 'sai'");
        createIndex(sourceKs, "CREATE INDEX myindex_1_1 ON %s (c) USING 'sai'");
        createIndex(sourceKs, "CREATE INDEX myindex__1 ON %s (c) USING 'legacy_local_table'");
        String targetTb = createTableLike("CREATE TABLE %s LIKE %s WITH indexes", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb, true, false, false);
        Set<String> resultIndexNames = getTableMetadata(targetKs, targetTb).indexes.stream().map(meta -> meta.name).collect(Collectors.toSet());
        Set<String> expectedIndexNames = differentKs ? Sets.newHashSet("myindex", "myindex_1", "myindex_1_1", "myindex__1") :
                                                       Sets.newHashSet("myindex_2", "myindex_3", "myindex_1_2", "myindex__2");
        assertEquals(0, Sets.difference(resultIndexNames, expectedIndexNames).size());
    }

    @Test
    public void testTableCopyWithIndexesAndTableProperty()
    {
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b int, c text, d int)", "sourcetb");
        createIndex(sourceKs, "CREATE INDEX ON %s (b)");

        String targetTbWithAll = createTableLike("CREATE TABLE %s LIKE %s WITH INDEXES AND crc_check_chance = 0.8 AND cdc = true", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTbWithAll, false, true, true);

        TableMetadata source = getTableMetadata(sourceKs, sourceTb);
        TableMetadata target = getTableMetadata(targetKs, targetTbWithAll);
        assertNotEquals(source.params, target.params);
        assertTrue(target.params.cdc);
        assertFalse(source.params.cdc);
        assertEquals(0.8, target.params.crcCheckChance, 0.0);
    }

    @Test
    public void testIndexesNameForCopiedTable()
    {
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b int, c text, d int, e text)", "sourcetb");
        String idx1 = createIndex(sourceKs, "CREATE INDEX ON %s (d)");
        String idx2 = createIndex(sourceKs, "CREATE INDEX ON %s (c)");
        String idx3 = createIndex(sourceKs, "CREATE INDEX ON %s (b) USING 'sai'");
        String idx4 = createIndex(sourceKs, "CREATE INDEX idx_for_e_column ON %s (e) USING 'sai'");
        assertEquals(sourceTb + "_d_idx" , idx1);
        assertEquals(sourceTb + "_c_idx" , idx2);
        assertEquals(sourceTb + "_b_idx" , idx3);
        assertEquals("idx_for_e_column", idx4);
        String targetTb = createTableLike("CREATE TABLE %s LIKE %s WITH INDEXES", sourceTb, sourceKs, targetKs);
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb, true, true, true);
        TableMetadata target = getTableMetadata(targetKs, targetTb);
        assertTrue(target.indexes.has(targetTb + "_d_idx"));
        assertTrue(target.indexes.has(targetTb + "_c_idx"));
        assertTrue(target.indexes.has(targetTb + "_b_idx"));

        if (differentKs)
        {
            assertTrue(target.indexes.has("idx_for_e_column"));
        }
        else
        {
            // if within the same keyspace, the target table will be created with a new
            // index name in the format oldindexname_number, where the number starts from 1.
            assertTrue(target.indexes.has("idx_for_e_column_1"));
        }
    }

    @Test
    public void testTableCopyWithCustomIndexes()
    {
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b int, c text, d int)", "sourcetb");
        String idx = createIndex(sourceKs, "CREATE CUSTOM INDEX testidx ON %s (b) USING '" + StubIndex.class.getName() + "'");
        Set<String> customIndexes = new TreeSet<>(Arrays.asList(idx));
        ResultSet res = executeNet("CREATE TABLE " + targetKs + ".targettbb LIKE " + sourceKs + "." + sourceTb + " WITH INDEXES");
        assertWarningsContain(res.getExecutionInfo().getWarnings(),
                              "Source table " + sourceKs + "." + sourceTb + " to copy indexes from to " + targetKs + ".targettbb has custom indexes. These indexes were not copied: " + customIndexes);
    }

    private void assertTableMetaEqualsWithoutKs(String sourceKs, String targetKs, String sourceTb, String targetTb)
    {
        assertTableMetaEqualsWithoutKs(sourceKs, targetKs, sourceTb, targetTb, true, true, false);
    }

    private void assertTableMetaEqualsWithoutKs(String sourceKs, String targetKs, String sourceTb, String targetTb, boolean compareParams, boolean compareIndexes, boolean compareIndexWithOutName)
    {
        TableMetadata left = getTableMetadata(sourceKs, sourceTb);
        TableMetadata right = getTableMetadata(targetKs, targetTb);
        assertNotNull(left);
        assertNotNull(right);
        assertTrue(equalsWithoutTableNameAndDropCns(left, right, compareParams, compareIndexes, compareIndexWithOutName));
        assertNotEquals(left.id, right.id);
        assertNotEquals(left.name, right.name);
    }
}
