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

package org.apache.cassandra.cql3.validation;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.validation.operations.ThriftCQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.thrift.ConsistencyLevel.ONE;
import static org.junit.Assert.assertEquals;

public class ThriftIntegrationTest extends ThriftCQLTester
{
    final static AtomicInteger seqNumber = new AtomicInteger();
    final String KEYSPACE = "thrift_compact_table_with_supercolumns_test_" + seqNumber.incrementAndGet();

    @Before
    public void setupSuperColumnFamily() throws Throwable
    {
        StorageService.instance.setRpcReady(true);

        final String denseTableName = createTableName();
        final String sparseTableName =  currentSparseTable();
        final String counterTableName = currentCounterTable();

        CfDef cfDef = new CfDef().setColumn_type("Super")
                                 .setSubcomparator_type(Int32Type.instance.toString())
                                 .setComparator_type(AsciiType.instance.toString())
                                 .setDefault_validation_class(AsciiType.instance.toString())
                                 .setKey_validation_class(AsciiType.instance.toString())
                                 .setKeyspace(KEYSPACE)
                                 .setName(denseTableName);

        CfDef sparseCfDef = new CfDef().setColumn_type("Super")
                                       .setComparator_type(AsciiType.instance.toString())
                                       .setSubcomparator_type(AsciiType.instance.toString())
                                       .setKey_validation_class(AsciiType.instance.toString())
                                       .setColumn_metadata(Arrays.asList(new ColumnDef(ByteBufferUtil.bytes("col1"), LongType.instance.toString()),
                                                                         new ColumnDef(ByteBufferUtil.bytes("col2"), LongType.instance.toString())))
                                       .setKeyspace(KEYSPACE)
                                       .setName(sparseTableName);

        CfDef counterCfDef = new CfDef().setColumn_type("Super")
                                        .setSubcomparator_type(AsciiType.instance.toString())
                                        .setComparator_type(AsciiType.instance.toString())
                                        .setDefault_validation_class(CounterColumnType.instance.toString())
                                        .setKey_validation_class(AsciiType.instance.toString())
                                        .setKeyspace(KEYSPACE)
                                        .setName(counterTableName);

        KsDef ksDef = new KsDef(KEYSPACE,
                                SimpleStrategy.class.getName(),
                                Arrays.asList(cfDef, sparseCfDef, counterCfDef));

        ksDef.setStrategy_options(Collections.singletonMap("replication_factor", "1"));

        Cassandra.Client client = getClient();
        client.system_add_keyspace(ksDef);
        client.set_keyspace(KEYSPACE);
    }

    @After
    public void tearDown() throws Throwable
    {
        getClient().send_system_drop_keyspace(KEYSPACE);
    }

    @Test
    public void testCounterTableReads() throws Throwable
    {
        populateCounterTable();
        beforeAndAfterFlush(this::testCounterTableReadsInternal);
    }

    private void testCounterTableReadsInternal() throws Throwable
    {
        UntypedResultSet resultSet = execute(String.format("select * from %s.%s", KEYSPACE, currentCounterTable()));
        assertRows(resultSet,
                   row("key1", "ck1", "counter1", 10L),
                   row("key1", "ck1", "counter2", 5L),
                   row("key2", "ck1", "counter1", 10L),
                   row("key2", "ck1", "counter2", 5L));
    }

    @Test
    public void testCounterTableThriftUpdates() throws Throwable
    {
        populateCounterTable();

        Cassandra.Client client = getClient();
        Mutation mutation = new Mutation();
        ColumnOrSuperColumn csoc = new ColumnOrSuperColumn();
        csoc.setCounter_super_column(new CounterSuperColumn(ByteBufferUtil.bytes("ck1"),
                                                            Arrays.asList(new CounterColumn(ByteBufferUtil.bytes("counter1"), 1))));
        mutation.setColumn_or_supercolumn(csoc);

        Mutation mutation2 = new Mutation();
        ColumnOrSuperColumn csoc2 = new ColumnOrSuperColumn();
        csoc2.setCounter_super_column(new CounterSuperColumn(ByteBufferUtil.bytes("ck1"),
                                                             Arrays.asList(new CounterColumn(ByteBufferUtil.bytes("counter1"), 100))));
        mutation2.setColumn_or_supercolumn(csoc2);
        client.batch_mutate(Collections.singletonMap(ByteBufferUtil.bytes("key1"),
                                                     Collections.singletonMap(currentCounterTable(), Arrays.asList(mutation))),
                            ONE);
        client.batch_mutate(Collections.singletonMap(ByteBufferUtil.bytes("key2"),
                                                     Collections.singletonMap(currentCounterTable(), Arrays.asList(mutation2))),
                            ONE);

        beforeAndAfterFlush(() -> {
            UntypedResultSet resultSet = execute(String.format("select * from %s.%s", KEYSPACE, currentCounterTable()));
            assertRows(resultSet,
                       row("key1", "ck1", "counter1", 11L),
                       row("key1", "ck1", "counter2", 5L),
                       row("key2", "ck1", "counter1", 110L),
                       row("key2", "ck1", "counter2", 5L));
        });
    }

    @Test
    public void testCounterTableCqlUpdates() throws Throwable
    {
        populateCounterTable();

        execute(String.format("UPDATE %s.%s set value = value + 1 WHERE key = ? AND column1 = ? AND column2 = ?", KEYSPACE, currentCounterTable()),
                "key1", "ck1", "counter1");
        execute(String.format("UPDATE %s.%s set value = value + 100 WHERE key = 'key2' AND column1 = 'ck1' AND column2 = 'counter1'", KEYSPACE, currentCounterTable()));

        execute(String.format("UPDATE %s.%s set value = value - ? WHERE key = 'key1' AND column1 = 'ck1' AND column2 = 'counter2'", KEYSPACE, currentCounterTable()), 2L);
        execute(String.format("UPDATE %s.%s set value = value - ? WHERE key = 'key2' AND column1 = 'ck1' AND column2 = 'counter2'", KEYSPACE, currentCounterTable()), 100L);

        beforeAndAfterFlush(() -> {
            UntypedResultSet resultSet = execute(String.format("select * from %s.%s", KEYSPACE, currentCounterTable()));
            assertRows(resultSet,
                       row("key1", "ck1", "counter1", 11L),
                       row("key1", "ck1", "counter2", 3L),
                       row("key2", "ck1", "counter1", 110L),
                       row("key2", "ck1", "counter2", -95L));
        });
    }

    @Test
    public void testCounterTableCqlDeletes() throws Throwable
    {
        populateCounterTable();

        assertRows(execute(String.format("select * from %s.%s", KEYSPACE, currentCounterTable())),
                   row("key1", "ck1", "counter1", 10L),
                   row("key1", "ck1", "counter2", 5L),
                   row("key2", "ck1", "counter1", 10L),
                   row("key2", "ck1", "counter2", 5L));

        execute(String.format("DELETE value FROM %s.%s WHERE key = ? AND column1 = ? AND column2 = ?", KEYSPACE, currentCounterTable()),
                "key1", "ck1", "counter1");

        assertRows(execute(String.format("select * from %s.%s", KEYSPACE, currentCounterTable())),
                   row("key1", "ck1", "counter2", 5L),
                   row("key2", "ck1", "counter1", 10L),
                   row("key2", "ck1", "counter2", 5L));

        execute(String.format("DELETE FROM %s.%s WHERE key = ? AND column1 = ?", KEYSPACE, currentCounterTable()),
                "key1", "ck1");

        assertRows(execute(String.format("select * from %s.%s", KEYSPACE, currentCounterTable())),
                   row("key2", "ck1", "counter1", 10L),
                   row("key2", "ck1", "counter2", 5L));

        execute(String.format("DELETE FROM %s.%s WHERE key = ?", KEYSPACE, currentCounterTable()),
                "key2");

        assertEmpty(execute(String.format("select * from %s.%s", KEYSPACE, currentCounterTable())));
    }

    @Test
    public void testDenseTableAlter() throws Throwable
    {
        populateDenseTable();

        alterTable(String.format("ALTER TABLE %s.%s RENAME column1 TO renamed_column1", KEYSPACE, currentDenseTable()));
        alterTable(String.format("ALTER TABLE %s.%s RENAME column2 TO renamed_column2", KEYSPACE, currentDenseTable()));
        alterTable(String.format("ALTER TABLE %s.%s RENAME key TO renamed_key", KEYSPACE, currentDenseTable()));
        alterTable(String.format("ALTER TABLE %s.%s RENAME value TO renamed_value", KEYSPACE, currentDenseTable()));

        beforeAndAfterFlush(() -> {
            UntypedResultSet resultSet = execute(String.format("select * from %s.%s", KEYSPACE, currentDenseTable()));
            assertEquals("renamed_key", resultSet.metadata().get(0).name.toString());
            assertEquals("renamed_column1", resultSet.metadata().get(1).name.toString());
                                assertEquals("renamed_column2", resultSet.metadata().get(2).name.toString());
                                assertEquals("renamed_value", resultSet.metadata().get(3).name.toString());
            assertRows(resultSet,
                       row("key1", "val1", 1, "value1"),
                       row("key1", "val1", 2, "value2"),
                       row("key1", "val2", 4, "value4"),
                       row("key1", "val2", 5, "value5"),
                       row("key2", "val1", 1, "value1"),
                       row("key2", "val1", 2, "value2"),
                       row("key2", "val2", 4, "value4"),
                       row("key2", "val2", 5, "value5"));
        });
    }

    @Test
    public void testDenseTableReads() throws Throwable
    {
        populateDenseTable();
        beforeAndAfterFlush(this::testDenseTableReadsInternal);
    }

    private void testDenseTableReadsInternal() throws Throwable
    {
        UntypedResultSet resultSet = execute(String.format("select * from %s.%s", KEYSPACE, currentDenseTable()));
        assertEquals("key", resultSet.metadata().get(0).name.toString());
        assertEquals("column1", resultSet.metadata().get(1).name.toString());
        assertEquals("column2", resultSet.metadata().get(2).name.toString());
        assertEquals("value", resultSet.metadata().get(3).name.toString());


        assertRows(resultSet,
                   row("key1", "val1", 1, "value1"),
                   row("key1", "val1", 2, "value2"),
                   row("key1", "val2", 4, "value4"),
                   row("key1", "val2", 5, "value5"),
                   row("key2", "val1", 1, "value1"),
                   row("key2", "val1", 2, "value2"),
                   row("key2", "val2", 4, "value4"),
                   row("key2", "val2", 5, "value5"));

        assertRows(execute(String.format("select * from %s.%s LIMIT 5", KEYSPACE, currentDenseTable())),
                   row("key1", "val1", 1, "value1"),
                   row("key1", "val1", 2, "value2"),
                   row("key1", "val2", 4, "value4"),
                   row("key1", "val2", 5, "value5"),
                   row("key2", "val1", 1, "value1"));

        assertRows(execute(String.format("select value, column2, column1, key from %s.%s", KEYSPACE, currentDenseTable())),
                   row("value1", 1, "val1", "key1"),
                   row("value2", 2, "val1", "key1"),
                   row("value4", 4, "val2", "key1"),
                   row("value5", 5, "val2", "key1"),
                   row("value1", 1, "val1", "key2"),
                   row("value2", 2, "val1", "key2"),
                   row("value4", 4, "val2", "key2"),
                   row("value5", 5, "val2", "key2"));

        assertRows(execute(String.format("select * from %s.%s WHERE key = ? AND column1 = ?", KEYSPACE, currentDenseTable()), "key1", "val2"),
                   row("key1", "val2", 4, "value4"),
                   row("key1", "val2", 5, "value5"));

        assertRows(execute(String.format("select * from %s.%s where key IN ('key1', 'key2') and column1 = 'val1' and column2 = 2", KEYSPACE, currentDenseTable())),
                   row("key1", "val1", 2, "value2"),
                   row("key2", "val1", 2, "value2"));
        assertRows(execute(String.format("select * from %s.%s where key IN ('key1', 'key2') and column1 = 'val1' and column2 > 1", KEYSPACE, currentDenseTable())),
                   row("key1", "val1", 2, "value2"),
                   row("key2", "val1", 2, "value2"));
        assertRows(execute(String.format("select * from %s.%s where key IN ('key1', 'key2') and column1 = 'val1' and column2 >= 2", KEYSPACE, currentDenseTable())),
                   row("key1", "val1", 2, "value2"),
                   row("key2", "val1", 2, "value2"));
        assertEmpty(execute(String.format("select * from %s.%s where key IN ('key1', 'key2') and column1 = 'val1' and column2 > 2", KEYSPACE, currentDenseTable())));

        assertRows(execute(String.format("select column2, key from %s.%s WHERE key = ? AND column1 = ? and column2 = 5", KEYSPACE, currentDenseTable()), "key1", "val2"),
                   row(5, "key1"));
        assertRows(execute(String.format("select * from %s.%s WHERE key = ? AND column1 = ? and column2 >= ?", KEYSPACE, currentDenseTable()), "key1", "val2", 5),
                   row("key1", "val2", 5, "value5"));
        assertRows(execute(String.format("select * from %s.%s WHERE key = ? AND column1 = ? and column2 > ?", KEYSPACE, currentDenseTable()), "key1", "val2", 4),
                   row("key1", "val2", 5, "value5"));
        assertRows(execute(String.format("select * from %s.%s WHERE key = ? AND column1 = ? and column2 < ?", KEYSPACE, currentDenseTable()), "key1", "val2", 5),
                   row("key1", "val2", 4, "value4"));
        assertRows(execute(String.format("select * from %s.%s WHERE key = ? AND column1 = ? and column2 <= ?", KEYSPACE, currentDenseTable()), "key1", "val2", 5),
                   row("key1", "val2", 4, "value4"),
                   row("key1", "val2", 5, "value5"));

        assertRows(execute(String.format("select * from %s.%s where key = 'key1' and column1 in ('val1', 'val2') and column2 IN (1, 4)", KEYSPACE, currentDenseTable())),
                   row("key1", "val1", 1, "value1"),
                   row("key1", "val2", 4, "value4"));

        assertRows(execute(String.format("select * from %s.%s where key = 'key1' and column1 in ('val1', 'val2')", KEYSPACE, currentDenseTable())),
                   row("key1", "val1", 1, "value1"),
                   row("key1", "val1", 2, "value2"),
                   row("key1", "val2", 4, "value4"),
                   row("key1", "val2", 5, "value5"));

        assertRows(execute(String.format("select * from %s.%s where key = 'key1' and column1 in ('val1', 'val2') and column2 = 1", KEYSPACE, currentDenseTable())),
                   row("key1", "val1", 1, "value1"));

        assertRows(execute(String.format("select * from %s.%s where key = 'key1' and (column1, column2) = ('val2', 4)", KEYSPACE, currentDenseTable())),
                   row("key1", "val2", 4, "value4"));

        assertRows(execute(String.format("select * from %s.%s where key = 'key1' and (column1, column2) >= ('val2', 4)", KEYSPACE, currentDenseTable())),
                   row("key1", "val2", 4, "value4"),
                   row("key1", "val2", 5, "value5"));

        assertRows(execute(String.format("select * from %s.%s where key = 'key1' and (column1, column2) > ('val1', 1)", KEYSPACE, currentDenseTable())),
                   row("key1", "val1", 2, "value2"),
                   row("key1", "val2", 4, "value4"),
                   row("key1", "val2", 5, "value5"));

        assertRows(execute(String.format("select * from %s.%s where key = 'key1' and (column1, column2) > ('val2', 1)", KEYSPACE, currentDenseTable())),
                   row("key1", "val2", 4, "value4"),
                   row("key1", "val2", 5, "value5"));

        resultSet = execute(String.format("select key as a, column1 as b, column2 as c, value as d " +
                                          "from %s.%s WHERE key = ? AND column1 = ?", KEYSPACE, currentDenseTable()), "key1", "val2");
        assertRows(resultSet,
                   row("key1", "val2", 4, "value4"),
                   row("key1", "val2", 5, "value5"));
        assertEquals(resultSet.metadata().get(2).type, Int32Type.instance);
        assertEquals(resultSet.metadata().get(3).type, AsciiType.instance);

        assertRows(execute(String.format("select column2, value from %s.%s WHERE key = ? AND column1 = ?", KEYSPACE, currentDenseTable()), "key1", "val2"),
                   row(4, "value4"),
                   row(5, "value5"));

        assertRows(execute(String.format("select column1, value from %s.%s WHERE key = ? AND column1 = ?", KEYSPACE, currentDenseTable()), "key2", "val1"),
                   row("val1", "value1"),
                   row("val1", "value2"));

        assertInvalidMessage("Secondary indexes are not supported on COMPACT STORAGE tables that have clustering columns",
                             String.format("CREATE INDEX ON %s.%s (column2)", KEYSPACE, currentDenseTable()));
        assertInvalidMessage("Secondary indexes are not supported on COMPACT STORAGE tables that have clustering columns",
                             String.format("CREATE INDEX ON %s.%s (value)", KEYSPACE, currentDenseTable()));

        assertRows(execute(String.format("SELECT JSON * FROM %s.%s WHERE key = ? AND column1 = ?", KEYSPACE, currentDenseTable()), "key1", "val2"),
                   row("{\"key\": \"key1\", \"column1\": \"val2\", \"column2\": 4, \"value\": \"value4\"}"),
                   row("{\"key\": \"key1\", \"column1\": \"val2\", \"column2\": 5, \"value\": \"value5\"}"));
    }

    @Test
    public void testDenseTablePartialCqlInserts() throws Throwable
    {
        assertInvalidMessage("Column value is mandatory for SuperColumn tables",
                             String.format("INSERT INTO %s.%s (key, column1, column2) VALUES ('key1', 'val1', 1)", KEYSPACE, currentDenseTable()));

        // That's slightly different from 2.X, since null map keys are not allowed
        assertInvalidMessage("Column key is mandatory for SuperColumn tables",
                             String.format("INSERT INTO %s.%s (key, column1, value) VALUES ('key1', 'val1', 'value1')", KEYSPACE, currentDenseTable()));

        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key1', 'val1', 1, NULL)", KEYSPACE, currentDenseTable()));
        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key1', 'val1', 1, ?)", KEYSPACE, currentDenseTable()), unset());
        assertEmpty(execute(String.format("select * from %s.%s", KEYSPACE, currentDenseTable())));
    }

    @Test
    public void testDenseTableCqlInserts() throws Throwable
    {
        Cassandra.Client client = getClient();

        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES (?, ?, ?, ?)", KEYSPACE, currentDenseTable()),
                "key1", "val1", 1, "value1");
        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES (?, ?, 2, ?)", KEYSPACE, currentDenseTable()),
                "key1", "val1", "value2");
        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key1', 'val2', 4, 'value4')", KEYSPACE, currentDenseTable()));
        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key1', 'val2', 5, 'value5')", KEYSPACE, currentDenseTable()));

        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key2', 'val1', 1, 'value1')", KEYSPACE, currentDenseTable()));
        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key2', 'val1', 2, 'value2')", KEYSPACE, currentDenseTable()));
        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key2', 'val2', 4, 'value4')", KEYSPACE, currentDenseTable()));
        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key2', 'val2', 5, 'value5')", KEYSPACE, currentDenseTable()));

        ColumnPath path = new ColumnPath(currentDenseTable());
        path.setSuper_column(ByteBufferUtil.bytes("val1"));

        ColumnOrSuperColumn cosc = client.get(ByteBufferUtil.bytes("key1"), path, ONE);
        assertEquals(cosc.getSuper_column().columns.get(0).name, ByteBufferUtil.bytes(1));
        assertEquals(cosc.getSuper_column().columns.get(0).value, ByteBufferUtil.bytes("value1"));
        assertEquals(cosc.getSuper_column().columns.get(1).name, ByteBufferUtil.bytes(2));
        assertEquals(cosc.getSuper_column().columns.get(1).value, ByteBufferUtil.bytes("value2"));
    }

    @Test
    public void testDenseTableCqlUpdates() throws Throwable
    {
        assertInvalidMessage("Column key is mandatory for SuperColumn tables",
                             String.format("UPDATE %s.%s SET column2 = 1, value = 'value1' WHERE key = 'key1' AND column1 = 'val1'", KEYSPACE, currentDenseTable()));
        assertInvalidMessage("Column `column2` of type `int` found in SET part",
                             String.format("UPDATE %s.%s SET column2 = 1, value = 'value1' WHERE key = 'key1' AND column1 = 'val1' AND column2 = 1", KEYSPACE, currentDenseTable()));
        assertInvalidMessage("Some clustering keys are missing: column1",
                             String.format("UPDATE %s.%s SET value = 'value1' WHERE key = 'key1' AND column2 = 1", KEYSPACE, currentDenseTable()));

        execute(String.format("UPDATE %s.%s SET value = 'value1' WHERE key = 'key1' AND column1 = 'val1' AND column2 = 1", KEYSPACE, currentDenseTable()));
        execute(String.format("UPDATE %s.%s SET value = 'value2' WHERE key = 'key1' AND column1 = 'val1' AND column2 = 2", KEYSPACE, currentDenseTable()));

        execute(String.format("UPDATE %s.%s SET value = ? WHERE key = ? AND column1 = ? AND column2 = ?", KEYSPACE, currentDenseTable()),
                "value1", "key2", "val2", 1);
        execute(String.format("UPDATE %s.%s SET value = 'value2' WHERE key = 'key2' AND column1 = ? AND column2 = ?", KEYSPACE, currentDenseTable()),
                "val2", 2);

        Cassandra.Client client = getClient();
        ColumnPath path = new ColumnPath(currentDenseTable());
        path.setSuper_column(ByteBufferUtil.bytes("val1"));

        ColumnOrSuperColumn cosc = client.get(ByteBufferUtil.bytes("key1"), path, ONE);
        assertEquals(cosc.getSuper_column().columns.get(0).name, ByteBufferUtil.bytes(1));
        assertEquals(cosc.getSuper_column().columns.get(0).value, ByteBufferUtil.bytes("value1"));
        assertEquals(cosc.getSuper_column().columns.get(1).name, ByteBufferUtil.bytes(2));
        assertEquals(cosc.getSuper_column().columns.get(1).value, ByteBufferUtil.bytes("value2"));

        path = new ColumnPath(currentDenseTable());
        path.setSuper_column(ByteBufferUtil.bytes("val2"));

        cosc = client.get(ByteBufferUtil.bytes("key2"), path, ONE);
        assertEquals(cosc.getSuper_column().columns.get(0).name, ByteBufferUtil.bytes(1));
        assertEquals(cosc.getSuper_column().columns.get(0).value, ByteBufferUtil.bytes("value1"));
        assertEquals(cosc.getSuper_column().columns.get(1).name, ByteBufferUtil.bytes(2));
        assertEquals(cosc.getSuper_column().columns.get(1).value, ByteBufferUtil.bytes("value2"));
    }


    @Test
    public void testDenseTableCqlDeletes() throws Throwable
    {
        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key1', 'val1', 1, 'value1')", KEYSPACE, currentDenseTable()));
        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key1', 'val1', 2, 'value2')", KEYSPACE, currentDenseTable()));
        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key1', 'val2', 4, 'value4')", KEYSPACE, currentDenseTable()));
        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key1', 'val2', 5, 'value5')", KEYSPACE, currentDenseTable()));

        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key2', 'val1', 1, 'value1')", KEYSPACE, currentDenseTable()));
        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key2', 'val1', 2, 'value2')", KEYSPACE, currentDenseTable()));
        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key2', 'val2', 4, 'value4')", KEYSPACE, currentDenseTable()));
        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key2', 'val2', 5, 'value5')", KEYSPACE, currentDenseTable()));

        execute(String.format("DELETE FROM %s.%s WHERE key = 'key1' AND column1 = 'val2' AND column2 = 5", KEYSPACE, currentDenseTable()));
        assertRows(execute(String.format("SELECT * FROM %s.%s WHERE key = 'key1'", KEYSPACE, currentDenseTable())),
                   row("key1", "val1", 1, "value1"),
                   row("key1", "val1", 2, "value2"),
                   row("key1", "val2", 4, "value4"));
        execute(String.format("DELETE FROM %s.%s WHERE key = 'key1' AND column1 = 'val2'", KEYSPACE, currentDenseTable()));
        assertRows(execute(String.format("SELECT * FROM %s.%s WHERE key = 'key1'", KEYSPACE, currentDenseTable())),
                   row("key1", "val1", 1, "value1"),
                   row("key1", "val1", 2, "value2"));
        execute(String.format("DELETE FROM %s.%s WHERE key = 'key1'", KEYSPACE, currentDenseTable()));
        assertEmpty(execute(String.format("SELECT * FROM %s.%s WHERE key = 'key1'", KEYSPACE, currentDenseTable())));

        Cassandra.Client client = getClient();

        Mutation mutation1 = new Mutation();
        SlicePredicate slicePredicate = new SlicePredicate();
        slicePredicate.setSlice_range(new SliceRange(ByteBufferUtil.bytes("val1"), ByteBufferUtil.bytes("val1"), false, 1));
        Deletion deletion1 = new Deletion();
        deletion1.setTimestamp(FBUtilities.timestampMicros());
        deletion1.setPredicate(slicePredicate);
        mutation1.setDeletion(deletion1);
        client.batch_mutate(Collections.singletonMap(ByteBufferUtil.bytes("key2"),
                                                     Collections.singletonMap(currentDenseTable(), Arrays.asList(mutation1))),
                            ONE);
        assertRows(execute(String.format("SELECT * FROM %s.%s WHERE key = 'key2'", KEYSPACE, currentDenseTable())),
                   row("key2", "val2", 4, "value4"),
                   row("key2", "val2", 5, "value5"));

        Mutation mutation2 = new Mutation();
        Deletion deletion2 = new Deletion();
        deletion2.setTimestamp(FBUtilities.timestampMicros());
        deletion2.setSuper_column(ByteBufferUtil.bytes("val2"));
        mutation2.setDeletion(deletion2);
        client.batch_mutate(Collections.singletonMap(ByteBufferUtil.bytes("key2"),
                                                     Collections.singletonMap(currentDenseTable(), Arrays.asList(mutation2))),
                            ONE);

        assertEmpty(execute(String.format("SELECT * FROM %s.%s WHERE key = 'key2'", KEYSPACE, currentDenseTable())));

        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key1', 'val1', 1, 'value1')", KEYSPACE, currentDenseTable()));
        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key2', 'val1', 1, 'value1')", KEYSPACE, currentDenseTable()));
        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES ('key3', 'val1', 1, 'value1')", KEYSPACE, currentDenseTable()));

        execute(String.format("DELETE FROM %s.%s WHERE key IN ('key1', 'key2')", KEYSPACE, currentDenseTable()));
        assertRows(execute(String.format("SELECT * FROM %s.%s", KEYSPACE, currentDenseTable())),
                   row("key3", "val1", 1, "value1"));

        assertInvalidMessage("Multi-column relations cannot be used in WHERE clauses for UPDATE and DELETE statements",
                             String.format("DELETE FROM %s.%s WHERE key = 'key3' AND (column1, column2) = ('val1', 1)", KEYSPACE, currentDenseTable()));

        assertInvalidMessage("Token relations cannot be used in WHERE clauses for UPDATE and DELETE statements: token(key) > token('key3')",
                             String.format("DELETE FROM %s.%s WHERE token(key) > token('key3')", KEYSPACE, currentDenseTable()));
    }

    @Test
    public void testSparseTableAlter() throws Throwable
    {
        populateSparseTable();

        alterTable(String.format("ALTER TABLE %s.%s RENAME column1 TO renamed_column1", KEYSPACE, currentSparseTable()));
        alterTable(String.format("ALTER TABLE %s.%s RENAME key TO renamed_key", KEYSPACE, currentSparseTable()));
        assertInvalidMessage("Cannot rename non PRIMARY KEY part col1",
                             String.format("ALTER TABLE %s.%s RENAME col1 TO renamed_col1", KEYSPACE, currentSparseTable()));
        assertInvalidMessage("Cannot rename non PRIMARY KEY part col2",
                             String.format("ALTER TABLE %s.%s RENAME col2 TO renamed_col2", KEYSPACE, currentSparseTable()));
        assertInvalidMessage("Cannot rename unknown column column2 in keyspace",
                             String.format("ALTER TABLE %s.%s RENAME column2 TO renamed_column2", KEYSPACE, currentSparseTable()));
        assertInvalidMessage("Cannot rename unknown column value in keyspace",
                             String.format("ALTER TABLE %s.%s RENAME value TO renamed_value", KEYSPACE, currentSparseTable()));


        UntypedResultSet resultSet = execute(String.format("select * from %s.%s", KEYSPACE, currentSparseTable()));
        assertEquals("renamed_key", resultSet.metadata().get(0).name.toString());
        assertEquals("renamed_column1", resultSet.metadata().get(1).name.toString());
        assertEquals("col1", resultSet.metadata().get(2).name.toString());
        assertEquals("col2", resultSet.metadata().get(3).name.toString());

        assertRows(resultSet,
                   row("key1", "val1", 3L, 4L),
                   row("key1", "val2", 3L, 4L),
                   row("key2", "val1", 3L, 4L),
                   row("key2", "val2", 3L, 4L));
    }

    @Test
    public void testSparseTableCqlReads() throws Throwable
    {
        populateSparseTable();
        beforeAndAfterFlush(this::testSparseTableCqlReadsInternal);
    }

    private void testSparseTableCqlReadsInternal() throws Throwable
    {
        UntypedResultSet resultSet = execute(String.format("select * from %s.%s", KEYSPACE, currentSparseTable()));
        assertEquals("key", resultSet.metadata().get(0).name.toString());
        assertEquals("column1", resultSet.metadata().get(1).name.toString());
        assertEquals("col1", resultSet.metadata().get(2).name.toString());
        assertEquals("col2", resultSet.metadata().get(3).name.toString());

        assertRows(resultSet,
                   row("key1", "val1", 3L, 4L),
                   row("key1", "val2", 3L, 4L),
                   row("key2", "val1", 3L, 4L),
                   row("key2", "val2", 3L, 4L));

        assertRows(execute(String.format("select col1, col2, column1, key from %s.%s", KEYSPACE, currentSparseTable())),
                   row(3L, 4L, "val1", "key1"),
                   row(3L, 4L, "val2", "key1"),
                   row(3L, 4L, "val1", "key2"),
                   row(3L, 4L, "val2", "key2"));

        assertInvalidMessage("Undefined column name value",
                             String.format("select value from %s.%s", KEYSPACE, currentSparseTable()));

        assertRows(execute(String.format("select * from %s.%s WHERE key = ? AND column1 = ?", KEYSPACE, currentSparseTable()), "key1", "val2"),
                   row("key1", "val2", 3L, 4L));

        resultSet = execute(String.format("select col1 as a, col2 as b, column1 as c, key as d from %s.%s WHERE key = ? AND column1 = ?", KEYSPACE, currentSparseTable()), "key1", "val2");
        assertRows(resultSet,
                   row(3L, 4L, "val2", "key1"));
        assertEquals(resultSet.metadata().get(0).name, ColumnIdentifier.getInterned("a", true));
        assertEquals(resultSet.metadata().get(1).name, ColumnIdentifier.getInterned("b", true));
        assertEquals(resultSet.metadata().get(2).name, ColumnIdentifier.getInterned("c", true));
        assertEquals(resultSet.metadata().get(3).name, ColumnIdentifier.getInterned("d", true));

        assertRows(execute(String.format("select col1, col2 from %s.%s WHERE key = ? AND column1 = ?", KEYSPACE, currentSparseTable()), "key1", "val2"),
                   row(3L, 4L));

        assertInvalidMessage("Secondary indexes are not supported on COMPACT STORAGE tables that have clustering columns",
                             String.format("CREATE INDEX ON %s.%s (column1)", KEYSPACE, currentSparseTable()));
        assertInvalidMessage("Secondary indexes are not supported on COMPACT STORAGE tables that have clustering columns",
                             String.format("CREATE INDEX ON %s.%s (col1)", KEYSPACE, currentSparseTable()));

        assertRows(execute(String.format("SELECT JSON * FROM %s.%s WHERE key = ? AND column1 = ?", KEYSPACE, currentSparseTable()), "key1", "val2"),
                   row("{\"key\": \"key1\", \"column1\": \"val2\", \"col1\": 3, \"col2\": 4}"));
    }

    @Test
    public void testSparseTableCqlInserts() throws Throwable
    {
        execute(String.format("insert into %s.%s (key, column1, col1, col2) values ('key1', 'val1', 1, 2)", KEYSPACE, currentSparseTable()));
        execute(String.format("insert into %s.%s (key, column1, col1, col2) values ('key1', 'val2', 3, 4)", KEYSPACE, currentSparseTable()));
        execute(String.format("insert into %s.%s (key, column1, col1, col2) values ('key2', 'val1', 5, 6)", KEYSPACE, currentSparseTable()));
        execute(String.format("insert into %s.%s (key, column1, col1, col2) values ('key2', 'val2', 7, 8)", KEYSPACE, currentSparseTable()));

        assertRows(execute(String.format("SELECT * FROM %s.%s", KEYSPACE, currentSparseTable())),
                   row("key1", "val1", 1L, 2L),
                   row("key1", "val2", 3L, 4L),
                   row("key2", "val1", 5L, 6L),
                   row("key2", "val2", 7L, 8L));

        execute(String.format("truncate %s.%s", KEYSPACE, currentSparseTable()));

        execute(String.format("insert into %s.%s (key, column1) values ('key1', 'val1')", KEYSPACE, currentSparseTable()));
        assertRows(execute(String.format("select * from %s.%s", KEYSPACE, currentSparseTable())));

        execute(String.format("insert into %s.%s (key, column1, col1) values ('key1', 'val1', 1)", KEYSPACE, currentSparseTable()));
        execute(String.format("insert into %s.%s (key, column1, col2) values ('key1', 'val1', 2)", KEYSPACE, currentSparseTable()));
        assertRows(execute(String.format("select * from %s.%s", KEYSPACE, currentSparseTable())),
                   row("key1", "val1", 1L, 2L));

        Cassandra.Client client = getClient();
        ColumnPath path = new ColumnPath(currentSparseTable());
        path.setSuper_column(ByteBufferUtil.bytes("val1"));

        ColumnOrSuperColumn cosc = client.get(ByteBufferUtil.bytes("key1"), path, ONE);
        assertEquals(cosc.getSuper_column().columns.get(0).value, ByteBufferUtil.bytes(1L));
        assertEquals(cosc.getSuper_column().columns.get(0).name, ByteBufferUtil.bytes("col1"));
        assertEquals(cosc.getSuper_column().columns.get(1).value, ByteBufferUtil.bytes(2L));
        assertEquals(cosc.getSuper_column().columns.get(1).name, ByteBufferUtil.bytes("col2"));
    }

    @Test
    public void testSparseTableCqlUpdates() throws Throwable
    {
        execute(String.format("UPDATE %s.%s set col1 = 1, col2 = 2 WHERE key = 'key1' AND column1 = 'val1'", KEYSPACE, currentSparseTable()));
        execute(String.format("UPDATE %s.%s set col1 = 3, col2 = 4 WHERE key = 'key1' AND column1 = 'val2'", KEYSPACE, currentSparseTable()));
        execute(String.format("UPDATE %s.%s set col1 = 5, col2 = 6 WHERE key = 'key2' AND column1 = 'val1'", KEYSPACE, currentSparseTable()));
        execute(String.format("UPDATE %s.%s set col1 = 7, col2 = 8 WHERE key = 'key2' AND column1 = 'val2'", KEYSPACE, currentSparseTable()));

        assertRows(execute(String.format("SELECT * FROM %s.%s", KEYSPACE, currentSparseTable())),
                   row("key1", "val1", 1L, 2L),
                   row("key1", "val2", 3L, 4L),
                   row("key2", "val1", 5L, 6L),
                   row("key2", "val2", 7L, 8L));

        Cassandra.Client client = getClient();
        ColumnPath path = new ColumnPath(currentSparseTable());
        path.setSuper_column(ByteBufferUtil.bytes("val1"));

        ColumnOrSuperColumn cosc = client.get(ByteBufferUtil.bytes("key1"), path, ONE);
        assertEquals(cosc.getSuper_column().columns.get(0).value, ByteBufferUtil.bytes(1L));
        assertEquals(cosc.getSuper_column().columns.get(0).name, ByteBufferUtil.bytes("col1"));
        assertEquals(cosc.getSuper_column().columns.get(1).value, ByteBufferUtil.bytes(2L));
        assertEquals(cosc.getSuper_column().columns.get(1).name, ByteBufferUtil.bytes("col2"));
    }

    @Test
    public void testSparseTableCqlDeletes() throws Throwable
    {
        execute(String.format("insert into %s.%s (key, column1, col1, col2) values ('key1', 'val1', 1, 2)", KEYSPACE, currentSparseTable()));
        execute(String.format("insert into %s.%s (key, column1, col1, col2) values ('key1', 'val2', 3, 4)", KEYSPACE, currentSparseTable()));
        execute(String.format("insert into %s.%s (key, column1, col1, col2) values ('key2', 'val1', 5, 6)", KEYSPACE, currentSparseTable()));
        execute(String.format("insert into %s.%s (key, column1, col1, col2) values ('key2', 'val2', 7, 8)", KEYSPACE, currentSparseTable()));

        execute(String.format("DELETE col1 FROM %s.%s WHERE key = 'key1' AND column1 = 'val1'", KEYSPACE, currentSparseTable()));

        assertRows(execute(String.format("SELECT * FROM %s.%s", KEYSPACE, currentSparseTable())),
                   row("key1", "val1", null, 2L),
                   row("key1", "val2", 3L, 4L),
                   row("key2", "val1", 5L, 6L),
                   row("key2", "val2", 7L, 8L));

        execute(String.format("DELETE FROM %s.%s WHERE key = 'key1' AND column1 = 'val2'", KEYSPACE, currentSparseTable()));

        assertRows(execute(String.format("SELECT * FROM %s.%s", KEYSPACE, currentSparseTable())),
                   row("key1", "val1", null, 2L),
                   row("key2", "val1", 5L, 6L),
                   row("key2", "val2", 7L, 8L));

        execute(String.format("DELETE FROM %s.%s WHERE key = 'key2'", KEYSPACE, currentSparseTable()));

        assertRows(execute(String.format("SELECT * FROM %s.%s", KEYSPACE, currentSparseTable())),
                   row("key1", "val1", null, 2L));
    }

    @Test
    public void testInsertJson() throws Throwable
    {
        execute(String.format("INSERT INTO %s.%s JSON ?", KEYSPACE, currentDenseTable()),
                "{\"key\": \"key5\", \"column1\": \"val2\", \"column2\": 4, \"value\": \"value4\"}");
        assertRows(execute(String.format("SELECT * FROM %s.%s", KEYSPACE, currentDenseTable())),
                   row("key5", "val2", 4, "value4"));

        execute(String.format("INSERT INTO %s.%s JSON ?", KEYSPACE, currentSparseTable()),
                "{\"key\": \"key1\", \"column1\": \"val1\", \"col1\": 1, \"col2\": 2}");
        assertRows(execute(String.format("SELECT * FROM %s.%s", KEYSPACE, currentSparseTable())),
                   row("key1", "val1", 1L, 2L));
    }

    @Test
    public void testFiltering() throws Throwable
    {
        assertInvalidMessage("Filtering is not supported on SuperColumn tables",
                             String.format("select * from %s.%s WHERE value = ?", KEYSPACE, currentDenseTable()),
                             "value5");
        assertInvalidMessage("Filtering is not supported on SuperColumn tables",
                             String.format("select * from %s.%s WHERE value = ? ALLOW FILTERING", KEYSPACE, currentDenseTable()),
                             "value5");
        assertInvalidMessage("Filtering is not supported on SuperColumn tables",
                             String.format("SELECT * FROM %s.%s WHERE value = 'value2' ALLOW FILTERING", KEYSPACE, currentDenseTable()));
        assertInvalidMessage("Filtering is not supported on SuperColumn tables",
                             String.format("SELECT * FROM %s.%s WHERE column2 = 1 ALLOW FILTERING", KEYSPACE, currentDenseTable()));
    }

    @Test
    public void testLwt() throws Throwable
    {
        assertRows(execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES (?, ?, ?, ?) IF NOT EXISTS", KEYSPACE, currentDenseTable()),
                           "key1", "val1", 1, "value1"),
                   row(true));
        assertRows(execute(String.format("SELECT * FROM %s.%s", KEYSPACE, currentDenseTable())),
                   row("key1", "val1", 1, "value1"));
        assertRows(execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES (?, ?, ?, ?) IF NOT EXISTS", KEYSPACE, currentDenseTable()),
                           "key1", "val1", 1, "value1"),
                   row(false, "key1", "val1", 1, "value1"));

        // in 2.2 this query was a no-op
        assertInvalidMessage("Lightweight transactions on SuperColumn tables are only supported with supplied SuperColumn key",
                             String.format("UPDATE %s.%s SET value = 'changed' WHERE key = ? AND column1 = ? IF value = ?", KEYSPACE, currentDenseTable()));

        assertRows(execute(String.format("UPDATE %s.%s SET value = 'changed' WHERE key = ? AND column1 = ? AND column2 = ? IF value = ?", KEYSPACE, currentDenseTable()),
                           "key1", "val1", 1, "value1"),
                   row(true));
        assertRows(execute(String.format("SELECT * FROM %s.%s", KEYSPACE, currentDenseTable())),
                   row("key1", "val1", 1, "changed"));
        assertRows(execute(String.format("UPDATE %s.%s SET value = 'changed' WHERE key = ? AND column1 = ? AND column2 = ? IF value = ?", KEYSPACE, currentDenseTable()),
                           "key1", "val1", 1, "value1"),
                   row(false, "changed"));

        assertRows(execute(String.format("UPDATE %s.%s SET value = 'changed2' WHERE key = ? AND column1 = ? AND column2 = ? IF value > ?", KEYSPACE, currentDenseTable()),
                           "key1", "val1", 1, "a"),
                   row(true));
        assertRows(execute(String.format("SELECT * FROM %s.%s", KEYSPACE, currentDenseTable())),
                   row("key1", "val1", 1, "changed2"));
        assertRows(execute(String.format("UPDATE %s.%s SET value = 'changed2' WHERE key = ? AND column1 = ? AND column2 = ? IF value < ?", KEYSPACE, currentDenseTable()),
                           "key1", "val1", 1, "a"),
                   row(false, "changed2"));

        assertInvalidMessage("PRIMARY KEY column 'column2' cannot have IF conditions",
                             String.format("UPDATE %s.%s SET value = 'changed2' WHERE key = ? AND column1 = ? AND column2 = ? IF value > ? AND column2 = ?", KEYSPACE, currentDenseTable()));

        assertInvalidMessage("Lightweight transactions on SuperColumn tables are only supported with supplied SuperColumn key",
                             String.format("UPDATE %s.%s SET value = 'changed2' WHERE key = ? AND column1 = ? IF value > ?", KEYSPACE, currentDenseTable()));

        execute(String.format("DELETE FROM %s.%s WHERE key = 'key1' AND column1 = 'val1' AND column2 = 1 IF EXISTS", KEYSPACE, currentDenseTable()));
        assertEmpty(execute(String.format("SELECT * FROM %s.%s", KEYSPACE, currentDenseTable())));

        execute(String.format("INSERT INTO %s.%s (key, column1, column2, value) VALUES (?, ?, ?, ?)", KEYSPACE, currentDenseTable()),
                "key1", "val1", 1, "value1");

        assertRows(execute(String.format("DELETE FROM %s.%s WHERE key = 'key1' AND column1 = 'val1' AND column2 = 1 IF value = 'value1'", KEYSPACE, currentDenseTable())),
                   row(true));
        assertEmpty(execute(String.format("SELECT * FROM %s.%s", KEYSPACE, currentDenseTable())));

        assertRows(execute(String.format("DELETE FROM %s.%s WHERE key = 'key1' AND column1 = 'val1' AND column2 = 1 IF value = 'value1'", KEYSPACE, currentDenseTable())),
                   row(false));
        assertEmpty(execute(String.format("SELECT * FROM %s.%s", KEYSPACE, currentDenseTable())));
    }

    @Test
    public void testCqlAggregateFunctions() throws Throwable
    {
        populateDenseTable();
        populateSparseTable();

        assertRows(execute(String.format("select count(*) from %s.%s", KEYSPACE, currentDenseTable())),
                   row(8L));
        assertRows(execute(String.format("select count(*) from %s.%s", KEYSPACE, currentSparseTable())),
                   row(4L));

        assertRows(execute(String.format("select count(*) from %s.%s where key = ? AND column1 = ?", KEYSPACE, currentDenseTable()), "key1", "val1"),
                   row(2L));
        assertRows(execute(String.format("select count(*) from %s.%s where key = ? AND column1 = ?", KEYSPACE, currentSparseTable()), "key1", "val1"),
                   row(1L));
        assertRows(execute(String.format("select count(*) from %s.%s where key = ?", KEYSPACE, currentSparseTable()), "key1"),
                   row(2L));

        assertRows(execute(String.format("select max(value) from %s.%s", KEYSPACE, currentDenseTable())),
                   row("value5"));
        assertRows(execute(String.format("select max(col1) from %s.%s", KEYSPACE, currentSparseTable())),
                   row(3L));

        assertRows(execute(String.format("select avg(column2) from %s.%s", KEYSPACE, currentDenseTable())),
                   row(3));
        assertRows(execute(String.format("select avg(col1) from %s.%s", KEYSPACE, currentSparseTable())),
                   row(3L));
    }

    private void populateDenseTable() throws Throwable
    {
        Cassandra.Client client = getClient();

        Mutation mutation = new Mutation();
        ColumnOrSuperColumn csoc = new ColumnOrSuperColumn();
        csoc.setSuper_column(getSuperColumnForInsert(ByteBufferUtil.bytes("val1"),
                                                     Arrays.asList(getColumnForInsert(ByteBufferUtil.bytes(1), ByteBufferUtil.bytes("value1")),
                                                                   getColumnForInsert(ByteBufferUtil.bytes(2), ByteBufferUtil.bytes("value2")))));
        mutation.setColumn_or_supercolumn(csoc);

        Mutation mutation2 = new Mutation();
        ColumnOrSuperColumn csoc2 = new ColumnOrSuperColumn();
        csoc2.setSuper_column(getSuperColumnForInsert(ByteBufferUtil.bytes("val2"),
                                                      Arrays.asList(getColumnForInsert(ByteBufferUtil.bytes(4), ByteBufferUtil.bytes("value4")),
                                                                    getColumnForInsert(ByteBufferUtil.bytes(5), ByteBufferUtil.bytes("value5")))));
        mutation2.setColumn_or_supercolumn(csoc2);

        client.batch_mutate(Collections.singletonMap(ByteBufferUtil.bytes("key1"),
                                                     Collections.singletonMap(currentDenseTable(), Arrays.asList(mutation, mutation2))),
                            ONE);

        client.batch_mutate(Collections.singletonMap(ByteBufferUtil.bytes("key2"),
                                                     Collections.singletonMap(currentDenseTable(), Arrays.asList(mutation, mutation2))),
                            ONE);
    }

    private void populateSparseTable() throws Throwable
    {
        Cassandra.Client client = getClient();

        Mutation mutation = new Mutation();
        ColumnOrSuperColumn csoc = new ColumnOrSuperColumn();
        csoc.setSuper_column(getSuperColumnForInsert(ByteBufferUtil.bytes("val1"),
                                                     Arrays.asList(getColumnForInsert(ByteBufferUtil.bytes("value1"), ByteBufferUtil.bytes(1L)),
                                                                   getColumnForInsert(ByteBufferUtil.bytes("value2"), ByteBufferUtil.bytes(2L)),
                                                                   getColumnForInsert(ByteBufferUtil.bytes("col1"), ByteBufferUtil.bytes(3L)),
                                                                   getColumnForInsert(ByteBufferUtil.bytes("col2"), ByteBufferUtil.bytes(4L)))));
        mutation.setColumn_or_supercolumn(csoc);

        Mutation mutation2 = new Mutation();
        ColumnOrSuperColumn csoc2 = new ColumnOrSuperColumn();
        csoc2.setSuper_column(getSuperColumnForInsert(ByteBufferUtil.bytes("val2"),
                                                      Arrays.asList(getColumnForInsert(ByteBufferUtil.bytes("value1"), ByteBufferUtil.bytes(1L)),
                                                                    getColumnForInsert(ByteBufferUtil.bytes("value2"), ByteBufferUtil.bytes(2L)),
                                                                    getColumnForInsert(ByteBufferUtil.bytes("col1"), ByteBufferUtil.bytes(3L)),
                                                                    getColumnForInsert(ByteBufferUtil.bytes("col2"), ByteBufferUtil.bytes(4L)))));
        mutation2.setColumn_or_supercolumn(csoc2);

        client.batch_mutate(Collections.singletonMap(ByteBufferUtil.bytes("key1"),
                                                     Collections.singletonMap(currentSparseTable(), Arrays.asList(mutation, mutation2))),
                            ONE);

        client.batch_mutate(Collections.singletonMap(ByteBufferUtil.bytes("key2"),
                                                     Collections.singletonMap(currentSparseTable(), Arrays.asList(mutation, mutation2))),
                            ONE);
    }

    private void populateCounterTable() throws Throwable
    {
        Cassandra.Client client = getClient();

        ColumnParent cp = new ColumnParent(currentCounterTable());
        cp.setSuper_column(ByteBufferUtil.bytes("ck1"));
        client.add(ByteBufferUtil.bytes("key1"),
                   cp,
                   new CounterColumn(ByteBufferUtil.bytes("counter1"), 10L),
                   ONE);
        cp = new ColumnParent(currentCounterTable());
        cp.setSuper_column(ByteBufferUtil.bytes("ck1"));
        client.add(ByteBufferUtil.bytes("key1"),
                   cp,
                   new CounterColumn(ByteBufferUtil.bytes("counter2"), 5L),
                   ONE);
        cp = new ColumnParent(currentCounterTable());
        cp.setSuper_column(ByteBufferUtil.bytes("ck1"));
        client.add(ByteBufferUtil.bytes("key2"),
                   cp,
                   new CounterColumn(ByteBufferUtil.bytes("counter1"), 10L),
                   ONE);
        cp = new ColumnParent(currentCounterTable());
        cp.setSuper_column(ByteBufferUtil.bytes("ck1"));
        client.add(ByteBufferUtil.bytes("key2"),
                   cp,
                   new CounterColumn(ByteBufferUtil.bytes("counter2"), 5L),
                   ONE);
    }

    private String currentCounterTable()
    {
        return currentTable() + "_counter";
    }

    private String currentSparseTable()
    {
        return currentTable() + "_sparse";
    }

    private String currentDenseTable()
    {
        return currentTable();
    }

    private Column getColumnForInsert(ByteBuffer columnName, ByteBuffer value)
    {
        Column column = new Column();
        column.setName(columnName);
        column.setValue(value);
        column.setTimestamp(System.currentTimeMillis());
        return column;
    }

    private SuperColumn getSuperColumnForInsert(ByteBuffer columnName, List<Column> columns)
    {
        SuperColumn column = new SuperColumn();
        column.setName(columnName);
        for (Column c : columns)
            column.addToColumns(c);
        return column;
    }

    public void beforeAndAfterFlush(CheckedFunction runnable) throws Throwable
    {
        runnable.apply();
        flushAll();
        runnable.apply();
    }

    private void flushAll()
    {
        for (String cfName : new String[]{ currentTable(), currentSparseTable(), currentCounterTable() })
            Keyspace.open(KEYSPACE).getColumnFamilyStore(cfName);
    }
}
