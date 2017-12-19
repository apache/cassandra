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

package org.apache.cassandra.cql3.validation.operations;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.cassandra.thrift.ConsistencyLevel.ONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DropCompactStorageThriftTest extends ThriftCQLTester
{
    @Test
    public void thriftCreatedTableTest() throws Throwable
    {
        final String KEYSPACE = "thrift_created_table_test_ks";
        final String TABLE = "test_table_1";

        CfDef cfDef = new CfDef().setDefault_validation_class(Int32Type.instance.toString())
                                 .setKey_validation_class(AsciiType.instance.toString())
                                 .setComparator_type(AsciiType.instance.toString())
                                 .setColumn_metadata(Arrays.asList(new ColumnDef(ByteBufferUtil.bytes("col1"),
                                                                                 AsciiType.instance.toString())
                                                                   .setIndex_name("col1Index")
                                                                   .setIndex_type(IndexType.KEYS),
                                                                   new ColumnDef(ByteBufferUtil.bytes("col2"),
                                                                                 AsciiType.instance.toString())
                                                                   .setIndex_name("col2Index")
                                                                   .setIndex_type(IndexType.KEYS)))
                                 .setKeyspace(KEYSPACE)
                                 .setName(TABLE);

        KsDef ksDef = new KsDef(KEYSPACE,
                                SimpleStrategy.class.getName(),
                                Arrays.asList(cfDef));
        ksDef.setStrategy_options(Collections.singletonMap("replication_factor", "1"));

        Cassandra.Client client = getClient();
        client.system_add_keyspace(ksDef);
        client.set_keyspace(KEYSPACE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("col1"), ByteBufferUtil.bytes("val1")),
                      ONE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("col2"), ByteBufferUtil.bytes("val2")),
                      ONE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("dynamicKey1"), ByteBufferUtil.bytes(100)),
                      ONE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("dynamicKey2"), ByteBufferUtil.bytes(200)),
                      ONE);


        execute(String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE;", KEYSPACE, TABLE));
        UntypedResultSet resultSet = execute(String.format("select * from %s.%s",
                                                           KEYSPACE, TABLE));
        assertColumnType(AsciiType.instance, resultSet, "key");
        assertColumnType(AsciiType.instance, resultSet, "column1");
        assertColumnType(Int32Type.instance, resultSet, "value");
        assertColumnType(AsciiType.instance, resultSet, "col1");
        assertColumnType(AsciiType.instance, resultSet, "col2");

        assertRows(resultSet,
                   row("key1", "dynamicKey1", "val1", "val2", 100),
                   row("key1", "dynamicKey2", "val1", "val2", 200));
    }

    @Test
    public void thriftStaticCompatTableTest() throws Throwable
    {
        String KEYSPACE = keyspace();
        String TABLE = createTable("CREATE TABLE %s (key ascii PRIMARY KEY, val ascii) WITH COMPACT STORAGE");

        Cassandra.Client client = getClient();
        client.set_keyspace(KEYSPACE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("val"), ByteBufferUtil.bytes("val1")),
                      ONE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("dynamicKey1"), ByteBufferUtil.bytes("dynamicValue1")),
                      ONE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("dynamicKey2"), ByteBufferUtil.bytes("dynamicValue2")),
                      ONE);

        execute(String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE;", KEYSPACE, TABLE));
        UntypedResultSet resultSet = execute(String.format("select * from %s.%s",
                                                           KEYSPACE, TABLE));
        assertColumnType(AsciiType.instance, resultSet, "key");
        assertColumnType(UTF8Type.instance, resultSet, "column1");
        assertColumnType(AsciiType.instance, resultSet, "val");
        assertColumnType(BytesType.instance, resultSet, "value");

        // Values are interpreted as bytes by default:
        assertRows(resultSet,
                   row("key1", "dynamicKey1", "val1", ByteBufferUtil.bytes("dynamicValue1")),
                   row("key1", "dynamicKey2", "val1", ByteBufferUtil.bytes("dynamicValue2")));
    }

    @Test
    public void testSparseCompactTableIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (key ascii PRIMARY KEY, val ascii) WITH COMPACT STORAGE");

        // Indexes are allowed only on the sparse compact tables
        createIndex("CREATE INDEX ON %s(val)");
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (key, val) VALUES (?, ?)", Integer.toString(i), Integer.toString(i * 10));

        alterTable("ALTER TABLE %s DROP COMPACT STORAGE");

        assertRows(execute("SELECT * FROM %s WHERE val = '50'"),
                   row("5", null, "50", null));
        assertRows(execute("SELECT * FROM %s WHERE key = '5'"),
                   row("5", null, "50", null));
    }

    @Test
    public void thriftCompatTableTest() throws Throwable
    {
        String KEYSPACE = keyspace();
        String TABLE = createTable("CREATE TABLE %s (pkey ascii, ckey ascii, PRIMARY KEY (pkey, ckey)) WITH COMPACT STORAGE");

        Cassandra.Client client = getClient();
        client.set_keyspace(KEYSPACE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("ckeyValue1"), ByteBufferUtil.EMPTY_BYTE_BUFFER),
                      ONE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("ckeyValue2"), ByteBufferUtil.EMPTY_BYTE_BUFFER),
                      ONE);

        execute(String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE;", KEYSPACE, TABLE));
        UntypedResultSet resultSet = execute(String.format("select * from %s.%s",
                                                           KEYSPACE, TABLE));
        assertColumnType(AsciiType.instance, resultSet, "pkey");
        assertColumnType(AsciiType.instance, resultSet, "ckey");
        assertColumnType(EmptyType.instance, resultSet, "value");

        // Value is always empty
        assertRows(resultSet,
                   row("key1", "ckeyValue1", ByteBufferUtil.EMPTY_BYTE_BUFFER),
                   row("key1", "ckeyValue2", ByteBufferUtil.EMPTY_BYTE_BUFFER));
    }

    @Test
    public void thriftDenseTableTest() throws Throwable
    {
        String KEYSPACE = keyspace();
        String TABLE = createTable("CREATE TABLE %s (pkey text, ckey text, v text, PRIMARY KEY (pkey, ckey)) WITH COMPACT STORAGE");

        Cassandra.Client client = getClient();
        client.set_keyspace(KEYSPACE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("ckey1"), ByteBufferUtil.bytes("cvalue1")),
                      ONE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("ckey2"), ByteBufferUtil.bytes("cvalue2")),
                      ONE);

        execute(String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE;", KEYSPACE, TABLE));
        UntypedResultSet resultSet = execute(String.format("select * from %s.%s",
                                                           KEYSPACE, TABLE));
        assertColumnType(UTF8Type.instance, resultSet, "pkey");
        assertColumnType(UTF8Type.instance, resultSet, "ckey");
        assertColumnType(UTF8Type.instance, resultSet, "v");

        assertRows(resultSet,
                   row("key1", "ckey1", "cvalue1"),
                   row("key1", "ckey2", "cvalue2"));
    }

    @Test
    public void thriftTableWithIntKey() throws Throwable
    {
        final String KEYSPACE = "thrift_table_with_int_key_ks";
        final String TABLE = "test_table_1";

        ByteBuffer columnName = ByteBufferUtil.bytes("columnname");
        CfDef cfDef = new CfDef().setDefault_validation_class(UTF8Type.instance.toString())
                                 .setKey_validation_class(BytesType.instance.toString())
                                 .setComparator_type(BytesType.instance.toString())
                                 .setColumn_metadata(Arrays.asList(new ColumnDef(columnName,
                                                                                 Int32Type.instance.toString())
                                                                   .setIndex_name("col1Index")
                                                                   .setIndex_type(IndexType.KEYS)))
                                 .setKeyspace(KEYSPACE)
                                 .setName(TABLE);

        KsDef ksDef = new KsDef(KEYSPACE,
                                SimpleStrategy.class.getName(),
                                Arrays.asList(cfDef));
        ksDef.setStrategy_options(Collections.singletonMap("replication_factor", "1"));

        Cassandra.Client client = getClient();
        client.system_add_keyspace(ksDef);
        client.set_keyspace(KEYSPACE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(columnName, ByteBufferUtil.bytes(100)),
                      ONE);

        execute(String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE;", KEYSPACE, TABLE));
        UntypedResultSet resultSet = execute(String.format("select * from %s.%s",
                                                           KEYSPACE, TABLE));

        assertEquals(resultSet.metadata()
                              .stream()
                              .filter((cs) -> cs.name.toString().equals(BytesType.instance.getString(columnName)))
                              .findFirst()
                              .get().type,
                     Int32Type.instance);

        assertRows(resultSet,
                   row(UTF8Type.instance.decompose("key1"), null, 100, null));
    }

    @Test
    public void thriftCompatTableWithSupercolumnsTest() throws Throwable
    {
        final String KEYSPACE = "thrift_compact_table_with_supercolumns_test";
        final String TABLE = "test_table_1";

        CfDef cfDef = new CfDef().setColumn_type("Super")
                                 .setSubcomparator_type(Int32Type.instance.toString())
                                 .setComparator_type(AsciiType.instance.toString())
                                 .setDefault_validation_class(AsciiType.instance.toString())
                                 .setKey_validation_class(AsciiType.instance.toString())
                                 .setKeyspace(KEYSPACE)
                                 .setName(TABLE);

        KsDef ksDef = new KsDef(KEYSPACE,
                                SimpleStrategy.class.getName(),
                                Arrays.asList(cfDef));
        ksDef.setStrategy_options(Collections.singletonMap("replication_factor", "1"));

        Cassandra.Client client = getClient();
        client.system_add_keyspace(ksDef);

        client.set_keyspace(KEYSPACE);

        Mutation mutation = new Mutation();
        ColumnOrSuperColumn csoc = new ColumnOrSuperColumn();
        csoc.setSuper_column(getSuperColumnForInsert(ByteBufferUtil.bytes("val1"),
                                                     Arrays.asList(getColumnForInsert(ByteBufferUtil.bytes(1), ByteBufferUtil.bytes("value1")),
                                                                   getColumnForInsert(ByteBufferUtil.bytes(2), ByteBufferUtil.bytes("value2")),
                                                                   getColumnForInsert(ByteBufferUtil.bytes(3), ByteBufferUtil.bytes("value3")))));
        mutation.setColumn_or_supercolumn(csoc);

        Mutation mutation2 = new Mutation();
        ColumnOrSuperColumn csoc2 = new ColumnOrSuperColumn();
        csoc2.setSuper_column(getSuperColumnForInsert(ByteBufferUtil.bytes("val2"),
                                                     Arrays.asList(getColumnForInsert(ByteBufferUtil.bytes(4), ByteBufferUtil.bytes("value7")),
                                                                   getColumnForInsert(ByteBufferUtil.bytes(5), ByteBufferUtil.bytes("value8")),
                                                                   getColumnForInsert(ByteBufferUtil.bytes(6), ByteBufferUtil.bytes("value9")))));
        mutation2.setColumn_or_supercolumn(csoc2);

        client.batch_mutate(Collections.singletonMap(ByteBufferUtil.bytes("key1"),
                                                     Collections.singletonMap(TABLE, Arrays.asList(mutation, mutation2))),
                            ONE);

        execute(String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE;", KEYSPACE, TABLE));
        UntypedResultSet resultSet = execute(String.format("select * from %s.%s",
                                                           KEYSPACE, TABLE));
        assertColumnType(AsciiType.instance, resultSet, "key");
        assertColumnType(AsciiType.instance, resultSet, "column1");
        assertColumnType(MapType.getInstance(Int32Type.instance, AsciiType.instance, true), resultSet, "");

        assertRows(resultSet,
                   row("key1", "val1", map(1, "value1", 2, "value2", 3, "value3")),
                   row("key1", "val2", map(4, "value7", 5, "value8", 6, "value9")));

        assertRows(execute(String.format("SELECT \"\" FROM %s.%s;", KEYSPACE, TABLE)),
                   row(map(1, "value1", 2, "value2", 3, "value3")),
                   row(map(4, "value7", 5, "value8", 6, "value9")));

        assertInvalidMessage("Range deletions are not supported for specific columns",
                             String.format("DELETE \"\" FROM %s.%s WHERE key=?;", KEYSPACE, TABLE),
                             "key1");

        execute(String.format("TRUNCATE %s.%s;", KEYSPACE, TABLE));

        execute(String.format("INSERT INTO %s.%s (key, column1, \"\") VALUES (?, ?, ?);", KEYSPACE, TABLE),
                "key3", "val1", map(7, "value7", 8, "value8"));

        assertRows(execute(String.format("SELECT \"\" FROM %s.%s;", KEYSPACE, TABLE)),
                   row(map(7, "value7", 8, "value8")));
    }

    @Test
    public void thriftCreatedTableWithCompositeColumnsTest() throws Throwable
    {
        final String KEYSPACE = "thrift_created_table_with_composites_test_ks";
        final String TABLE = "test_table_1";

        CompositeType type = CompositeType.getInstance(AsciiType.instance, AsciiType.instance, AsciiType.instance);
        CfDef cfDef = new CfDef().setDefault_validation_class(AsciiType.instance.toString())
                                 .setComparator_type(type.toString())
                                 .setKey_validation_class(AsciiType.instance.toString())
                                 .setKeyspace(KEYSPACE)
                                 .setName(TABLE);

        KsDef ksDef = new KsDef(KEYSPACE,
                                SimpleStrategy.class.getName(),
                                Arrays.asList(cfDef));
        ksDef.setStrategy_options(Collections.singletonMap("replication_factor", "1"));

        Cassandra.Client client = getClient();
        client.system_add_keyspace(ksDef);
        client.set_keyspace(KEYSPACE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(type.decompose("a", "b", "c"), ByteBufferUtil.bytes("val1")),
                      ONE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(type.decompose("d", "e", "f"), ByteBufferUtil.bytes("val2")),
                      ONE);

        execute(String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE;", KEYSPACE, TABLE));
        UntypedResultSet resultSet = execute(String.format("select * from %s.%s",
                                                           KEYSPACE, TABLE));

        assertColumnType(AsciiType.instance, resultSet, "key");
        assertColumnType(AsciiType.instance, resultSet, "column1");
        assertColumnType(AsciiType.instance, resultSet, "column2");
        assertColumnType(AsciiType.instance, resultSet, "column3");
        assertColumnType(AsciiType.instance, resultSet, "value");

        assertRows(resultSet,
                   row("key1", "a", "b", "c", "val1"),
                   row("key1", "d", "e", "f", "val2"));
    }

    @Test
    public void compactTableWithoutClusteringKeyTest() throws Throwable
    {
        String KEYSPACE = keyspace();
        String TABLE = createTable("CREATE TABLE %s (pkey text PRIMARY KEY, s1 text, s2 text) WITH COMPACT STORAGE");

        Cassandra.Client client = getClient();
        client.set_keyspace(KEYSPACE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("ckey1"), ByteBufferUtil.bytes("val1")),
                      ONE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("ckey2"), ByteBufferUtil.bytes("val2")),
                      ONE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("s1"), ByteBufferUtil.bytes("s1Val")),
                      ONE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("s2"), ByteBufferUtil.bytes("s2Val")),
                      ONE);

        execute(String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE;", KEYSPACE, TABLE));
        UntypedResultSet resultSet = execute(String.format("select * from %s.%s",
                                                           KEYSPACE, TABLE));

        assertColumnType(UTF8Type.instance, resultSet, "pkey");
        assertColumnType(UTF8Type.instance, resultSet, "s1");
        assertColumnType(UTF8Type.instance, resultSet, "s2");
        assertColumnType(UTF8Type.instance, resultSet, "column1");
        assertColumnType(BytesType.instance, resultSet, "value");

        assertRows(resultSet,
                   row("key1", "ckey1", "s1Val", "s2Val", ByteBufferUtil.bytes("val1")),
                   row("key1", "ckey2", "s1Val", "s2Val", ByteBufferUtil.bytes("val2")));
    }

    @Test
    public void denseTableTestTest() throws Throwable
    {
        String KEYSPACE = keyspace();
        String TABLE = createTable("CREATE TABLE %s (pkey text PRIMARY KEY, s text) WITH COMPACT STORAGE");

        Cassandra.Client client = getClient();
        client.set_keyspace(KEYSPACE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("ckey1"), ByteBufferUtil.bytes("val1")),
                      ONE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("ckey2"), ByteBufferUtil.bytes("val2")),
                      ONE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("s"), ByteBufferUtil.bytes("sval1")),
                      ONE);

        client.insert(UTF8Type.instance.decompose("key1"),
                      new ColumnParent(TABLE),
                      getColumnForInsert(ByteBufferUtil.bytes("s"), ByteBufferUtil.bytes("sval2")),
                      ONE);

        // `s` becomes static, `column1` becomes a clustering key, `value` becomes visible
        execute(String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE;", KEYSPACE, TABLE));
        UntypedResultSet resultSet = execute(String.format("select * from %s.%s",
                                                           KEYSPACE, TABLE));
        assertColumnType(UTF8Type.instance, resultSet, "pkey");
        assertColumnType(UTF8Type.instance, resultSet, "s");
        assertColumnType(UTF8Type.instance, resultSet, "column1");
        assertColumnType(BytesType.instance, resultSet, "value");

        assertRows(resultSet,
                   row("key1", "ckey1", "sval2", ByteBufferUtil.bytes("val1")),
                   row("key1", "ckey2", "sval2", ByteBufferUtil.bytes("val2")));
    }

    @Test
    public void denseCompositeWithIndexesTest() throws Throwable
    {
        final String KEYSPACE = "thrift_dense_composite_table_test_ks";
        final String TABLE = "dense_composite_table";

        ByteBuffer aCol = createDynamicCompositeKey(ByteBufferUtil.bytes("a"));
        ByteBuffer bCol = createDynamicCompositeKey(ByteBufferUtil.bytes("b"));
        ByteBuffer cCol = createDynamicCompositeKey(ByteBufferUtil.bytes("c"));

        String compositeType = "DynamicCompositeType(a => BytesType, b => TimeUUIDType, c => UTF8Type)";

        CfDef cfDef = new CfDef();
        cfDef.setName(TABLE);
        cfDef.setComparator_type(compositeType);
        cfDef.setKeyspace(KEYSPACE);

        cfDef.setColumn_metadata(
        Arrays.asList(new ColumnDef(aCol, "BytesType").setIndex_type(IndexType.KEYS).setIndex_name(KEYSPACE + "_a"),
                      new ColumnDef(bCol, "BytesType").setIndex_type(IndexType.KEYS).setIndex_name(KEYSPACE + "_b"),
                      new ColumnDef(cCol, "BytesType").setIndex_type(IndexType.KEYS).setIndex_name(KEYSPACE + "_c")));


        KsDef ksDef = new KsDef(KEYSPACE,
                                SimpleStrategy.class.getName(),
                                Collections.singletonList(cfDef));
        ksDef.setStrategy_options(Collections.singletonMap("replication_factor", "1"));

        Cassandra.Client client = getClient();
        client.system_add_keyspace(ksDef);
        client.set_keyspace(KEYSPACE);

        CFMetaData cfm = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata;
        assertFalse(cfm.isCQLTable());

        List<Pair<ColumnDefinition, IndexTarget.Type>> compactTableTargets = new ArrayList<>();
        compactTableTargets.add(TargetParser.parse(cfm, "a"));
        compactTableTargets.add(TargetParser.parse(cfm, "b"));
        compactTableTargets.add(TargetParser.parse(cfm, "c"));

        execute(String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE", KEYSPACE, TABLE));
        cfm = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata;
        assertTrue(cfm.isCQLTable());

        List<Pair<ColumnDefinition, IndexTarget.Type>> cqlTableTargets = new ArrayList<>();
        cqlTableTargets.add(TargetParser.parse(cfm, "a"));
        cqlTableTargets.add(TargetParser.parse(cfm, "b"));
        cqlTableTargets.add(TargetParser.parse(cfm, "c"));

        assertEquals(compactTableTargets, cqlTableTargets);
    }

    private static ByteBuffer createDynamicCompositeKey(Object... objects)
    {
        int length = 0;

        for (Object object : objects)
        {
            length += 2 * Short.BYTES +  Byte.BYTES;
            if (object instanceof String)
                length += ((String) object).length();
            else if (object instanceof UUID)
                length += 2 * Long.BYTES;
            else if (object instanceof ByteBuffer)
                length += ((ByteBuffer) object).remaining();
            else
                throw new MarshalException(object.getClass().getName() + " is not recognized as a valid type for this composite");
        }

        ByteBuffer out = ByteBuffer.allocate(length);

        for (Object object : objects)
        {
            if (object instanceof String)
            {
                String cast = (String) object;

                out.putShort((short) (0x8000 | 's'));
                out.putShort((short) cast.length());
                out.put(cast.getBytes());
                out.put((byte) 0);
            }
            else if (object instanceof UUID)
            {
                out.putShort((short) (0x8000 | 't'));
                out.putShort((short) 16);
                out.put(UUIDGen.decompose((UUID) object));
                out.put((byte) 0);
            }
            else if (object instanceof ByteBuffer)
            {
                ByteBuffer bytes = ((ByteBuffer) object).duplicate();
                out.putShort((short) (0x8000 | 'b'));
                out.putShort((short) bytes.remaining());
                out.put(bytes);
                out.put((byte) 0);
            }
            else
            {
                throw new MarshalException(object.getClass().getName() + " is not recognized as a valid type for this composite");
            }
        }

        return out;
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

    private static void assertColumnType(AbstractType t, UntypedResultSet resultSet, String columnName)
    {
        for (ColumnSpecification columnSpecification : resultSet.metadata())
        {
            if (columnSpecification.name.toString().equals(columnName))
            {
                assertEquals(t, columnSpecification.type);
                return;
            }
        }

        fail(String.format("Could not find a column with name '%s'", columnName));
    }
}
