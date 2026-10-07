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

package org.apache.cassandra.db.memtable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

/// Trie memtables identify columns by their index in the table's column set and encode cell paths using the column
/// types, thus they must be switched when the columns change. Check that this is done correctly, including when the
/// memtable is empty at the time of the schema change.
public class TrieMemtableSchemaChangeTest extends CQLTester
{
    @Test
    public void testAddColumnToEmptyMemtable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, b text) WITH memtable = 'trie'");
        assertEquals(TrieMemtable.class, getCurrentColumnFamilyStore().getCurrentMemtable().getClass());

        // a is inserted before b in the column set, changing its index
        alterTable("ALTER TABLE %s ADD a list<text>");
        execute("INSERT INTO %s (k, a, b) VALUES (1, ['x'], 'y')");
        assertRows(execute("SELECT k, a, b FROM %s"), row(1, list("x"), "y"));

        flush();
        assertRows(execute("SELECT k, a, b FROM %s"), row(1, list("x"), "y"));
    }

    @Test
    public void testAddColumnToNonEmptyMemtable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, b text, d text) WITH memtable = 'trie'");
        execute("INSERT INTO %s (k, b, d) VALUES (1, 'b1', 'd1')");

        alterTable("ALTER TABLE %s ADD c text");
        execute("INSERT INTO %s (k, b, c, d) VALUES (2, 'b2', 'c2', 'd2')");
        assertRows(execute("SELECT k, b, c, d FROM %s"),
                   row(1, "b1", null, "d1"),
                   row(2, "b2", "c2", "d2"));

        flush();
        assertRows(execute("SELECT k, b, c, d FROM %s"),
                   row(1, "b1", null, "d1"),
                   row(2, "b2", "c2", "d2"));
    }

    @Test
    public void testDropAndReAddColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int) WITH memtable = 'trie'");
        execute("INSERT INTO %s (k, a, b) VALUES (1, 1, 1) USING TIMESTAMP 10000");

        alterTable("ALTER TABLE %s DROP a USING TIMESTAMP 20000");
        execute("INSERT INTO %s (k, b) VALUES (2, 2) USING TIMESTAMP 25000");
        assertRows(execute("SELECT k, b FROM %s"), row(1, 1), row(2, 2));

        alterTable("ALTER TABLE %s ADD a int");
        execute("INSERT INTO %s (k, a, b) VALUES (3, 3, 3) USING TIMESTAMP 30000");
        assertRows(execute("SELECT k, a, b FROM %s"), row(1, null, 1), row(2, null, 2), row(3, 3, 3));

        flush();
        assertRows(execute("SELECT k, a, b FROM %s"), row(1, null, 1), row(2, null, 2), row(3, 3, 3));
    }

    @Test
    public void testAlterNestedUserType() throws Throwable
    {
        String type = KEYSPACE + '.' + createType("CREATE TYPE %s (a int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s set<frozen<" + type + ">>) WITH memtable = 'trie'");
        execute("INSERT INTO %s (k, s) VALUES (1, {{a: 1}})");

        schemaChange("ALTER TYPE " + type + " ADD b int");
        execute("INSERT INTO %s (k, s) VALUES (2, {{a: 2, b: 2}})");
        // Values written before the type change may be returned with or without the new field, compare field values.
        assertEquals(ImmutableSet.of(Arrays.asList(1, 1, null), Arrays.asList(2, 2, 2)), readNestedFields());

        flush();
        assertEquals(ImmutableSet.of(Arrays.asList(1, 1, null), Arrays.asList(2, 2, 2)), readNestedFields());
    }

    /// Returns the key and the fields of the single element of the set of user types in each row.
    private Set<List<Integer>> readNestedFields() throws Throwable
    {
        SetType<?> setType = (SetType<?>) getCurrentColumnFamilyStore().metadata().getColumn(ByteBufferUtil.bytes("s")).type;
        UserType userType = (UserType) setType.getElementsType();
        Set<List<Integer>> result = new HashSet<>();
        for (UntypedResultSet.Row row : execute("SELECT k, s FROM %s"))
        {
            Set<?> elements = setType.compose(row.getBytes("s"));
            assertEquals(1, elements.size());
            List<ByteBuffer> fields = userType.unpack((ByteBuffer) elements.iterator().next());
            ByteBuffer b = fields.size() > 1 ? fields.get(1) : null;
            result.add(Arrays.asList(row.getInt("k"),
                                     Int32Type.instance.compose(fields.get(0)),
                                     b != null ? Int32Type.instance.compose(b) : null));
        }
        return result;
    }
}
