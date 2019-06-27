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

import org.junit.Test;

import org.apache.cassandra.cql3.validation.operations.ThriftCQLTester;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.thrift.ConsistencyLevel.ONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ThriftIllegalColumnsTest extends ThriftCQLTester
{
    final String NON_COMPACT_TABLE = "t1";
    final String COMPACT_TABLE = "t2";

    @Test
    public void testNonCompactUpdateWithPrimaryKeyColumnName() throws Throwable
    {
        Cassandra.Client client = getClient();
        client.set_keyspace(KEYSPACE);
        String table = createTable(KEYSPACE, "CREATE TABLE %s (k int, c1 int,  c2 int, v int, PRIMARY KEY (k, c1, c2))");

        // A cell name which represents a primary key column
        ByteBuffer badCellName = CompositeType.build(ByteBufferUtil.bytes(0), ByteBufferUtil.bytes(0), ByteBufferUtil.bytes("c1"));
        // A cell name which represents a regular column
        ByteBuffer goodCellName = CompositeType.build(ByteBufferUtil.bytes(0), ByteBufferUtil.bytes(0), ByteBufferUtil.bytes("v"));

        ColumnParent parent = new ColumnParent(table);
        ByteBuffer key = ByteBufferUtil.bytes(0);
        Column column = new Column();
        column.setName(badCellName);
        column.setValue(ByteBufferUtil.bytes(999));
        column.setTimestamp(System.currentTimeMillis());

        try
        {
            client.insert(key, parent, column, ONE);
            fail("Expected exception");
        } catch (InvalidRequestException e) {
            assertEquals("Cannot add primary key column c1 to partition update", e.getWhy());
        }

        column.setName(goodCellName);
        client.insert(key, parent, column, ONE);
        assertRows(execute("SELECT v from %s WHERE k = 0"), row(999));
    }

    @Test
    public void testThriftCompactUpdateWithPrimaryKeyColumnName() throws Throwable
    {
        Cassandra.Client client = getClient();
        client.set_keyspace(KEYSPACE);
        String table = createTable(KEYSPACE, "CREATE TABLE %s (k int, v int, PRIMARY KEY (k)) WITH COMPACT STORAGE");

        // A cell name which represents a primary key column
        ByteBuffer badCellName = ByteBufferUtil.bytes("k");
        // A cell name which represents a regular column
        ByteBuffer goodCellName = ByteBufferUtil.bytes("v");

        ColumnParent parent = new ColumnParent(table);
        ByteBuffer key = ByteBufferUtil.bytes(0);
        Column column = new Column();
        column.setName(badCellName);
        column.setValue(ByteBufferUtil.bytes(999));
        column.setTimestamp(System.currentTimeMillis());
        // if the table is compact, a cell name which appears to reference a primary
        // key column is treated as a dynamic column and so the update is allowed
        client.insert(key, parent, column, ONE);

        column.setName(goodCellName);
        client.insert(key, parent, column, ONE);
        assertRows(execute("SELECT v from %s where k=0"), row(999));
    }
}
