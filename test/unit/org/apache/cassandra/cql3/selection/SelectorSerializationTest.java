/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.cql3.selection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.Constants.Literal;
import org.apache.cassandra.cql3.functions.AggregateFcts;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.TimeFcts;
import org.apache.cassandra.cql3.selection.Selectable.RawIdentifier;
import org.apache.cassandra.cql3.selection.Selector.Serializer;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;

public class SelectorSerializationTest extends CQLTester
{
    @Test
    public void testSerDes() throws IOException
    {
        createTable("CREATE TABLE %s (pk int, c1 int, c2 timestamp, v int, PRIMARY KEY(pk, c1, c2))");

        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(KEYSPACE);
        TableMetadata table = keyspace.getTableOrViewNullable(currentTable());

        // Test SimpleSelector serialization
        checkSerialization(table.getColumn(new ColumnIdentifier("c1", false)), table);

        // Test WritetimeOrTTLSelector serialization
        ColumnMetadata column = table.getColumn(new ColumnIdentifier("v", false));
        checkSerialization(new Selectable.WritetimeOrTTL(column, column, Selectable.WritetimeOrTTL.Kind.WRITE_TIME), table);
        checkSerialization(new Selectable.WritetimeOrTTL(column, column, Selectable.WritetimeOrTTL.Kind.TTL), table);
        checkSerialization(new Selectable.WritetimeOrTTL(column, column, Selectable.WritetimeOrTTL.Kind.MAX_WRITE_TIME), table);

        // Test ListSelector serialization
        checkSerialization(new Selectable.WithList(asList(table.getColumn(new ColumnIdentifier("v", false)),
                                                          table.getColumn(new ColumnIdentifier("c1", false)))), table);

        // Test SetSelector serialization
        checkSerialization(new Selectable.WithSet(asList(table.getColumn(new ColumnIdentifier("v", false)),
                                                         table.getColumn(new ColumnIdentifier("c1", false)))), table);

        // Test MapSelector serialization
        Pair<Selectable.Raw, Selectable.Raw> pair = Pair.create(RawIdentifier.forUnquoted("v"),
                                                                RawIdentifier.forUnquoted("c1"));
        checkSerialization(new Selectable.WithMapOrUdt(table, asList(pair)), table, MapType.getInstance(Int32Type.instance, Int32Type.instance, false));

        // Test TupleSelector serialization
        checkSerialization(new Selectable.BetweenParenthesesOrWithTuple(asList(table.getColumn(new ColumnIdentifier("c2", false)),
                                                                               table.getColumn(new ColumnIdentifier("c1", false)))), table);
        // Test TermSelector serialization
        checkSerialization(new Selectable.WithTerm(Literal.duration("5m")), table, DurationType.instance);

        // Test UserTypeSelector serialization
        String typeName = createType("CREATE TYPE %s (f1 int, f2 int)");

        UserType type = new UserType(KEYSPACE, ByteBufferUtil.bytes(typeName),
                                     asList(FieldIdentifier.forUnquoted("f1"),
                                            FieldIdentifier.forUnquoted("f2")),
                                     asList(Int32Type.instance,
                                            Int32Type.instance),
                                     false);

        List<Pair<Selectable.Raw, Selectable.Raw>> list = asList(Pair.create(RawIdentifier.forUnquoted("f1"),
                                                                             RawIdentifier.forUnquoted("c1")),
                                                                 Pair.create(RawIdentifier.forUnquoted("f2"),
                                                                             RawIdentifier.forUnquoted("pk")));

        checkSerialization(new Selectable.WithMapOrUdt(table, list), table, type);

        // Test FieldSelector serialization
        checkSerialization(new Selectable.WithFieldSelection(new Selectable.WithTypeHint(typeName, type, new Selectable.WithMapOrUdt(table, list)), FieldIdentifier.forUnquoted("f1")), table, type);

        // Test AggregateFunctionSelector serialization
        Function max = AggregateFcts.makeMaxFunction(Int32Type.instance);
        checkSerialization(new Selectable.WithFunction(max, asList(table.getColumn(new ColumnIdentifier("v", false)))), table);

        // Test SCalarFunctionSelector serialization
        Function toDate = TimeFcts.toDate(TimestampType.instance);
        checkSerialization(new Selectable.WithFunction(toDate, asList(table.getColumn(new ColumnIdentifier("c2", false)))), table);

        Function floor = TimeFcts.FloorTimestampFunction.newInstanceWithStartTimeArgument();
        checkSerialization(new Selectable.WithFunction(floor, asList(table.getColumn(new ColumnIdentifier("c2", false)),
                                                                     new Selectable.WithTerm(Literal.duration("5m")),
                                                                     new Selectable.WithTerm(Literal.string("2016-09-27 16:00:00 UTC")))), table);
    }

    private static void checkSerialization(Selectable selectable, TableMetadata table) throws IOException
    {
        checkSerialization(selectable, table, null);
    }

    private static void checkSerialization(Selectable selectable, TableMetadata table, AbstractType<?> expectedType) throws IOException
    {
        int version = MessagingService.current_version;

        Serializer serializer = Selector.serializer;
        Selector.Factory factory = selectable.newSelectorFactory(table, expectedType, new ArrayList<>(), VariableSpecifications.empty());
        Selector selector = factory.newInstance(QueryOptions.DEFAULT);
        int size = serializer.serializedSize(selector, version);
        DataOutputBuffer out = new DataOutputBuffer(size);
        serializer.serialize(selector, out, version);
        ByteBuffer buffer = out.asNewBuffer();
        DataInputBuffer in = new DataInputBuffer(buffer, false);
        Selector deserialized = serializer.deserialize(in, version, table);

        assertEquals(selector, deserialized);
    }
}
