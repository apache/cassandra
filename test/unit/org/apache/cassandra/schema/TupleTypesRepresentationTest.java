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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

/**
 * Verifies that the string representations of {@link AbstractType} and {@link CQL3Type} are as expected and compatible.
 *
 * C* 3.0 is known to <em>not</em> enclose a frozen UDT in a "frozen bracket" in the {@link AbstractType}.
 * The string representation of a frozuen UDT using the {@link CQL3Type} type hierarchy is correct in C* 3.0.
 */
public class TupleTypesRepresentationTest
{
    static
    {
        DatabaseDescriptor.toolInitialization();
    }

    private static final String keyspace = "ks";
    private static final String mcUdtName = "mc_udt";
    private static final ByteBuffer mcUdtNameBytes = ByteBufferUtil.bytes(mcUdtName);
    private static final String iUdtName = "i_udt";
    private static final ByteBuffer iUdtNameBytes = ByteBufferUtil.bytes(iUdtName);
    private static final String fUdtName = "f_udt";
    private static final ByteBuffer fUdtNameBytes = ByteBufferUtil.bytes(fUdtName);
    private static final String udtField1 = "a";
    private static final String udtField2 = "b";
    private static final AbstractType<?> udtType1 = UTF8Type.instance;
    private static final AbstractType<?> udtType2 = UTF8Type.instance;

    private static final Types types = Types.builder()
                                            .add(new UserType(keyspace,
                                                              mcUdtNameBytes,
                                                              Arrays.asList(new FieldIdentifier(ByteBufferUtil.bytes(udtField1)),
                                                                            new FieldIdentifier(ByteBufferUtil.bytes(udtField2))),
                                                              Arrays.asList(udtType1,
                                                                            udtType2),
                                                              true))
                                            .add(new UserType(keyspace,
                                                              iUdtNameBytes,
                                                              Arrays.asList(new FieldIdentifier(ByteBufferUtil.bytes(udtField1)),
                                                                            new FieldIdentifier(ByteBufferUtil.bytes(udtField2))),
                                                              Arrays.asList(udtType1,
                                                                            udtType2),
                                                              true))
                                            .add(new UserType(keyspace,
                                                              fUdtNameBytes,
                                                              Arrays.asList(new FieldIdentifier(ByteBufferUtil.bytes(udtField1)),
                                                                            new FieldIdentifier(ByteBufferUtil.bytes(udtField2))),
                                                              Arrays.asList(udtType1,
                                                                            udtType2),
                                                              true))
                                            .build();

    static class TypeDef
    {
        final String typeString;
        final String cqlTypeString;
        final String droppedCqlTypeString;
        final boolean multiCell;
        final String cqlValue;

        final AbstractType<?> type;
        final CQL3Type cqlType;

        final AbstractType<?> droppedType;
        final CQL3Type droppedCqlType;

        TypeDef(String typeString, String cqlTypeString, String droppedCqlTypeString, boolean multiCell, String cqlValue)
        {
            this.typeString = typeString;
            this.cqlTypeString = cqlTypeString;
            this.droppedCqlTypeString = droppedCqlTypeString;
            this.multiCell = multiCell;
            this.cqlValue = cqlValue;

            cqlType = CQLFragmentParser.parseAny(CqlParser::comparatorType, cqlTypeString, "non-dropped type")
                                       .prepare(keyspace, types);
            type = cqlType.getType();

            droppedCqlType = CQLFragmentParser.parseAny(CqlParser::comparatorType, droppedCqlTypeString, "dropped type")
                                              .prepare(keyspace, types);
            // NOTE: TupleType is *always* parsed as frozen, but never toString()'d with the surrounding FrozenType
            droppedType = droppedCqlType.getType();
        }

        @Override
        public String toString()
        {
            return "TypeDef{\n" +
                   "typeString='" + typeString + "'\n" +
                   ", type=" + type + '\n' +
                   ", cqlTypeString='" + cqlTypeString + "'\n" +
                   ", cqlType=" + cqlType + '\n' +
                   ", droppedType=" + droppedType + '\n' +
                   ", droppedCqlTypeString='" + droppedCqlTypeString + "'\n" +
                   ", droppedCqlType=" + droppedCqlType + '\n' +
                   '}';
        }
    }

    private static final TypeDef text = new TypeDef(
            "org.apache.cassandra.db.marshal.UTF8Type",
            "text",
            "text",
            false,
            "'foobar'");

    private static final TypeDef tuple_text__text_ = new TypeDef(
            "org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)",
            "tuple<text, text>",
            "frozen<tuple<text, text>>",
            false,
            "('foo','bar')");

    // Currently, dropped non-frozen-UDT columns are recorded as frozen<tuple<...>>, which is technically wrong
    //private static final TypeDef mc_udt = new TypeDef(
    //        "org.apache.cassandra.db.marshal.UserType(ks,6d635f756474,61:org.apache.cassandra.db.marshal.UTF8Type,62:org.apache.cassandra.db.marshal.UTF8Type)",
    //        "mc_udt",
    //        "tuple<text, text>",
    //        true,
    //        "{a:'foo',b:'bar'}");

    private static final TypeDef frozen_f_udt_ = new TypeDef(
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.UserType(ks,665f756474,61:org.apache.cassandra.db.marshal.UTF8Type,62:org.apache.cassandra.db.marshal.UTF8Type))",
            "frozen<f_udt>",
            "frozen<tuple<text, text>>",
            false,
            "{a:'foo',b:'bar'}");

    private static final TypeDef list_text_ = new TypeDef(
            "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)",
            "list<text>",
            "list<text>",
            true,
            "['foobar']");

    private static final TypeDef frozen_list_text__ = new TypeDef(
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type))",
            "frozen<list<text>>",
            "frozen<list<text>>",
            true,
            "['foobar']");

    private static final TypeDef set_text_ = new TypeDef(
            "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)",
            "set<text>",
            "set<text>",
            true,
            "{'foobar'}");

    private static final TypeDef frozen_set_text__ = new TypeDef(
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type))",
            "frozen<set<text>>",
            "frozen<set<text>>",
            true,
            "{'foobar'}");

    private static final TypeDef map_text__text_ = new TypeDef(
            "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)",
            "map<text, text>",
            "map<text, text>",
            true,
            "{'foo':'bar'}");

    private static final TypeDef frozen_map_text__text__ = new TypeDef(
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type))",
            "frozen<map<text, text>>",
            "frozen<map<text, text>>",
            true,
            "{'foo':'bar'}");

    private static final TypeDef list_frozen_tuple_text__text___ = new TypeDef(
            // in consequence, this should be:
            // "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)))",
            "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type))",
            "list<frozen<tuple<text, text>>>",
            "list<frozen<tuple<text, text>>>",
            true,
            "[('foo','bar')]");

    private static final TypeDef frozen_list_tuple_text__text___ = new TypeDef(
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)))",
            "frozen<list<frozen<tuple<text, text>>>>",
            "frozen<list<frozen<tuple<text, text>>>>",
            true,
            "[('foo','bar')]");

    private static final TypeDef set_frozen_tuple_text__text___ = new TypeDef(
            // in consequence, this should be:
            // "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)))",
            "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type))",
            "set<frozen<tuple<text, text>>>",
            "set<frozen<tuple<text, text>>>",
            true,
            "{('foo','bar')}");

    private static final TypeDef frozen_set_tuple_text__text___ = new TypeDef(
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)))",
            "frozen<set<frozen<tuple<text, text>>>>",
            "frozen<set<frozen<tuple<text, text>>>>",
            true,
            "{('foo','bar')}");

    private static final TypeDef map_text__frozen_tuple_text__text___ = new TypeDef(
            // in consequence, this should be:
            // "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)))",
            "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type))",
            "map<text, frozen<tuple<text, text>>>",
            "map<text, frozen<tuple<text, text>>>",
            true,
            "{'foobar':('foo','bar')}");

    private static final TypeDef frozen_map_text__tuple_text__text___ = new TypeDef(
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)))",
            "frozen<map<text, frozen<tuple<text, text>>>>",
            "frozen<map<text, frozen<tuple<text, text>>>>",
            true,
            "{'foobar':('foo','bar')}");

    private static final TypeDef list_frozen_i_udt__ = new TypeDef(
            "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.UserType(ks,695f756474,61:org.apache.cassandra.db.marshal.UTF8Type,62:org.apache.cassandra.db.marshal.UTF8Type)))",
            "list<frozen<i_udt>>",
            "list<frozen<tuple<text, text>>>",
            true,
            "[{a:'foo',b:'bar'}]");

    private static final TypeDef frozen_list_i_udt__ = new TypeDef(
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UserType(ks,695f756474,61:org.apache.cassandra.db.marshal.UTF8Type,62:org.apache.cassandra.db.marshal.UTF8Type)))",
            "frozen<list<frozen<i_udt>>>",
            "frozen<list<frozen<tuple<text, text>>>>",
            true,
            "[{a:'foo',b:'bar'}]");

    private static final TypeDef set_frozen_i_udt__ = new TypeDef(
            "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.UserType(ks,695f756474,61:org.apache.cassandra.db.marshal.UTF8Type,62:org.apache.cassandra.db.marshal.UTF8Type)))",
            "set<frozen<i_udt>>",
            "set<frozen<tuple<text, text>>>",
            true,
            "{{a:'foo',b:'bar'}}");

    private static final TypeDef frozen_set_i_udt__ = new TypeDef(
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UserType(ks,695f756474,61:org.apache.cassandra.db.marshal.UTF8Type,62:org.apache.cassandra.db.marshal.UTF8Type)))",
            "frozen<set<frozen<i_udt>>>",
            "frozen<set<frozen<tuple<text, text>>>>",
            true,
            "{{a:'foo',b:'bar'}}");

    private static final TypeDef map_text__frozen_i_udt__ = new TypeDef(
            "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.UserType(ks,695f756474,61:org.apache.cassandra.db.marshal.UTF8Type,62:org.apache.cassandra.db.marshal.UTF8Type)))",
            "map<text, frozen<i_udt>>",
            "map<text, frozen<tuple<text, text>>>",
            true,
            "{'foobar':{a:'foo',b:'bar'}}");

    private static final TypeDef frozen_map_text__i_udt__ = new TypeDef(
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UserType(ks,695f756474,61:org.apache.cassandra.db.marshal.UTF8Type,62:org.apache.cassandra.db.marshal.UTF8Type)))",
            "frozen<map<text, frozen<i_udt>>>",
            "frozen<map<text, frozen<tuple<text, text>>>>",
            true,
            "{'foobar':{a:'foo',b:'bar'}}");

    private static final TypeDef[] allTypes = {
            text,
            tuple_text__text_,
            frozen_f_udt_,
            list_text_,
            frozen_list_text__,
            set_text_,
            frozen_set_text__,
            map_text__text_,
            frozen_map_text__text__,
            list_frozen_tuple_text__text___,
            frozen_list_tuple_text__text___,
            set_frozen_tuple_text__text___,
            frozen_set_tuple_text__text___,
            map_text__frozen_tuple_text__text___,
            frozen_map_text__tuple_text__text___,
            list_frozen_i_udt__,
            frozen_list_i_udt__,
            set_frozen_i_udt__,
            frozen_set_i_udt__,
            map_text__frozen_i_udt__,
            frozen_map_text__i_udt__,
            };

    @Ignore("Only used to ")
    @Test
    public void generateCqlStatements() throws InterruptedException
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        pw.println("DROP TABLE sstableheaderfixtest;");
        pw.println();
        pw.println("CREATE TYPE i_udt (a text, b text);");
        pw.println("CREATE TYPE f_udt (a text, b text);");
        pw.println("CREATE TYPE mc_udt (a text, b text);");
        pw.println();
        pw.println("CREATE TABLE sstableheaderfixtest (");
        pw.print("  id int PRIMARY KEY");
        for (TypeDef typeDef : allTypes)
        {
            String cname = typeDef.cqlTypeString.replaceAll("[, <>]", "_");
            pw.printf(",%n  %s %s", cname, typeDef.cqlTypeString);
        }
        pw.println(");");
        pw.println();

        pw.printf("INSERT INTO sstableheaderfixtest%n  (id");
        for (TypeDef typeDef : allTypes)
        {
            String cname = typeDef.cqlTypeString.replaceAll("[, <>]", "_");
            pw.printf(",%n    %s", cname);
        }
        pw.printf(")%n  VALUES%n  (1");
        for (TypeDef typeDef : allTypes)
        {
            pw.printf(",%n    %s", typeDef.cqlValue);
        }
        pw.println(");");

        pw.println();
        pw.println();
        pw.println("-- Run tools/bin/sstablemetadata data/data/<keyspace>/<table>/*-Data.db to show the sstable");
        pw.println("-- serialization-header (types not shown in the C* 3.0 variant of the sstablemetadata tool)");

        sw.flush();

        System.out.println(sw.toString());

        Thread.sleep(1000);
    }

    @Test
    public void verifyTypes()
    {
        AssertionError master = null;
        for (TypeDef typeDef : allTypes)
        {
            try
            {
                assertEquals(typeDef.toString() + "\n typeString vs type\n", typeDef.typeString, typeDef.type.toString());
                assertEquals(typeDef.toString() + "\n typeString vs cqlType.getType()\n", typeDef.typeString, typeDef.cqlType.getType().toString());
                AbstractType<?> expanded = typeDef.type.expandUserTypes();
                CQL3Type expandedCQL = expanded.asCQL3Type();
                // Note: cannot include this commented-out assertion, because the parsed CQL3Type instance for
                // 'frozen<list<tuple<text, text>>>' returns 'frozen<list<frozen<tuple<text, text>>>>' via it's CQL3Type.toString()
                // implementation.
                assertEquals(typeDef.toString() + "\n droppedCqlType\n", typeDef.droppedCqlType, expandedCQL);
                assertEquals(typeDef.toString() + "\n droppedCqlTypeString\n", typeDef.droppedCqlTypeString, expandedCQL.toString());
                assertEquals(typeDef.toString() + "\n multiCell\n", typeDef.type.isMultiCell(), typeDef.droppedType.isMultiCell());

                AbstractType<?> parsedType = TypeParser.parse(typeDef.typeString);
                assertEquals(typeDef.toString(), typeDef.typeString, parsedType.toString());
            }
            catch (AssertionError ae)
            {
                if (master == null)
                    master = ae;
                else
                    master.addSuppressed(ae);
            }
        }
        if (master != null)
            throw master;
    }
}
