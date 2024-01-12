/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db.marshal;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;

import static org.apache.cassandra.Util.makeUDT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class TypeParserTest
{
    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testParse() throws ConfigurationException, SyntaxException
    {
        AbstractType<?> type;

        type = TypeParser.parse(null);
        assert type == BytesType.instance;

        type = TypeParser.parse("");
        assert type == BytesType.instance;

        type = TypeParser.parse("    ");
        assert type == BytesType.instance;

        type = TypeParser.parse("LongType");
        assert type == LongType.instance;

        type = TypeParser.parse("  LongType   ");
        assert type == LongType.instance;

        type = TypeParser.parse("LongType()");
        assert type == LongType.instance;

        type = TypeParser.parse("LongType(reversed=false)");
        assert type == LongType.instance;

        type = TypeParser.parse("LongType(reversed=true)");
        assert type == ReversedType.getInstance(LongType.instance);
        assert ((ReversedType<?>)type).baseType == LongType.instance;

        type = TypeParser.parse("LongType(reversed)");
        assert type == ReversedType.getInstance(LongType.instance);
        assert ((ReversedType<?>)type).baseType == LongType.instance;
    }

    @Test
    public void testParseError()
    {
        try
        {
            TypeParser.parse("y");
            fail("Should not pass");
        }
        catch (ConfigurationException e) {}
        catch (SyntaxException e) {}

        try
        {
            TypeParser.parse("LongType(reversed@)");
            fail("Should not pass");
        }
        catch (ConfigurationException e) {}
        catch (SyntaxException e) {}
    }

    @Test
    public void testParsePartitionerOrder() throws ConfigurationException, SyntaxException
    {
        for (IPartitioner partitioner: new IPartitioner[] { Murmur3Partitioner.instance,
                                                            ByteOrderedPartitioner.instance,
                                                            RandomPartitioner.instance,
                                                            OrderPreservingPartitioner.instance })
        {
            AbstractType<?> type = partitioner.partitionOrdering();
            assertSame(type, TypeParser.parse(type.toString()));
        }
        assertSame(DatabaseDescriptor.getPartitioner().partitionOrdering(), TypeParser.parse("PartitionerDefinedOrder"));
    }

    @Test
    public void testTuple()
    {
        List<TupleType> tupleTypes = Arrays.asList(
                new TupleType(Arrays.asList(UTF8Type.instance, Int32Type.instance), false),
                new TupleType(Arrays.asList(UTF8Type.instance, Int32Type.instance), true),
                new TupleType(Arrays.asList(UTF8Type.instance, new TupleType(Arrays.asList(Int32Type.instance, LongType.instance), false)), false),
                new TupleType(Arrays.asList(UTF8Type.instance, new TupleType(Arrays.asList(Int32Type.instance, LongType.instance), false)), true),
                new TupleType(Arrays.asList(UTF8Type.instance, new TupleType(Arrays.asList(Int32Type.instance, LongType.instance), true)), false),
                new TupleType(Arrays.asList(UTF8Type.instance, new TupleType(Arrays.asList(Int32Type.instance, LongType.instance), true)), true),
                new TupleType(Arrays.asList(UTF8Type.instance, makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), false)), false),
                new TupleType(Arrays.asList(UTF8Type.instance, makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), false)), true),
                new TupleType(Arrays.asList(UTF8Type.instance, makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), true)), false),
                new TupleType(Arrays.asList(UTF8Type.instance, makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), true)), true)
        );

        for (TupleType tupleType : tupleTypes)
        {
            assertEquals(tupleType, TypeParser.parse(tupleType.toString()));
            assertEquals(tupleType.freeze(), TypeParser.parse(tupleType.freeze().toString()));
            assertEquals(tupleType.expandUserTypes(), TypeParser.parse(tupleType.expandUserTypes().toString()));
        }
    }

    @Test
    public void testParseUDT()
    {
        List<UserType> userTypes = Arrays.asList(
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), false),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), true),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", new TupleType(Arrays.asList(Int32Type.instance, LongType.instance), false)), false),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", new TupleType(Arrays.asList(Int32Type.instance, LongType.instance), false)), true),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", new TupleType(Arrays.asList(Int32Type.instance, LongType.instance), true)), false),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", new TupleType(Arrays.asList(Int32Type.instance, LongType.instance), true)), true),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", makeUDT("udt2", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), false)), false),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", makeUDT("udt2", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), false)), true),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", makeUDT("udt2", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), true)), false),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", makeUDT("udt2", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), true)), true)
        );

        for (UserType userType : userTypes)
        {
            assertEquals(userType, TypeParser.parse(userType.toString()));
            assertEquals(userType.freeze(), TypeParser.parse(userType.freeze().toString()));
            assertEquals(userType.expandUserTypes(), TypeParser.parse(userType.expandUserTypes().toString()));
        }
    }
}
