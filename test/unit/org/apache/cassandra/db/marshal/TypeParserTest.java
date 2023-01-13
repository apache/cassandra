/**
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

import com.google.common.collect.Lists;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;

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
    public void testParsePartitionerOrderWithBaseType() throws ConfigurationException, SyntaxException
    {
        // default partitioner
        assertSame(DatabaseDescriptor.getPartitioner().partitionOrdering(), TypeParser.parse("PartitionerDefinedOrder"));

        // PartitionerDefinedOrder's base type is not composite type
        for (IPartitioner partitioner: new IPartitioner[] { Murmur3Partitioner.instance,
            ByteOrderedPartitioner.instance,
            RandomPartitioner.instance,
            OrderPreservingPartitioner.instance })
        {
            AbstractType<?> type = partitioner.partitionOrdering();
            if (type instanceof PartitionerDefinedOrder)
            {
                PartitionerDefinedOrder tmp = (PartitionerDefinedOrder) type;
                type = tmp.withBaseType(Int32Type.instance);
            }
            System.out.println(type.toString());
            assertEquals(type, TypeParser.parse(type.toString()));
        }
        // PartitionerDefinedOrder's base type is  composite type
        for (IPartitioner partitioner: new IPartitioner[] { Murmur3Partitioner.instance,
            ByteOrderedPartitioner.instance,
            RandomPartitioner.instance,
            OrderPreservingPartitioner.instance })
        {
            AbstractType<?> type = partitioner.partitionOrdering();
            if (type instanceof PartitionerDefinedOrder)
            {
                PartitionerDefinedOrder tmp = (PartitionerDefinedOrder) type;
                type = tmp.withBaseType(CompositeType.getInstance(Int32Type.instance, UTF8Type.instance));
            }
            System.out.println(type.toString());
            assertEquals(type, TypeParser.parse(type.toString()));
        }
        // PartitionerDefinedOrder's base type is  tuple type
        for (IPartitioner partitioner: new IPartitioner[] { Murmur3Partitioner.instance,
            ByteOrderedPartitioner.instance,
            RandomPartitioner.instance,
            OrderPreservingPartitioner.instance })
        {
            AbstractType<?> type = partitioner.partitionOrdering();
            if (type instanceof PartitionerDefinedOrder)
            {
                PartitionerDefinedOrder tmp = (PartitionerDefinedOrder) type;
                type = tmp.withBaseType(new TupleType(Lists.newArrayList(Int32Type.instance, UTF8Type.instance)));
            }
            System.out.println(type.toString());
            assertEquals(type, TypeParser.parse(type.toString()));
        }
        // PartitionerDefinedOrder's base type is  ReversedType
        for (IPartitioner partitioner: new IPartitioner[] { Murmur3Partitioner.instance,
            ByteOrderedPartitioner.instance,
            RandomPartitioner.instance,
            OrderPreservingPartitioner.instance })
        {
            AbstractType<?> type = partitioner.partitionOrdering();
            if (type instanceof PartitionerDefinedOrder)
            {
                PartitionerDefinedOrder tmp = (PartitionerDefinedOrder) type;
                type = tmp.withBaseType(ReversedType.getInstance(Int32Type.instance));
            }
            System.out.println(type.toString());
            assertEquals(type, TypeParser.parse(type.toString()));
        }
        // PartitionerDefinedOrder's base type is  CollectionType
        for (IPartitioner partitioner: new IPartitioner[] { Murmur3Partitioner.instance,
            ByteOrderedPartitioner.instance,
            RandomPartitioner.instance,
            OrderPreservingPartitioner.instance })
        {
            AbstractType<?> type = partitioner.partitionOrdering();
            if (type instanceof PartitionerDefinedOrder)
            {
                PartitionerDefinedOrder tmp = (PartitionerDefinedOrder) type;
                type = tmp.withBaseType(MapType.getInstance(Int32Type.instance, UTF8Type.instance, false));
            }
            System.out.println(type.toString());
            assertEquals(type, TypeParser.parse(type.toString()));
        }
    }

    @Test
    public void testParsePartitionerOrderMistMatch() throws ConfigurationException, SyntaxException
    {
        for (IPartitioner partitioner: new IPartitioner[] { Murmur3Partitioner.instance,
            ByteOrderedPartitioner.instance,
            RandomPartitioner.instance,
            OrderPreservingPartitioner.instance })
        {
            AbstractType<?> type = partitioner.partitionOrdering();
            if (type instanceof PartitionerDefinedOrder)
            {
                PartitionerDefinedOrder tmp = (PartitionerDefinedOrder) type;
                type = tmp.withBaseType(Int32Type.instance);
                boolean result = partitioner.partitionOrdering().equals(TypeParser.parse(type.toString()));
                assertFalse(result);
            }
            else
            {
                // ByteOrderedPartitioner.instance and OrderPreservingPartitioner.instance's partitionOrdering will not be PartitionerDefinedOrder
                boolean result = partitioner.partitionOrdering().equals(TypeParser.parse(type.toString()));
                assertTrue(result);
            }

        }
        assertEquals(DatabaseDescriptor.getPartitioner().partitionOrdering(), TypeParser.parse("PartitionerDefinedOrder"));
    }

    @Test
    public void testParsePartitionerOrderWithErrorFormat()
    {
        for (IPartitioner partitioner: new IPartitioner[] { Murmur3Partitioner.instance,
            ByteOrderedPartitioner.instance,
            RandomPartitioner.instance,
            OrderPreservingPartitioner.instance })
        {
            AbstractType<?> type = partitioner.partitionOrdering();
            if (type instanceof PartitionerDefinedOrder)
            {
                // only Murmur3Partitioner and RandomPartitioner's partitionOrdering() are instanceof PartitionerDefinedOrder
                String msgPartitioner = partitioner instanceof Murmur3Partitioner ? "Murmur3Partitioner" : "RandomPartitioner";
                // error format PartitionerDefinedOrder(org.apache.cassandra.dht.Murmur3Partitioner,
                String tmpStr1 =  type.toString().replace(')', ',');
                try
                {
                    TypeParser.parse(tmpStr1);
                    fail();
                }
                catch (Throwable t)
                {
                    String tmp = t.getCause().getMessage();
                    System.out.println(tmp);
                    assertTrue(t.getCause().getMessage().contains("Syntax error parsing 'org.apache.cassandra.db.marshal.PartitionerDefinedOrder(org.apache.cassandra.dht." + msgPartitioner + ",: for msg unexpected character ','"));
                }

                // error format PartitionerDefinedOrder(org.apache.cassandra.dht.Murmur3Partitioner>
                String tmpStr2 =  type.toString().replace(')', '>');
                try
                {
                    TypeParser.parse(tmpStr2);
                    fail();
                }
                catch (Throwable t)
                {
                    String tmp = t.getCause().getMessage();
                    System.out.println(tmp);
                    assertTrue(t.getCause().getMessage().contains("Syntax error parsing 'org.apache.cassandra.db.marshal.PartitionerDefinedOrder(org.apache.cassandra.dht." + msgPartitioner + ">: for msg unexpected character '>'"));
                }

                // error format PartitionerDefinedOrder(org.apache.cassandra.dht.Murmur3Partitioner>
                String tmpStr3 =  type.toString().replace(')', ':');
                try
                {
                    TypeParser.parse(tmpStr3);
                    fail();
                }
                catch (Throwable t)
                {
                    String tmp = t.getCause().getMessage();
                    System.out.println(tmp);
                    assertTrue(t.getCause().getMessage().contains("Unable to find abstract-type class 'org.apache.cassandra.db.marshal.'"));
                }
            }
        }
        assertSame(DatabaseDescriptor.getPartitioner().partitionOrdering(), TypeParser.parse("PartitionerDefinedOrder"));
    }
}
