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
package org.apache.cassandra.utils;

import java.io.*;

import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.vint.EncodedDataInputStream;
import org.apache.cassandra.utils.vint.EncodedDataOutputStream;


public class EncodedStreamsTest
{
    private static final String KEYSPACE1 = "Keyspace1";
    private static final String CF_STANDARD = "Standard1";
    private static final String CF_COUNTER = "Counter1";
    private int version = MessagingService.current_version;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
    SchemaLoader.prepareServer();
    SchemaLoader.createKeyspace(KEYSPACE1,
            SimpleStrategy.class,
            KSMetaData.optsWithRF(1),
            SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD),
            SchemaLoader.counterCFMD(KEYSPACE1, CF_COUNTER));
    }

    @Test
    public void testStreams() throws IOException
    {
        ByteArrayOutputStream byteArrayOStream1 = new ByteArrayOutputStream();
        EncodedDataOutputStream odos = new EncodedDataOutputStream(byteArrayOStream1);

        ByteArrayOutputStream byteArrayOStream2 = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteArrayOStream2);
        
        for (short i = 0; i < 10000; i++)
        {
            out.writeShort(i);
            odos.writeShort(i);
        }
        out.flush();
        odos.flush();

        for (int i = Short.MAX_VALUE; i < ((int)Short.MAX_VALUE + 10000); i++)
        {
            out.writeInt(i);
            odos.writeInt(i);
        }
        out.flush();
        odos.flush();

        for (long i = Integer.MAX_VALUE; i < ((long)Integer.MAX_VALUE + 10000);i++)
        {
            out.writeLong(i);
            odos.writeLong(i);
        }
        out.flush();
        odos.flush();
        Assert.assertTrue(byteArrayOStream1.size() < byteArrayOStream2.size());

        ByteArrayInputStream byteArrayIStream1 = new ByteArrayInputStream(byteArrayOStream1.toByteArray());
        EncodedDataInputStream idis = new EncodedDataInputStream(new DataInputStream(byteArrayIStream1));

        // assert reading Short
        for (int i = 0; i < 10000; i++)
            Assert.assertEquals(i, idis.readShort());

        // assert reading Integer
        for (int i = Short.MAX_VALUE; i < ((int)Short.MAX_VALUE + 10000); i++)
            Assert.assertEquals(i, idis.readInt());

        // assert reading Long
        for (long i = Integer.MAX_VALUE; i < ((long)Integer.MAX_VALUE) + 1000; i++)
            Assert.assertEquals(i, idis.readLong());
    }

    private UnfilteredRowIterator createTable()
    {
        CFMetaData cfm = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).metadata;

        RowUpdateBuilder builder = new RowUpdateBuilder(cfm, 0, "key");

        builder.clustering("vijay").add(cfm.partitionColumns().iterator().next(), "try").build();
        builder.clustering("to").add(cfm.partitionColumns().iterator().next(), "be_nice").build();

        return builder.unfilteredIterator();
    }

    private UnfilteredRowIterator createCounterTable()
    {
        CFMetaData cfm = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUNTER).metadata;
        RowUpdateBuilder builder = new RowUpdateBuilder(cfm, 0, "key");

        builder.clustering("vijay").add(cfm.partitionColumns().iterator().next(), 1L).build();
        builder.clustering("wants").add(cfm.partitionColumns().iterator().next(), 1000000L).build();

        return builder.unfilteredIterator();
    }

    @Test
    public void testCFSerialization() throws IOException
    {
        ByteArrayOutputStream byteArrayOStream1 = new ByteArrayOutputStream();
        EncodedDataOutputStream odos = new EncodedDataOutputStream(byteArrayOStream1);
        UnfilteredRowIteratorSerializer.serializer.serialize(createTable(), odos, version, 1);

        ByteArrayInputStream byteArrayIStream1 = new ByteArrayInputStream(byteArrayOStream1.toByteArray());
        EncodedDataInputStream odis = new EncodedDataInputStream(new DataInputStream(byteArrayIStream1));
        UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(odis, version, SerializationHelper.Flag.LOCAL);
        Assert.assertTrue(Iterators.elementsEqual(partition, createTable()));
        Assert.assertEquals(byteArrayOStream1.size(), (int) UnfilteredRowIteratorSerializer.serializer.serializedSize(createTable(), version, 1, TypeSizes.VINT));
    }

    @Test
    public void testCounterCFSerialization() throws IOException
    {
        ByteArrayOutputStream byteArrayOStream1 = new ByteArrayOutputStream();
        EncodedDataOutputStream odos = new EncodedDataOutputStream(byteArrayOStream1);
        UnfilteredRowIteratorSerializer.serializer.serialize(createCounterTable(), odos, version, 1);

        ByteArrayInputStream byteArrayIStream1 = new ByteArrayInputStream(byteArrayOStream1.toByteArray());
        EncodedDataInputStream odis = new EncodedDataInputStream(new DataInputStream(byteArrayIStream1));
        UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(odis, version, SerializationHelper.Flag.LOCAL);
        Assert.assertTrue(Iterators.elementsEqual(partition, createCounterTable()));
        Assert.assertEquals(byteArrayOStream1.size(), (int) UnfilteredRowIteratorSerializer.serializer.serializedSize(createCounterTable(), version, 1, TypeSizes.VINT));
    }
}

