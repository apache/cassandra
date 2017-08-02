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
package org.apache.cassandra.db;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.*;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class PartitionRangeReadTest
{
    public static final String KEYSPACE1 = "PartitionRangeReadTest1";
    public static final String KEYSPACE2 = "PartitionRangeReadTest2";
    public static final String CF_STANDARD1 = "Standard1";
    public static final String CF_STANDARDINT = "StandardInteger1";
    public static final String CF_COMPACT1 = "Compact1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.denseCFMD(KEYSPACE1, CF_STANDARDINT, IntegerType.instance),
                                    TableMetadata.builder(KEYSPACE1, CF_COMPACT1)
                                                 .isCompound(false)
                                                 .addPartitionKeyColumn("key", AsciiType.instance)
                                                 .addClusteringColumn("column1", AsciiType.instance)
                                                 .addRegularColumn("value", AsciiType.instance)
                                                 .addStaticColumn("val", AsciiType.instance));
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD1));
    }

    @Test
    public void testInclusiveBounds()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_STANDARD1);
        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key1"))
                .clustering("cc1")
                .add("val", "asdf").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key2"))
                .clustering("cc2")
                .add("val", "asdf").build().applyUnsafe();

        assertEquals(2, Util.getAll(Util.cmd(cfs).fromIncl("cc1").toIncl("cc2").build()).size());
    }

    @Test
    public void testCassandra6778() throws CharacterCodingException
    {
        String cfname = CF_STANDARDINT;
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();

        ByteBuffer col = ByteBufferUtil.bytes("val");
        ColumnMetadata cDef = cfs.metadata().getColumn(col);

        // insert two columns that represent the same integer but have different binary forms (the
        // second one is padded with extra zeros)
        new RowUpdateBuilder(cfs.metadata(), 0, "k1")
                .clustering(new BigInteger(new byte[]{1}))
                .add("val", "val1")
                .build()
                .applyUnsafe();
        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata(), 1, "k1")
                .clustering(new BigInteger(new byte[]{0, 0, 1}))
                .add("val", "val2")
                .build()
                .applyUnsafe();
        cfs.forceBlockingFlush();

        // fetch by the first column name; we should get the second version of the column value
        Row row = Util.getOnlyRow(Util.cmd(cfs, "k1").includeRow(new BigInteger(new byte[]{1})).build());
        assertTrue(row.getCell(cDef).value().equals(ByteBufferUtil.bytes("val2")));

        // fetch by the second column name; we should get the second version of the column value
        row = Util.getOnlyRow(Util.cmd(cfs, "k1").includeRow(new BigInteger(new byte[]{0, 0, 1})).build());
        assertTrue(row.getCell(cDef).value().equals(ByteBufferUtil.bytes("val2")));
    }

    @Test
    public void testLimits()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COMPACT1);
        for (int i = 0; i < 10; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), 0, Integer.toString(i))
            .add("val", "abcd")
            .build()
            .applyUnsafe();

            new RowUpdateBuilder(cfs.metadata(), 0, Integer.toString(i))
            .clustering("column1")
            .add("value", "")
            .build()
            .applyUnsafe();
        }

        assertEquals(10, Util.getAll(Util.cmd(cfs).build()).size());

        for (int i = 0; i < 10; i++)
            assertEquals(i, Util.getAll(Util.cmd(cfs).withLimit(i).build()).size());
    }

    @Test
    public void testRangeSliceInclusionExclusion() throws Throwable
    {
        String keyspaceName = KEYSPACE1;
        String cfName = CF_STANDARD1;
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        for (int i = 0; i < 10; ++i)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 10, String.valueOf(i));
            builder.clustering("c");
            builder.add("val", String.valueOf(i));
            builder.build().applyUnsafe();
        }

        cfs.forceBlockingFlush();

        ColumnMetadata cDef = cfs.metadata().getColumn(ByteBufferUtil.bytes("val"));

        List<FilteredPartition> partitions;

        // Start and end inclusive
        partitions = Util.getAll(Util.cmd(cfs).fromKeyIncl("2").toKeyIncl("7").build());
        assertEquals(6, partitions.size());
        assertTrue(partitions.get(0).iterator().next().getCell(cDef).value().equals(ByteBufferUtil.bytes("2")));
        assertTrue(partitions.get(partitions.size() - 1).iterator().next().getCell(cDef).value().equals(ByteBufferUtil.bytes("7")));

        // Start and end excluded
        partitions = Util.getAll(Util.cmd(cfs).fromKeyExcl("2").toKeyExcl("7").build());
        assertEquals(4, partitions.size());
        assertTrue(partitions.get(0).iterator().next().getCell(cDef).value().equals(ByteBufferUtil.bytes("3")));
        assertTrue(partitions.get(partitions.size() - 1).iterator().next().getCell(cDef).value().equals(ByteBufferUtil.bytes("6")));

        // Start excluded, end included
        partitions = Util.getAll(Util.cmd(cfs).fromKeyExcl("2").toKeyIncl("7").build());
        assertEquals(5, partitions.size());
        assertTrue(partitions.get(0).iterator().next().getCell(cDef).value().equals(ByteBufferUtil.bytes("3")));
        assertTrue(partitions.get(partitions.size() - 1).iterator().next().getCell(cDef).value().equals(ByteBufferUtil.bytes("7")));

        // Start included, end excluded
        partitions = Util.getAll(Util.cmd(cfs).fromKeyIncl("2").toKeyExcl("7").build());
        assertEquals(5, partitions.size());
        assertTrue(partitions.get(0).iterator().next().getCell(cDef).value().equals(ByteBufferUtil.bytes("2")));
        assertTrue(partitions.get(partitions.size() - 1).iterator().next().getCell(cDef).value().equals(ByteBufferUtil.bytes("6")));
    }
}

