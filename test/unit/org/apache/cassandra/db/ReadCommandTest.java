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

package org.apache.cassandra.db;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class ReadCommandTest
{
    private static final String KEYSPACE = "ReadCommandTest";
    private static final String CF1 = "Standard1";
    private static final String CF2 = "Standard2";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        CFMetaData metadata1 = SchemaLoader.standardCFMD(KEYSPACE, CF1);

        CFMetaData metadata2 = CFMetaData.Builder.create(KEYSPACE, CF2)
                                                         .addPartitionKey("key", BytesType.instance)
                                                         .addClusteringColumn("col", AsciiType.instance)
                                                         .addRegularColumn("a", AsciiType.instance)
                                                         .addRegularColumn("b", AsciiType.instance).build();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    metadata1,
                                    metadata2);
    }

    @Test
    public void testPartitionRangeAbort() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1);

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key1"))
                .clustering("Column1")
                .add("val", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key2"))
                .clustering("Column1")
                .add("val", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        ReadCommand readCommand = Util.cmd(cfs).build();
        assertEquals(2, Util.getAll(readCommand).size());

        readCommand.abort();
        assertEquals(0, Util.getAll(readCommand).size());
    }

    @Test
    public void testSinglePartitionSliceAbort() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);

        cfs.truncateBlocking();

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key"))
                .clustering("cc")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key"))
                .clustering("dd")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).build();

        List<FilteredPartition> partitions = Util.getAll(readCommand);
        assertEquals(1, partitions.size());
        assertEquals(2, partitions.get(0).rowCount());

        readCommand.abort();
        assertEquals(0, Util.getAll(readCommand).size());
    }

    @Test
    public void testSinglePartitionNamesAbort() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);

        cfs.truncateBlocking();

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key"))
                .clustering("cc")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key"))
                .clustering("dd")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).includeRow("cc").includeRow("dd").build();

        List<FilteredPartition> partitions = Util.getAll(readCommand);
        assertEquals(1, partitions.size());
        assertEquals(2, partitions.get(0).rowCount());

        readCommand.abort();
        assertEquals(0, Util.getAll(readCommand).size());
    }
}
