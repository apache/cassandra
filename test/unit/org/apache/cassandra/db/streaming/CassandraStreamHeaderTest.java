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

package org.apache.cassandra.db.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.streaming.CassandraStreamHeader.CassandraStreamHeaderSerializer;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.SerializationUtils;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CassandraStreamHeaderTest
{
    @Test
    public void serializerTest()
    {
        String ddl = "CREATE TABLE tbl (k INT PRIMARY KEY, v INT)";
        TableMetadata metadata = CreateTableStatement.parse(ddl, "ks").build();
        CassandraStreamHeader header = new CassandraStreamHeader(BigFormat.latestVersion,
                                                                 SSTableFormat.Type.BIG,
                                                                 0,
                                                                 new ArrayList<>(),
                                                                 ((CompressionMetadata) null),
                                                                 0,
                                                                 SerializationHeader.makeWithoutStats(metadata).toComponent(),
                                                                 metadata.id);

        SerializationUtils.assertSerializationCycle(header, CassandraStreamHeader.serializer);
    }

    @Test
    public void serializerTest_FullSSTableTransfer()
    {
        String ddl = "CREATE TABLE tbl (k INT PRIMARY KEY, v INT)";
        TableMetadata metadata = CreateTableStatement.parse(ddl, "ks").build();

        List<ComponentInfo> ci = ImmutableList.of(new ComponentInfo(Component.Type.DATA, 100));

        CassandraStreamHeader header = new CassandraStreamHeader(BigFormat.latestVersion,
                                                                 SSTableFormat.Type.BIG,
                                                                 0,
                                                                 new ArrayList<>(),
                                                                 ((CompressionMetadata) null),
                                                                 0,
                                                                 SerializationHeader.makeWithoutStats(metadata).toComponent(),
                                                                 ci, true, Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.EMPTY_BYTE_BUFFER), metadata.id);

        SerializationUtils.assertSerializationCycle(header, new TestableCassandraStreamHeaderSerializer());
    }

    private static class TestableCassandraStreamHeaderSerializer extends CassandraStreamHeaderSerializer
    {
        @Override
        public CassandraStreamHeader deserialize(DataInputPlus in, int version) throws IOException
        {
            return deserialize(in, version, tableId -> Murmur3Partitioner.instance);
        }
    }
}
