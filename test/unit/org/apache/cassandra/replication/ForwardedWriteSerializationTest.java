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
package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.CassandraTestBase;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class ForwardedWriteSerializationTest extends CassandraTestBase
{
    private static final String KEYSPACE = "test_forwarded_write_ks";
    private static final String TABLE = "tbl";

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    private static Mutation createMutation(String key, String value)
    {
        TableMetadata cfm = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        PartitionUpdate update = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), key)
                                 .clustering("c1")
                                 .add("val", ByteBufferUtil.bytes(value))
                                 .buildUpdate();
        return new Mutation(update);
    }

    @Test
    public void testRoundtripCurrentVersion() throws IOException
    {
        Mutation mutation = createMutation("key1", "val1");
        Participants live = new Participants(List.of(1, 2));
        Participants down = new Participants(List.of(3));
        Epoch epoch = Epoch.create(10);
        ForwardedWrite.MutationRequest request = new ForwardedWrite.MutationRequest(mutation, live, down, epoch);

        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            ForwardedWrite.serializer.serialize(request, output, Version.CURRENT);
            Assert.assertEquals(ForwardedWrite.serializer.serializedSize(request, Version.CURRENT), output.getLength());

            DataInputBuffer in = new DataInputBuffer(output.buffer(), false);
            ForwardedWrite.MutationRequest deserialized = ForwardedWrite.serializer.deserialize(in, Version.CURRENT);
            Assert.assertEquals(request.key(), deserialized.key());
        }
    }

    @Test
    public void testV1Compatibility() throws IOException
    {
        Mutation mutation = createMutation("key1", "val1");
        Participants live = new Participants(List.of(1, 2));
        Participants down = new Participants(List.of(3));
        Epoch epoch = Epoch.create(10);
        ForwardedWrite.MutationRequest request = new ForwardedWrite.MutationRequest(mutation, live, down, epoch);

        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            ForwardedWrite.serializer.serialize(request, output, Version.V1);
            Assert.assertEquals(ForwardedWrite.serializer.serializedSize(request, Version.V1), output.getLength());

            DataInputBuffer in = new DataInputBuffer(output.buffer(), false);
            ForwardedWrite.MutationRequest deserialized = ForwardedWrite.serializer.deserialize(in, Version.V1);
            Assert.assertEquals(request.key(), deserialized.key());
        }
    }
}
