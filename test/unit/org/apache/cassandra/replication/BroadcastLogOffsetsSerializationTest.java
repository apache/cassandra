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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;

public class BroadcastLogOffsetsSerializationTest extends CassandraTestBase
{
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataTestHelper.setInstanceForTest();
    }

    @Test
    public void testRoundtripCurrentVersion() throws IOException
    {
        Range<Token> range = new Range<>(new ByteOrderedPartitioner.BytesToken("key1".getBytes()),
                                         new ByteOrderedPartitioner.BytesToken("key100".getBytes()));
        ShardMetadata shardMetadata = new ShardMetadata("test_ks", 10L, range, new Participants(List.of(1, 2, 3)));
        Offsets.Immutable offsets = new Offsets.Immutable(new CoordinatorLogId(1, 42), new int[]{10, 20});
        BroadcastLogOffsets broadcast = new BroadcastLogOffsets(shardMetadata, List.of(offsets), true);

        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            Serializers.testSerde(output, BroadcastLogOffsets.serializer, broadcast, Version.CURRENT);
        }
    }

    @Test
    public void testV1Compatibility() throws IOException
    {
        Range<Token> range = new Range<>(new ByteOrderedPartitioner.BytesToken("key1".getBytes()),
                                         new ByteOrderedPartitioner.BytesToken("key100".getBytes()));
        ShardMetadata shardMetadata = new ShardMetadata("test_ks", 10L, range, new Participants(List.of(1, 2, 3)));
        Offsets.Immutable offsets = new Offsets.Immutable(new CoordinatorLogId(1, 42), new int[]{10, 20});
        BroadcastLogOffsets broadcast = new BroadcastLogOffsets(shardMetadata, List.of(offsets), true);

        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            BroadcastLogOffsets.serializer.serialize(broadcast, output, Version.V1);
            Assert.assertEquals(BroadcastLogOffsets.serializer.serializedSize(broadcast, Version.V1), output.getLength());

            DataInputBuffer in = new DataInputBuffer(output.buffer(), false);
            BroadcastLogOffsets deserialized = BroadcastLogOffsets.serializer.deserialize(in, Version.V1);
            Assert.assertEquals(broadcast.isEmpty(), deserialized.isEmpty());
        }
    }
}
