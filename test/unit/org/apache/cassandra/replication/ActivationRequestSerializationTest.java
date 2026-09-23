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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.CassandraTestBase;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class ActivationRequestSerializationTest extends CassandraTestBase
{

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataTestHelper.setInstanceForTest();
    }

    @Test
    public void testRoundtripPreparePhase() throws IOException
    {
        Pair<InetAddressAndPort, InetAddressAndPort> pair = Pair.create(InetAddressAndPort.getLocalHost(), InetAddressAndPort.getLocalHost());
        TimeUUID planId = nextTimeUUID();
        ShortMutationId transferId = new ShortMutationId(1L, 100);
        NodeId coordinatorId = new NodeId(1);
        ActivationRequest.Phase phase = ActivationRequest.Phase.PREPARE;
        String keyspace = "test_ks";
        Range<Token> range = new Range<>(new ByteOrderedPartitioner.BytesToken("key1".getBytes()), new ByteOrderedPartitioner.BytesToken("key100".getBytes()));

        ActivationRequest activation = new ActivationRequest(StreamOperation.IMPORT, pair, phase, transferId, coordinatorId, range, 42L, keyspace, planId);

        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            Serializers.testSerde(output, ActivationRequest.serializer, activation, Version.CURRENT);
        }
    }

    @Test
    public void testRoundtripCommitPhase() throws IOException
    {
        Pair<InetAddressAndPort, InetAddressAndPort> pair = Pair.create(InetAddressAndPort.getLocalHost(), InetAddressAndPort.getLocalHost());
        TimeUUID planId = nextTimeUUID();
        ShortMutationId transferId = new ShortMutationId(2L, 300);
        NodeId coordinatorId = new NodeId(2);
        ActivationRequest.Phase phase = ActivationRequest.Phase.COMMIT;
        String keyspace = "test_ks";
        Range<Token> range = new Range<>(new ByteOrderedPartitioner.BytesToken("key1".getBytes()), new ByteOrderedPartitioner.BytesToken("key100".getBytes()));

        ActivationRequest activation = new ActivationRequest(StreamOperation.IMPORT, pair, phase, transferId, coordinatorId, range, 42L, keyspace, planId);

        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            Serializers.testSerde(output, ActivationRequest.serializer, activation, Version.CURRENT);
        }
    }
}
