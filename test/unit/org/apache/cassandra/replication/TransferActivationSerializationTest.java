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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class TransferActivationSerializationTest
{
    private static final int VERSION = MessagingService.current_version;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testRoundtripPreparePhase() throws IOException
    {
        TimeUUID planId = nextTimeUUID();
        MutationId transferId = new MutationId(1L, 100, 200);
        NodeId coordinatorId = new NodeId(1);
        TransferActivation.Phase phase = TransferActivation.Phase.PREPARE;

        TransferActivation activation = new TransferActivation(planId, transferId, coordinatorId, phase);

        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            Serializers.testSerde(output, TransferActivation.serializer, activation, VERSION);
        }
    }

    @Test
    public void testRoundtripCommitPhase() throws IOException
    {
        TimeUUID planId = nextTimeUUID();
        MutationId transferId = new MutationId(2L, 300, 400);
        NodeId coordinatorId = new NodeId(2);
        TransferActivation.Phase phase = TransferActivation.Phase.COMMIT;

        TransferActivation activation = new TransferActivation(planId, transferId, coordinatorId, phase);

        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            Serializers.testSerde(output, TransferActivation.serializer, activation, VERSION);
        }
    }
}
