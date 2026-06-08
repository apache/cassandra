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

package org.apache.cassandra.tcm.discovery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SurveyRequestHandlerTest
{
    @BeforeClass
    public static void initClusterMetadata()
    {
        ServerTestUtils.prepareServerNoRegister();
    }

    @Test
    public void testRequestWithMismatchingMetadataIdIsRejected() throws IOException
    {
        StubMessageDelivery messaging = new StubMessageDelivery();
        SurveyRequestHandler handler = new SurveyRequestHandler(999, () -> messaging);
        try
        {
            handler.doVerb(Message.out(Verb.TCM_DISCOVER_SURVEY_REQ, new SurveyRequest(0)));
            fail("Expected InvalidRequestException");
        }
        catch (InvalidRequestException e)
        {
            assertEquals("Mismatching metadata id in survey request from /127.0.0.1:7012 (0)",e.getMessage());
        }
    }

    @Test
    public void testRespondWithNodeIdFromSystemTable() throws IOException
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        StubMessageDelivery messaging = new StubMessageDelivery();
        SurveyRequestHandler handler = new SurveyRequestHandler(metadata.metadataIdentifier, () -> messaging);
        assertEquals(NodeId.UNREGISTERED, ClusterMetadata.current().myNodeId());
        NodeId id = new NodeId(555);
        SystemKeyspace.setLocalHostId(id.toUUID());
        handler.doVerb(Message.out(Verb.TCM_DISCOVER_SURVEY_REQ, new SurveyRequest(0)));
        assertEquals(1, messaging.responses.size());
        SurveyResponse response = (SurveyResponse) messaging.responses.get(0);
        assertEquals(id, response.nodeId);
        assertEquals(metadata.metadataIdentifier, response.metadataId);
    }

    private static class StubMessageDelivery implements MessageDelivery
    {

        List<Object> responses = new ArrayList<>();
        @Override
        public <V> void respond(V response, Message<?> message)
        {
            responses.add(response);
        }

        @Override
        public <REQ> void send(Message<REQ> message, InetAddressAndPort to) {}

        @Override
        public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb) {}

        @Override
        public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection) {}

        @Override
        public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to) {return null;}
    }
}
