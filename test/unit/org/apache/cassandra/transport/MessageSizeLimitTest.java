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

package org.apache.cassandra.transport;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.transport.messages.QueryMessage;

public class MessageSizeLimitTest extends CQLTester
{
    // set MAX_CQL_MESSAGE_SIZE less than a frame size to be able to test both decoding options on a server side:
    // - org.apache.cassandra.transport.CQLMessageHandler.processOneContainedMessage
    // - org.apache.cassandra.transport.CQLMessageHandler.processFirstFrameOfLargeMessage
    private static final int MAX_CQL_MESSAGE_SIZE = FrameEncoder.Payload.MAX_SIZE - 1000;
    private static final int TOO_BIG_SINGLE_FRAME_MESSAGE_SIZE = FrameEncoder.Payload.MAX_SIZE - 500;
    private static final int TOO_BIG_MULTI_FRAME_MESSAGE_SIZE = 2 * FrameEncoder.Payload.MAX_SIZE;


    private static final QueryOptions V5_DEFAULT_OPTIONS =
    QueryOptions.create(QueryOptions.DEFAULT.getConsistency(),
                        QueryOptions.DEFAULT.getValues(),
                        QueryOptions.DEFAULT.skipMetadata(),
                        QueryOptions.DEFAULT.getPageSize(),
                        QueryOptions.DEFAULT.getPagingState(),
                        QueryOptions.DEFAULT.getSerialConsistency(),
                        ProtocolVersion.V5,
                        KEYSPACE);

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.setNativeTransportReceiveQueueCapacityInBytes(1);
        DatabaseDescriptor.setNativeTransportMaxRequestDataInFlightPerIpInBytes(MAX_CQL_MESSAGE_SIZE);
        DatabaseDescriptor.setNativeTransportConcurrentRequestDataInFlightInBytes(MAX_CQL_MESSAGE_SIZE);
        DatabaseDescriptor.setNativeTransportMaxMessageSizeInBytes(MAX_CQL_MESSAGE_SIZE);


        requireNetwork();
    }

    @Before
    public void setLimits()
    {
        ClientResourceLimits.setGlobalLimit(MAX_CQL_MESSAGE_SIZE);
        ClientResourceLimits.setEndpointLimit(MAX_CQL_MESSAGE_SIZE);
    }

    @After
    public void dropCreatedTable()
    {
        try
        {
            QueryProcessor.executeOnceInternal("DROP TABLE " + KEYSPACE + ".test_table");
        }
        catch (Throwable t)
        {
            // ignore
        }
    }

    @SuppressWarnings({"resource", "SameParameterValue"})
    private SimpleClient client()
    {
        try
        {
            return SimpleClient.builder(nativeAddr.getHostAddress(), nativePort)
                               .protocolVersion(ProtocolVersion.V5)
                               .useBeta()
                               .build()
                               .connect(false, false);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error initializing client", e);
        }
    }

    @Test
    public void sendMessageWithSizeMoreThanMaxMessageSizeAndLessThanFrameSize()
    {
        sendTooBigMessage(TOO_BIG_SINGLE_FRAME_MESSAGE_SIZE);
    }

    @Test
    public void sendMessageWithSizeMoreThanMaxMessageSizeAndMoreThanFrameSize()
    {
        sendTooBigMessage(TOO_BIG_MULTI_FRAME_MESSAGE_SIZE);
    }

    private void sendTooBigMessage(int valueSize)
    {
        doTest((client) ->
               {
                   QueryMessage queryMessage = createQueryMessage(valueSize);
                   try
                   {
                       client.execute(queryMessage);
                   } catch (RuntimeException e) {
                       // ProtocolException: CQL Message of size 120070 bytes exceeds allowed maximum of 60000 bytes
                       Assert.assertTrue(e.getCause() instanceof ProtocolException);
                   }
                   Util.spinAssertEquals(false, () -> client.connection.channel().isOpen(), 10);

               }
        );
    }

    @Test
    public void sendMessageWithSizeBelowLimit()
    {
        doTest((client) ->
               {
                   int valueLessThanMessageMaxSize = MAX_CQL_MESSAGE_SIZE - 500;
                   QueryMessage queryMessage = createQueryMessage(valueLessThanMessageMaxSize);
                   client.execute(queryMessage);

                   // run one more time, to validate that the connection is still alive
                   queryMessage = createQueryMessage(valueLessThanMessageMaxSize);
                   client.execute(queryMessage);
               }
        );
    }

    private void doTest(TestLogic testLogic)
    {
        try (SimpleClient client = client())
        {
            QueryMessage queryMessage = new QueryMessage("CREATE TABLE test_table (pk int PRIMARY KEY, v text)",
                                                         V5_DEFAULT_OPTIONS);
            client.execute(queryMessage);
            testLogic.run(client);
        }
    }
    private interface TestLogic
    {
        void run(SimpleClient simpleClient);
    }

    private QueryMessage createQueryMessage(int valueSize)
    {
        StringBuilder query = new StringBuilder("INSERT INTO test_table (pk, v) VALUES (1, '");
        for (int i=0; i < valueSize; i++)
            query.append('a');
        query.append("')");
        return new QueryMessage(query.toString(), V5_DEFAULT_OPTIONS);
    }
}
