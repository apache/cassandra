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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.transport.messages.QueryMessage;

public class MessageSizeLimitTest extends CQLTester
{
    private static final int MAX_CQL_MESSAGE_SIZE = FrameEncoder.Payload.MAX_SIZE * 3;
    private static final int TOO_BIG_MESSAGE_SIZE = MAX_CQL_MESSAGE_SIZE * 2;
    private static final int NORMAL_MESSAGE_SIZE = MAX_CQL_MESSAGE_SIZE - 500;
    private static final QueryOptions V5_DEFAULT_OPTIONS = QueryOptions.create(QueryOptions.DEFAULT.getConsistency(),
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
    public void sendMessageWithSizeMoreThanMaxMessageSize()
    {
        runClientLogic((client) ->
               {
                   try
                   {
                       QueryMessage tooBigQueryMessage = createQueryMessage(TOO_BIG_MESSAGE_SIZE);
                       client.execute(tooBigQueryMessage);
                   } catch (RuntimeException e) {
                       // InvalidRequestException: CQL Message of size 524362 bytes exceeds allowed maximum of 262144 bytes
                       Assert.assertTrue(e.getCause() instanceof InvalidRequestException);
                   }

                   // we send one more message to check that the server continues to process new messages in the opened connection
                   QueryMessage queryMessage = createQueryMessage(NORMAL_MESSAGE_SIZE);
                   client.execute(queryMessage);
               }
        );
    }

    @Test(timeout = 30_000)
    public void checkThatThereIsNoStarvationForMultiFrameMessages() throws InterruptedException
    {
        runClientLogic((client) -> {}, true); // to create table
        AtomicInteger completedSuccessfully = new AtomicInteger(0);
        int threadsCount = 2;
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadsCount; i++)
        {
            threads.add(new Thread(() -> runClientLogic((client) -> {
                    sendMessages(client, 100, NORMAL_MESSAGE_SIZE);
                    completedSuccessfully.incrementAndGet();
                }, false))
            );
        }
        for (Thread thread : threads)
            thread.start();

        for (Thread thread : threads)
            thread.join();

        Assert.assertEquals("not all messages were sent successfully by all threads",
                            threadsCount, completedSuccessfully.get());
    }

    private void sendMessages(SimpleClient client, int messagesCount, int messageSize)
    {
        for (int i = 0; i < messagesCount; i++)
        {
            QueryMessage queryMessage1 = createQueryMessage(messageSize);
            client.execute(queryMessage1);
        }
    }

    @Test
    public void sendMessageWithSizeBelowLimit()
    {
        runClientLogic((client) ->
               {
                   QueryMessage queryMessage = createQueryMessage(NORMAL_MESSAGE_SIZE);
                   client.execute(queryMessage);

                   // run one more time, to validate that the connection is still alive
                   queryMessage = createQueryMessage(NORMAL_MESSAGE_SIZE);
                   client.execute(queryMessage);
               }
        );
    }

    private void runClientLogic(ClientLogic clientLogic)
    {
        runClientLogic(clientLogic, true);
    }

    private void runClientLogic(ClientLogic clientLogic, boolean createTable)
    {
        try (SimpleClient client = client())
        {
            if (createTable)
            {
                QueryMessage queryMessage = new QueryMessage("CREATE TABLE test_table (pk int PRIMARY KEY, v text)",
                                                             V5_DEFAULT_OPTIONS);
                client.execute(queryMessage);
            }
            clientLogic.run(client);
        }
    }
    private interface ClientLogic
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
