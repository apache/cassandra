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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.BatchMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.ReflectionUtils;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class MessagePayloadTest extends CQLTester
{
    public static Map<String, ByteBuffer> requestPayload;
    public static Map<String, ByteBuffer> responsePayload;

    private static Field cqlQueryHandlerField;
    private static boolean modifiersAccessible;

    @BeforeClass
    public static void makeCqlQueryHandlerAccessible()
    {
        try
        {
            cqlQueryHandlerField = ClientState.class.getDeclaredField("cqlQueryHandler");
            cqlQueryHandlerField.setAccessible(true);

            Field modifiersField = ReflectionUtils.getModifiersField();
            modifiersAccessible = modifiersField.isAccessible();
            modifiersField.setAccessible(true);
            modifiersField.setInt(cqlQueryHandlerField, cqlQueryHandlerField.getModifiers() & ~Modifier.FINAL);
        }
        catch (IllegalAccessException | NoSuchFieldException e)
        {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void resetCqlQueryHandlerField()
    {
        if (cqlQueryHandlerField == null)
            return;
        try
        {
            Field modifiersField = ReflectionUtils.getModifiersField();
            modifiersField.setAccessible(true);
            modifiersField.setInt(cqlQueryHandlerField, cqlQueryHandlerField.getModifiers() | Modifier.FINAL);

            cqlQueryHandlerField.setAccessible(false);

            modifiersField.setAccessible(modifiersAccessible);
        }
        catch (IllegalAccessException | NoSuchFieldException e)
        {
            throw new RuntimeException(e);
        }
    }

    @After
    public void dropCreatedTable()
    {
        try
        {
            QueryProcessor.executeOnceInternal("DROP TABLE " + KEYSPACE + ".atable");
        }
        catch (Throwable t)
        {
            // ignore
        }
    }

    @Test
    public void testMessagePayloadBeta() throws Throwable
    {
        QueryHandler queryHandler = (QueryHandler) cqlQueryHandlerField.get(null);
        cqlQueryHandlerField.set(null, new TestQueryHandler());
        try
        {
            requireNetwork();

            Assert.assertSame(TestQueryHandler.class, ClientState.getCQLQueryHandler().getClass());

            SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(),
                                                   nativePort,
                                                   ProtocolVersion.V5,
                                                   true,
                                                   new EncryptionOptions());
            try
            {
                client.connect(false);

                Map<String, ByteBuffer> reqMap;
                Map<String, ByteBuffer> respMap;

                QueryOptions queryOptions = QueryOptions.create(
                  QueryOptions.DEFAULT.getConsistency(),
                  QueryOptions.DEFAULT.getValues(),
                  QueryOptions.DEFAULT.skipMetadata(),
                  QueryOptions.DEFAULT.getPageSize(),
                  QueryOptions.DEFAULT.getPagingState(),
                  QueryOptions.DEFAULT.getSerialConsistency(),
                  ProtocolVersion.V5,
                  KEYSPACE);
                QueryMessage queryMessage = new QueryMessage("CREATE TABLE atable (pk int PRIMARY KEY, v text)",
                                                             queryOptions);
                PrepareMessage prepareMessage = new PrepareMessage("SELECT * FROM atable", KEYSPACE);

                reqMap = Collections.singletonMap("foo", bytes(42));
                responsePayload = respMap = Collections.singletonMap("bar", bytes(42));
                queryMessage.setCustomPayload(reqMap);
                Message.Response queryResponse = client.execute(queryMessage);
                payloadEquals(reqMap, requestPayload);
                payloadEquals(respMap, queryResponse.getCustomPayload());

                reqMap = Collections.singletonMap("foo", bytes(43));
                responsePayload = respMap = Collections.singletonMap("bar", bytes(43));
                prepareMessage.setCustomPayload(reqMap);
                ResultMessage.Prepared prepareResponse = (ResultMessage.Prepared) client.execute(prepareMessage);
                payloadEquals(reqMap, requestPayload);
                payloadEquals(respMap, prepareResponse.getCustomPayload());

                ExecuteMessage executeMessage = new ExecuteMessage(prepareResponse.statementId, prepareResponse.resultMetadataId, QueryOptions.DEFAULT);
                reqMap = Collections.singletonMap("foo", bytes(44));
                responsePayload = respMap = Collections.singletonMap("bar", bytes(44));
                executeMessage.setCustomPayload(reqMap);
                Message.Response executeResponse = client.execute(executeMessage);
                payloadEquals(reqMap, requestPayload);
                payloadEquals(respMap, executeResponse.getCustomPayload());

                BatchMessage batchMessage = new BatchMessage(BatchStatement.Type.UNLOGGED,
                                                             Collections.<Object>singletonList("INSERT INTO " + KEYSPACE + ".atable (pk,v) VALUES (1, 'foo')"),
                                                             Collections.singletonList(Collections.<ByteBuffer>emptyList()),
                                                             queryOptions);
                reqMap = Collections.singletonMap("foo", bytes(45));
                responsePayload = respMap = Collections.singletonMap("bar", bytes(45));
                batchMessage.setCustomPayload(reqMap);
                Message.Response batchResponse = client.execute(batchMessage);
                payloadEquals(reqMap, requestPayload);
                payloadEquals(respMap, batchResponse.getCustomPayload());
            }
            finally
            {
                client.close();
            }
        }
        finally
        {
            cqlQueryHandlerField.set(null, queryHandler);
        }
    }

    @Test
    public void testMessagePayload() throws Throwable
    {
        QueryHandler queryHandler = (QueryHandler) cqlQueryHandlerField.get(null);
        cqlQueryHandlerField.set(null, new TestQueryHandler());
        try
        {
            requireNetwork();

            Assert.assertSame(TestQueryHandler.class, ClientState.getCQLQueryHandler().getClass());

            SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort);
            try
            {
                client.connect(false);

                Map<String, ByteBuffer> reqMap;
                Map<String, ByteBuffer> respMap;

                QueryMessage queryMessage = new QueryMessage(
                                                            "CREATE TABLE " + KEYSPACE + ".atable (pk int PRIMARY KEY, v text)",
                                                            QueryOptions.DEFAULT
                );
                PrepareMessage prepareMessage = new PrepareMessage("SELECT * FROM " + KEYSPACE + ".atable", null);

                reqMap = Collections.singletonMap("foo", bytes(42));
                responsePayload = respMap = Collections.singletonMap("bar", bytes(42));
                queryMessage.setCustomPayload(reqMap);
                Message.Response queryResponse = client.execute(queryMessage);
                payloadEquals(reqMap, requestPayload);
                payloadEquals(respMap, queryResponse.getCustomPayload());

                reqMap = Collections.singletonMap("foo", bytes(43));
                responsePayload = respMap = Collections.singletonMap("bar", bytes(43));
                prepareMessage.setCustomPayload(reqMap);
                ResultMessage.Prepared prepareResponse = (ResultMessage.Prepared) client.execute(prepareMessage);
                payloadEquals(reqMap, requestPayload);
                payloadEquals(respMap, prepareResponse.getCustomPayload());

                ExecuteMessage executeMessage = new ExecuteMessage(prepareResponse.statementId, prepareResponse.resultMetadataId, QueryOptions.DEFAULT);
                reqMap = Collections.singletonMap("foo", bytes(44));
                responsePayload = respMap = Collections.singletonMap("bar", bytes(44));
                executeMessage.setCustomPayload(reqMap);
                Message.Response executeResponse = client.execute(executeMessage);
                payloadEquals(reqMap, requestPayload);
                payloadEquals(respMap, executeResponse.getCustomPayload());

                BatchMessage batchMessage = new BatchMessage(BatchStatement.Type.UNLOGGED,
                                                             Collections.<Object>singletonList("INSERT INTO " + KEYSPACE + ".atable (pk,v) VALUES (1, 'foo')"),
                                                             Collections.singletonList(Collections.<ByteBuffer>emptyList()),
                                                             QueryOptions.DEFAULT);
                reqMap = Collections.singletonMap("foo", bytes(45));
                responsePayload = respMap = Collections.singletonMap("bar", bytes(45));
                batchMessage.setCustomPayload(reqMap);
                Message.Response batchResponse = client.execute(batchMessage);
                payloadEquals(reqMap, requestPayload);
                payloadEquals(respMap, batchResponse.getCustomPayload());
            }
            finally
            {
                client.close();
            }
        }
        finally
        {
            cqlQueryHandlerField.set(null, queryHandler);
        }
    }

    @Test
    public void testMessagePayloadVersion3() throws Throwable
    {
        QueryHandler queryHandler = (QueryHandler) cqlQueryHandlerField.get(null);
        cqlQueryHandlerField.set(null, new TestQueryHandler());
        try
        {
            requireNetwork();

            Assert.assertSame(TestQueryHandler.class, ClientState.getCQLQueryHandler().getClass());

            SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, ProtocolVersion.V3);
            try
            {
                client.connect(false);

                Map<String, ByteBuffer> reqMap;

                QueryMessage queryMessage = new QueryMessage(
                                                            "CREATE TABLE " + KEYSPACE + ".atable (pk int PRIMARY KEY, v text)",
                                                            QueryOptions.DEFAULT
                );
                PrepareMessage prepareMessage = new PrepareMessage("SELECT * FROM " + KEYSPACE + ".atable", null);

                reqMap = Collections.singletonMap("foo", bytes(42));
                responsePayload = Collections.singletonMap("bar", bytes(42));
                queryMessage.setCustomPayload(reqMap);
                try
                {
                    client.execute(queryMessage);
                    Assert.fail();
                }
                catch (RuntimeException e)
                {
                    Assert.assertTrue(e.getCause() instanceof ProtocolException);
                }
                // when a protocol exception is thrown by the server the connection is closed, so need to re-connect
                client.close();
                client.connect(false);

                queryMessage.setCustomPayload(null);
                client.execute(queryMessage);

                reqMap = Collections.singletonMap("foo", bytes(43));
                responsePayload = Collections.singletonMap("bar", bytes(43));
                prepareMessage.setCustomPayload(reqMap);
                try
                {
                    client.execute(prepareMessage);
                    Assert.fail();
                }
                catch (RuntimeException e)
                {
                    Assert.assertTrue(e.getCause() instanceof ProtocolException);
                }
                // when a protocol exception is thrown by the server the connection is closed, so need to re-connect
                client.close();
                client.connect(false);

                prepareMessage.setCustomPayload(null);
                ResultMessage.Prepared prepareResponse = (ResultMessage.Prepared) client.execute(prepareMessage);

                ExecuteMessage executeMessage = new ExecuteMessage(prepareResponse.statementId, prepareResponse.resultMetadataId, QueryOptions.DEFAULT);
                reqMap = Collections.singletonMap("foo", bytes(44));
                responsePayload = Collections.singletonMap("bar", bytes(44));
                executeMessage.setCustomPayload(reqMap);
                try
                {
                    client.execute(executeMessage);
                    Assert.fail();
                }
                catch (RuntimeException e)
                {
                    Assert.assertTrue(e.getCause() instanceof ProtocolException);
                }
                // when a protocol exception is thrown by the server the connection is closed, so need to re-connect
                client.close();
                client.connect(false);

                BatchMessage batchMessage = new BatchMessage(BatchStatement.Type.UNLOGGED,
                                                             Collections.<Object>singletonList("INSERT INTO " + KEYSPACE + ".atable (pk,v) VALUES (1, 'foo')"),
                                                             Collections.singletonList(Collections.<ByteBuffer>emptyList()),
                                                             QueryOptions.DEFAULT);
                reqMap = Collections.singletonMap("foo", bytes(45));
                responsePayload = Collections.singletonMap("bar", bytes(45));
                batchMessage.setCustomPayload(reqMap);
                try
                {
                    client.execute(batchMessage);
                    Assert.fail();
                }
                catch (RuntimeException e)
                {
                    Assert.assertTrue(e.getCause() instanceof ProtocolException);
                }
            }
            finally
            {
                client.close();
            }
        }
        finally
        {
            cqlQueryHandlerField.set(null, queryHandler);
        }
    }

    private static void payloadEquals(Map<String, ByteBuffer> map1, Map<String, ByteBuffer> map2)
    {
        Assert.assertNotNull(map1);
        Assert.assertNotNull(map2);
        Assert.assertEquals(map1.keySet(), map2.keySet());
        for (Map.Entry<String, ByteBuffer> e : map1.entrySet())
            Assert.assertEquals(e.getValue(), map2.get(e.getKey()));
    }

    public static class TestQueryHandler implements QueryHandler
    {
        public QueryProcessor.Prepared getPrepared(MD5Digest id)
        {
            return QueryProcessor.instance.getPrepared(id);
        }

        public CQLStatement parse(String query, QueryState state, QueryOptions options)
        {
            return QueryProcessor.instance.parse(query, state, options);
        }

        public ResultMessage.Prepared prepare(String query,
                                              ClientState clientState,
                                              Map<String, ByteBuffer> customPayload)
                                                      throws RequestValidationException
        {
            if (customPayload != null)
                requestPayload = customPayload;
            ResultMessage.Prepared result = QueryProcessor.instance.prepare(query, clientState, customPayload);
            if (customPayload != null)
            {
                result.setCustomPayload(responsePayload);
                responsePayload = null;
            }
            return result;
        }

        public ResultMessage process(CQLStatement statement,
                                     QueryState state,
                                     QueryOptions options,
                                     Map<String, ByteBuffer> customPayload,
                                     long queryStartNanoTime)
                                            throws RequestExecutionException, RequestValidationException
        {
            if (customPayload != null)
                requestPayload = customPayload;
            ResultMessage result = QueryProcessor.instance.process(statement, state, options, customPayload, queryStartNanoTime);
            if (customPayload != null)
            {
                result.setCustomPayload(responsePayload);
                responsePayload = null;
            }
            return result;
        }

        public ResultMessage processBatch(BatchStatement statement,
                                          QueryState state,
                                          BatchQueryOptions options,
                                          Map<String, ByteBuffer> customPayload,
                                          long queryStartNanoTime)
                                                  throws RequestExecutionException, RequestValidationException
        {
            if (customPayload != null)
                requestPayload = customPayload;
            ResultMessage result = QueryProcessor.instance.processBatch(statement, state, options, customPayload, queryStartNanoTime);
            if (customPayload != null)
            {
                result.setCustomPayload(responsePayload);
                responsePayload = null;
            }
            return result;
        }

        public ResultMessage processPrepared(CQLStatement statement,
                                             QueryState state,
                                             QueryOptions options,
                                             Map<String, ByteBuffer> customPayload,
                                             long queryStartNanoTime)
                                                    throws RequestExecutionException, RequestValidationException
        {
            if (customPayload != null)
                requestPayload = customPayload;
            ResultMessage result = QueryProcessor.instance.processPrepared(statement, state, options, customPayload, queryStartNanoTime);
            if (customPayload != null)
            {
                result.setCustomPayload(responsePayload);
                responsePayload = null;
            }
            return result;
        }
    }
}
