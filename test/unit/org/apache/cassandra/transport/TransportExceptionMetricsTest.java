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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.CDCWriteException;
import org.apache.cassandra.exceptions.CasWriteUnknownResultException;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.TruncateException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.metrics.TransportMetrics;
import org.apache.cassandra.transport.messages.BatchMessage;
import org.apache.cassandra.transport.messages.ExceptionMetricsCollection;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class TransportExceptionMetricsTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(TransportExceptionMetricsTest.class);
    private SimpleClient client = null;

    @Test
    public void testCollectMetrics()
    {
        long cdcWriteFailureExceptionCount = TransportMetrics.cdcWriteFailureExceptionCount.getCount();
        long writeTimeoutExceptionCount = TransportMetrics.writeTimeoutExceptionCount.getCount();
        long casWriteUnknownExceptionCount = TransportMetrics.casWriteUnknownExceptionCount.getCount();
        long functionFailureExceptionCount = TransportMetrics.functionFailureExceptionCount.getCount();
        long isBootstrappingExceptionCount = TransportMetrics.isBootstrappingExceptionCount.getCount();
        long overloadedExceptionCount = TransportMetrics.overloadedExceptionCount.getCount();
        long readFailureExceptionCount = TransportMetrics.readFailureExceptionCount.getCount();
        long readTimeoutExceptionCount = TransportMetrics.readTimeoutExceptionCount.getCount();
        long truncateErrorExceptionCount = TransportMetrics.truncateErrorExceptionCount.getCount();
        long unavailableExceptionCount = TransportMetrics.unavailableExceptionCount.getCount();
        long writeFailureExceptionCount = TransportMetrics.writeFailureExceptionCount.getCount();
        long serverErrorExceptionCount = TransportMetrics.serverErrorExceptionCount.getCount();
        long otherExceptionCount = TransportMetrics.otherExceptionCount.getCount();

        ExceptionMetricsCollection.collectMetrics(new CDCWriteException("CDC write error"));
        assertEquals(TransportMetrics.cdcWriteFailureExceptionCount.getCount(), cdcWriteFailureExceptionCount + 1);

        ExceptionMetricsCollection.collectMetrics(
          new WriteTimeoutException(WriteType.CAS, ConsistencyLevel.LOCAL_QUORUM, 0, 2));
        assertEquals(TransportMetrics.writeTimeoutExceptionCount.getCount(), writeTimeoutExceptionCount + 1);

        ExceptionMetricsCollection.collectMetrics(
          new CasWriteUnknownResultException(ConsistencyLevel.LOCAL_QUORUM, 0, 2));
        assertEquals(TransportMetrics.casWriteUnknownExceptionCount.getCount(), casWriteUnknownExceptionCount + 1);

        ExceptionMetricsCollection.collectMetrics(new FunctionExecutionException(
        new FunctionName("dummy", "dummy"), new ArrayList<String>(), "failed"));
        assertEquals(TransportMetrics.functionFailureExceptionCount.getCount(), functionFailureExceptionCount + 1);

        ExceptionMetricsCollection.collectMetrics(new IsBootstrappingException());
        assertEquals(TransportMetrics.isBootstrappingExceptionCount.getCount(), isBootstrappingExceptionCount + 1);

        ExceptionMetricsCollection.collectMetrics(new OverloadedException("overloaded"));
        assertEquals(TransportMetrics.overloadedExceptionCount.getCount(), overloadedExceptionCount + 1);

        ExceptionMetricsCollection.collectMetrics(new ReadFailureException(
        ConsistencyLevel.LOCAL_QUORUM, 0, 2, true, new HashMap<>()));
        assertEquals(TransportMetrics.readFailureExceptionCount.getCount(), readFailureExceptionCount + 1);

        ExceptionMetricsCollection.collectMetrics(new ReadTimeoutException(ConsistencyLevel.LOCAL_QUORUM, 0, 2, true));
        assertEquals(TransportMetrics.readTimeoutExceptionCount.getCount(), readTimeoutExceptionCount + 1);

        ExceptionMetricsCollection.collectMetrics(new TruncateException("truncte error"));
        assertEquals(TransportMetrics.truncateErrorExceptionCount.getCount(), truncateErrorExceptionCount + 1);

        ExceptionMetricsCollection.collectMetrics(
          new UnavailableException("unavailable", ConsistencyLevel.LOCAL_QUORUM, 2, 1));
        assertEquals(TransportMetrics.unavailableExceptionCount.getCount(), unavailableExceptionCount + 1);

        ExceptionMetricsCollection.collectMetrics(new WriteFailureException(
        ConsistencyLevel.LOCAL_QUORUM, 0, 2, WriteType.CAS, new HashMap<>()));
        assertEquals(TransportMetrics.writeFailureExceptionCount.getCount(), writeFailureExceptionCount + 1);

        ExceptionMetricsCollection.collectMetrics(new ServerError("server error"));
        assertEquals(TransportMetrics.serverErrorExceptionCount.getCount(), serverErrorExceptionCount + 1);

        ExceptionMetricsCollection.collectMetrics(new Exception("dummy exception"));
        assertEquals(TransportMetrics.otherExceptionCount.getCount(), otherExceptionCount + 1);
    }

    @Test
    public void testExceptionMetricsFromDifferentPath() throws Exception
    {
        QueryOptions queryOptions = QueryOptions.create(
        ConsistencyLevel.THREE,
        QueryOptions.DEFAULT.getValues(),
        QueryOptions.DEFAULT.skipMetadata(),
        QueryOptions.DEFAULT.getPageSize(),
        QueryOptions.DEFAULT.getPagingState(),
        QueryOptions.DEFAULT.getSerialConsistency(),
        ProtocolVersion.V5,
        KEYSPACE);

        client.execute(new QueryMessage("INSERT INTO " + KEYSPACE + ".table1 (pk, v) VALUES (1, 'foo')",
                                        QueryOptions.DEFAULT));
        client.execute(new QueryMessage("INSERT INTO " + KEYSPACE + ".table1 (pk, v) VALUES (2, 'bar')",
                                        QueryOptions.DEFAULT));

        // Exception in the path of QueryMessage
        long beforeTestValue = TransportMetrics.unavailableExceptionCount.getCount();
        try
        {
           client.execute(new QueryMessage("SELECT * FROM " + KEYSPACE + ".table1",
                                            queryOptions));
            fail();
        }
        catch (Exception ex)
        {
            assertTrue(ex.getCause() instanceof UnavailableException);
            Assert.assertEquals("UnavailableExceptionCount should be incremented by 1",
                                TransportMetrics.unavailableExceptionCount.getCount(), beforeTestValue+1);
        }

        // Exception in the path of BatchMessage
        beforeTestValue = TransportMetrics.unavailableExceptionCount.getCount();
        try
        {
            BatchMessage batchMessage = new BatchMessage(BatchStatement.Type.UNLOGGED,
                                                         Collections.<Object>singletonList(
                                                           "DELETE FROM " + KEYSPACE + ".table1 WHERE pk = 1"),
                                                         Collections.singletonList(Collections.<ByteBuffer>emptyList()),
                                                         queryOptions);
            client.execute(batchMessage);
            fail();
        }
        catch (Exception ex)
        {
            assertTrue(ex.getCause() instanceof UnavailableException);
            Assert.assertEquals("UnavailableExceptionCount should be incremented by 1",
                                TransportMetrics.unavailableExceptionCount.getCount(), beforeTestValue+1);
        }

        // Exception in the path of ExecuteMessage
        beforeTestValue = TransportMetrics.otherExceptionCount.getCount();
        try
        {
            PrepareMessage prepareMessage = new PrepareMessage("SELECT * FROM table1", KEYSPACE);
            ResultMessage.Prepared prepareResponse = (ResultMessage.Prepared) client.execute(prepareMessage);
            ExecuteMessage executeMessage = new ExecuteMessage(prepareResponse.statementId,
                                                               prepareResponse.resultMetadataId, QueryOptions.DEFAULT);
            client.execute(executeMessage);
            fail();
        }
        catch (Exception ex)
        {
            Assert.assertEquals("OtherExceptionCount should be incremented by 1",
                                TransportMetrics.otherExceptionCount.getCount(), beforeTestValue+1);
        }
    }

    @Before
    public void creatTable() throws Exception
    {
        requireNetwork();
        client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, ProtocolVersion.V5, true,
                                  new EncryptionOptions());
        client.connect(false);
        QueryMessage queryMessage = new QueryMessage(
        "CREATE TABLE " + KEYSPACE +".table1 (pk int PRIMARY KEY, v text)",
              QueryOptions.DEFAULT);
        client.execute(queryMessage);
    }

    @After
    public void dropCreatedTable()
    {
        try
        {
            QueryProcessor.executeOnceInternal("DROP TABLE " + KEYSPACE + ".table1");
        }
        catch (Throwable t)
        {
            // ignore
        }
    }
}
