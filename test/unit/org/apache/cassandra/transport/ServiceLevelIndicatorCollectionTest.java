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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.junit.*;

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
import org.apache.cassandra.metrics.ServiceLevelIndicatorMetrics;
import org.apache.cassandra.transport.messages.BatchMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.NoSpamLoggerTest;
import org.apache.cassandra.utils.Pair;

import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class ServiceLevelIndicatorCollectionTest extends CQLTester
{
    private SimpleClient client = null;

    @Test
    public void testCollectMetrics()
    {
        long cdcWriteFailureExceptionCount = ServiceLevelIndicatorMetrics.cdcWriteFailureExceptionMetrics.getCount();
        long writeTimeoutExceptionCount = ServiceLevelIndicatorMetrics.writeTimeoutExceptionMetrics.getCount();
        long casWriteUnknownExceptionCount = ServiceLevelIndicatorMetrics.casWriteUnknownExceptionMetrics.getCount();
        long functionFailureExceptionCount = ServiceLevelIndicatorMetrics.functionFailureExceptionMetrics.getCount();
        long isBootstrappingExceptionCount = ServiceLevelIndicatorMetrics.isBootstrappingExceptionMetrics.getCount();
        long overloadedExceptionCount = ServiceLevelIndicatorMetrics.overloadedExceptionMetrics.getCount();
        long readFailureExceptionCount = ServiceLevelIndicatorMetrics.readFailureExceptionMetrics.getCount();
        long readTimeoutExceptionCount = ServiceLevelIndicatorMetrics.readTimeoutExceptionMetrics.getCount();
        long truncateErrorExceptionCount = ServiceLevelIndicatorMetrics.truncateErrorExceptionMetrics.getCount();
        long unavailableExceptionCount = ServiceLevelIndicatorMetrics.unavailableExceptionMetrics.getCount();
        long writeFailureExceptionCount = ServiceLevelIndicatorMetrics.writeFailureExceptionMetrics.getCount();
        long serverErrorExceptionCount = ServiceLevelIndicatorMetrics.serverErrorExceptionMetrics.getCount();
        long otherExceptionCount = ServiceLevelIndicatorMetrics.otherExceptionMetrics.getCount();

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(new CDCWriteException("CDC write error"));
        assertEquals(ServiceLevelIndicatorMetrics.cdcWriteFailureExceptionMetrics.getCount(), cdcWriteFailureExceptionCount + 1);

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(
          new WriteTimeoutException(WriteType.CAS, ConsistencyLevel.LOCAL_QUORUM, 0, 2));
        assertEquals(ServiceLevelIndicatorMetrics.writeTimeoutExceptionMetrics.getCount(), writeTimeoutExceptionCount + 1);

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(
          new CasWriteUnknownResultException(ConsistencyLevel.LOCAL_QUORUM, 0, 2));
        assertEquals(ServiceLevelIndicatorMetrics.casWriteUnknownExceptionMetrics.getCount(), casWriteUnknownExceptionCount + 1);

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(new FunctionExecutionException(
        new FunctionName("dummy", "dummy"), new ArrayList<String>(), "failed"));
        assertEquals(ServiceLevelIndicatorMetrics.functionFailureExceptionMetrics.getCount(), functionFailureExceptionCount + 1);

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(new IsBootstrappingException());
        assertEquals(ServiceLevelIndicatorMetrics.isBootstrappingExceptionMetrics.getCount(), isBootstrappingExceptionCount + 1);

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(new OverloadedException("overloaded"));
        assertEquals(ServiceLevelIndicatorMetrics.overloadedExceptionMetrics.getCount(), overloadedExceptionCount + 1);

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(new ReadFailureException(
        ConsistencyLevel.LOCAL_QUORUM, 0, 2, true, new HashMap<>()));
        assertEquals(ServiceLevelIndicatorMetrics.readFailureExceptionMetrics.getCount(), readFailureExceptionCount + 1);

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(new ReadTimeoutException(ConsistencyLevel.LOCAL_QUORUM, 0, 2, true));
        assertEquals(ServiceLevelIndicatorMetrics.readTimeoutExceptionMetrics.getCount(), readTimeoutExceptionCount + 1);

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(new TruncateException("truncte error"));
        assertEquals(ServiceLevelIndicatorMetrics.truncateErrorExceptionMetrics.getCount(), truncateErrorExceptionCount + 1);

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(
          new UnavailableException("unavailable", ConsistencyLevel.LOCAL_QUORUM, 2, 1));
        assertEquals(ServiceLevelIndicatorMetrics.unavailableExceptionMetrics.getCount(), unavailableExceptionCount + 1);

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(new WriteFailureException(
        ConsistencyLevel.LOCAL_QUORUM, 0, 2, WriteType.CAS, new HashMap<>()));
        assertEquals(ServiceLevelIndicatorMetrics.writeFailureExceptionMetrics.getCount(), writeFailureExceptionCount + 1);

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(new ServerError("server error"));
        assertEquals(ServiceLevelIndicatorMetrics.serverErrorExceptionMetrics.getCount(), serverErrorExceptionCount + 1);

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(new Exception("dummy exception"));
        assertEquals(ServiceLevelIndicatorMetrics.otherExceptionMetrics.getCount(), otherExceptionCount + 1);
    }

    @Test
    public void testNoSpamLogger()
    {
        NoSpamLoggerTest.logged.clear();
        ServiceLevelIndicatorMetricsCollection.setLogger(NoSpamLoggerTest.mock);
        NoSpamLoggerTest.logged.put(NoSpamLogger.Level.INFO, new ArrayDeque<Pair<String, Object[]>>());
        NoSpamLoggerTest.logged.put(NoSpamLogger.Level.WARN, new ArrayDeque<Pair<String, Object[]>>());
        NoSpamLoggerTest.logged.put(NoSpamLogger.Level.ERROR, new ArrayDeque<Pair<String, Object[]>>());
        NoSpamLogger.clearWrappedLoggersForTest();

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(new CDCWriteException("CDC write error"));
        assertEquals(1, NoSpamLoggerTest.logged.get(NoSpamLogger.Level.ERROR).size());

        // verify a same kind of error happened, only 1 log is logged
        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(new CDCWriteException("CDC write error"));
        assertEquals(1, NoSpamLoggerTest.logged.get(NoSpamLogger.Level.ERROR).size());

        // verify a same kind of error with different error message happened, only 1 log is logged
        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(new CDCWriteException("CDC write error again!"));
        assertEquals(1, NoSpamLoggerTest.logged.get(NoSpamLogger.Level.ERROR).size());

        // verify a new kind of error happened, number of logged messages increased
        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(new Exception("dummy exception"));
        assertEquals(2, NoSpamLoggerTest.logged.get(NoSpamLogger.Level.ERROR).size());
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
        long beforeTestValue = ServiceLevelIndicatorMetrics.unavailableExceptionMetrics.getCount();
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
                                ServiceLevelIndicatorMetrics.unavailableExceptionMetrics.getCount(), beforeTestValue+1);
        }

        // Exception in the path of BatchMessage
        beforeTestValue = ServiceLevelIndicatorMetrics.unavailableExceptionMetrics.getCount();
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
                                ServiceLevelIndicatorMetrics.unavailableExceptionMetrics.getCount(), beforeTestValue+1);
        }

        // Exception in the path of ExecuteMessage
        beforeTestValue = ServiceLevelIndicatorMetrics.otherExceptionMetrics.getCount();
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
                                ServiceLevelIndicatorMetrics.otherExceptionMetrics.getCount(), beforeTestValue+1);
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
