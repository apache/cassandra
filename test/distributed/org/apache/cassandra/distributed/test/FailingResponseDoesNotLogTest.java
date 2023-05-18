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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.LogAction;
import org.apache.cassandra.distributed.api.LogResult;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_QUERY_HANDLER_CLASS;

/**
 * This class is rather impelemntation specific.  It is possible that changes made will cause this tests to fail,
 * so updating to the latest logic is fine.
 *
 * This class makes sure we do not do logging/update metrics for client from a specific set of ip domains, so as long
 * as we still do not log/update metrics, then the test is doing the right thing.
 */
public class FailingResponseDoesNotLogTest extends TestBaseImpl
{
    @BeforeClass
    public static void beforeClassTopLevel() // need to make sure not to conflict with TestBaseImpl.beforeClass
    {

        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void dispatcherErrorDoesNotLock() throws IOException
    {
        try (WithProperties properties = new WithProperties().set(CUSTOM_QUERY_HANDLER_CLASS, AlwaysRejectErrorQueryHandler.class.getName());
             Cluster cluster = Cluster.build(1)
                                      .withConfig(c -> c.with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP)
                                                        .set("client_error_reporting_exclusions", ImmutableMap.of("subnets", Collections.singletonList("127.0.0.1"))))
                                      .start();

        )
        {

            try (SimpleClient client = SimpleClient.builder("127.0.0.1", 9042).build().connect(false))
            {
                client.execute("SELECT * FROM system.peers", ConsistencyLevel.ONE);
                Assert.fail("Query should have failed");
            }
            catch (Exception e)
            {
                // ignore; expected
            }

            // logs happen before client response; so grep is enough
            LogAction logs = cluster.get(1).logs();
            LogResult<List<String>> matches = logs.grep("address contained in client_error_reporting_exclusions");
            Assertions.assertThat(matches.getResult()).hasSize(1);
            matches = logs.grep("Unexpected exception during request");
            Assertions.assertThat(matches.getResult()).isEmpty();
        }
    }

    public static class AlwaysRejectErrorQueryHandler implements QueryHandler
    {
        @Override
        public CQLStatement parse(String queryString, QueryState queryState, QueryOptions options)
        {
            throw new AssertionError("reject");
        }

        @Override
        public ResultMessage process(CQLStatement statement, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
        {
            throw new AssertionError("reject");
        }

        @Override
        public ResultMessage.Prepared prepare(String query, ClientState clientState, Map<String, ByteBuffer> customPayload) throws RequestValidationException
        {
            throw new AssertionError("reject");
        }

        @Override
        public Prepared getPrepared(MD5Digest id)
        {
            throw new AssertionError("reject");
        }

        @Override
        public ResultMessage processPrepared(CQLStatement statement, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
        {
            throw new AssertionError("reject");
        }

        @Override
        public ResultMessage processBatch(BatchStatement statement, QueryState state, BatchQueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
        {
            throw new AssertionError("reject");
        }
    }
}
