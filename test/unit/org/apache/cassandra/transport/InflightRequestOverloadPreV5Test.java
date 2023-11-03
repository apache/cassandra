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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.transport.messages.QueryMessage;

public class InflightRequestOverloadPreV5Test extends CQLTester
{

    private static long LOW_LIMIT = 300L;
    private static long ORIGINAL_ENDPOINT_LIMIT = DatabaseDescriptor.getNativeTransportMaxRequestDataInFlightPerIpInBytes();
    private static long ORIGINAL_GLOBAL_LIMIT = DatabaseDescriptor.getNativeTransportMaxRequestDataInFlightInBytes();

    private static final QueryOptions V4_DEFAULT_OPTIONS = QueryOptions.create(
            QueryOptions.DEFAULT.getConsistency(),
            QueryOptions.DEFAULT.getValues(),
            QueryOptions.DEFAULT.skipMetadata(),
            QueryOptions.DEFAULT.getPageSize(),
            QueryOptions.DEFAULT.getPagingState(),
            QueryOptions.DEFAULT.getSerialConsistency(),
            ProtocolVersion.V4,
            KEYSPACE);

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.setNativeTransportMaxRequestDataInFlightPerIpInBytes(LOW_LIMIT);
        DatabaseDescriptor.setNativeTransportConcurrentRequestDataInFlightInBytes(LOW_LIMIT);
        requireNetwork();
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setNativeTransportMaxRequestDataInFlightPerIpInBytes(ORIGINAL_ENDPOINT_LIMIT);
        DatabaseDescriptor.setNativeTransportConcurrentRequestDataInFlightInBytes(ORIGINAL_GLOBAL_LIMIT);
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

    @After
    public void resetServerSideThrowFlag()
    {
        DatabaseDescriptor.setThrowOnOverload(false);
    }

    private SimpleClient client(boolean thrwoOnOverload) throws IOException
    {
        return new SimpleClient(nativeAddr.getHostAddress(),
                nativePort,
                ProtocolVersion.V4,
                true,
                new EncryptionOptions())
                .connect(false, thrwoOnOverload);
    }

    @Test
    public void testOverloadedExceptionOnlyClientSideFlagEnabled() throws Throwable
    {
        testScenario(true, false, true);
    }

    @Test
    public void testOverloadedExceptionOnlyServerSideFlagEnabled() throws Throwable
    {
        testScenario(false, true, true);
    }

    @Test
    public void testOverloadedExceptionBothClientServerSideFlagEnabled() throws Throwable
    {
        testScenario(true, true, true);
    }

    @Test
    public void testOverloadedExceptionBothClientServerSideFlagDisabled() throws Throwable
    {
        testScenario(false, false, false);
    }

    private void testScenario(boolean throwEnabledOnClient, boolean throwEnabledOnServer, boolean expectThrow) throws Throwable {
        DatabaseDescriptor.setThrowOnOverload(throwEnabledOnServer);
        try (SimpleClient client = client(throwEnabledOnClient))
        {
            QueryMessage queryMessage = new QueryMessage(String.format("CREATE TABLE %s.atable (pk int PRIMARY KEY, v text)", KEYSPACE),
                    V4_DEFAULT_OPTIONS);
            client.execute(queryMessage);

            queryMessage = new QueryMessage(
                    String.format("INSERT INTO %s.atable (pk, v) VALUES (1, '%s')", KEYSPACE, generatePayload(LOW_LIMIT*2)),
                    V4_DEFAULT_OPTIONS);
            try
            {
                client.execute(queryMessage);
                if (expectThrow) {
                    Assert.fail();
                }
            }
            catch (RuntimeException e)
            {
                if (!expectThrow) {
                    Assert.fail();
                }
                Assert.assertTrue(e.getCause() instanceof OverloadedException);
            } finally {
                client.close();
            }
        }
    }

    @Test
    public void testChangingServerSideFlagAtRuntime() throws Throwable
    {
        DatabaseDescriptor.setThrowOnOverload(false);
        SimpleClient client = client(false);
        try {
            QueryMessage queryMessage = new QueryMessage(String.format("CREATE TABLE %s.atable (pk int PRIMARY KEY, v text)", KEYSPACE),
                    V4_DEFAULT_OPTIONS);
            client.execute(queryMessage);

            queryMessage = new QueryMessage(
                    String.format("INSERT INTO %s.atable (pk, v) VALUES (1, '%s')", KEYSPACE, generatePayload(LOW_LIMIT * 2)),
                    V4_DEFAULT_OPTIONS);
            try {
                client.execute(queryMessage);
            } catch (RuntimeException e) {
                // the query should succeed because the flag is off at both server side and client side
                Assert.fail();
            }

            // change server side flag to enable throw_on_overload
            DatabaseDescriptor.setThrowOnOverload(true);
            queryMessage = new QueryMessage(
                    String.format("INSERT INTO %s.atable (pk, v) VALUES (1, '%s')", KEYSPACE, generatePayload(LOW_LIMIT * 2)),
                    V4_DEFAULT_OPTIONS);
            try {
                client.execute(queryMessage);
                Assert.fail();
            } catch (RuntimeException e) {
                // the query should fail because the flag is on at server side
                Assert.assertTrue(e.getCause() instanceof OverloadedException);
            }
        }
        finally
        {
            client.close();
        }
    }

    private String generatePayload(long length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append('a');
        }
        return sb.toString();
    }
}
