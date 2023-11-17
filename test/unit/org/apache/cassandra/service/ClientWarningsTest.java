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
package org.apache.cassandra.service;

import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.QueryMessage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class ClientWarningsTest extends CQLTester
{
    @Parameterized.Parameter
    public ProtocolVersion version;

    @Parameterized.Parameters()
    public static Collection<Object[]> versions()
    {
        return ProtocolVersion.SUPPORTED.stream()
                                        .skip(1)
                                        .map(v -> new Object[]{v})
                                        .collect(Collectors.toList());
    }

    @BeforeClass
    public static void setUp()
    {
        requireNetwork();
        DatabaseDescriptor.setBatchSizeWarnThresholdInKiB(1);
    }

    @Test
    public void testUnloggedBatch() throws Exception
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        // v4 and higher
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, version, true, new EncryptionOptions()))
        {
            client.connect(false);

            Message.Response resp = executeWithRetries(client,
                                                        new QueryMessage(createBatchStatement2(1), QueryOptions.DEFAULT));
            assertNull(resp.getWarnings());

            resp = executeWithRetries(client,
                                      new QueryMessage(createBatchStatement2(DatabaseDescriptor.getBatchSizeWarnThreshold()), QueryOptions.DEFAULT));
            assertEquals(1, resp.getWarnings().size());
        }
    }

    @Test
    public void testLargeBatch() throws Exception
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        // v4 and higher
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, version, true, new EncryptionOptions()))
        {
            client.connect(false);

            Message.Response resp = executeWithRetries(client,
                                                       new QueryMessage(createBatchStatement2(DatabaseDescriptor.getBatchSizeWarnThreshold() / 2 + 1), QueryOptions.DEFAULT));
            assertEquals(1, resp.getWarnings().size());


            resp = executeWithRetries(client,
                                      new QueryMessage(createBatchStatement(DatabaseDescriptor.getBatchSizeWarnThreshold()), QueryOptions.DEFAULT));

            assertNull(resp.getWarnings());
        }
    }

    @Test
    public void testTombstoneWarning() throws Exception
    {
        final int iterations = 10000;
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, version, true, new EncryptionOptions()))
        {
            client.connect(false);

            for (int i = 0; i < iterations; i++)
            {
                executeWithRetries(client,
                                   new QueryMessage(String.format("INSERT INTO %s.%s (pk, ck, v) VALUES (1, %s, 1)",
                                                                  KEYSPACE,
                                                                  currentTable(),
                                                                  i), QueryOptions.DEFAULT));
            }
            ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
            Util.flush(store);

            for (int i = 0; i < iterations; i++)
            {
                executeWithRetries(client,
                                   new QueryMessage(String.format("DELETE v FROM %s.%s WHERE pk = 1 AND ck = %s",
                                                                  KEYSPACE,
                                                                  currentTable(),
                                                                  i), QueryOptions.DEFAULT));
            }
            Util.flush(store);

            {
                Message.Response resp = executeWithRetries(client,
                                                           new QueryMessage(String.format("SELECT * FROM %s.%s WHERE pk = 1",
                                                                                          KEYSPACE,
                                                                                          currentTable()), QueryOptions.DEFAULT));
                assertEquals(1, resp.getWarnings().size());
            }
        }
    }

    private static Message.Response executeWithRetries(SimpleClient client, QueryMessage query)
    {
        for (int i = 0; i < 10; i++)
        {
            try
            {
                return client.execute(query);
            }
            catch (RequestTimeoutException t)
            {
                logger.warn("Timed out. Retrying.");
            }
        }
        throw new RuntimeException("Could not execute query after 10 tries");
    }
    @Test
    public void testLargeBatchWithProtoV2() throws Exception
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, ProtocolVersion.V3))
        {
            client.connect(false);

            Message.Response resp = executeWithRetries(client,
                                                       new QueryMessage(createBatchStatement(DatabaseDescriptor.getBatchSizeWarnThreshold()), QueryOptions.DEFAULT));
            assertNull(resp.getWarnings());
        }
    }

    private String createBatchStatement(int minSize)
    {
        return String.format("BEGIN UNLOGGED BATCH INSERT INTO %s.%s (pk, v) VALUES (1, '%s') APPLY BATCH;",
                             KEYSPACE,
                             currentTable(),
                             StringUtils.repeat('1', minSize));
    }

    private String createBatchStatement2(int minSize)
    {
        return String.format("BEGIN UNLOGGED BATCH INSERT INTO %s.%s (pk, v) VALUES (1, '%s'); INSERT INTO %s.%s (pk, v) VALUES (2, '%s'); APPLY BATCH;",
                             KEYSPACE,
                             currentTable(),
                             StringUtils.repeat('1', minSize),
                             KEYSPACE,
                             currentTable(),
                             StringUtils.repeat('1', minSize));
    }

}
