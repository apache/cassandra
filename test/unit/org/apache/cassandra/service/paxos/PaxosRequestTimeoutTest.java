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

package org.apache.cassandra.service.paxos;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;

import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_SERIAL;

@RunWith(Parameterized.class)
public class PaxosRequestTimeoutTest extends CQLTester
{
    static TableMetadata metadata;
    public PaxosRequestTimeoutTest(Config.PaxosVariant paxosVariant)
    {
        requireNetwork();
        Paxos.setPaxosVariant(paxosVariant);
    }

    @Parameterized.Parameters()
    public static List<Object> buildParameterizedVariants()
    {
        return Arrays.asList(new Object[]{ Config.PaxosVariant.v1, Config.PaxosVariant.v2 });
    }

    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace("paxosrequesttimeouttestkeyspace",
                                    KeyspaceParams.simple(1),
                                    TableMetadata.builder("paxosrequesttimeouttestkeyspace", "standard")
                                                 .addPartitionKeyColumn("key", AsciiType.instance)
                                                 .addRegularColumn("col", AsciiType.instance));
        metadata = Keyspace.open("paxosrequesttimeouttestkeyspace").getColumnFamilyStore("standard").metadata.get();

    }

    @Before
    public void before()
    {
        // truncate the table before every test
        Keyspace.open("paxosrequesttimeouttestkeyspace").getColumnFamilyStore("standard").truncateBlocking();
    }

    @Test
    public void testPaxosReadTimeout()
    {
        QueryState state = new QueryState(ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 1234)));
        QueryOptions options = QueryOptions.forInternalCalls(LOCAL_SERIAL, Collections.emptyList());
        CQLStatement statement = QueryProcessor.instance.parse("SELECT * FROM paxosrequesttimeouttestkeyspace.standard WHERE key = 'test'",
                                                               state, options);

        // normal read should success
        QueryProcessor.instance.processStatement(statement, state, options, Dispatcher.RequestTime.forImmediateExecution());

        try
        {
            // Set startedAt to 0 to simulate a timeout scenario
            QueryProcessor.instance.processStatement(statement, state, options, new Dispatcher.RequestTime(0, 0));
            Assert.fail("Expected a ReadTimeoutException to be thrown");
        }
        catch (ReadTimeoutException e)
        {
            // expected
        }
        catch (Throwable e)
        {
            Assert.fail("Expected a ReadTimeoutException, but got: " + e.getClass().getSimpleName() + e.getCause());
        }
    }

    @Test
    public void testLWTProposeTimeout()
    {
        QueryState state = new QueryState(ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 1234)));
        QueryOptions options = QueryOptions.forInternalCalls(LOCAL_ONE, Collections.emptyList());
        CQLStatement statement = QueryProcessor.instance.parse("INSERT INTO paxosrequesttimeouttestkeyspace.standard (key, col) VALUES ('1', '2') IF NOT EXISTS",
                                                               state, options);

        // normal LWT should success
        QueryProcessor.instance.processStatement(statement, state, options, Dispatcher.RequestTime.forImmediateExecution());

        try
        {
            // Set startedAt to 0 to simulate a timeout scenario
            QueryProcessor.instance.processStatement(statement, state, options, new Dispatcher.RequestTime(0, 0));
            Assert.fail("Expected a WriteTimeoutException to be thrown");
        }
        catch (WriteTimeoutException e)
        {
            // expected
        }
        catch (Exception e)
        {
            Assert.fail("Expected a WriteTimeoutException, but got: " + e.getClass().getSimpleName() + e.getCause());
        }
    }
}
