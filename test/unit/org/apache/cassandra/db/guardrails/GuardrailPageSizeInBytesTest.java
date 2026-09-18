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

package org.apache.cassandra.db.guardrails;

import java.nio.ByteBuffer;
import java.util.Collections;

import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.PlainTextAuthProvider;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.transformations.ForceSnapshot;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.AuthResponse;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;

import static java.lang.String.format;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.BYTES;
import static org.assertj.core.api.Assertions.assertThat;

public class GuardrailPageSizeInBytesTest extends ThresholdTester
{
    private PageSize previousSubPageSize;
    private int previousRowWarn;
    private int previousRowFail;
    private Directory previousDirectory;

    public GuardrailPageSizeInBytesTest()
    {
        super("5B", "10B", Guardrails.pageSizeInBytes,
              Guardrails::setPageSizeInBytesThreshold,
              Guardrails::getPageSizeInBytesWarnThreshold,
              Guardrails::getPageSizeInBytesFailThreshold,
              bytes -> new DataStorageSpec.LongBytesBound(bytes, BYTES).toString(),
              size -> new DataStorageSpec.LongBytesBound(size).toBytes());
    }

    @Before
    public void setupTest()
    {
        previousRowWarn = guardrails().getPageSizeWarnThreshold();
        previousRowFail = guardrails().getPageSizeFailThreshold();
        previousSubPageSize = DatabaseDescriptor.getAggregationSubPageSize();
        previousDirectory = ClusterMetadata.current().directory;
        guardrails().setPageSizeThreshold(-1, -1);
        DatabaseDescriptor.setAggregationSubPageSize(PageSize.inBytes(1));
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        for (int c = 0; c < 5; c++)
            execute("INSERT INTO %s (k, c, v) VALUES (0, ?, 'value larger than the byte page')", c);
    }

    @After
    public void restoreConfiguration()
    {
        guardrails().setPageSizeThreshold(previousRowWarn, previousRowFail);
        guardrails().setPageSizeInBytesThreshold(null, null);
        DatabaseDescriptor.setAggregationSubPageSize(previousSubPageSize);
        ClusterMetadata metadata = ClusterMetadata.current();
        if (!metadata.directory.equals(previousDirectory))
            ClusterMetadataService.instance().commit(new ForceSnapshot(metadata.transformer().with(previousDirectory).build().metadata));
    }

    @Test
    public void testThresholds() throws Throwable
    {
        for (boolean local : new boolean[]{ false, true })
        {
            assertValid(() -> executePage(userClientState, PageSize.inBytes(1), local));
            assertValid(() -> executePage(userClientState, PageSize.inBytes(5), local));
            assertWarns(() -> executePage(userClientState, PageSize.inBytes(6), local), warning(6, 5));
            assertWarns(() -> executePage(userClientState, PageSize.inBytes(10), local), warning(10, 5));
            assertFails(() -> executePage(userClientState, PageSize.inBytes(11), local), failure(11, 10));
        }
    }

    @Test
    public void testIndependentUnitsAndServerDefault() throws Throwable
    {
        guardrails().setPageSizeInBytesThreshold(null, "0B");
        assertValid(() -> executePage(userClientState, PageSize.inRows(100), false));
        assertValid(() -> executePage(userClientState, PageSize.NONE, false));
        assertValid(() -> executePage(userClientState, "SELECT count(*) FROM %s WHERE k = 0", PageSize.NONE, false));
        assertFails(() -> executePage(userClientState, PageSize.inBytes(1), false), failure(1, 0));

        guardrails().setPageSizeInBytesThreshold("5B", "10B");
        guardrails().setPageSizeThreshold(-1, 0);
        assertValid(() -> executePage(userClientState, PageSize.inBytes(1), false));
    }

    @Test
    public void testDisabledAndDynamicThresholds() throws Throwable
    {
        guardrails().setPageSizeInBytesThreshold(null, null);
        assertValid(() -> executePage(userClientState, PageSize.inBytes(100), false));
        guardrails().setPageSizeInBytesThreshold("0B", null);
        assertWarns(() -> executePage(userClientState, PageSize.inBytes(1), false), warning(1, 0));
        guardrails().setPageSizeInBytesThreshold(null, "0B");
        assertFails(() -> executePage(userClientState, PageSize.inBytes(1), false), failure(1, 0));
        guardrails().setPageSizeInBytesThreshold("1KiB", "1MiB");
        assertThat(guardrails().getPageSizeInBytesWarnThreshold()).isEqualTo("1KiB");
        assertThat(guardrails().getPageSizeInBytesFailThreshold()).isEqualTo("1MiB");
        assertValid(() -> executePage(userClientState, PageSize.inBytes(1024), false));
        assertWarns(() -> executePage(userClientState, PageSize.inBytes(1025), false), warning(1025, 1024));
        assertFails(() -> executePage(userClientState, PageSize.inBytes(1048577), false), failure(1048577, 1048576));
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        for (ClientState state : new ClientState[]{ superClientState, systemClientState })
            assertValid(() -> executePage(state, PageSize.inBytes(11), true));
        userClientState.pauseGuardrails();
        try
        {
            assertValid(() -> executePage(userClientState, PageSize.inBytes(11), false));
        }
        finally
        {
            userClientState.resumeGuardrails();
        }
    }

    @Test
    public void testOversizedRowRemainsReadable() throws Throwable
    {
        assertValid(() -> assertThat(executePage(userClientState, PageSize.inBytes(1), false).result.rows).hasSize(1));
    }

    @Test
    public void testNativeQueryAndExecute() throws Exception
    {
        try (SimpleClient client = newSimpleClient(ProtocolVersion.CURRENT))
        {
            authenticate(client);
            String query = formatQuery("SELECT * FROM %s WHERE k = 0");
            ResultMessage.Prepared prepared = client.prepare(query);
            for (boolean usePrepared : new boolean[]{ false, true })
            {
                ResultMessage.Rows valid = rows(client.execute(request(query, prepared, usePrepared, 5, null)));
                assertThat(valid.getWarnings()).isNullOrEmpty();
                ResultMessage.Rows warned = rows(client.execute(request(query, prepared, usePrepared, 6, null)));
                assertThat(warned.getWarnings()).anySatisfy(warning -> assertThat(warning).contains(warning(6, 5)));
                assertFailure(client.execute(request(query, prepared, usePrepared, 11, null), false), failure(11, 10));
            }
        }
    }

    @Test
    public void testDowngradeFallbackAndContinuation() throws Exception
    {
        guardrails().setPageSizeThreshold(-1, 2);
        guardrails().setPageSizeInBytesThreshold("1B", "2B");
        try (SimpleClient client = newSimpleClient(ProtocolVersion.CURRENT))
        {
            authenticate(client);
            for (String oldVersion : new String[]{ "4.0.20", "4.1.11", "5.0.8" })
            {
                for (boolean usePrepared : new boolean[]{ false, true })
                {
                    setPeerVersion(NodeVersion.CURRENT);
                    String query = formatQuery("SELECT * FROM %s WHERE k = 0 LIMIT 4");
                    ResultMessage.Prepared prepared = client.prepare(query);
                    ResultMessage.Rows first = rows(client.execute(request(query, prepared, usePrepared, 1, null)));
                    assertClustering(first, 0);
                    PagingState state = first.result.metadata.getPagingState();
                    assertThat(state).isNotNull();

                    setPeerVersion(NodeVersion.fromCassandraVersion(new CassandraVersion(oldVersion)));
                    ResultMessage.Rows second = rows(client.execute(request(query, prepared, usePrepared, 1, state)));
                    assertClustering(second, 1, 2);
                    assertFallbackWarning(second, 2);
                    state = second.result.metadata.getPagingState();
                    assertThat(state).isNotNull();
                    ResultMessage.Rows last = rows(client.execute(request(query, prepared, usePrepared, 1, state)));
                    assertClustering(last, 3);
                    assertFallbackWarning(last, 2);
                    assertThat(last.result.metadata.getPagingState()).isNull();

                    assertFailure(client.execute(request(query, prepared, usePrepared, 3, null), false), failure(3, 2));
                    String limited = formatQuery("SELECT * FROM %s WHERE k = 0 LIMIT 1");
                    ResultMessage.Prepared limitedPrepared = client.prepare(limited);
                    assertFailure(client.execute(request(limited, limitedPrepared, usePrepared, 3, null), false), failure(3, 2));
                }
            }
        }
    }

    @Test
    public void testFallbackWithZeroDisabledAndExcludedRowGuardrails() throws Throwable
    {
        setPeerVersion(NodeVersion.fromCassandraVersion(new CassandraVersion("5.0.8")));
        try (SimpleClient client = newSimpleClient(ProtocolVersion.CURRENT))
        {
            authenticate(client);
            String query = formatQuery("SELECT * FROM %s WHERE k = 0");
            for (int rowThreshold : new int[]{ 0, -1 })
            {
                guardrails().setPageSizeThreshold(-1, rowThreshold);
                ResultMessage.Rows result = rows(client.execute(new QueryMessage(query, options(PageSize.inBytes(1), null))));
                assertClustering(result, 0, 1, 2, 3, 4);
                assertFallbackWarning(result, 10000);
            }
        }

        guardrails().setPageSizeThreshold(-1, 2);
        for (ClientState state : new ClientState[]{ superClientState, systemClientState })
            assertThat(executePage(state, PageSize.inBytes(1), false).result.rows).hasSize(5);
        userClientState.pauseGuardrails();
        try
        {
            assertThat(executePage(userClientState, PageSize.inBytes(1), false).result.rows).hasSize(5);
        }
        finally
        {
            userClientState.resumeGuardrails();
        }
    }

    @Test
    public void testAggregationFallbackUsesRowCap() throws Exception
    {
        guardrails().setPageSizeThreshold(-1, 2);
        guardrails().setPageSizeInBytesThreshold(null, "0B");
        setPeerVersion(NodeVersion.fromCassandraVersion(new CassandraVersion("5.0.8")));
        try (SimpleClient client = newSimpleClient(ProtocolVersion.CURRENT))
        {
            authenticate(client);
            String query = formatQuery("SELECT count(*) FROM %s WHERE k = 0");
            long before = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).metric.coordinatorReadLatency.getCount();
            ResultMessage.Rows result = rows(client.execute(new QueryMessage(query, options(PageSize.NONE, null))));
            assertThat(result.result.rows).hasSize(1);
            assertThat(ByteBufferUtil.toLong(ByteBuffer.wrap(result.result.rows.get(0).get(0)))).isEqualTo(5);
            long reads = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).metric.coordinatorReadLatency.getCount() - before;
            assertThat(reads).isEqualTo(3);
        }
    }

    private ResultMessage.Rows executePage(ClientState state, PageSize pageSize, boolean local)
    {
        return executePage(state, "SELECT * FROM %s WHERE k = 0", pageSize, local);
    }

    private ResultMessage.Rows executePage(ClientState state, String query, PageSize pageSize, boolean local)
    {
        QueryState queryState = new QueryState(state);
        CQLStatement statement = QueryProcessor.parseStatement(formatQuery(query), state);
        statement.validate(state);
        QueryOptions options = options(pageSize, null);
        return (ResultMessage.Rows) (local ? statement.executeLocally(queryState, options)
                                          : statement.execute(queryState, options, Dispatcher.RequestTime.forImmediateExecution()));
    }

    private String warning(int value, int threshold)
    {
        return format("Query for table %s with page size %s bytes exceeds warning threshold of %s bytes.", currentTable(), value, threshold);
    }

    private String failure(int value, int threshold)
    {
        return format("Aborting query for table %s, page size %s bytes exceeds fail threshold of %s bytes.", currentTable(), value, threshold);
    }

    private static void authenticate(SimpleClient client)
    {
        PlainTextAuthProvider auth = new PlainTextAuthProvider("guardrail_user", "guardrail_password");
        client.execute(new AuthResponse(auth.newAuthenticator((EndPoint) null, null).initialResponse()));
    }

    private static Message.Request request(String query, ResultMessage.Prepared prepared, boolean usePrepared, int bytes, PagingState state)
    {
        QueryOptions options = options(PageSize.inBytes(bytes), state);
        return usePrepared ? new ExecuteMessage(prepared.statementId, prepared.resultMetadataId, options)
                           : new QueryMessage(query, options);
    }

    private static QueryOptions options(PageSize pageSize, PagingState state)
    {
        return QueryOptions.create(ConsistencyLevel.ONE, Collections.emptyList(), false, pageSize, state, null,
                                   ProtocolVersion.CURRENT, KEYSPACE);
    }

    private static ResultMessage.Rows rows(Message.Response response)
    {
        assertThat(response).isInstanceOf(ResultMessage.Rows.class);
        return (ResultMessage.Rows) response;
    }

    private static void assertFailure(Message.Response response, String message)
    {
        assertThat(response).isInstanceOf(ErrorMessage.class);
        assertThat(((ErrorMessage) response).error.getMessage()).contains(message);
    }

    private static void assertClustering(ResultMessage.Rows page, int... expected)
    {
        assertThat(page.result.rows).hasSize(expected.length);
        for (int i = 0; i < expected.length; i++)
            assertThat(ByteBufferUtil.toInt(ByteBuffer.wrap(page.result.rows.get(i).get(1)))).isEqualTo(expected[i]);
    }

    private static void assertFallbackWarning(ResultMessage.Rows page, int rows)
    {
        assertThat(page.getWarnings()).anySatisfy(warning -> assertThat(warning).contains("Cassandra 6.0", rows + " rows"));
        assertThat(page.getWarnings()).noneSatisfy(warning -> assertThat(warning).contains("Guardrail"));
    }

    private static void setPeerVersion(NodeVersion version)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        InetAddressAndPort endpoint = InetAddressAndPort.getByNameUnchecked("127.0.0.99");
        NodeId peer = metadata.directory.peerId(endpoint);
        Directory directory = peer == null
                              ? metadata.directory.with(new NodeAddresses(endpoint), new Location("dc1", "rack1"), version)
                              : metadata.directory.withNodeVersion(peer, version);
        ClusterMetadataService.instance().commit(new ForceSnapshot(metadata.transformer().with(directory).build().metadata));
    }
}
