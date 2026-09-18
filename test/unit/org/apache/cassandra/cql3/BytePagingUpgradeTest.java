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

package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.transformations.ForceSnapshot;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;


import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class BytePagingUpgradeTest extends CQLTester
{
    @Parameterized.Parameters(name = "oldVersion={0}")
    public static Collection<Object[]> versions()
    {
        return Arrays.asList(new Object[]{ "4.0.20" }, new Object[]{ "4.1.11" }, new Object[]{ "5.0.8" });
    }

    @Parameterized.Parameter
    public String oldVersion;

    private PageSize previousSubPageSize;

    @Before
    public void setupUpgrade()
    {
        previousSubPageSize = DatabaseDescriptor.getAggregationSubPageSize();
        DatabaseDescriptor.setAggregationSubPageSize(PageSize.inBytes(1));
        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))");
        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (pk, ck) VALUES (0, ?)", i);
        requireNetwork();
    }

    @After
    public void restoreSubPageSize()
    {
        DatabaseDescriptor.setAggregationSubPageSize(previousSubPageSize);
    }

    @Test
    public void testBytePagingFallsBackDuringUpgrade() throws Exception
    {
        // TRIGGER: a recorded peer is still on 4.x or 5.0, whether or not it is live.
        setPeerVersion(NodeVersion.fromCassandraVersion(new CassandraVersion(oldVersion)));
        String query = formatQuery("SELECT * FROM %s WHERE pk = 0");

        // HARNESS: send both QUERY and EXECUTE through the native protocol.
        try (SimpleClient client = newSimpleClient(ProtocolVersion.CURRENT))
        {
            ResultMessage.Prepared prepared = client.prepare(query);
            for (boolean usePrepared : new boolean[]{ false, true })
            {
                ResultMessage.Rows rowPage = rows(client.execute(request(query, prepared, usePrepared, PageSize.inRows(1), null)));
                assertThat(rowPage.result.rows).hasSize(1);
                assertThat(rowPage.getWarnings()).isNullOrEmpty();
                assertThat(rows(client.execute(request(query, prepared, usePrepared, PageSize.NONE, null))).result.rows).hasSize(5);
                assertRowFallback(client.execute(request(query, prepared, usePrepared, PageSize.inBytes(1), null)), 5);

                setPeerVersion(NodeVersion.CURRENT);
                assertThat(rows(client.execute(request(query, prepared, usePrepared, PageSize.inBytes(1024), null))).result.rows).hasSize(5);
                ResultMessage.Rows result = rows(client.execute(request(query, prepared, usePrepared, PageSize.inBytes(1), null)));
                assertThat(result.result.rows).hasSize(1);
                assertThat(result.getWarnings()).isNullOrEmpty();
                PagingState pagingState = result.result.metadata.getPagingState();
                assertThat(pagingState).isNotNull();

                // ORACLE: a downgrade between pages must preserve availability and continuation.
                setPeerVersion(NodeVersion.fromCassandraVersion(new CassandraVersion(oldVersion)));
                ResultMessage.Rows remaining = assertRowFallback(client.execute(request(query, prepared, usePrepared, PageSize.inBytes(1), pagingState)), 4);
                for (int i = 0; i < 4; i++)
                    assertThat(ByteBufferUtil.toInt(ByteBuffer.wrap(remaining.result.rows.get(i).get(1)))).isEqualTo(i + 1);
                assertThat(remaining.result.metadata.getPagingState()).isNull();
            }
        }
    }

    @Test
    public void testFallbackResumesLimitedQuery() throws Exception
    {
        String query = formatQuery("SELECT * FROM %s WHERE pk = 0 LIMIT 3");
        try (SimpleClient client = newSimpleClient(ProtocolVersion.CURRENT))
        {
            ResultMessage.Prepared prepared = client.prepare(query);
            for (boolean usePrepared : new boolean[]{ false, true })
            {
                setPeerVersion(NodeVersion.CURRENT);
                ResultMessage.Rows first = rows(client.execute(request(query, prepared, usePrepared, PageSize.inBytes(1), null)));
                assertThat(first.result.rows).hasSize(1);
                PagingState state = first.result.metadata.getPagingState();
                assertThat(state).isNotNull();

                // A larger fallback page must not skip the paging state, even when LIMIT fits in one page.
                setPeerVersion(NodeVersion.fromCassandraVersion(new CassandraVersion(oldVersion)));
                ResultMessage.Rows last = assertRowFallback(client.execute(request(query, prepared, usePrepared, PageSize.inBytes(1), state)), 2);
                assertThat(ByteBufferUtil.toInt(ByteBuffer.wrap(last.result.rows.get(0).get(1)))).isEqualTo(1);
                assertThat(ByteBufferUtil.toInt(ByteBuffer.wrap(last.result.rows.get(1).get(1)))).isEqualTo(2);
                assertThat(last.result.metadata.getPagingState()).isNull();
            }
        }
    }

    @Test
    public void testFallbackPagesAreBounded() throws Exception
    {
        for (int i = 5; i < 10001; i++)
            execute("INSERT INTO %s (pk, ck) VALUES (0, ?)", i);
        setPeerVersion(NodeVersion.fromCassandraVersion(new CassandraVersion(oldVersion)));
        String query = formatQuery("SELECT * FROM %s WHERE pk = 0");
        try (SimpleClient client = newSimpleClient(ProtocolVersion.CURRENT))
        {
            ResultMessage.Rows first = assertRowFallback(client.execute(new QueryMessage(query, options(PageSize.inBytes(1)))), 10000);
            PagingState state = first.result.metadata.getPagingState();
            assertThat(state).isNotNull();
            ResultMessage.Rows last = assertRowFallback(client.execute(new QueryMessage(query, options(PageSize.inBytes(1), state))), 1);
            assertThat(ByteBufferUtil.toInt(ByteBuffer.wrap(last.result.rows.get(0).get(1)))).isEqualTo(10000);
            assertThat(last.result.metadata.getPagingState()).isNull();
        }
    }

    @Test
    public void testAggregationUsesRowSubpagesDuringUpgrade() throws Exception
    {
        // TRIGGER: byte-sized aggregation subpages are configured during a rolling upgrade.
        setPeerVersion(NodeVersion.fromCassandraVersion(new CassandraVersion(oldVersion)));
        try (SimpleClient client = newSimpleClient(ProtocolVersion.CURRENT))
        {
            for (String query : new String[]{ "SELECT count(*) FROM %s WHERE pk = 0",
                                             "SELECT pk, count(*) FROM %s WHERE pk = 0 GROUP BY pk" })
            {
                // HARNESS: actual reads count the subpages while checking the aggregate result.
                assertThat(aggregationReads(client, query, PageSize.inRows(2))).isEqualTo(3);
                assertThat(aggregationReads(client, query, PageSize.NONE)).isEqualTo(1);
            }
        }
    }

    @Test
    public void testAggregationUsesClientPageSizeBeforeServerDefault() throws Exception
    {
        setPeerVersion(NodeVersion.CURRENT);
        try (SimpleClient client = newSimpleClient(ProtocolVersion.CURRENT))
        {
            for (String query : new String[]{ "SELECT count(*) FROM %s WHERE pk = 0",
                                             "SELECT pk, count(*) FROM %s WHERE pk = 0 GROUP BY pk" })
            {
                // ORACLE: the client size wins; omitted sizes use the configured byte subpages.
                assertThat(aggregationReads(client, query, PageSize.inRows(2))).isEqualTo(3);
                assertThat(aggregationReads(client, query, PageSize.NONE)).isGreaterThanOrEqualTo(5);
            }
        }
    }

    private long aggregationReads(SimpleClient client, String query, PageSize pageSize)
    {
        long before = readCount();
        ResultMessage.Rows result = rows(client.execute(new QueryMessage(formatQuery(query), options(pageSize))));
        assertThat(result.result.rows).hasSize(1);
        assertThat(ByteBufferUtil.toLong(ByteBuffer.wrap(result.result.rows.get(0).get(result.result.rows.get(0).size() - 1)))).isEqualTo(5);
        return readCount() - before;
    }

    private long readCount()
    {
        return Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).metric.coordinatorReadLatency.getCount();
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

    private static ResultMessage.Rows assertRowFallback(Message.Response response, int expectedRows)
    {
        ResultMessage.Rows result = rows(response);
        assertThat(result.result.rows).hasSize(expectedRows);
        assertThat(result.getWarnings()).anySatisfy(warning -> assertThat(warning).contains("Cassandra 6.0", "10000 rows"));
        return result;
    }

    private static ResultMessage.Rows rows(Message.Response response)
    {
        assertThat(response).isInstanceOf(ResultMessage.Rows.class);
        return (ResultMessage.Rows) response;
    }

    private static Message.Request request(String query, ResultMessage.Prepared prepared, boolean usePrepared, PageSize pageSize, PagingState state)
    {
        return usePrepared ? new ExecuteMessage(prepared.statementId, prepared.resultMetadataId, options(pageSize, state))
                           : new QueryMessage(query, options(pageSize, state));
    }

    private static QueryOptions options(PageSize pageSize)
    {
        return options(pageSize, null);
    }

    private static QueryOptions options(PageSize pageSize, PagingState state)
    {
        return QueryOptions.create(ConsistencyLevel.ONE, Collections.emptyList(), false, pageSize, state, null,
                                   ProtocolVersion.CURRENT, null);
    }
}
