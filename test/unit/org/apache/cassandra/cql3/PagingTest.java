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

import java.util.Iterator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;

import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CONFIG;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;


public class PagingTest
{
    private static Cluster cluster;
    private static Session session;

    private static final String KEYSPACE = "paging_test";
    private static final String createKsStatement = "CREATE KEYSPACE " + KEYSPACE +
                                                    " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };";

    private static final String dropKsStatement = "DROP KEYSPACE IF EXISTS " + KEYSPACE;
    private static EmbeddedCassandraService cassandra;

    @BeforeClass
    public static void setup() throws Exception
    {
        CASSANDRA_CONFIG.setString("cassandra-murmur.yaml");

        cassandra = ServerTestUtils.startEmbeddedCassandraService();

        // Currently the native server start method return before the server is fully binded to the socket, so we need
        // to wait slightly before trying to connect to it. We should fix this but in the meantime using a sleep.
        Thread.sleep(500);

        cluster = Cluster.builder().addContactPoint("127.0.0.1")
                                   .withPort(DatabaseDescriptor.getNativeTransportPort())
                                   .build();
        session = cluster.connect();

        session.execute(dropKsStatement);
        session.execute(createKsStatement);
    }

    @AfterClass
    public static void tearDown()
    {
        if (cluster != null)
            cluster.close();
        if (cassandra != null)
            cassandra.stop();
    }

    /**
     * Makes sure that we don't drop any live rows when paging with DISTINCT queries
     *
     * * We need to have more rows than fetch_size
     * * The node must have a token within the first page (so that the range gets split up in StorageProxy#getRestrictedRanges)
     *   - This means that the second read in the second range will read back too many rows
     * * The extra rows are dropped (so that we only return fetch_size rows to client)
     * * This means that the last row recorded in AbstractQueryPager#recordLast is a non-live one
     * * For the next page, the first row returned will be the same non-live row as above
     * * The bug in CASSANDRA-14956 caused us to drop that non-live row + the first live row in the next page
     */
    @Test
    public void testPaging() throws InterruptedException
    {
        String table = KEYSPACE + ".paging";
        String createTableStatement = "CREATE TABLE IF NOT EXISTS " + table + " (id int, id2 int, id3 int, val text, PRIMARY KEY ((id, id2), id3));";
        String dropTableStatement = "DROP TABLE IF EXISTS " + table + ';';

        // custom snitch to avoid merging ranges back together after StorageProxy#getRestrictedRanges splits them up
        IEndpointSnitch snitch = new AbstractEndpointSnitch()
        {
            private IEndpointSnitch oldSnitch = DatabaseDescriptor.getEndpointSnitch();
            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2)
            {
                return oldSnitch.compareEndpoints(target, a1, a2);
            }

            public String getRack(InetAddressAndPort endpoint)
            {
                return oldSnitch.getRack(endpoint);
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return oldSnitch.getDatacenter(endpoint);
            }

            @Override
            public boolean isWorthMergingForRangeQuery(ReplicaCollection merged, ReplicaCollection l1, ReplicaCollection l2)
            {
                return false;
            }
        };
        DatabaseDescriptor.setEndpointSnitch(snitch);
        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalToken(new LongToken(5097162189738624638L), FBUtilities.getBroadcastAddressAndPort());
        session.execute(createTableStatement);

        for (int i = 0; i < 110; i++)
        {
            // removing row with idx 10 causes the last row in the first page read to be empty
            String ttlClause = i == 10 ? "USING TTL 1" : "";
            session.execute(String.format("INSERT INTO %s (id, id2, id3, val) VALUES (%d, %d, %d, '%d') %s", table, i, i, i, i, ttlClause));
        }
        Thread.sleep(1500);

        Statement stmt = new SimpleStatement(String.format("SELECT DISTINCT token(id, id2), id, id2 FROM %s", table));
        stmt.setFetchSize(100);
        ResultSet res = session.execute(stmt);
        stmt.setFetchSize(200);
        ResultSet res2 = session.execute(stmt);

        Iterator<Row> iter1 = res.iterator();
        Iterator<Row> iter2 = res2.iterator();

        while (iter1.hasNext() && iter2.hasNext())
        {
            Row row1 = iter1.next();
            Row row2 = iter2.next();
            assertEquals(row1.getInt("id"), row2.getInt("id"));
        }
        assertFalse(iter1.hasNext());
        assertFalse(iter2.hasNext());
        session.execute(dropTableStatement);
    }
}
