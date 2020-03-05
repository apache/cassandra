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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.RowUtil;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class NativeProtocolTest extends DistributedTestBase
{

    @Test
    public void withClientRequests() throws Throwable
    {
        try (Cluster ignored = init(Cluster.create(3,
                                                   config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))))
        {
            final com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
            Session session = cluster.connect();
            session.execute("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));");
            session.execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) values (1,1,1);");
            Statement select = new SimpleStatement("select * from " + KEYSPACE + ".tbl;").setConsistencyLevel(ConsistencyLevel.ALL);
            final ResultSet resultSet = session.execute(select);
            assertRows(resultSet, row(1, 1, 1));
            Assert.assertEquals(3, cluster.getMetadata().getAllHosts().size());
            session.close();
            cluster.close();
        }
    }

    @Test
    public void withCounters() throws Throwable
    {
        try (Cluster dtCluster = init(Cluster.create(3,
                config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))))
        {
            final com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
            Session session = cluster.connect();
            session.execute("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck counter, PRIMARY KEY (pk));");
            session.execute("UPDATE " + KEYSPACE + ".tbl set ck = ck + 10 where pk = 1;");
            Statement select = new SimpleStatement("select * from " + KEYSPACE + ".tbl;").setConsistencyLevel(ConsistencyLevel.ALL);
            final ResultSet resultSet = session.execute(select);
            assertRows(resultSet, row(1, 10L));
            Assert.assertEquals(3, cluster.getMetadata().getAllHosts().size());
            session.close();
            cluster.close();
        }
    }
}