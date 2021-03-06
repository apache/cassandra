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

import org.apache.cassandra.distributed.impl.RowUtil;
import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.distributed.api.ICluster;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.*;

// TODO: this test should be removed after running in-jvm dtests is set up via the shared API repository
public class NativeProtocolTest extends TestBaseImpl
{

    @Test
    public void withClientRequests() throws Throwable
    {
        try (ICluster ignored = init(builder().withNodes(3)
                                              .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                              .start()))
        {

            try (com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
                 Session session = cluster.connect())
            {
                session.execute("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));");
                session.execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) values (1,1,1);");
                Statement select = new SimpleStatement("select * from " + KEYSPACE + ".tbl;").setConsistencyLevel(ConsistencyLevel.ALL);
                final ResultSet resultSet = session.execute(select);
                assertRows(RowUtil.toObjects(resultSet), row(1, 1, 1));
                Assert.assertEquals(3, cluster.getMetadata().getAllHosts().size());
            }
        }
    }

    @Test
    public void withCounters() throws Throwable
    {
        try (ICluster ignored = init(builder().withNodes(3)
                                              .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                              .start()))
        {
            final com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
            Session session = cluster.connect();
            session.execute("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck counter, PRIMARY KEY (pk));");
            session.execute("UPDATE " + KEYSPACE + ".tbl set ck = ck + 10 where pk = 1;");
            Statement select = new SimpleStatement("select * from " + KEYSPACE + ".tbl;").setConsistencyLevel(ConsistencyLevel.ALL);
            final ResultSet resultSet = session.execute(select);
            assertRows(RowUtil.toObjects(resultSet), row(1, 10L));
            Assert.assertEquals(3, cluster.getMetadata().getAllHosts().size());
            session.close();
            cluster.close();
        }
    }
}