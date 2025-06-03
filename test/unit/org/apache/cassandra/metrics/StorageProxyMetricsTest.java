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

package org.apache.cassandra.metrics;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageProxy;

import static com.datastax.driver.core.Cluster.builder;

public class StorageProxyMetricsTest
{
    private static Cluster cluster;
    private static Session session;

    private static final String KEYSPACE = "junit";
    private static final String TABLE = "storageproxymetricstest";

    private static PreparedStatement paxosLWT;
    private static PreparedStatement readPS;

    private static EmbeddedCassandraService cassandra;

    private static List<String> paxosVariants = Arrays.asList("v1", "v2");
    @BeforeClass
    public static void setup() throws ConfigurationException, IOException
    {
        cassandra = ServerTestUtils.startEmbeddedCassandraService();

        cluster = builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("USE " + KEYSPACE);
        session.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " (id int, ord int, val text, PRIMARY KEY (id, ord));");

        readPS = session.prepare("SELECT * FROM " + KEYSPACE + '.' + TABLE + " WHERE id=?;");
        paxosLWT = session.prepare("INSERT INTO " + KEYSPACE + '.' + TABLE + " (id, ord, val) VALUES (?, ?, ?) IF NOT EXISTS;");
    }

    @Test
    public void testPaxosLWT()
    {
        String original = StorageProxy.instance.getPaxosVariant();
        try
        {
            for (String v: paxosVariants)
            {
                StorageProxy.instance.setPaxosVariant(v);
                StorageProxyMetrics s = StorageProxyMetricsManager.getMetrics(KEYSPACE, ConsistencyLevel.SERIAL);
                long prevCount = s.casWriteMetrics.latency.getCount();
                executeLWTWithCL(1, 1, "aaaa", com.datastax.driver.core.ConsistencyLevel.SERIAL);
                Assert.assertEquals(1, s.casWriteMetrics.latency.getCount() - prevCount);
            }
        }
        finally
        {
            StorageProxy.instance.setPaxosVariant(original);
        }
    }

    @Test
    public void testPaxosRead()
    {
        String original = StorageProxy.instance.getPaxosVariant();
        try
        {
            for (String v: paxosVariants)
            {
                StorageProxy.instance.setPaxosVariant(v);
                StorageProxyMetrics s = StorageProxyMetricsManager.getMetrics(KEYSPACE, ConsistencyLevel.LOCAL_SERIAL);
                long prevCount = s.casReadMetrics.latency.getCount();
                executeReadWithCL(1, com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL);
                Assert.assertEquals(1, s.casReadMetrics.latency.getCount() - prevCount);
            }
        }
        finally
        {
            StorageProxy.instance.setPaxosVariant(original);
        }
    }

    private void executeLWTWithCL(int id, int ord, String val, com.datastax.driver.core.ConsistencyLevel cl)
    {
        BoundStatement bs = paxosLWT.bind(id, ord, val);
        bs.setSerialConsistencyLevel(cl);
        session.execute(bs);
    }

    private void executeReadWithCL(int id, com.datastax.driver.core.ConsistencyLevel cl)
    {
        BoundStatement bs = readPS.bind(id);
        bs.setConsistencyLevel(cl);
        session.execute(bs);
    }

    @AfterClass
    public static void tearDown()
    {
        if (cluster != null)
            cluster.close();
        if (cassandra != null)
            cassandra.stop();
    }
}
