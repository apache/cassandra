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
import java.util.Objects;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PaxosMetricsTest
{
    private static Cluster cluster;
    private static Session session;
    private static EmbeddedCassandraService cassandra;

    @BeforeClass()
    public static void setup() throws ConfigurationException, IOException
    {
        cassandra = ServerTestUtils.startEmbeddedCassandraService();
        DatabaseDescriptor.setPaxosStatePurging(Config.PaxosStatePurging.repaired);
        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("CREATE KEYSPACE IF NOT EXISTS junit WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("CREATE TABLE IF NOT EXISTS junit.paxosmetricstest (id int PRIMARY KEY, val int);");
    }

    @AfterClass
    public static void tearDown()
    {
        if (cluster != null)
            cluster.close();
        if (cassandra != null)
            cassandra.stop();
    }

    @Test
    public void testPaxosRowsPurged()
    {
        // immediate purging
        DatabaseDescriptor.setPaxosPurgeGrace(0);
        assertEquals(0, PaxosMetrics.paxosRowsPurged.getCount());
        // LWT
        session.execute("INSERT INTO junit.paxosmetricstest (id, val) values (1, 1)");
        session.execute("UPDATE junit.paxosmetricstest SET val=2 WHERE id=1 IF EXISTS");

        TableId tableId = Objects.requireNonNull(Schema.instance.getTableMetadata("junit", "paxosmetricstest")).id;
        // schedule a paxos cleanup session
        StorageService.instance.autoRepairPaxos(tableId).addCallback((success, failure) -> {
            assertNull(failure);
            // flush and submit a compaction task to system.paxos and wait for it to finish
            ColumnFamilyStore paxos = Keyspace.open(SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.PAXOS);
            FBUtilities.waitOnFuture(paxos.forceFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
            FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(paxos, 0, false));

            assertTrue(PaxosMetrics.paxosRowsPurged.getCount() > 0);
        });
    }
}
