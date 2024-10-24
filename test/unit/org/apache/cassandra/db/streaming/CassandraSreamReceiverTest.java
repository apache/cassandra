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

package org.apache.cassandra.db.streaming;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamSession;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class CassandraSreamReceiverTest extends CQLTester
{
    @Mock
    private StreamSession session;

    private static final String CDC_TABLE = "cdc_table";
    private static final String MV_TABLE = "mv_table";
    private static final String CDC_MV_TABLE = "cdc_mv_table";
    private static final String NO_CDC_MV_TABLE = "no_cdc_mv_table";

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.setCDCOnRepairEnabled(false);
    }

    @Before
    public void setup()
    {
        MockitoAnnotations.initMocks(this);
        QueryProcessor.executeInternal(String.format("CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int) WITH cdc=true", KEYSPACE, CDC_TABLE));
        QueryProcessor.executeInternal(String.format("CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int) WITH cdc=false", KEYSPACE, MV_TABLE));
        QueryProcessor.executeInternal(String.format("CREATE MATERIALIZED VIEW IF NOT EXISTS  %s.mv AS SELECT * FROM %s.%s WHERE pk IS NOT NULL PRIMARY KEY (pk)", KEYSPACE, KEYSPACE, MV_TABLE));
        QueryProcessor.executeInternal(String.format("CREATE TABLE IF NOT EXISTS  %s.%s (pk int PRIMARY KEY, v int) WITH cdc=true", KEYSPACE, CDC_MV_TABLE));
        QueryProcessor.executeInternal(String.format("CREATE MATERIALIZED VIEW IF NOT EXISTS  %s.mv2 AS SELECT * FROM %s.%s WHERE pk IS NOT NULL PRIMARY KEY (pk)", KEYSPACE, KEYSPACE, CDC_MV_TABLE));
        QueryProcessor.executeInternal(String.format("CREATE TABLE IF NOT EXISTS  %s.%s (pk int PRIMARY KEY, v int) WITH cdc=false", KEYSPACE, NO_CDC_MV_TABLE));
    }

    @Test
    public void testRequiresWritePathRepair()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CDC_MV_TABLE);
        when(session.streamOperation()).thenReturn(StreamOperation.REPAIR);
        CassandraStreamReceiver receiver = new CassandraStreamReceiver(cfs, session, 1);

        assertTrue(receiver.requiresWritePath(cfs));
    }

    @Test
    public void testRequiresWritePathBulkLoad()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CDC_MV_TABLE);
        when(session.streamOperation()).thenReturn(StreamOperation.BULK_LOAD);
        CassandraStreamReceiver receiver = new CassandraStreamReceiver(cfs, session, 1);

        assertTrue(receiver.requiresWritePath(cfs));
    }

    @Test
    public void testRequiresWritePathNoCDCOrMV()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(NO_CDC_MV_TABLE);
        when(session.streamOperation()).thenReturn(StreamOperation.BULK_LOAD);
        CassandraStreamReceiver receiver = new CassandraStreamReceiver(cfs, session, 1);

        assertFalse(receiver.requiresWritePath(cfs));
    }

    @Test
    public void testRequiresWritePathRepairMVOnly()
    {
        // Access the private field using reflection
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(MV_TABLE);
        when(session.streamOperation()).thenReturn(StreamOperation.REPAIR);
        CassandraStreamReceiver receiver = new CassandraStreamReceiver(cfs, session, 1);

        assertTrue(receiver.requiresWritePath(cfs));
    }

    @Test
    public void testRequiresWritePathRepairCDCWithSystemProp()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CDC_TABLE);
        when(session.streamOperation()).thenReturn(StreamOperation.REPAIR);
        CassandraStreamReceiver receiver = new CassandraStreamReceiver(cfs, session, 1);
        DatabaseDescriptor.setCDCOnRepairEnabled(true);
        assertTrue(receiver.requiresWritePath(cfs));
    }

    @Test
    public void testDoesNotRequiresWritePathRepairCDCOnly()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CDC_TABLE);
        when(session.streamOperation()).thenReturn(StreamOperation.BULK_LOAD);
        CassandraStreamReceiver receiver1 = new CassandraStreamReceiver(cfs, session, 1);
        assertFalse(receiver1.requiresWritePath(cfs));

        CassandraStreamReceiver receiver2 = new CassandraStreamReceiver(cfs, session, 1);
        DatabaseDescriptor.setCDCOnRepairEnabled(true);
        assertTrue(receiver2.requiresWritePath(cfs));

    }
}
