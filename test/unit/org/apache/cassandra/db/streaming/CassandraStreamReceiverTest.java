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

import java.util.Collections;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.junit.Before;
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

/**
 * Unit tests for {@link org.apache.cassandra.db.streaming.CassandraStreamReceiver}
 */
public class CassandraStreamReceiverTest extends CQLTester
{
    @Mock
    private StreamSession session;

    private static final String CDC_TABLE = "cdc_table";
    private static final String MV_TABLE = "mv_table";
    private static final String CDC_MV_TABLE = "cdc_mv_table";
    private static final String NO_CDC_MV_TABLE = "no_cdc_mv_table";

    @Before
    public void setup()
    {
        // Set cdc_on_repair_enabled materialized_views_on_repair to true
        DatabaseDescriptor.setCDCOnRepairEnabled(true);
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(true);
        // Enable materialized views
        DatabaseDescriptor.setMaterializedViewsEnabled(true);

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
        // given a CDC table with a materialized view attached to it.
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CDC_MV_TABLE);
        when(session.streamOperation()).thenReturn(StreamOperation.REPAIR);
        CassandraStreamReceiver receiver = new CassandraStreamReceiver(cfs, session, Collections.EMPTY_LIST, 1);

        // Should require write path since cdc_on_repair_enabled and materialized_views_on_repair_enabled are both true.
        assertTrue(receiver.requiresWritePath(cfs));
    }

    @Test
    public void testRequiresWritePathBulkLoad()
    {
        // given a CDC table with a materialized view attached to it.
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CDC_MV_TABLE);
        when(session.streamOperation()).thenReturn(StreamOperation.BULK_LOAD);
        CassandraStreamReceiver receiver = new CassandraStreamReceiver(cfs, session, Collections.EMPTY_LIST, 1);

        // Should require write path since cdc_on_repair_enabled and materialized_views_on_repair_enabled are both true.
        assertTrue(receiver.requiresWritePath(cfs));
    }

    @Test
    public void testDoesNotRequireWritePathNoCDCOrMV()
    {
        // Given cdc_on_repaired_enabled and materialized_views_on_repair_enabled are false
        // requiresWritePath should still return false for a non-CDC table.
        DatabaseDescriptor.setCDCOnRepairEnabled(false);
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(false);

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(NO_CDC_MV_TABLE);
        when(session.streamOperation()).thenReturn(StreamOperation.BULK_LOAD);
        CassandraStreamReceiver receiver = new CassandraStreamReceiver(cfs, session, Collections.EMPTY_LIST, 1);

        assertFalse(receiver.requiresWritePath(cfs));
    }

    @Test
    public void testRequiresWritePathRepairMVOnly()
    {
        // Given cdc_on_repaired_enabled and materialized_views_on_repair_enabled are true
        // requiresWritePath should return true for a table with materialized views.
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(MV_TABLE);
        when(session.streamOperation()).thenReturn(StreamOperation.REPAIR);
        CassandraStreamReceiver receiver = new CassandraStreamReceiver(cfs, session, Collections.EMPTY_LIST, 1);

        assertTrue(receiver.requiresWritePath(cfs));
    }

    @Test
    public void testRequiresWritePathRepairCDCOnRepairEnabled()
    {
        // Given cdc_on_repaired_enabled and materialized_views_on_repair_enabled are true
        // requiresWritePath should return true for a table with CDC enabled.
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CDC_TABLE);
        when(session.streamOperation()).thenReturn(StreamOperation.REPAIR);
        CassandraStreamReceiver receiver = new CassandraStreamReceiver(cfs, session, Collections.EMPTY_LIST, 1);
        DatabaseDescriptor.setCDCOnRepairEnabled(true);
        assertTrue(receiver.requiresWritePath(cfs));
    }

    @Test
    public void testDoesNotRequireWritePathRepairCDCOnRepairEnabledFalse()
    {
        // Given cdc_on_repaired_enabled and materialized_views_on_repair_enabled are false
        // requiresWritePath should return false for a table with CDC enabled.
        DatabaseDescriptor.setCDCOnRepairEnabled(false);
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(false);

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CDC_TABLE);
        when(session.streamOperation()).thenReturn(StreamOperation.BULK_LOAD);
        CassandraStreamReceiver receiver1 = new CassandraStreamReceiver(cfs, session, Collections.EMPTY_LIST, 1);
        assertFalse(receiver1.requiresWritePath(cfs));

        // When flipping cdc_on_repair_enabled to true
        // requiresWritePath should return true.
        DatabaseDescriptor.setCDCOnRepairEnabled(true);
        CassandraStreamReceiver receiver2 = new CassandraStreamReceiver(cfs, session, Collections.EMPTY_LIST, 1);
        assertTrue(receiver2.requiresWritePath(cfs));
    }
}
