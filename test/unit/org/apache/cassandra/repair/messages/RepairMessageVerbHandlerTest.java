/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.repair.messages;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.repair.state.Completable;
import org.apache.cassandra.repair.state.ParticipateState;
import org.apache.cassandra.service.disk.usage.DiskUsageMonitor;
import org.apache.cassandra.utils.TimeUUID;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.net.Message;
import org.mockito.Mock;

import static org.apache.cassandra.net.Verb.PREPARE_MSG;
import static org.apache.cassandra.repair.RepairMessageVerbHandler.instance;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class RepairMessageVerbHandlerTest extends CQLTester
{
    public String cfname;
    public ColumnFamilyStore store;
    public static InetAddressAndPort LOCAL, REMOTE;
    @Mock
    public DiskUsageMonitor diskUsageMonitor;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException, UnknownHostException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.startGossiper();
        LOCAL = FBUtilities.getBroadcastAddressAndPort();
        REMOTE = InetAddressAndPort.getByName("127.0.0.2");
    }

    @Before
    public void prepare() throws Exception
    {
        // creating the table in @Before, since we would like a fresh table for each test (and future tests). this method is also not static, so it is to be declared in @Before
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ActiveRepairService.instance.clearLocalRepairState();
        store = getCurrentColumnFamilyStore();
        initMocks(this);
    }

    @Test
    public void testPrepareMessageInsufficientDiskHeadroom()
    {
        TimeUUID sessionId = nextTimeUUID();
        List<TableId> tableIds = Collections.singletonList(store.metadata.id);
    
        PrepareMessage prepareMessage = new PrepareMessage(sessionId, tableIds, Collections.emptyList(), true, 0L, false, PreviewKind.NONE);
        Message<RepairMessage> message = Message.<RepairMessage>builder(PREPARE_MSG, prepareMessage)
                                                .from(REMOTE)
                                                .withId(1L)
                                                .build();
        
        // disk usage will be mocked high -- about 150%
        DiskUsageMonitor.instance = diskUsageMonitor;
        when(diskUsageMonitor.getDiskUsage()).thenReturn(1.5);
        DatabaseDescriptor.setIncrementalRepairDiskHeadroomRejectRatio(0.5);

        // testing that the disk check fails for incremental repair with insufficient disk by calling verifyDiskHeadroomThreshold -- this should return false
        assertFalse("Disk headroom check should fail for incremental repair with insufficient disk",
                   ActiveRepairService.verifyDiskHeadroomThreshold(sessionId, PreviewKind.NONE, true));
        
        instance.doVerb(message);
        // The method should complete without throwing an exception, but the disk check should have failed

        Collection<ParticipateState> repairSessions = ActiveRepairService.instance.participates();
        assertEquals(1, repairSessions.size());
        assertEquals(Completable.Result.Kind.FAILURE, repairSessions.stream().findFirst().get().getResult().kind);
    }


    @Test
    public void testPrepareMessageSufficientDiskHeadroom()
    {
        TimeUUID sessionId = nextTimeUUID();
        List<TableId> tableIds = Collections.singletonList(store.metadata.id);
        
        PrepareMessage prepareMessage = new PrepareMessage(sessionId, tableIds, Collections.emptyList(), true, 0L, false, PreviewKind.NONE);
        Message<RepairMessage> message = Message.<RepairMessage>builder(PREPARE_MSG, prepareMessage)
                                                .from(REMOTE)
                                                .withId(1L)
                                                .build();
        
        DiskUsageMonitor.instance = diskUsageMonitor;
        when(diskUsageMonitor.getDiskUsage()).thenReturn(0.1);
        DatabaseDescriptor.setIncrementalRepairDiskHeadroomRejectRatio(0.5);
        
        assertTrue("Disk headroom check should pass for incremental repair with sufficient disk",
                  ActiveRepairService.verifyDiskHeadroomThreshold(sessionId, PreviewKind.NONE, true));
        
        instance.doVerb(message);

        Collection<ParticipateState> repairSessions = ActiveRepairService.instance.participates();
        assertEquals(1, repairSessions.size());
        assertNull(repairSessions.stream().findFirst().get().getResult());
    }
    
    @Test
    public void testPrepareMessageFullRepairBypassesDiskCheck()
    {
        TimeUUID sessionId = nextTimeUUID();
        List<TableId> tableIds = Collections.singletonList(store.metadata.id);
        
        // create a full repair session this time
        PrepareMessage prepareMessage = new PrepareMessage(sessionId, tableIds, Collections.emptyList(), false, 0L, false, PreviewKind.NONE);
        Message<RepairMessage> message = Message.<RepairMessage>builder(PREPARE_MSG, prepareMessage)
                                                .from(REMOTE)
                                                .withId(1L)
                                                .build();
        
        // like previous time, we are mocking disk usage to be extremely high -- 200%
        DiskUsageMonitor.instance = diskUsageMonitor;
        when(diskUsageMonitor.getDiskUsage()).thenReturn(2.0);
        DatabaseDescriptor.setIncrementalRepairDiskHeadroomRejectRatio(0.9);
        
        // the disk check should be bypassed for the full repair session, where isIncremental = false
        assertTrue("Disk headroom check should be bypassed for full repairs",
                  ActiveRepairService.verifyDiskHeadroomThreshold(sessionId, PreviewKind.NONE, false));
        
        instance.doVerb(message);
        
        // once again, the method should complete without throwing an exception, proving that full repairs bypass disk checks

        Collection<ParticipateState> repairSessions = ActiveRepairService.instance.participates();
        assertEquals(1, repairSessions.size());
        assertNull(repairSessions.stream().findFirst().get().getResult());
    }
}
