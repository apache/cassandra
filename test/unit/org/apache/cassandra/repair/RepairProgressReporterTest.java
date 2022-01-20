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

package org.apache.cassandra.repair;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.repair.SystemDistributedKeyspace.PARENT_REPAIR_HISTORY;
import static org.apache.cassandra.repair.SystemDistributedKeyspace.REPAIR_HISTORY;
import static org.apache.cassandra.schema.SchemaConstants.DISTRIBUTED_KEYSPACE_NAME;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RepairProgressReporterTest extends CQLTester
{
    protected static final Range<Token> RANGE1 = new Range<>(t(1), t(2));

    public static final InetAddressAndPort COORDINATOR;
    protected static final InetAddressAndPort PARTICIPANT1;

    static
    {
        try
        {
            COORDINATOR = InetAddressAndPort.getByName("10.0.0.1");
            PARTICIPANT1 = InetAddressAndPort.getByName("10.0.0.1");
        }
        catch (UnknownHostException e)
        {

            throw new AssertionError(e);
        }
    }

    private static final UUID sessionId = UUIDGen.getTimeUUID();
    private static final UUID parentRepairSession = UUIDGen.getTimeUUID();
    private static final String keyspace = "ks";
    private static final String[] cfs = { "table" };
    private static final CommonRange commonRange = new CommonRange(Sets.newHashSet(COORDINATOR, PARTICIPANT1), Collections.emptySet(), Collections.singleton(RANGE1));

    @BeforeClass
    public static void init()
    {
        // required to access distributed keyspace
        Schema.instance.transform(SchemaTransformations.updateSystemKeyspace(SystemDistributedKeyspace.metadata(), SystemDistributedKeyspace.GENERATION));
        StorageService.instance.getTokenMetadata().updateNormalToken(t(1), FBUtilities.getBroadcastAddressAndPort());
    }

    @After
    public void after()
    {
        Keyspace.open(DISTRIBUTED_KEYSPACE_NAME).getColumnFamilyStore(REPAIR_HISTORY).truncateBlockingWithoutSnapshot();
        Keyspace.open(DISTRIBUTED_KEYSPACE_NAME).getColumnFamilyStore(PARENT_REPAIR_HISTORY).truncateBlockingWithoutSnapshot();
    }

    @Test
    public void testStartParentRepairs()
    {
        RepairOption option = RepairOption.parse(Collections.emptyMap(), DatabaseDescriptor.getPartitioner());
        option.getRanges().add(RANGE1);

        RepairProgressReporter.instance.onParentRepairStarted(parentRepairSession, keyspace, cfs, option);
        assertRows(parentRepairHistory(), row(keyspace, Sets.newHashSet(cfs), parentRepairSession,
                                              Stream.of(RANGE1).map(Range::toString).collect(Collectors.toSet()),
                                              null));
    }

    @Test
    public void testSuccessfulParentRepairs()
    {
        testStartParentRepairs();

        RepairProgressReporter.instance.onParentRepairSucceeded(parentRepairSession, Collections.singleton(RANGE1));
        assertRows(parentRepairHistory(), row(keyspace, Sets.newHashSet(cfs), parentRepairSession,
                                              Stream.of(RANGE1).map(Range::toString).collect(Collectors.toSet()),
                                              Stream.of(RANGE1).map(Range::toString).collect(Collectors.toSet())));
    }

    @Test
    public void testFailParentRepairs()
    {
        testStartParentRepairs();

        RepairProgressReporter.instance.onParentRepairFailed(parentRepairSession, new RuntimeException("Mock Error"));
        assertRows(parentRepairHistoryWithError(), row(keyspace, Sets.newHashSet(cfs), parentRepairSession,
                                                       Stream.of(RANGE1).map(Range::toString).collect(Collectors.toSet()),
                                                       null,
                                                       "Mock Error"));
    }

    @Test
    public void testStartRepairs()
    {
        RepairProgressReporter.instance.onRepairsStarted(sessionId, parentRepairSession, keyspace, cfs, commonRange);
        assertRows(repairHistory(), row(keyspace, cfs[0], sessionId, parentRepairSession, "STARTED", RANGE1.left.toString(), RANGE1.right.toString()));
    }

    @Test
    public void testSuccessfulRepair()
    {
        testStartRepairs();

        RepairProgressReporter.instance.onRepairSucceeded(sessionId, keyspace, cfs[0]);
        assertRows(repairHistory(), row(keyspace, cfs[0], sessionId, parentRepairSession, "SUCCESS", RANGE1.left.toString(), RANGE1.right.toString()));
    }

    @Test
    public void testFailRepairs()
    {
        testStartRepairs();

        RepairProgressReporter.instance.onRepairsFailed(sessionId, keyspace, cfs, new RuntimeException("Mock Error"));
        assertRows(repairHistoryWithError(), row(keyspace, cfs[0], sessionId, parentRepairSession, "FAILED", RANGE1.left.toString(), RANGE1.right.toString(), "Mock Error"));
    }

    @Test
    public void testFailedRepairJob()
    {
        testStartRepairs();

        RepairProgressReporter.instance.onRepairFailed(sessionId, keyspace, cfs[0], new RuntimeException("Mock Error"));
        assertRows(repairHistoryWithError(), row(keyspace, cfs[0], sessionId, parentRepairSession, "FAILED", RANGE1.left.toString(), RANGE1.right.toString(), "Mock Error"));
    }

    @Test
    public void testCustomInstance()
    {
        RepairProgressReporter reporter = RepairProgressReporter.make(MockReporter.class.getName());
        Assertions.assertThat(reporter).isInstanceOf(MockReporter.class);

        assertThatThrownBy(() -> RepairProgressReporter.make("unknown")).hasMessageContaining("Unknown repair progress report");
    }

    private UntypedResultSet repairHistory()
    {
        return repairHistory("keyspace_name, columnfamily_name, id, parent_id, status, range_begin, range_end");
    }

    private UntypedResultSet repairHistoryWithError()
    {
        return repairHistory("keyspace_name, columnfamily_name, id, parent_id, status, range_begin, range_end, exception_message");
    }

    private UntypedResultSet repairHistory(String columns)
    {
        try
        {
            return execute(String.format("SELECT %s FROM %s.%s", columns, DISTRIBUTED_KEYSPACE_NAME, REPAIR_HISTORY));
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    private UntypedResultSet parentRepairHistory()
    {
        return parentRepairHistory("keyspace_name, columnfamily_names, parent_id, requested_ranges, successful_ranges");
    }

    private UntypedResultSet parentRepairHistoryWithError()
    {
        return parentRepairHistory("keyspace_name, columnfamily_names, parent_id, requested_ranges, successful_ranges, exception_message");
    }

    private UntypedResultSet parentRepairHistory(String columns)
    {
        try
        {
            return execute(String.format("SELECT %s FROM %s.%s", columns, DISTRIBUTED_KEYSPACE_NAME, PARENT_REPAIR_HISTORY));
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    protected static Token t(int v)
    {
        return DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(v));
    }

    private static class MockReporter implements RepairProgressReporter
    {
        public MockReporter()
        {
        }

        @Override
        public void onParentRepairStarted(UUID parentSession, String keyspaceName, String[] cfnames, RepairOption options)
        {

        }

        @Override
        public void onParentRepairSucceeded(UUID parentSession, Collection<Range<Token>> successfulRanges)
        {

        }

        @Override
        public void onParentRepairFailed(UUID parentSession, Throwable t)
        {

        }

        @Override
        public void onRepairsStarted(UUID id, UUID parentRepairSession, String keyspaceName, String[] cfnames, CommonRange commonRange)
        {

        }

        @Override
        public void onRepairsFailed(UUID id, String keyspaceName, String[] cfnames, Throwable t)
        {

        }

        @Override
        public void onRepairFailed(UUID id, String keyspaceName, String cfname, Throwable t)
        {

        }

        @Override
        public void onRepairSucceeded(UUID id, String keyspaceName, String cfname)
        {

        }
    }
}
