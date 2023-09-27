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
package org.apache.cassandra.db.virtual;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.CommonRange;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairCoordinator;
import org.apache.cassandra.repair.messages.PrepareMessage;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.repair.state.Completable;
import org.apache.cassandra.repair.state.CoordinatorState;
import org.apache.cassandra.repair.state.JobState;
import org.apache.cassandra.repair.state.ParticipateState;
import org.apache.cassandra.repair.state.SessionState;
import org.apache.cassandra.repair.state.State;
import org.apache.cassandra.repair.state.ValidationState;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

public class LocalRepairTablesTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static final ImmutableSet<InetAddressAndPort> ADDRESSES = ImmutableSet.of(address(127, 0, 0, 1));
    private static final List<String> ADDRESSES_STR = ADDRESSES.stream().map(Object::toString).collect(Collectors.toList());
    private static final CommonRange COMMON_RANGE = new CommonRange(ADDRESSES, Collections.emptySet(), Collections.singleton(range(0, 100)));
    private static final String REPAIR_KS = "system";
    private static final String REPAIR_TABLE = "peers";

    @BeforeClass
    public static void before()
    {
        CQLTester.setUpClass();

        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, LocalRepairTables.getAll(KS_NAME)));
    }

    @Before
    public void cleanupRepairs()
    {
        ActiveRepairService.instance().clearLocalRepairState();
    }

    @Test
    public void repairs() throws Throwable
    {
        assertEmpty("repairs");

        CoordinatorState state = coordinator();
        assertInit("repairs", state);

        state.phase.setup();
        assertState("repairs", state, CoordinatorState.State.SETUP);

        List<ColumnFamilyStore> tables = Collections.singletonList(table());
        RepairCoordinator.NeighborsAndRanges neighbors = neighbors();
        state.phase.start(tables, neighbors);
        assertState("repairs", state, CoordinatorState.State.START);
        List<List<String>> expectedRanges = neighbors.commonRanges.stream().map(a -> a.ranges.stream().map(Object::toString).collect(Collectors.toList())).collect(Collectors.toList());
        assertRowsIgnoringOrder(execute(t("SELECT id, completed, participants, table_names, ranges, unfiltered_ranges, participants FROM %s.repairs")),
                                row(state.id, false, ADDRESSES_STR, tables.stream().map(a -> a.name).collect(Collectors.toList()), expectedRanges, expectedRanges, neighbors.participants.stream().map(Object::toString).collect(Collectors.toList())));

        state.phase.prepareStart();
        assertState("repairs", state, CoordinatorState.State.PREPARE_START);

        state.phase.prepareComplete();
        assertState("repairs", state, CoordinatorState.State.PREPARE_COMPLETE);

        state.phase.repairSubmitted();
        assertState("repairs", state, CoordinatorState.State.REPAIR_START);

        state.phase.repairCompleted();
        assertState("repairs", state, CoordinatorState.State.REPAIR_COMPLETE);

        state.phase.success("testing");
        assertSuccess("repairs", state);

        // make sure serialization works
        execute(t("SELECT * FROM %s.repairs"));
    }

    @Test
    public void sessions() throws Throwable
    {
        assertEmpty("repair_sessions");

        SessionState state = session();
        assertInit("repair_sessions", state);

        state.phase.start();
        assertState("repair_sessions", state, SessionState.State.START);

        state.phase.jobsSubmitted();
        assertState("repair_sessions", state, SessionState.State.JOBS_START);

        state.phase.success("testing");
        assertSuccess("repair_sessions", state);

        assertRowsIgnoringOrder(execute(t("SELECT participants FROM %s.repair_sessions WHERE id=?"), state.id),
                                row(ADDRESSES_STR));

        // make sure serialization works
        execute(t("SELECT * FROM %s.repair_sessions"));
    }

    @Test
    public void jobs() throws Throwable
    {
        assertEmpty("repair_jobs");

        JobState state = job();
        assertInit("repair_jobs", state);

        state.phase.start();
        assertState("repair_jobs", state, JobState.State.START);

        state.phase.snapshotsSubmitted();
        assertState("repair_jobs", state, JobState.State.SNAPSHOT_START);
        state.phase.snapshotsCompleted();
        assertState("repair_jobs", state, JobState.State.SNAPSHOT_COMPLETE);
        state.phase.validationSubmitted();
        assertState("repair_jobs", state, JobState.State.VALIDATION_START);
        state.phase.validationCompleted();
        assertState("repair_jobs", state, JobState.State.VALIDATION_COMPLETE);
        state.phase.streamSubmitted();
        assertState("repair_jobs", state, JobState.State.STREAM_START);

        state.phase.success("testing");
        assertSuccess("repair_jobs", state);

        assertRowsIgnoringOrder(execute(t("SELECT participants FROM %s.repair_jobs WHERE id=?"), state.id),
                                row(ADDRESSES_STR));

        // make sure serialization works
        execute(t("SELECT * FROM %s.repair_jobs"));
    }

    @Test
    public void participates() throws Throwable
    {
        assertEmpty("repair_participates");
        ParticipateState state = participate();

        assertInit("repair_participates", state);
        state.phase.success("testing");
        assertRowsIgnoringOrder(execute(t("SELECT id, initiator, ranges, failure_cause, success_message, state_init_timestamp, state_success_timestamp, state_failure_timestamp FROM %s.repair_participates WHERE id = ?"), state.id),
                                row(state.getId(), FBUtilities.getBroadcastAddressAndPort().toString(), Arrays.asList("(0,42]"), null, "testing", new Date(state.getInitializedAtMillis()), new Date(state.getLastUpdatedAtMillis()), null));

        state = participate();
        assertInit("repair_participates", state);
        state.phase.fail("testing");
        assertRowsIgnoringOrder(execute(t("SELECT id, completed, initiator, ranges, failure_cause, success_message, state_init_timestamp, state_success_timestamp, state_failure_timestamp FROM %s.repair_participates WHERE id = ?"), state.id),
                                row(state.getId(), true, FBUtilities.getBroadcastAddressAndPort().toString(), Arrays.asList("(0,42]"), "testing", null, new Date(state.getInitializedAtMillis()), null, new Date(state.getLastUpdatedAtMillis())));

        // make sure serialization works
        execute(t("SELECT * FROM %s.repair_participates"));
    }

    @Test
    public void validations() throws Throwable
    {
        assertEmpty("repair_validations");

        ValidationState state = validation();
        assertInit("repair_validations", state);

        // progress is defined by estimated partitions and how many partitions have been processed; disable checking in shared functions
        state.phase.start(100, 100);
        assertState("repair_validations", state, ValidationState.State.START);

        for (int i = 0; i < 10; i++)
        {
            state.partitionsProcessed += 10;
            state.bytesRead += 10;
            state.updated();

            // min 99% is because >= 100 gets lowered to 99% and the last 1% is when validation is actualy complete
            assertRowsIgnoringOrder(execute(t("SELECT id, initiator, status, progress_percentage, estimated_partitions, estimated_total_bytes, partitions_processed, bytes_read, failure_cause, success_message FROM %s.repair_validations")),
                                    row(state.getId(), FBUtilities.getBroadcastAddressAndPort().toString(), "start", Math.min(99.0F, (float) state.partitionsProcessed), 100L, 100L, state.partitionsProcessed, state.bytesRead, null, null));
        }

        state.phase.sendingTrees();
        assertState("repair_validations", state, ValidationState.State.SENDING_TREES);

        state.phase.success("testing");
        assertSuccess("repair_validations", state);

        // make sure serialization works
        execute(t("SELECT * FROM %s.repair_validations"));
    }

    private void assertEmpty(String table) throws Throwable
    {
        assertRowCount(execute(t("SELECT * FROM %s." + table)), 0);
    }

    private void assertInit(String table, Completable<?> state) throws Throwable
    {
        assertRowsIgnoringOrder(execute(t("SELECT id, state_init_timestamp, failure_cause, success_message FROM %s." + table + " WHERE id = ?"), state.getId()),
                                row(state.getId(), new Date(state.getInitializedAtMillis()), null, null));
    }

    private void assertInit(String table, State<?, ?> state) throws Throwable
    {
        assertRowsIgnoringOrder(execute(t("SELECT id, status, state_init_timestamp, failure_cause, success_message FROM %s." + table + " WHERE id = ?"), state.getId()),
                                row(state.getId(), "init", new Date(state.getInitializedAtMillis()), null, null));
    }

    private <T extends Enum<T>> void assertState(String table, State<?, ?> state, T expectedState) throws Throwable
    {
        assertRowsIgnoringOrder(execute(t("SELECT id, completed, status, failure_cause, success_message FROM %s." + table + " WHERE id = ?"), state.getId()),
                                row(state.getId(), false, expectedState.name().toLowerCase(), null, null));
    }

    private void assertSuccess(String table, State<?, ?> state) throws Throwable
    {
        assertRowsIgnoringOrder(execute(t("SELECT id, completed, status, failure_cause, success_message FROM %s." + table + " WHERE id = ?"), state.getId()),
                                row(state.getId(), true, "success", null, "testing"));
    }

    private static ColumnFamilyStore table()
    {
        return table(REPAIR_KS, REPAIR_TABLE);
    }

    private static ColumnFamilyStore table(String ks, String name)
    {
        return Schema.instance.getColumnFamilyStoreInstance(Schema.instance.getTableMetadata(ks, name).id);
    }

    private static RepairCoordinator.NeighborsAndRanges neighbors()
    {
        return new RepairCoordinator.NeighborsAndRanges(false, ADDRESSES, ImmutableList.of(COMMON_RANGE));
    }

    private static Range<Token> range(long a, long b)
    {
        return new Range<>(new Murmur3Partitioner.LongToken(a), new Murmur3Partitioner.LongToken(b));
    }

    private static InetAddressAndPort address(int a, int b, int c, int d)
    {
        try
        {
            return InetAddressAndPort.getByAddress(new byte[] {(byte) a, (byte) b, (byte) c, (byte) d});
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static CoordinatorState coordinator()
    {
        RepairOption options = RepairOption.parse(Collections.emptyMap(), DatabaseDescriptor.getPartitioner());
        CoordinatorState state = new CoordinatorState(Clock.Global.clock(), 0, "test", options);
        ActiveRepairService.instance().register(state);
        return state;
    }

    private static SessionState session()
    {
        CoordinatorState parent = coordinator();
        SessionState state = new SessionState(Clock.Global.clock(), parent.id, REPAIR_KS, new String[]{ REPAIR_TABLE }, COMMON_RANGE);
        parent.register(state);
        return state;
    }

    private static JobState job()
    {
        SessionState session = session();
        JobState state = new JobState(Clock.Global.clock(), new RepairJobDesc(session.parentRepairSession, session.id, session.keyspace, session.cfnames[0], session.commonRange.ranges), session.commonRange.endpoints);
        session.register(state);
        return state;
    }

    private ValidationState validation()
    {
        JobState job = job(); // job isn't needed but makes getting the descriptor easier
        ParticipateState participate = participate();
        ValidationState state = new ValidationState(Clock.Global.clock(), job.desc, ADDRESSES.stream().findFirst().get());
        participate.register(state);
        return state;
    }

    private ParticipateState participate()
    {
        List<Range<Token>> ranges = Arrays.asList(new Range<>(new Murmur3Partitioner.LongToken(0), new Murmur3Partitioner.LongToken(42)));
        ParticipateState state = new ParticipateState(Clock.Global.clock(), FBUtilities.getBroadcastAddressAndPort(), new PrepareMessage(TimeUUID.Generator.nextTimeUUID(), Collections.emptyList(), ranges, true, 42, true, PreviewKind.ALL));
        ActiveRepairService.instance().register(state);
        return state;
    }

    private static String t(String string)
    {
        return String.format(string, KS_NAME);
    }
}