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

package org.apache.cassandra.repair.consistent;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;

import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.KeyspaceRepairManager;
import org.apache.cassandra.repair.consistent.admin.CleanupSummary;
import org.apache.cassandra.repair.consistent.admin.PendingStat;
import org.apache.cassandra.repair.consistent.admin.PendingStats;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.repair.messages.FailSession;
import org.apache.cassandra.repair.messages.FinalizeCommit;
import org.apache.cassandra.repair.messages.FinalizePromise;
import org.apache.cassandra.repair.messages.FinalizePropose;
import org.apache.cassandra.repair.messages.PrepareConsistentRequest;
import org.apache.cassandra.repair.messages.PrepareConsistentResponse;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.StatusRequest;
import org.apache.cassandra.repair.messages.StatusResponse;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.repair.NoSuchRepairSessionException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.config.CassandraRelevantProperties.REPAIR_CLEANUP_INTERVAL_SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.REPAIR_DELETE_TIMEOUT_SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.REPAIR_FAIL_TIMEOUT_SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.REPAIR_STATUS_CHECK_TIMEOUT_SECONDS;
import static org.apache.cassandra.net.Verb.FAILED_SESSION_MSG;
import static org.apache.cassandra.net.Verb.FINALIZE_PROMISE_MSG;
import static org.apache.cassandra.net.Verb.PREPARE_CONSISTENT_RSP;
import static org.apache.cassandra.net.Verb.STATUS_REQ;
import static org.apache.cassandra.net.Verb.STATUS_RSP;
import static org.apache.cassandra.repair.consistent.ConsistentSession.State.*;
import static org.apache.cassandra.repair.messages.RepairMessage.always;
import static org.apache.cassandra.repair.messages.RepairMessage.sendAck;
import static org.apache.cassandra.repair.messages.RepairMessage.sendFailureResponse;

/**
 * Manages all consistent repair sessions a node is participating in.
 * <p/>
 * Since sessions need to be loaded, and since we need to handle cases where sessions might not exist, most of the logic
 * around local sessions is implemented in this class, with the LocalSession class being treated more like a simple struct,
 * in contrast with {@link CoordinatorSession}
 */
public class LocalSessions
{
    private static final Logger logger = LoggerFactory.getLogger(LocalSessions.class);
    private static final Set<Listener> listeners = new CopyOnWriteArraySet<>();

    /**
     * Amount of time a session can go without any activity before we start checking the status of other
     * participants to see if we've missed a message
     */
    static final int CHECK_STATUS_TIMEOUT = REPAIR_STATUS_CHECK_TIMEOUT_SECONDS.getInt();

    /**
     * Amount of time a session can go without any activity before being automatically set to FAILED
     */
    static final int AUTO_FAIL_TIMEOUT = REPAIR_FAIL_TIMEOUT_SECONDS.getInt();

    /**
     * Amount of time a completed session is kept around after completion before being deleted, this gives
     * compaction plenty of time to move sstables from successful sessions into the repaired bucket
     */
    static final int AUTO_DELETE_TIMEOUT = REPAIR_DELETE_TIMEOUT_SECONDS.getInt();
    /**
     * How often LocalSessions.cleanup is run
     */
    public static final int CLEANUP_INTERVAL = REPAIR_CLEANUP_INTERVAL_SECONDS.getInt();

    private static Set<TableId> uuidToTableId(Set<UUID> src)
    {
        return ImmutableSet.copyOf(Iterables.transform(src, TableId::fromUUID));
    }

    private static Set<UUID> tableIdToUuid(Set<TableId> src)
    {
        return ImmutableSet.copyOf(Iterables.transform(src, TableId::asUUID));
    }

    private final String keyspace = SchemaConstants.SYSTEM_KEYSPACE_NAME;
    private final String table = SystemKeyspace.REPAIRS;
    private final SharedContext ctx;
    private boolean started = false;
    private volatile ImmutableMap<TimeUUID, LocalSession> sessions = ImmutableMap.of();
    private volatile ImmutableMap<TableId, RepairedState> repairedStates = ImmutableMap.of();

    public LocalSessions(SharedContext ctx)
    {
        this.ctx = ctx;
    }

    @VisibleForTesting
    int getNumSessions()
    {
        return sessions.size();
    }

    @VisibleForTesting
    protected InetAddressAndPort getBroadcastAddressAndPort()
    {
        return ctx.broadcastAddressAndPort();
    }

    @VisibleForTesting
    protected boolean isAlive(InetAddressAndPort address)
    {
        return ctx.failureDetector().isAlive(address);
    }

    @VisibleForTesting
    protected boolean isNodeInitialized()
    {
        return StorageService.instance.isInitialized();
    }

    public List<Map<String, String>> sessionInfo(boolean all, Set<Range<Token>> ranges)
    {
        Iterable<LocalSession> currentSessions = sessions.values();

        if (!all)
            currentSessions = Iterables.filter(currentSessions, s -> !s.isCompleted());

        if (!ranges.isEmpty())
            currentSessions = Iterables.filter(currentSessions, s -> s.intersects(ranges));

        return Lists.newArrayList(Iterables.transform(currentSessions, LocalSessionInfo::sessionToMap));
    }

    private RepairedState getRepairedState(TableId tid)
    {
        if (!repairedStates.containsKey(tid))
        {
            synchronized (this)
            {
                if (!repairedStates.containsKey(tid))
                {
                    repairedStates = ImmutableMap.<TableId, RepairedState>builder()
                                     .putAll(repairedStates)
                                     .put(tid, new RepairedState())
                                     .build();
                }
            }
        }
        return Verify.verifyNotNull(repairedStates.get(tid));
    }

    private void maybeUpdateRepairedState(LocalSession session)
    {
        if (!shouldStoreSession(session))
            return;

        for (TableId tid : session.tableIds)
        {
            RepairedState state = getRepairedState(tid);
            state.add(session.ranges, session.repairedAt);
        }
    }

    private boolean shouldStoreSession(LocalSession session)
    {
        if (session.getState() != FINALIZED)
            return false;

        // if the session is finalized but has repairedAt set to 0, it was
        // a forced repair, and we shouldn't update the repaired state
        return session.repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE;
    }

    /**
     * Determine if all ranges and tables covered by this session
     * have since been re-repaired by a more recent session
     */
    private boolean isSuperseded(LocalSession session)
    {
        for (TableId tid : session.tableIds)
        {
            RepairedState state = repairedStates.get(tid);

            if (state == null)
                return false;

            long minRepaired = state.minRepairedAt(session.ranges);
            if (minRepaired <= session.repairedAt)
                return false;
        }

        return true;
    }

    public RepairedState.Stats getRepairedStats(TableId tid, Collection<Range<Token>> ranges)
    {
        RepairedState state = repairedStates.get(tid);

        if (state == null)
            return RepairedState.Stats.EMPTY;

        return state.getRepairedStats(ranges);
    }

    public PendingStats getPendingStats(TableId tid, Collection<Range<Token>> ranges)
    {
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tid);
        Preconditions.checkArgument(cfs != null);

        PendingStat.Builder pending = new PendingStat.Builder();
        PendingStat.Builder finalized = new PendingStat.Builder();
        PendingStat.Builder failed = new PendingStat.Builder();

        Map<TimeUUID, PendingStat> stats = cfs.getPendingRepairStats();
        for (Map.Entry<TimeUUID, PendingStat> entry : stats.entrySet())
        {
            TimeUUID sessionID = entry.getKey();
            PendingStat stat = entry.getValue();
            Verify.verify(sessionID.equals(Iterables.getOnlyElement(stat.sessions)));

            LocalSession session = sessions.get(sessionID);
            Verify.verifyNotNull(session);

            if (!Iterables.any(ranges, r -> r.intersects(session.ranges)))
                continue;

            switch (session.getState())
            {
                case FINALIZED:
                    finalized.addStat(stat);
                    break;
                case FAILED:
                    failed.addStat(stat);
                    break;
                default:
                    pending.addStat(stat);
            }
        }

        return new PendingStats(cfs.getKeyspaceName(), cfs.name, pending.build(), finalized.build(), failed.build());
    }

    public CleanupSummary cleanup(TableId tid, Collection<Range<Token>> ranges, boolean force)
    {
        Iterable<LocalSession> candidates = Iterables.filter(sessions.values(),
                                                             ls -> ls.isCompleted()
                                                                   && ls.tableIds.contains(tid)
                                                                   && Range.intersects(ls.ranges, ranges));

        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tid);
        Set<TimeUUID> sessionIds = Sets.newHashSet(Iterables.transform(candidates, s -> s.sessionID));


        return cfs.releaseRepairData(sessionIds, force);
    }

    /**
     * hook for operators to cancel sessions, cancelling from a non-coordinator is an error, unless
     * force is set to true. Messages are sent out to other participants, but we don't wait for a response
     */
    public void cancelSession(TimeUUID sessionID, boolean force)
    {
        logger.info("Cancelling local repair session {}", sessionID);
        LocalSession session = getSession(sessionID);
        Preconditions.checkArgument(session != null, "Session {} does not exist", sessionID);
        Preconditions.checkArgument(force || session.coordinator.equals(getBroadcastAddressAndPort()),
                                    "Cancel session %s from it's coordinator (%s) or use --force",
                                    sessionID, session.coordinator);

        setStateAndSave(session, FAILED);
        FailSession payload = new FailSession(sessionID);
        for (InetAddressAndPort participant : session.participants)
        {
            if (!participant.equals(getBroadcastAddressAndPort()))
                sendMessageWithRetries(payload, FAILED_SESSION_MSG, participant);
        }
    }

    /**
     * Loads sessions out of the repairs table and sets state to started
     */
    public synchronized void start()
    {
        Preconditions.checkArgument(!started, "LocalSessions.start can only be called once");
        Preconditions.checkArgument(sessions.isEmpty(), "No sessions should be added before start");
        UntypedResultSet rows = QueryProcessor.executeInternalWithPaging(String.format("SELECT * FROM %s.%s", keyspace, table), 1000);
        Map<TimeUUID, LocalSession> loadedSessions = new HashMap<>();
        Map<TableId, List<RepairedState.Level>> initialLevels = new HashMap<>();
        for (UntypedResultSet.Row row : rows)
        {
            try
            {
                LocalSession session = load(row);
                loadedSessions.put(session.sessionID, session);
                if (shouldStoreSession(session))
                {
                    for (TableId tid : session.tableIds)
                        initialLevels.computeIfAbsent(tid, (t) -> new ArrayList<>())
                                     .add(new RepairedState.Level(session.ranges, session.repairedAt));
                }
            }
            catch (IllegalArgumentException | NullPointerException e)
            {
                logger.warn("Unable to load malformed repair session {}, removing", row.has("parent_id") ? row.getTimeUUID("parent_id") : null);
                if (row.has("parent_id"))
                    deleteRow(row.getTimeUUID("parent_id"));
            }
        }
        for (Map.Entry<TableId, List<RepairedState.Level>> entry : initialLevels.entrySet())
            getRepairedState(entry.getKey()).addAll(entry.getValue());

        sessions = ImmutableMap.copyOf(loadedSessions);
        failOngoingRepairs();
        started = true;
    }

    public synchronized void stop()
    {
        if (!started)
            return;
        started = false;
        failOngoingRepairs();
    }

    private void failOngoingRepairs()
    {
        for (LocalSession session : sessions.values())
        {
            synchronized (session)
            {
                switch (session.getState())
                {
                    case FAILED:
                    case FINALIZED:
                    case FINALIZE_PROMISED:
                        continue;
                    default:
                        logger.info("Found repair session {} with state = {} - failing the repair", session.sessionID, session.getState());
                        failSession(session, true);
                }
            }
        }
    }

    public boolean isStarted()
    {
        return started;
    }

    private static boolean shouldCheckStatus(LocalSession session, long now)
    {
        return !session.isCompleted() && (now > session.getLastUpdate() + CHECK_STATUS_TIMEOUT);
    }

    private static boolean shouldFail(LocalSession session, long now)
    {
        return !session.isCompleted() && (now > session.getLastUpdate() + AUTO_FAIL_TIMEOUT);
    }

    private static boolean shouldDelete(LocalSession session, long now)
    {
        return session.isCompleted() && (now > session.getLastUpdate() + AUTO_DELETE_TIMEOUT);
    }

    /**
     * Auto fails and auto deletes timed out and old sessions
     * Compaction will clean up the sstables still owned by a deleted session
     */
    public void cleanup()
    {
        logger.trace("Running LocalSessions.cleanup");
        if (!isNodeInitialized())
        {
            logger.trace("node not initialized, aborting local session cleanup");
            return;
        }
        Set<LocalSession> currentSessions = new HashSet<>(sessions.values());
        for (LocalSession session : currentSessions)
        {
            synchronized (session)
            {
                long now = ctx.clock().nowInSeconds();
                if (shouldFail(session, now))
                {
                    logger.warn("Auto failing timed out repair session {}", session);
                    failSession(session.sessionID, false);
                }
                else if (shouldDelete(session, now))
                {
                    if (session.getState() == FINALIZED && !isSuperseded(session))
                    {
                        // if we delete a non-superseded session, some ranges will be mis-reported as
                        // not having been repaired in repair_admin after a restart
                        logger.info("Skipping delete of FINALIZED LocalSession {} because it has " +
                                    "not been superseded by a more recent session", session.sessionID);
                    }
                    else if (!sessionHasData(session))
                    {
                        logger.info("Auto deleting repair session {}", session);
                        deleteSession(session.sessionID);
                    }
                    else
                    {
                        logger.warn("Skipping delete of LocalSession {} because it still contains sstables", session.sessionID);
                    }
                }
                else if (shouldCheckStatus(session, now))
                {
                    sendStatusRequest(session);
                }
            }
        }
    }

    private static ByteBuffer serializeRange(Range<Token> range)
    {
        int size = (int) Token.serializer.serializedSize(range.left, 0);
        size += (int) Token.serializer.serializedSize(range.right, 0);
        try (DataOutputBuffer buffer = new DataOutputBuffer(size))
        {
            Token.serializer.serialize(range.left, buffer, 0);
            Token.serializer.serialize(range.right, buffer, 0);
            return buffer.buffer();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Set<ByteBuffer> serializeRanges(Set<Range<Token>> ranges)
    {
        Set<ByteBuffer> buffers = new HashSet<>(ranges.size());
        ranges.forEach(r -> buffers.add(serializeRange(r)));
        return buffers;
    }

    private static Range<Token> deserializeRange(ByteBuffer bb)
    {
        try (DataInputBuffer in = new DataInputBuffer(bb, false))
        {
            IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
            Token left = Token.serializer.deserialize(in, partitioner, 0);
            Token right = Token.serializer.deserialize(in, partitioner, 0);
            return new Range<>(left, right);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Set<Range<Token>> deserializeRanges(Set<ByteBuffer> buffers)
    {
        Set<Range<Token>> ranges = new HashSet<>(buffers.size());
        buffers.forEach(bb -> ranges.add(deserializeRange(bb)));
        return ranges;
    }

    /**
     * Save session state to table
     */
    @VisibleForTesting
    void save(LocalSession session)
    {
        String query = "INSERT INTO %s.%s " +
                       "(parent_id, " +
                       "started_at, " +
                       "last_update, " +
                       "repaired_at, " +
                       "state, " +
                       "coordinator, " +
                       "coordinator_port, " +
                       "participants, " +
                       "participants_wp," +
                       "ranges, " +
                       "cfids) " +
                       "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        QueryProcessor.executeInternal(String.format(query, keyspace, table),
                                       session.sessionID,
                                       Date.from(Instant.ofEpochSecond(session.startedAt)),
                                       Date.from(Instant.ofEpochSecond(session.getLastUpdate())),
                                       Date.from(Instant.ofEpochMilli(session.repairedAt)),
                                       session.getState().ordinal(),
                                       session.coordinator.getAddress(),
                                       session.coordinator.getPort(),
                                       session.participants.stream().map(participant -> participant.getAddress()).collect(Collectors.toSet()),
                                       session.participants.stream().map(participant -> participant.getHostAddressAndPort()).collect(Collectors.toSet()),
                                       serializeRanges(session.ranges),
                                       tableIdToUuid(session.tableIds));

        maybeUpdateRepairedState(session);
    }

    private static int dateToSeconds(Date d)
    {
        return Ints.checkedCast(TimeUnit.MILLISECONDS.toSeconds(d.getTime()));
    }

    private LocalSession load(UntypedResultSet.Row row)
    {
        LocalSession.Builder builder = LocalSession.builder(ctx);
        builder.withState(ConsistentSession.State.valueOf(row.getInt("state")));
        builder.withSessionID(row.getTimeUUID("parent_id"));
        InetAddressAndPort coordinator = InetAddressAndPort.getByAddressOverrideDefaults(
            row.getInetAddress("coordinator"),
            row.getInt("coordinator_port"));
        builder.withCoordinator(coordinator);
        builder.withTableIds(uuidToTableId(row.getSet("cfids", UUIDType.instance)));
        builder.withRepairedAt(row.getTimestamp("repaired_at").getTime());
        builder.withRanges(deserializeRanges(row.getSet("ranges", BytesType.instance)));
        //There is no cross version streaming and thus no cross version repair so assume that
        //any valid repair sessions has the participants_wp column and any that doesn't is malformed
        Set<String> participants = row.getSet("participants_wp", UTF8Type.instance);
        builder.withParticipants(participants.stream().map(participant ->
                                                             {
                                                                 try
                                                                 {
                                                                     return InetAddressAndPort.getByName(participant);
                                                                 }
                                                                 catch (UnknownHostException e)
                                                                 {
                                                                     throw new RuntimeException(e);
                                                                 }
                                                             }).collect(Collectors.toSet()));
        builder.withStartedAt(dateToSeconds(row.getTimestamp("started_at")));
        builder.withLastUpdate(dateToSeconds(row.getTimestamp("last_update")));

        return buildSession(builder);
    }

    private void deleteRow(TimeUUID sessionID)
    {
        String query = "DELETE FROM %s.%s WHERE parent_id=?";
        QueryProcessor.executeInternal(String.format(query, keyspace, table), sessionID);
    }

    private void syncTable()
    {
        TableId tid = Schema.instance.getTableMetadata(keyspace, table).id;
        ColumnFamilyStore cfm = Schema.instance.getColumnFamilyStoreInstance(tid);
        cfm.forceBlockingFlush(ColumnFamilyStore.FlushReason.INTERNALLY_FORCED);
    }

    /**
     * Loads a session directly from the table. Should be used for testing only
     */
    @VisibleForTesting
    LocalSession loadUnsafe(TimeUUID sessionId)
    {
        String query = "SELECT * FROM %s.%s WHERE parent_id=?";
        UntypedResultSet result = QueryProcessor.executeInternal(String.format(query, keyspace, table), sessionId);
        if (result.isEmpty())
            return null;

        UntypedResultSet.Row row = result.one();
        return load(row);
    }

    @VisibleForTesting
    protected LocalSession buildSession(LocalSession.Builder builder)
    {
        return new LocalSession(builder);
    }

    public LocalSession getSession(TimeUUID sessionID)
    {
        return sessions.get(sessionID);
    }

    @VisibleForTesting
    synchronized boolean putSessionUnsafe(LocalSession session)
    {
        if (sessions.containsKey(session.sessionID))
            return false;
        putSession(session);
        save(session);
        return true;
    }

    private synchronized void putSession(LocalSession session)
    {
        Preconditions.checkArgument(!sessions.containsKey(session.sessionID),
                                    "LocalSession %s already exists", session.sessionID);
        Preconditions.checkArgument(started, "sessions cannot be added before LocalSessions is started");
        sessions = ImmutableMap.<TimeUUID, LocalSession>builder()
                               .putAll(sessions)
                               .put(session.sessionID, session)
                               .build();
    }

    private synchronized void removeSession(TimeUUID sessionID)
    {
        Preconditions.checkArgument(sessionID != null);
        Map<TimeUUID, LocalSession> temp = new HashMap<>(sessions);
        temp.remove(sessionID);
        sessions = ImmutableMap.copyOf(temp);
    }

    @VisibleForTesting
    LocalSession createSessionUnsafe(TimeUUID sessionId, ActiveRepairService.ParentRepairSession prs, Set<InetAddressAndPort> peers)
    {
        LocalSession.Builder builder = LocalSession.builder(ctx);
        builder.withState(ConsistentSession.State.PREPARING);
        builder.withSessionID(sessionId);
        builder.withCoordinator(prs.coordinator);

        builder.withTableIds(prs.getTableIds());
        builder.withRepairedAt(prs.repairedAt);
        builder.withRanges(prs.getRanges());
        builder.withParticipants(peers);

        long now = ctx.clock().nowInSeconds();
        builder.withStartedAt(now);
        builder.withLastUpdate(now);

        return buildSession(builder);
    }

    protected ActiveRepairService.ParentRepairSession getParentRepairSession(TimeUUID sessionID) throws NoSuchRepairSessionException
    {

        return ctx.repair().getParentRepairSession(sessionID);
    }

    protected void sendMessage(InetAddressAndPort destination, Message<? extends RepairMessage> message)
    {
        logger.trace("sending {} to {}", message.payload, destination);
        ctx.messaging().send(message, destination);
    }

    @VisibleForTesting
    void setStateAndSave(LocalSession session, ConsistentSession.State state)
    {
        maybeSetStateAndSave(session, null, state);
    }

    private boolean maybeSetStateAndSave(LocalSession session, @Nullable ConsistentSession.State expected, ConsistentSession.State state)
    {
        synchronized (session)
        {
            Preconditions.checkArgument(session.getState().canTransitionTo(state),
                                        "Invalid state transition %s -> %s",
                                        session.getState(), state);
            if (expected != null && session.getState() != expected)
                return false;
            logger.trace("Changing LocalSession state from {} -> {} for {}", session.getState(), state, session.sessionID);
            boolean wasCompleted = session.isCompleted();
            session.setState(state);
            session.setLastUpdate();
            save(session);

            if (session.isCompleted() && !wasCompleted)
            {
                sessionCompleted(session);
            }
            for (Listener listener : listeners)
                listener.onIRStateChange(session);
            return true;
        }
    }

    public void failSession(TimeUUID sessionID)
    {
        failSession(sessionID, true);
    }

    public void failSession(TimeUUID sessionID, boolean sendMessage)
    {
        failSession(getSession(sessionID), sendMessage);
    }

    public void failSession(LocalSession session, boolean sendMessage)
    {
        if (session != null)
        {
            synchronized (session)
            {
                if (session.getState() == FINALIZED)
                {
                    logger.error("Can't change the state of session {} from FINALIZED to FAILED", session.sessionID, new RuntimeException());
                    return;
                }
                else if (session.getState() != FAILED)
                {
                    logger.info("Failing local repair session {}", session.sessionID);
                    setStateAndSave(session, FAILED);
                }
            }
            if (sendMessage)
            {
                sendMessageWithRetries(new FailSession(session.sessionID), FAILED_SESSION_MSG, session.coordinator);
            }
        }
    }

    public synchronized void deleteSession(TimeUUID sessionID)
    {
        logger.info("Deleting local repair session {}", sessionID);
        LocalSession session = getSession(sessionID);
        Preconditions.checkArgument(session.isCompleted(), "Cannot delete incomplete sessions");

        deleteRow(sessionID);
        removeSession(sessionID);
    }

    @VisibleForTesting
    Future<List<Void>> prepareSession(KeyspaceRepairManager repairManager,
                                      TimeUUID sessionID,
                                      Collection<ColumnFamilyStore> tables,
                                      RangesAtEndpoint tokenRanges,
                                      ExecutorService executor,
                                      BooleanSupplier isCancelled)
    {
        return repairManager.prepareIncrementalRepair(sessionID, tables, tokenRanges, executor, isCancelled);
    }

    RangesAtEndpoint filterLocalRanges(String keyspace, Set<Range<Token>> ranges)
    {
        RangesAtEndpoint localRanges = StorageService.instance.getLocalReplicas(keyspace);
        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(localRanges.endpoint());
        for (Range<Token> range : ranges)
        {
            for (Replica replica : localRanges)
            {
                if (replica.range().equals(range))
                {
                    builder.add(replica);
                }
                else if (replica.contains(range))
                {
                    builder.add(replica.decorateSubrange(range));
                }
            }

        }
        return builder.build();
    }

    /**
     * The PrepareConsistentRequest promotes the parent repair session to a consistent incremental
     * session, and isolates the data to be repaired from the rest of the table's data
     *
     * No response is sent to the repair coordinator until the data preparation / isolation has completed
     * successfully. If the data preparation fails, a failure message is sent to the coordinator,
     * cancelling the session.
     */
    public void handlePrepareMessage(Message<? extends RepairMessage> message)
    {
        InetAddressAndPort from = message.from();
        PrepareConsistentRequest request = (PrepareConsistentRequest) message.payload;
        logger.trace("received {} from {}", request, from);
        TimeUUID sessionID = request.parentSession;
        InetAddressAndPort coordinator = request.coordinator;
        Set<InetAddressAndPort> peers = request.participants;

        ActiveRepairService.ParentRepairSession parentSession;
        try
        {
            parentSession = getParentRepairSession(sessionID);
        }
        catch (Throwable e)
        {
            logger.error("Error retrieving ParentRepairSession for session {}, responding with failure", sessionID);
            sendFailureResponse(ctx, message);
            sendMessageWithRetries(always(), new PrepareConsistentResponse(sessionID, getBroadcastAddressAndPort(), false), PREPARE_CONSISTENT_RSP, coordinator);
            return;
        }

        LocalSession session = createSessionUnsafe(sessionID, parentSession, peers);
        sendAck(ctx, message);
        if (!putSessionUnsafe(session))
            return;
        logger.info("Beginning local incremental repair session {}", session);

        ExecutorService executor = ctx.executorFactory().pooled("Repair-" + sessionID, parentSession.getColumnFamilyStores().size());

        KeyspaceRepairManager repairManager = parentSession.getKeyspace().getRepairManager();
        RangesAtEndpoint tokenRanges = filterLocalRanges(parentSession.getKeyspace().getName(), parentSession.getRanges());
        Future<List<Void>> repairPreparation = prepareSession(repairManager, sessionID, parentSession.getColumnFamilyStores(),
                                                          tokenRanges, executor, () -> session.getState() != PREPARING);

        repairPreparation.addCallback(new FutureCallback<List<Void>>()
        {
            public void onSuccess(@Nullable List<Void> result)
            {
                try
                {
                    logger.info("Prepare phase for incremental repair session {} completed", sessionID);
                    if (!prepareSessionExceptFailed(session))
                        logger.info("Session {} failed before anticompaction completed", sessionID);
                    sendMessageWithRetries(always(), new PrepareConsistentResponse(sessionID, getBroadcastAddressAndPort(), session.getState() != FAILED), PREPARE_CONSISTENT_RSP, coordinator);
                }
                finally
                {
                    executor.shutdown();
                }
            }

            public void onFailure(Throwable t)
            {
                try
                {
                    if (Throwables.anyCauseMatches(t, (throwable) -> throwable instanceof CompactionInterruptedException))
                        logger.info("Anticompaction interrupted for session {}: {}", sessionID, t.getMessage());
                    else if (Throwables.anyCauseMatches(t, (throwable) -> throwable instanceof NoSuchRepairSessionException))
                        logger.warn("No such repair session: {}", sessionID);
                    else
                        logger.error("Prepare phase for incremental repair session {} failed", sessionID, t);
                    sendMessageWithRetries(always(), new PrepareConsistentResponse(sessionID, getBroadcastAddressAndPort(), false), PREPARE_CONSISTENT_RSP, coordinator);
                    failSession(sessionID, false);
                }
                finally
                {
                    executor.shutdown();
                }
            }
        });
    }

    private void sendMessageWithRetries(Supplier<Boolean> allowRetry, RepairMessage request, Verb verb, InetAddressAndPort endpoint)
    {
        RepairMessage.sendMessageWithRetries(ctx, allowRetry, request, verb, endpoint);
    }

    private void sendMessageWithRetries(RepairMessage request, Verb verb, InetAddressAndPort endpoint)
    {
        RepairMessage.sendMessageWithRetries(ctx, request, verb, endpoint);
    }

    /**
     * Checks for the session state, and sets it to prepared unless it is on a failed state.
     * Making the checks inside a synchronized block to prevent the session state from
     * being changed between the read and the update.
     *
     * @param session The local session to be set to prepared.
     * @return true if the session is prepared, false if not, i.e. session failed
     */
    private boolean prepareSessionExceptFailed(LocalSession session) {
        synchronized (session)
        {
            if (session.getState() == FAILED)
                return false;

            setStateAndSave(session, PREPARED);
            return true;
        }
    }

    public void maybeSetRepairing(TimeUUID sessionID)
    {
        LocalSession session = getSession(sessionID);
        if (session != null && session.getState() != REPAIRING)
        {
            logger.info("Setting local incremental repair session {} to REPAIRING", session);
            setStateAndSave(session, REPAIRING);
        }
    }

    public void handleFinalizeProposeMessage(Message<? extends RepairMessage> message)
    {
        InetAddressAndPort from = message.from();
        FinalizePropose propose = (FinalizePropose) message.payload;
        logger.trace("received {} from {}", propose, from);
        TimeUUID sessionID = propose.sessionID;
        LocalSession session = getSession(sessionID);
        if (session == null)
        {
            logger.info("Received FinalizePropose message for unknown repair session {}, responding with failure", sessionID);
            sendFailureResponse(ctx, message);
            sendMessageWithRetries(new FailSession(sessionID), FAILED_SESSION_MSG, from);
            return;
        }

        sendAck(ctx, message);
        try
        {
            if (!maybeSetStateAndSave(session, REPAIRING, FINALIZE_PROMISED))
                return;

            /*
             Flushing the repairs table here, *before* responding to the coordinator prevents a scenario where we respond
             with a promise to the coordinator, but there is a failure before the commit log mutation with the
             FINALIZE_PROMISED status is synced to disk. This could cause the state for this session to revert to an
             earlier status on startup, which would prevent the failure recovery mechanism from ever being able to promote
             this session to FINALIZED, likely creating inconsistencies in the repaired data sets across nodes.
             */
            syncTable();

            RepairMessage.sendMessageWithRetries(ctx, new FinalizePromise(sessionID, getBroadcastAddressAndPort(), true), FINALIZE_PROMISE_MSG, from);
            logger.info("Received FinalizePropose message for incremental repair session {}, responded with FinalizePromise", sessionID);
        }
        catch (IllegalArgumentException e)
        {
            logger.error("Error handling FinalizePropose message for {}", session, e);
            failSession(sessionID);
        }
    }

    @VisibleForTesting
    protected void sessionCompleted(LocalSession session)
    {
        for (TableId tid: session.tableIds)
        {
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tid);
            if (cfs != null)
            {
                cfs.getRepairManager().incrementalSessionCompleted(session.sessionID);
            }
        }
    }

    /**
     * Finalizes the repair session, completing it as successful.
     *
     * This only changes the state of the session, it doesn't promote the siloed sstables to repaired. That will happen
     * as part of the compaction process, and avoids having to worry about in progress compactions interfering with the
     * promotion.
     */
    public void handleFinalizeCommitMessage(Message<? extends RepairMessage> message)
    {
        InetAddressAndPort from = message.from();
        FinalizeCommit commit = (FinalizeCommit) message.payload;
        logger.trace("received {} from {}", commit, from);
        TimeUUID sessionID = commit.sessionID;
        LocalSession session = getSession(sessionID);
        if (session == null)
        {
            logger.warn("Ignoring FinalizeCommit message for unknown repair session {}", sessionID);
            sendFailureResponse(ctx, message);
            return;
        }
        sendAck(ctx, message);

        if (maybeSetStateAndSave(session, FINALIZE_PROMISED, FINALIZED))
            logger.info("Finalized local repair session {}", sessionID);
    }

    public void handleFailSessionMessage(InetAddressAndPort from, FailSession msg)
    {
        logger.trace("received {} from {}", msg, from);
        failSession(msg.sessionID, false);
    }

    public void sendStatusRequest(LocalSession session)
    {
        logger.info("Attempting to learn the outcome of unfinished local incremental repair session {}", session.sessionID);
        Message<StatusRequest> request = Message.out(STATUS_REQ, new StatusRequest(session.sessionID));

        for (InetAddressAndPort participant : session.participants)
        {
            if (!getBroadcastAddressAndPort().equals(participant) && isAlive(participant))
            {
                sendMessage(participant, request);
            }
        }
    }

    public void handleStatusRequest(InetAddressAndPort from, StatusRequest request)
    {
        logger.trace("received {} from {}", request, from);
        TimeUUID sessionID = request.sessionID;
        LocalSession session = getSession(sessionID);
        if (session == null)
        {
            logger.warn("Received status request message for unknown session {}", sessionID);
            sendMessage(from, Message.out(STATUS_RSP, new StatusResponse(sessionID, FAILED)));
        }
        else
        {
            sendMessage(from, Message.out(STATUS_RSP, new StatusResponse(sessionID, session.getState())));
            logger.info("Responding to status response message for incremental repair session {} with local state {}", sessionID, session.getState());
       }
    }

    public void handleStatusResponse(InetAddressAndPort from, StatusResponse response)
    {
        logger.trace("received {} from {}", response, from);
        TimeUUID sessionID = response.sessionID;
        LocalSession session = getSession(sessionID);
        if (session == null)
        {
            logger.warn("Received StatusResponse message for unknown repair session {}", sessionID);
            return;
        }

        // only change local state if response state is FINALIZED or FAILED, since those are
        // the only statuses that would indicate we've missed a message completing the session
        if (response.state == FINALIZED || response.state == FAILED)
        {
            setStateAndSave(session, response.state);
            logger.info("Unfinished local incremental repair session {} set to state {}", sessionID, response.state);
        }
        else
        {
            logger.info("Received StatusResponse for repair session {} with state {}, which is not actionable. Doing nothing.", sessionID, response.state);
        }
    }

    /**
     * determines if a local session exists, and if it's not finalized or failed
     */
    public boolean isSessionInProgress(TimeUUID sessionID)
    {
        LocalSession session = getSession(sessionID);
        return session != null && session.getState() != FINALIZED && session.getState() != FAILED;
    }

    /**
     * determines if a local session exists, and if it's in the finalized state
     */
    public boolean isSessionFinalized(TimeUUID sessionID)
    {
        LocalSession session = getSession(sessionID);
        return session != null && session.getState() == FINALIZED;
    }

    /**
     * determines if a local session exists
     */
    public boolean sessionExists(TimeUUID sessionID)
    {
        return getSession(sessionID) != null;
    }

    @VisibleForTesting
    protected boolean sessionHasData(LocalSession session)
    {
        Predicate<TableId> predicate = tid -> {
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tid);
            return cfs != null && cfs.getCompactionStrategyManager().hasDataForPendingRepair(session.sessionID);

        };
        return Iterables.any(session.tableIds, predicate::test);
    }

    /**
     * Returns the repairedAt time for a sessions which is unknown, failed, or finalized
     * calling this for a session which is in progress throws an exception
     */
    public long getFinalSessionRepairedAt(TimeUUID sessionID)
    {
        LocalSession session = getSession(sessionID);
        if (session == null || session.getState() == FAILED)
        {
            return ActiveRepairService.UNREPAIRED_SSTABLE;
        }
        else if (session.getState() == FINALIZED)
        {
            return session.repairedAt;
        }
        else
        {
            throw new IllegalStateException("Cannot get final repaired at value for in progress session: " + session);
        }
    }

    public static void registerListener(Listener listener)
    {
        listeners.add(listener);
    }

    public static void unregisterListener(Listener listener)
    {
        listeners.remove(listener);
    }

    @VisibleForTesting
    public static void unsafeClearListeners()
    {
        listeners.clear();
    }

    public interface Listener
    {
        void onIRStateChange(LocalSession session);
    }
}
