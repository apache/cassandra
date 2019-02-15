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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.cassandra.locator.RangesAtEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.repair.KeyspaceRepairManager;
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
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.FailSession;
import org.apache.cassandra.repair.messages.FinalizeCommit;
import org.apache.cassandra.repair.messages.FinalizePromise;
import org.apache.cassandra.repair.messages.FinalizePropose;
import org.apache.cassandra.repair.messages.PrepareConsistentRequest;
import org.apache.cassandra.repair.messages.PrepareConsistentResponse;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.StatusRequest;
import org.apache.cassandra.repair.messages.StatusResponse;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.repair.consistent.ConsistentSession.State.*;

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

    /**
     * Amount of time a session can go without any activity before we start checking the status of other
     * participants to see if we've missed a message
     */
    static final int CHECK_STATUS_TIMEOUT = Integer.getInteger("cassandra.repair_status_check_timeout_seconds",
                                                               Ints.checkedCast(TimeUnit.HOURS.toSeconds(1)));

    /**
     * Amount of time a session can go without any activity before being automatically set to FAILED
     */
    static final int AUTO_FAIL_TIMEOUT = Integer.getInteger("cassandra.repair_fail_timeout_seconds",
                                                            Ints.checkedCast(TimeUnit.DAYS.toSeconds(1)));

    /**
     * Amount of time a completed session is kept around after completion before being deleted, this gives
     * compaction plenty of time to move sstables from successful sessions into the repaired bucket
     */
    static final int AUTO_DELETE_TIMEOUT = Integer.getInteger("cassandra.repair_delete_timeout_seconds",
                                                              Ints.checkedCast(TimeUnit.DAYS.toSeconds(1)));
    /**
     * How often LocalSessions.cleanup is run
     */
    public static final int CLEANUP_INTERVAL = Integer.getInteger("cassandra.repair_cleanup_interval_seconds",
                                                                  Ints.checkedCast(TimeUnit.MINUTES.toSeconds(10)));

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
    private boolean started = false;
    private volatile ImmutableMap<UUID, LocalSession> sessions = ImmutableMap.of();

    @VisibleForTesting
    int getNumSessions()
    {
        return sessions.size();
    }

    @VisibleForTesting
    protected InetAddressAndPort getBroadcastAddressAndPort()
    {
        return FBUtilities.getBroadcastAddressAndPort();
    }

    @VisibleForTesting
    protected boolean isAlive(InetAddressAndPort address)
    {
        return FailureDetector.instance.isAlive(address);
    }

    @VisibleForTesting
    protected boolean isNodeInitialized()
    {
        return StorageService.instance.isInitialized();
    }

    public List<Map<String, String>> sessionInfo(boolean all)
    {
        Iterable<LocalSession> currentSessions = sessions.values();
        if (!all)
        {
            currentSessions = Iterables.filter(currentSessions, s -> !s.isCompleted());
        }
        return Lists.newArrayList(Iterables.transform(currentSessions, LocalSessionInfo::sessionToMap));
    }

    /**
     * hook for operators to cancel sessions, cancelling from a non-coordinator is an error, unless
     * force is set to true. Messages are sent out to other participants, but we don't wait for a response
     */
    public void cancelSession(UUID sessionID, boolean force)
    {
        logger.info("Cancelling local repair session {}", sessionID);
        LocalSession session = getSession(sessionID);
        Preconditions.checkArgument(session != null, "Session {} does not exist", sessionID);
        Preconditions.checkArgument(force || session.coordinator.equals(getBroadcastAddressAndPort()),
                                    "Cancel session %s from it's coordinator (%s) or use --force",
                                    sessionID, session.coordinator);

        setStateAndSave(session, FAILED);
        for (InetAddressAndPort participant : session.participants)
        {
            if (!participant.equals(getBroadcastAddressAndPort()))
                sendMessage(participant, new FailSession(sessionID));
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
        Map<UUID, LocalSession> loadedSessions = new HashMap<>();
        for (UntypedResultSet.Row row : rows)
        {
            try
            {
                LocalSession session = load(row);
                loadedSessions.put(session.sessionID, session);
            }
            catch (IllegalArgumentException | NullPointerException e)
            {
                logger.warn("Unable to load malformed repair session {}, ignoring", row.has("parent_id") ? row.getUUID("parent_id") : null);
            }
        }
        sessions = ImmutableMap.copyOf(loadedSessions);
        started = true;
    }

    public boolean isStarted()
    {
        return started;
    }

    private static boolean shouldCheckStatus(LocalSession session, int now)
    {
        return !session.isCompleted() && (now > session.getLastUpdate() + CHECK_STATUS_TIMEOUT);
    }

    private static boolean shouldFail(LocalSession session, int now)
    {
        return !session.isCompleted() && (now > session.getLastUpdate() + AUTO_FAIL_TIMEOUT);
    }

    private static boolean shouldDelete(LocalSession session, int now)
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
                int now = FBUtilities.nowInSeconds();
                if (shouldFail(session, now))
                {
                    logger.warn("Auto failing timed out repair session {}", session);
                    failSession(session.sessionID, false);
                }
                else if (shouldDelete(session, now))
                {
                    if (!sessionHasData(session))
                    {
                        logger.debug("Auto deleting repair session {}", session);
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
                                       session.coordinator.address,
                                       session.coordinator.port,
                                       session.participants.stream().map(participant -> participant.address).collect(Collectors.toSet()),
                                       session.participants.stream().map(participant -> participant.toString()).collect(Collectors.toSet()),
                                       serializeRanges(session.ranges),
                                       tableIdToUuid(session.tableIds));
    }

    private static int dateToSeconds(Date d)
    {
        return Ints.checkedCast(TimeUnit.MILLISECONDS.toSeconds(d.getTime()));
    }

    private LocalSession load(UntypedResultSet.Row row)
    {
        LocalSession.Builder builder = LocalSession.builder();
        builder.withState(ConsistentSession.State.valueOf(row.getInt("state")));
        builder.withSessionID(row.getUUID("parent_id"));
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

    private void deleteRow(UUID sessionID)
    {
        String query = "DELETE FROM %s.%s WHERE parent_id=?";
        QueryProcessor.executeInternal(String.format(query, keyspace, table), sessionID);
    }

    private void syncTable()
    {
        TableId tid = Schema.instance.getTableMetadata(keyspace, table).id;
        ColumnFamilyStore cfm = Schema.instance.getColumnFamilyStoreInstance(tid);
        cfm.forceBlockingFlush();
    }

    /**
     * Loads a session directly from the table. Should be used for testing only
     */
    @VisibleForTesting
    LocalSession loadUnsafe(UUID sessionId)
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

    protected LocalSession getSession(UUID sessionID)
    {
        return sessions.get(sessionID);
    }

    @VisibleForTesting
    synchronized void putSessionUnsafe(LocalSession session)
    {
        putSession(session);
        save(session);
    }

    private synchronized void putSession(LocalSession session)
    {
        Preconditions.checkArgument(!sessions.containsKey(session.sessionID),
                                    "LocalSession {} already exists", session.sessionID);
        Preconditions.checkArgument(started, "sessions cannot be added before LocalSessions is started");
        sessions = ImmutableMap.<UUID, LocalSession>builder()
                               .putAll(sessions)
                               .put(session.sessionID, session)
                               .build();
    }

    private synchronized void removeSession(UUID sessionID)
    {
        Preconditions.checkArgument(sessionID != null);
        Map<UUID, LocalSession> temp = new HashMap<>(sessions);
        temp.remove(sessionID);
        sessions = ImmutableMap.copyOf(temp);
    }

    @VisibleForTesting
    LocalSession createSessionUnsafe(UUID sessionId, ActiveRepairService.ParentRepairSession prs, Set<InetAddressAndPort> peers)
    {
        LocalSession.Builder builder = LocalSession.builder();
        builder.withState(ConsistentSession.State.PREPARING);
        builder.withSessionID(sessionId);
        builder.withCoordinator(prs.coordinator);

        builder.withTableIds(prs.getTableIds());
        builder.withRepairedAt(prs.repairedAt);
        builder.withRanges(prs.getRanges());
        builder.withParticipants(peers);

        int now = FBUtilities.nowInSeconds();
        builder.withStartedAt(now);
        builder.withLastUpdate(now);

        return buildSession(builder);
    }

    protected ActiveRepairService.ParentRepairSession getParentRepairSession(UUID sessionID)
    {
        return ActiveRepairService.instance.getParentRepairSession(sessionID);
    }

    protected void sendMessage(InetAddressAndPort destination, RepairMessage message)
    {
        logger.trace("sending {} to {}", message, destination);
        MessageOut<RepairMessage> messageOut = new MessageOut<RepairMessage>(MessagingService.Verb.REPAIR_MESSAGE, message, RepairMessage.serializer);
        MessagingService.instance().sendOneWay(messageOut, destination);
    }

    private void setStateAndSave(LocalSession session, ConsistentSession.State state)
    {
        synchronized (session)
        {
            Preconditions.checkArgument(session.getState().canTransitionTo(state),
                                        "Invalid state transition %s -> %s",
                                        session.getState(), state);
            logger.trace("Changing LocalSession state from {} -> {} for {}", session.getState(), state, session.sessionID);
            boolean wasCompleted = session.isCompleted();
            session.setState(state);
            session.setLastUpdate();
            save(session);

            if (session.isCompleted() && !wasCompleted)
            {
                sessionCompleted(session);
            }
        }
    }

    public void failSession(UUID sessionID)
    {
        failSession(sessionID, true);
    }

    public void failSession(UUID sessionID, boolean sendMessage)
    {
        LocalSession session = getSession(sessionID);
        if (session != null)
        {
            synchronized (session)
            {
                if (session.getState() != FAILED)
                {
                    logger.info("Failing local repair session {}", sessionID);
                    setStateAndSave(session, FAILED);
                }
            }
            if (sendMessage)
            {
                sendMessage(session.coordinator, new FailSession(sessionID));
            }
        }
    }

    public synchronized void deleteSession(UUID sessionID)
    {
        logger.debug("Deleting local repair session {}", sessionID);
        LocalSession session = getSession(sessionID);
        Preconditions.checkArgument(session.isCompleted(), "Cannot delete incomplete sessions");

        deleteRow(sessionID);
        removeSession(sessionID);
    }

    @VisibleForTesting
    ListenableFuture prepareSession(KeyspaceRepairManager repairManager,
                                    UUID sessionID,
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
    public void handlePrepareMessage(InetAddressAndPort from, PrepareConsistentRequest request)
    {
        logger.trace("received {} from {}", request, from);
        UUID sessionID = request.parentSession;
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
            sendMessage(coordinator, new PrepareConsistentResponse(sessionID, getBroadcastAddressAndPort(), false));
            return;
        }

        LocalSession session = createSessionUnsafe(sessionID, parentSession, peers);
        putSessionUnsafe(session);
        logger.info("Beginning local incremental repair session {}", session);

        ExecutorService executor = Executors.newFixedThreadPool(parentSession.getColumnFamilyStores().size());

        KeyspaceRepairManager repairManager = parentSession.getKeyspace().getRepairManager();
        RangesAtEndpoint tokenRanges = filterLocalRanges(parentSession.getKeyspace().getName(), parentSession.getRanges());
        ListenableFuture repairPreparation = prepareSession(repairManager, sessionID, parentSession.getColumnFamilyStores(),
                                                            tokenRanges, executor, () -> session.getState() != PREPARING);

        Futures.addCallback(repairPreparation, new FutureCallback<Object>()
        {
            public void onSuccess(@Nullable Object result)
            {
                try
                {
                    logger.info("Prepare phase for incremental repair session {} completed", sessionID);
                    if (session.getState() != FAILED)
                    {
                        setStateAndSave(session, PREPARED);
                        sendMessage(coordinator, new PrepareConsistentResponse(sessionID, getBroadcastAddressAndPort(), true));
                    }
                    else
                    {
                        logger.info("Session {} failed before anticompaction completed", sessionID);
                        sendMessage(coordinator, new PrepareConsistentResponse(sessionID, getBroadcastAddressAndPort(), false));
                    }
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
                    logger.error("Prepare phase for incremental repair session {} failed", sessionID, t);
                    sendMessage(coordinator, new PrepareConsistentResponse(sessionID, getBroadcastAddressAndPort(), false));
                    failSession(sessionID, false);
                }
                finally
                {
                    executor.shutdown();
                }
            }
        });
    }

    public void maybeSetRepairing(UUID sessionID)
    {
        LocalSession session = getSession(sessionID);
        if (session != null && session.getState() != REPAIRING)
        {
            logger.debug("Setting local incremental repair session {} to REPAIRING", session);
            setStateAndSave(session, REPAIRING);
        }
    }

    public void handleFinalizeProposeMessage(InetAddressAndPort from, FinalizePropose propose)
    {
        logger.trace("received {} from {}", propose, from);
        UUID sessionID = propose.sessionID;
        LocalSession session = getSession(sessionID);
        if (session == null)
        {
            logger.debug("Received FinalizePropose message for unknown repair session {}, responding with failure", sessionID);
            sendMessage(from, new FailSession(sessionID));
            return;
        }

        try
        {
            setStateAndSave(session, FINALIZE_PROMISED);

            /*
             Flushing the repairs table here, *before* responding to the coordinator prevents a scenario where we respond
             with a promise to the coordinator, but there is a failure before the commit log mutation with the
             FINALIZE_PROMISED status is synced to disk. This could cause the state for this session to revert to an
             earlier status on startup, which would prevent the failure recovery mechanism from ever being able to promote
             this session to FINALIZED, likely creating inconsistencies in the repaired data sets across nodes.
             */
            syncTable();

            sendMessage(from, new FinalizePromise(sessionID, getBroadcastAddressAndPort(), true));
            logger.debug("Received FinalizePropose message for incremental repair session {}, responded with FinalizePromise", sessionID);
        }
        catch (IllegalArgumentException e)
        {
            logger.error(String.format("Error handling FinalizePropose message for %s", session), e);
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
    public void handleFinalizeCommitMessage(InetAddressAndPort from, FinalizeCommit commit)
    {
        logger.trace("received {} from {}", commit, from);
        UUID sessionID = commit.sessionID;
        LocalSession session = getSession(sessionID);
        if (session == null)
        {
            logger.warn("Ignoring FinalizeCommit message for unknown repair session {}", sessionID);
            return;
        }

        setStateAndSave(session, FINALIZED);
        logger.info("Finalized local repair session {}", sessionID);
    }

    public void handleFailSessionMessage(InetAddressAndPort from, FailSession msg)
    {
        logger.trace("received {} from {}", msg, from);
        failSession(msg.sessionID, false);
    }

    public void sendStatusRequest(LocalSession session)
    {
        logger.debug("Attempting to learn the outcome of unfinished local incremental repair session {}", session.sessionID);
        StatusRequest request = new StatusRequest(session.sessionID);
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
        UUID sessionID = request.sessionID;
        LocalSession session = getSession(sessionID);
        if (session == null)
        {
            logger.warn("Received status response message for unknown session {}", sessionID);
            sendMessage(from, new StatusResponse(sessionID, FAILED));
        }
        else
        {
            sendMessage(from, new StatusResponse(sessionID, session.getState()));
            logger.debug("Responding to status response message for incremental repair session {} with local state {}", sessionID, session.getState());
       }
    }

    public void handleStatusResponse(InetAddressAndPort from, StatusResponse response)
    {
        logger.trace("received {} from {}", response, from);
        UUID sessionID = response.sessionID;
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
            logger.debug("Received StatusResponse for repair session {} with state {}, which is not actionable. Doing nothing.", sessionID, response.state);
        }
    }

    /**
     * determines if a local session exists, and if it's not finalized or failed
     */
    public boolean isSessionInProgress(UUID sessionID)
    {
        LocalSession session = getSession(sessionID);
        return session != null && session.getState() != FINALIZED && session.getState() != FAILED;
    }

    /**
     * determines if a local session exists, and if it's in the finalized state
     */
    public boolean isSessionFinalized(UUID sessionID)
    {
        LocalSession session = getSession(sessionID);
        return session != null && session.getState() == FINALIZED;
    }

    /**
     * determines if a local session exists
     */
    public boolean sessionExists(UUID sessionID)
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
    public long getFinalSessionRepairedAt(UUID sessionID)
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
}
