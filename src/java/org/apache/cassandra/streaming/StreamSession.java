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
package org.apache.cassandra.streaming;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.*;
import com.google.common.util.concurrent.Futures;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.async.NettyStreamingMessageSender;
import org.apache.cassandra.streaming.messages.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.Refs;

/**
 * Handles the streaming a one or more section of one of more sstables to and from a specific
 * remote node. The sending side performs a block-level transfer of the source sstable, while the receiver
 * must deserilaize that data stream into an partitions and rows, and then write that out as an sstable.
 *
 * Both this node and the remote one will create a similar symmetrical {@link StreamSession}. A streaming
 * session has the following life-cycle:
 *
 * 1. Session Initialization
 *
 *   (a) A node (the initiator in the following) create a new {@link StreamSession},
 *       initialize it {@link #init(StreamResultFuture)}, and then start it ({@link #start()}).
 *       Starting a session causes a {@link StreamInitMessage} to be sent.
 *   (b) Upon reception of that {@link StreamInitMessage}, the follower creates its own {@link StreamSession},
 *       and initializes it if it still does not exist.
 *   (c) After the initiator sends the {@link StreamInitMessage}, it invokes
 *       {@link StreamSession#onInitializationComplete()} to start the streaming prepare phase.
 *
 * 2. Streaming preparation phase
 *
 *   (a) A {@link PrepareSynMessage} is sent that includes a) what files/sections this node will stream to the follower
 *       (stored locally in a {@link StreamTransferTask}, one for each table) and b) what the follower needs to
 *       stream back (stored locally in a {@link StreamReceiveTask}, one for each table).
 *   (b) Upon reception of the {@link PrepareSynMessage}, the follower records which files/sections it will receive
 *       and send back a {@link PrepareSynAckMessage}, which contains a summary of the files/sections that will be sent to
 *       the initiator.
 *   (c) When the initiator receives the {@link PrepareSynAckMessage}, it records which files/sections it will
 *       receive, and then goes to it's Streaming phase (see next section). If the intiator is to receive files,
 *       it sends a {@link PrepareAckMessage} to the follower to indicate that it can start streaming to the initiator.
 *   (d) (Optional) If the follower receives a {@link PrepareAckMessage}, it enters it's Streaming phase.
 *
 * 3. Streaming phase
 *
 *   (a) The streaming phase is started at each node by calling {@link StreamSession#startStreamingFiles(boolean)}.
 *       This will send, sequentially on each outbound streaming connection (see {@link NettyStreamingMessageSender}),
 *       an {@link OutgoingFileMessage} for each file in each of the {@link StreamTransferTask}.
 *       Each {@link OutgoingFileMessage} consists of a {@link FileMessageHeader} that contains metadata about the file
 *       being streamed, followed by the file content itself. Once all the files for a {@link StreamTransferTask} are sent,
 *       the task is marked complete {@link StreamTransferTask#complete(int)}.
 *   (b) On the receiving side, a SSTable will be written for the incoming file, and once the file is fully received,
 *       the file will be marked as complete ({@link StreamReceiveTask#received(SSTableMultiWriter)}). When all files
 *       for the {@link StreamReceiveTask} have been received, the sstables are added to the CFS (and 2ndary indexes/MV are built),
 *        and the task is marked complete ({@link #taskCompleted(StreamReceiveTask)}).
 *   (b) If during the streaming of a particular file an error occurs on the receiving end of a stream
 *       (it may be either the initiator or the follower), the node will send a {@link SessionFailedMessage}
 *       to the sender and close the stream session.
 *   (c) When all transfer and receive tasks for a session are complete, the session moves to the Completion phase
 *       ({@link #maybeCompleted()}).
 *
 * 4. Completion phase
 *
 *   (a) When a node enters the completion phase, it sends a {@link CompleteMessage} to the peer, and then enter the
 *       {@link StreamSession.State#WAIT_COMPLETE} state. If it has already received a {@link CompleteMessage}
 *       from the peer, session is complete and is then closed ({@link #closeSession(State)}). Otherwise, the node
 *       switch to the {@link StreamSession.State#WAIT_COMPLETE} state and send a {@link CompleteMessage} to the other side.
 *
 * In brief, the message passing looks like this (I for initiator, F for follwer):
 * (session init)
 * I: StreamInitMessage
 * (session prepare)
 * I: PrepareSynMessage
 * F: PrepareSynAckMessage
 * I: PrepareAckMessage
 * (stream - this can happen in both directions)
 * I: OutgoingFileMessage
 * F: ReceivedMessage
 * (completion)
 * I/F: CompleteMessage
 *
 * All messages which derive from {@link StreamMessage} are sent by the standard internode messaging
 * (via {@link org.apache.cassandra.net.MessagingService}, while the actual files themselves are sent by a special
 * "streaming" connection type. See {@link NettyStreamingMessageSender} for details. Because of the asynchronous
 */
public class StreamSession implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(StreamSession.class);

    /**
     * Streaming endpoint.
     *
     * Each {@code StreamSession} is identified by this InetAddress which is broadcast address of the node streaming.
     */
    public final InetAddress peer;

    private final int index;

    /** Actual connecting address. Can be the same as {@linkplain #peer}. */
    public final InetAddress connecting;

    // should not be null when session is started
    private StreamResultFuture streamResult;

    // stream requests to send to the peer
    protected final Set<StreamRequest> requests = Sets.newConcurrentHashSet();
    // streaming tasks are created and managed per ColumnFamily ID
    @VisibleForTesting
    protected final ConcurrentHashMap<TableId, StreamTransferTask> transfers = new ConcurrentHashMap<>();
    // data receivers, filled after receiving prepare message
    private final Map<TableId, StreamReceiveTask> receivers = new ConcurrentHashMap<>();
    private final StreamingMetrics metrics;

    final Map<String, Set<Range<Token>>> transferredRangesPerKeyspace = new HashMap<>();

    private final NettyStreamingMessageSender messageSender;
    private final ConcurrentMap<ChannelId, Channel> incomingChannels = new ConcurrentHashMap<>();

    private final AtomicBoolean isAborted = new AtomicBoolean(false);
    private final boolean keepSSTableLevel;
    private final UUID pendingRepair;
    private final PreviewKind previewKind;

    public enum State
    {
        INITIALIZED,
        PREPARING,
        STREAMING,
        WAIT_COMPLETE,
        COMPLETE,
        FAILED,
    }

    private volatile State state = State.INITIALIZED;
    private volatile boolean completeSent = false;

    /**
     * Create new streaming session with the peer.
     *  @param peer Address of streaming peer
     * @param connecting Actual connecting address
     */
    public StreamSession(InetAddress peer, InetAddress connecting, StreamConnectionFactory factory, int index, boolean keepSSTableLevel, UUID pendingRepair, PreviewKind previewKind)
    {
        this.peer = peer;
        this.connecting = connecting;
        this.index = index;

        OutboundConnectionIdentifier id = OutboundConnectionIdentifier.stream(new InetSocketAddress(FBUtilities.getBroadcastAddress(), 0),
                                                                              new InetSocketAddress(connecting, MessagingService.instance().portFor(connecting)));
        this.messageSender = new NettyStreamingMessageSender(this, id, factory, StreamMessage.CURRENT_VERSION, previewKind.isPreview());
        this.metrics = StreamingMetrics.get(connecting);
        this.keepSSTableLevel = keepSSTableLevel;
        this.pendingRepair = pendingRepair;
        this.previewKind = previewKind;
    }

    public UUID planId()
    {
        return streamResult == null ? null : streamResult.planId;
    }

    public int sessionIndex()
    {
        return index;
    }

    public StreamOperation streamOperation()
    {
        return streamResult == null ? null : streamResult.streamOperation;
    }

    public boolean keepSSTableLevel()
    {
        return keepSSTableLevel;
    }

    public UUID getPendingRepair()
    {
        return pendingRepair;
    }

    public boolean isPreview()
    {
        return previewKind.isPreview();
    }

    public PreviewKind getPreviewKind()
    {
        return previewKind;
    }

    public LifecycleTransaction getTransaction(TableId tableId)
    {
        assert receivers.containsKey(tableId);
        return receivers.get(tableId).getTransaction();
    }

    /**
     * Bind this session to report to specific {@link StreamResultFuture} and
     * perform pre-streaming initialization.
     *
     * @param streamResult result to report to
     */
    public void init(StreamResultFuture streamResult)
    {
        this.streamResult = streamResult;
        StreamHook.instance.reportStreamFuture(this, streamResult);
    }

    public boolean attach(Channel channel)
    {
        if (!messageSender.hasControlChannel())
            messageSender.injectControlMessageChannel(channel);
        return incomingChannels.putIfAbsent(channel.id(), channel) == null;
    }

    /**
     * invoked by the node that begins the stream session (it may be sending files, receiving files, or both)
     */
    public void start()
    {
        if (requests.isEmpty() && transfers.isEmpty())
        {
            logger.info("[Stream #{}] Session does not have any tasks.", planId());
            closeSession(State.COMPLETE);
            return;
        }

        try
        {
            logger.info("[Stream #{}] Starting streaming to {}{}", planId(),
                                                                   peer,
                                                                   peer.equals(connecting) ? "" : " through " + connecting);
            messageSender.initialize();
            onInitializationComplete();
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            onError(e);
        }
    }

    /**
     * Request data fetch task to this session.
     *
     * @param keyspace Requesting keyspace
     * @param ranges Ranges to retrieve data
     * @param columnFamilies ColumnFamily names. Can be empty if requesting all CF under the keyspace.
     */
    public void addStreamRequest(String keyspace, Collection<Range<Token>> ranges, Collection<String> columnFamilies)
    {
        requests.add(new StreamRequest(keyspace, ranges, columnFamilies));
    }

    /**
     * Set up transfer for specific keyspace/ranges/CFs
     *
     * Used in repair - a streamed sstable in repair will be marked with the given repairedAt time
     *
     * @param keyspace Transfer keyspace
     * @param ranges Transfer ranges
     * @param columnFamilies Transfer ColumnFamilies
     * @param flushTables flush tables?
     */
    synchronized void addTransferRanges(String keyspace, Collection<Range<Token>> ranges, Collection<String> columnFamilies, boolean flushTables)
    {
        failIfFinished();
        Collection<ColumnFamilyStore> stores = getColumnFamilyStores(keyspace, columnFamilies);
        if (flushTables)
            flushSSTables(stores);

        List<Range<Token>> normalizedRanges = Range.normalize(ranges);
        List<SSTableStreamingSections> sections = getSSTableSectionsForRanges(normalizedRanges, stores, pendingRepair, previewKind);
        try
        {
            addTransferFiles(sections);
            Set<Range<Token>> toBeUpdated = transferredRangesPerKeyspace.get(keyspace);
            if (toBeUpdated == null)
            {
                toBeUpdated = new HashSet<>();
            }
            toBeUpdated.addAll(ranges);
            transferredRangesPerKeyspace.put(keyspace, toBeUpdated);
        }
        finally
        {
            for (SSTableStreamingSections release : sections)
                release.ref.release();
        }
    }

    private void failIfFinished()
    {
        if (state() == State.COMPLETE || state() == State.FAILED)
            throw new RuntimeException(String.format("Stream %s is finished with state %s", planId(), state().name()));
    }

    private Collection<ColumnFamilyStore> getColumnFamilyStores(String keyspace, Collection<String> columnFamilies)
    {
        Collection<ColumnFamilyStore> stores = new HashSet<>();
        // if columnfamilies are not specified, we add all cf under the keyspace
        if (columnFamilies.isEmpty())
        {
            stores.addAll(Keyspace.open(keyspace).getColumnFamilyStores());
        }
        else
        {
            for (String cf : columnFamilies)
                stores.add(Keyspace.open(keyspace).getColumnFamilyStore(cf));
        }
        return stores;
    }

    @VisibleForTesting
    public static List<SSTableStreamingSections> getSSTableSectionsForRanges(Collection<Range<Token>> ranges, Collection<ColumnFamilyStore> stores, UUID pendingRepair, PreviewKind previewKind)
    {
        Refs<SSTableReader> refs = new Refs<>();
        try
        {
            for (ColumnFamilyStore cfStore : stores)
            {
                final List<Range<PartitionPosition>> keyRanges = new ArrayList<>(ranges.size());
                for (Range<Token> range : ranges)
                    keyRanges.add(Range.makeRowRange(range));
                refs.addAll(cfStore.selectAndReference(view -> {
                    Set<SSTableReader> sstables = Sets.newHashSet();
                    SSTableIntervalTree intervalTree = SSTableIntervalTree.build(view.select(SSTableSet.CANONICAL));
                    Predicate<SSTableReader> predicate;
                    if (previewKind.isPreview())
                    {
                        predicate = previewKind.getStreamingPredicate();
                    }
                    else if (pendingRepair == ActiveRepairService.NO_PENDING_REPAIR)
                    {
                        predicate = Predicates.alwaysTrue();
                    }
                    else
                    {
                        predicate = s -> s.isPendingRepair() && s.getSSTableMetadata().pendingRepair.equals(pendingRepair);
                    }

                    for (Range<PartitionPosition> keyRange : keyRanges)
                    {
                        // keyRange excludes its start, while sstableInBounds is inclusive (of both start and end).
                        // This is fine however, because keyRange has been created from a token range through Range.makeRowRange (see above).
                        // And that later method uses the Token.maxKeyBound() method to creates the range, which return a "fake" key that
                        // sort after all keys having the token. That "fake" key cannot however be equal to any real key, so that even
                        // including keyRange.left will still exclude any key having the token of the original token range, and so we're
                        // still actually selecting what we wanted.
                        for (SSTableReader sstable : Iterables.filter(View.sstablesInBounds(keyRange.left, keyRange.right, intervalTree), predicate))
                        {
                            sstables.add(sstable);
                        }
                    }

                    if (logger.isDebugEnabled())
                        logger.debug("ViewFilter for {}/{} sstables", sstables.size(), Iterables.size(view.select(SSTableSet.CANONICAL)));
                    return sstables;
                }).refs);
            }

            List<SSTableStreamingSections> sections = new ArrayList<>(refs.size());
            for (SSTableReader sstable : refs)
            {
                sections.add(new SSTableStreamingSections(refs.get(sstable), sstable.getPositionsForRanges(ranges), sstable.estimatedKeysForRanges(ranges)));
            }
            return sections;
        }
        catch (Throwable t)
        {
            refs.release();
            throw t;
        }
    }

    synchronized void addTransferFiles(Collection<SSTableStreamingSections> sstableDetails)
    {
        failIfFinished();
        Iterator<SSTableStreamingSections> iter = sstableDetails.iterator();
        while (iter.hasNext())
        {
            SSTableStreamingSections details = iter.next();
            if (details.sections.isEmpty())
            {
                // A reference was acquired on the sstable and we won't stream it
                details.ref.release();
                iter.remove();
                continue;
            }

            TableId tableId = details.ref.get().metadata().id;
            StreamTransferTask task = transfers.get(tableId);
            if (task == null)
            {
                //guarantee atomicity
                StreamTransferTask newTask = new StreamTransferTask(this, tableId);
                task = transfers.putIfAbsent(tableId, newTask);
                if (task == null)
                    task = newTask;
            }
            task.addTransferFile(details.ref, details.estimatedKeys, details.sections);
            iter.remove();
        }
    }

    public static class SSTableStreamingSections
    {
        public final Ref<SSTableReader> ref;
        public final List<Pair<Long, Long>> sections;
        public final long estimatedKeys;

        public SSTableStreamingSections(Ref<SSTableReader> ref, List<Pair<Long, Long>> sections, long estimatedKeys)
        {
            this.ref = ref;
            this.sections = sections;
            this.estimatedKeys = estimatedKeys;
        }
    }

    private synchronized Future closeSession(State finalState)
    {
        Future abortedTasksFuture = null;
        if (isAborted.compareAndSet(false, true))
        {
            state(finalState);

            // ensure aborting the tasks do not happen on the network IO thread (read: netty event loop)
            // as we don't want any blocking disk IO to stop the network thread
            if (finalState == State.FAILED)
                abortedTasksFuture = ScheduledExecutors.nonPeriodicTasks.submit(this::abortTasks);

            incomingChannels.values().stream().map(channel -> channel.close());
            messageSender.close();

            streamResult.handleSessionComplete(this);
        }
        return abortedTasksFuture != null ? abortedTasksFuture : Futures.immediateFuture(null);
    }

    private void abortTasks()
    {
        try
        {
            receivers.values().forEach(StreamReceiveTask::abort);
            transfers.values().forEach(StreamTransferTask::abort);
        }
        catch (Exception e)
        {
            logger.warn("failed to abort some streaming tasks", e);
        }
    }

    /**
     * Set current state to {@code newState}.
     *
     * @param newState new state to set
     */
    public void state(State newState)
    {
        state = newState;
    }

    /**
     * @return current state
     */
    public State state()
    {
        return state;
    }

    public NettyStreamingMessageSender getMessageSender()
    {
        return messageSender;
    }

    /**
     * Return if this session completed successfully.
     *
     * @return true if session completed successfully.
     */
    public boolean isSuccess()
    {
        return state == State.COMPLETE;
    }

    public void messageReceived(StreamMessage message)
    {
        switch (message.type)
        {
            case STREAM_INIT:
                // nop
                break;
            case PREPARE_SYN:
                PrepareSynMessage msg = (PrepareSynMessage) message;
                prepare(msg.requests, msg.summaries);
                break;
            case PREPARE_SYNACK:
                prepareSynAck((PrepareSynAckMessage) message);
                break;
            case PREPARE_ACK:
                prepareAck((PrepareAckMessage) message);
                break;
            case FILE:
                receive((IncomingFileMessage) message);
                break;
            case RECEIVED:
                ReceivedMessage received = (ReceivedMessage) message;
                received(received.tableId, received.sequenceNumber);
                break;
            case COMPLETE:
                complete();
                break;
            case KEEP_ALIVE:
                // NOP - we only send/receive the KEEP_ALIVE to force the TCP connection to remain open
                break;
            case SESSION_FAILED:
                sessionFailed();
                break;
            default:
                throw new AssertionError("unhandled StreamMessage type: " + message.getClass().getName());
        }
    }

    /**
     * Call back when connection initialization is complete to start the prepare phase.
     */
    public void onInitializationComplete()
    {
        // send prepare message
        state(State.PREPARING);
        PrepareSynMessage prepare = new PrepareSynMessage();
        prepare.requests.addAll(requests);
        for (StreamTransferTask task : transfers.values())
            prepare.summaries.add(task.getSummary());
        messageSender.sendMessage(prepare);
    }

    /**
     * Call back for handling exception during streaming.
     */
    public Future onError(Throwable e)
    {
        logError(e);
        // send session failure message
        if (messageSender.connected())
            messageSender.sendMessage(new SessionFailedMessage());
        // fail session
        return closeSession(State.FAILED);
    }

    private void logError(Throwable e)
    {
        if (e instanceof SocketTimeoutException)
        {
            logger.error("[Stream #{}] Did not receive response from peer {}{} for {} secs. Is peer down? " +
                         "If not, maybe try increasing streaming_keep_alive_period_in_secs.", planId(),
                         peer.getHostAddress(),
                         peer.equals(connecting) ? "" : " through " + connecting.getHostAddress(),
                         2 * DatabaseDescriptor.getStreamingKeepAlivePeriod(),
                         e);
        }
        else
        {
            logger.error("[Stream #{}] Streaming error occurred on session with peer {}{}", planId(),
                         peer.getHostAddress(),
                         peer.equals(connecting) ? "" : " through " + connecting.getHostAddress(),
                         e);
        }
    }

    /**
     * Prepare this session for sending/receiving files.
     */
    public void prepare(Collection<StreamRequest> requests, Collection<StreamSummary> summaries)
    {
        // prepare tasks
        state(State.PREPARING);
        ScheduledExecutors.nonPeriodicTasks.execute(() -> prepareAsync(requests, summaries));
    }

    /**
     * Finish preparing the session. This method is blocking (memtables are flushed in {@link #addTransferRanges}),
     * so the logic should not execute on the main IO thread (read: netty event loop).
     */
    private void prepareAsync(Collection<StreamRequest> requests, Collection<StreamSummary> summaries)
    {

        for (StreamRequest request : requests)
            addTransferRanges(request.keyspace, request.ranges, request.columnFamilies, true); // always flush on stream request
        for (StreamSummary summary : summaries)
            prepareReceiving(summary);

        PrepareSynAckMessage prepareSynAck = new PrepareSynAckMessage();
        if (!peer.equals(FBUtilities.getBroadcastAddress()))
            for (StreamTransferTask task : transfers.values())
                prepareSynAck.summaries.add(task.getSummary());
        messageSender.sendMessage(prepareSynAck);


        streamResult.handleSessionPrepared(this);
        maybeCompleted();
    }

    private void prepareSynAck(PrepareSynAckMessage msg)
    {
        if (!msg.summaries.isEmpty())
        {
            for (StreamSummary summary : msg.summaries)
                prepareReceiving(summary);

            // only send the (final) ACK if we are expecting the peer to send this node (the initiator) some files
            messageSender.sendMessage(new PrepareAckMessage());
        }

        if (isPreview())
            completePreview();
        else
            startStreamingFiles(true);
    }

    private void prepareAck(PrepareAckMessage msg)
    {
        if (isPreview())
            completePreview();
        else
            startStreamingFiles(true);
    }

    /**
     * Call back after sending FileMessageHeader.
     *
     * @param header sent header
     */
    public void fileSent(FileMessageHeader header)
    {
        long headerSize = header.size();
        StreamingMetrics.totalOutgoingBytes.inc(headerSize);
        metrics.outgoingBytes.inc(headerSize);
        // schedule timeout for receiving ACK
        StreamTransferTask task = transfers.get(header.tableId);
        if (task != null)
        {
            task.scheduleTimeout(header.sequenceNumber, 12, TimeUnit.HOURS);
        }
    }

    /**
     * Call back after receiving a streamed file.
     *
     * @param message received file
     */
    public void receive(IncomingFileMessage message)
    {
        if (isPreview())
        {
            throw new RuntimeException("Cannot receive files for preview session");
        }

        long headerSize = message.header.size();
        StreamingMetrics.totalIncomingBytes.inc(headerSize);
        metrics.incomingBytes.inc(headerSize);
        // send back file received message
        messageSender.sendMessage(new ReceivedMessage(message.header.tableId, message.header.sequenceNumber));
        receivers.get(message.header.tableId).received(message.sstable);
    }

    public void progress(String filename, ProgressInfo.Direction direction, long bytes, long total)
    {
        ProgressInfo progress = new ProgressInfo(peer, index, filename, direction, bytes, total);
        streamResult.handleProgress(progress);
    }

    public void received(TableId tableId, int sequenceNumber)
    {
        transfers.get(tableId).complete(sequenceNumber);
    }

    /**
     * Check if session is completed on receiving {@code StreamMessage.Type.COMPLETE} message.
     */
    public synchronized void complete()
    {
        logger.debug("handling Complete message, state = {}, completeSent = {}", state, completeSent);
        if (state == State.WAIT_COMPLETE)
        {
            if (!completeSent)
            {
                messageSender.sendMessage(new CompleteMessage());
                completeSent = true;
            }
            closeSession(State.COMPLETE);
        }
        else
        {
            state(State.WAIT_COMPLETE);
        }
    }

    /**
     * Call back on receiving {@code StreamMessage.Type.SESSION_FAILED} message.
     */
    public synchronized void sessionFailed()
    {
        logger.error("[Stream #{}] Remote peer {} failed stream session.", planId(), peer.getHostAddress());
        closeSession(State.FAILED);
    }

    /**
     * @return Current snapshot of this session info.
     */
    public SessionInfo getSessionInfo()
    {
        List<StreamSummary> receivingSummaries = Lists.newArrayList();
        for (StreamTask receiver : receivers.values())
            receivingSummaries.add(receiver.getSummary());
        List<StreamSummary> transferSummaries = Lists.newArrayList();
        for (StreamTask transfer : transfers.values())
            transferSummaries.add(transfer.getSummary());
        return new SessionInfo(peer, index, connecting, receivingSummaries, transferSummaries, state);
    }

    public synchronized void taskCompleted(StreamReceiveTask completedTask)
    {
        receivers.remove(completedTask.tableId);
        maybeCompleted();
    }

    public synchronized void taskCompleted(StreamTransferTask completedTask)
    {
        transfers.remove(completedTask.tableId);
        maybeCompleted();
    }

    public void onJoin(InetAddress endpoint, EndpointState epState) {}
    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {}
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {}
    public void onAlive(InetAddress endpoint, EndpointState state) {}
    public void onDead(InetAddress endpoint, EndpointState state) {}

    public void onRemove(InetAddress endpoint)
    {
        logger.error("[Stream #{}] Session failed because remote peer {} has left.", planId(), peer.getHostAddress());
        closeSession(State.FAILED);
    }

    public void onRestart(InetAddress endpoint, EndpointState epState)
    {
        logger.error("[Stream #{}] Session failed because remote peer {} was restarted.", planId(), peer.getHostAddress());
        closeSession(State.FAILED);
    }

    private void completePreview()
    {
        try
        {
            state(State.WAIT_COMPLETE);
            closeSession(State.COMPLETE);
        }
        finally
        {
            // aborting the tasks here needs to be the last thing we do so that we
            // accurately report expected streaming, but don't leak any sstable refs
            for (StreamTask task : Iterables.concat(receivers.values(), transfers.values()))
                task.abort();
        }
    }

    private boolean maybeCompleted()
    {
        boolean completed = receivers.isEmpty() && transfers.isEmpty();
        if (completed)
        {
            if (state == State.WAIT_COMPLETE)
            {
                if (!completeSent)
                {
                    messageSender.sendMessage(new CompleteMessage());
                    completeSent = true;
                }
                closeSession(State.COMPLETE);
            }
            else
            {
                // notify peer that this session is completed
                messageSender.sendMessage(new CompleteMessage());
                completeSent = true;
                state(State.WAIT_COMPLETE);
            }
        }
        return completed;
    }

    /**
     * Flushes matching column families from the given keyspace, or all columnFamilies
     * if the cf list is empty.
     */
    private void flushSSTables(Iterable<ColumnFamilyStore> stores)
    {
        List<Future<?>> flushes = new ArrayList<>();
        for (ColumnFamilyStore cfs : stores)
            flushes.add(cfs.forceFlush());
        FBUtilities.waitOnFutures(flushes);
    }

    private synchronized void prepareReceiving(StreamSummary summary)
    {
        failIfFinished();
        if (summary.files > 0)
            receivers.put(summary.tableId, new StreamReceiveTask(this, summary.tableId, summary.files, summary.totalSize));
    }

    private void startStreamingFiles(boolean notifyPrepared)
    {
        if (notifyPrepared)
            streamResult.handleSessionPrepared(this);

        state(State.STREAMING);

        for (StreamTransferTask task : transfers.values())
        {
            Collection<OutgoingFileMessage> messages = task.getFileMessages();
            if (!messages.isEmpty())
            {
                for (OutgoingFileMessage ofm : messages)
                {
                    // pass the session planId/index to the OFM (which is only set at init(), after the transfers have already been created)
                    ofm.header.addSessionInfo(this);
                    messageSender.sendMessage(ofm);
                }
            }
            else
            {
                taskCompleted(task); // there are no files to send
            }
        }
        maybeCompleted();
    }
}
