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

import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import com.google.common.util.concurrent.Futures;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.async.NettyStreamingMessageSender;
import org.apache.cassandra.streaming.messages.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static com.google.common.collect.Iterables.all;

/**
 * Handles the streaming a one or more streams to and from a specific remote node.
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
 *       an {@link OutgoingStreamMessage} for each stream in each of the {@link StreamTransferTask}.
 *       Each {@link OutgoingStreamMessage} consists of a {@link StreamMessageHeader} that contains metadata about
 *       the stream, followed by the stream content itself. Once all the files for a {@link StreamTransferTask} are sent,
 *       the task is marked complete {@link StreamTransferTask#complete(int)}.
 *   (b) On the receiving side, the incoming data is written to disk, and once the stream is fully received,
 *       it will be marked as complete ({@link StreamReceiveTask#received(IncomingStream)}). When all streams
 *       for the {@link StreamReceiveTask} have been received, the data is added to the CFS (and 2ndary indexes/MV are built),
 *        and the task is marked complete ({@link #taskCompleted(StreamReceiveTask)}).
 *   (b) If during the streaming of a particular stream an error occurs on the receiving end of a stream
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
 * I: OutgoingStreamMessage
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

    private final StreamOperation streamOperation;

    /**
     * Streaming endpoint.
     *
     * Each {@code StreamSession} is identified by this InetAddressAndPort which is broadcast address of the node streaming.
     */
    public final InetAddressAndPort peer;

    /**
     * Preferred IP Address/Port of the peer; this is the address that will be connect to. Can be the same as {@linkplain #peer}.
     */
    private final InetAddressAndPort preferredPeerInetAddressAndPort;

    private final int index;

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
     */
    public StreamSession(StreamOperation streamOperation, InetAddressAndPort peer, StreamConnectionFactory factory,
                         int index, UUID pendingRepair, PreviewKind previewKind)
    {
        this(streamOperation, peer, factory, index, pendingRepair, previewKind, MessagingService.instance()::getPreferredRemoteAddr);
    }

    @VisibleForTesting
    public StreamSession(StreamOperation streamOperation, InetAddressAndPort peer, StreamConnectionFactory factory,
                         int index, UUID pendingRepair, PreviewKind previewKind,
                         Function<InetAddressAndPort, InetAddressAndPort> preferredIpMapper)
    {
        this.streamOperation = streamOperation;
        this.peer = peer;
        this.index = index;
        InetAddressAndPort preferredPeerEndpoint = preferredIpMapper.apply(peer);
        this.preferredPeerInetAddressAndPort = (preferredPeerEndpoint == null) ? peer : preferredPeerEndpoint;

        OutboundConnectionIdentifier id = OutboundConnectionIdentifier.stream(InetAddressAndPort.getByAddressOverrideDefaults(FBUtilities.getJustLocalAddress(), 0),
                                                                              preferredPeerInetAddressAndPort);

        this.messageSender = new NettyStreamingMessageSender(this, id, factory, StreamMessage.CURRENT_VERSION, previewKind.isPreview());
        this.metrics = StreamingMetrics.get(preferredPeerInetAddressAndPort);
        this.pendingRepair = pendingRepair;
        this.previewKind = previewKind;

        logger.debug("Creating stream session peer={} preferredPeerInetAddressAndPort={}", peer,
                     preferredPeerInetAddressAndPort);
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

    public StreamOperation getStreamOperation()
    {
        return streamOperation;
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

    public StreamReceiver getAggregator(TableId tableId)
    {
        assert receivers.containsKey(tableId) : "Missing tableId " + tableId;
        return receivers.get(tableId).getReceiver();
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
                                                                   peer.equals(preferredPeerInetAddressAndPort) ? "" : " through " + preferredPeerInetAddressAndPort);
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
     * Here, we have to encode both _local_ range transientness (encoded in Replica itself, in RangesAtEndpoint)
     * and _remote_ (source) range transientmess, which is encoded by splitting ranges into full and transient.
     *
     * @param keyspace Requesting keyspace
     * @param fullRanges Ranges to retrieve data that will return full data from the source
     * @param transientRanges Ranges to retrieve data that will return transient data from the source
     * @param columnFamilies ColumnFamily names. Can be empty if requesting all CF under the keyspace.
     */
    public void addStreamRequest(String keyspace, RangesAtEndpoint fullRanges, RangesAtEndpoint transientRanges, Collection<String> columnFamilies)
    {
        //It should either be a dummy address for repair or if it's a bootstrap/move/rebuild it should be this node
        assert all(fullRanges, Replica::isSelf) || RangesAtEndpoint.isDummyList(fullRanges) : fullRanges.toString();
        assert all(transientRanges, Replica::isSelf) || RangesAtEndpoint.isDummyList(transientRanges) : transientRanges.toString();

        requests.add(new StreamRequest(keyspace, fullRanges, transientRanges, columnFamilies));
    }

    /**
     * Set up transfer for specific keyspace/ranges/CFs
     *
     * @param keyspace Transfer keyspace
     * @param replicas Transfer ranges
     * @param columnFamilies Transfer ColumnFamilies
     * @param flushTables flush tables?
     */
    synchronized void addTransferRanges(String keyspace, RangesAtEndpoint replicas, Collection<String> columnFamilies, boolean flushTables)
    {
        failIfFinished();
        Collection<ColumnFamilyStore> stores = getColumnFamilyStores(keyspace, columnFamilies);
        if (flushTables)
            flushSSTables(stores);

        //Was it safe to remove this normalize, sorting seems not to matter, merging? Maybe we should have?
        //Do we need to unwrap here also or is that just making it worse?
        //Range and if it's transient
        RangesAtEndpoint unwrappedRanges = replicas.unwrap();
        List<OutgoingStream> streams = getOutgoingStreamsForRanges(unwrappedRanges, stores, pendingRepair, previewKind);
        addTransferStreams(streams);
        Set<Range<Token>> toBeUpdated = transferredRangesPerKeyspace.get(keyspace);
        if (toBeUpdated == null)
        {
            toBeUpdated = new HashSet<>();
        }
        toBeUpdated.addAll(replicas.ranges());
        transferredRangesPerKeyspace.put(keyspace, toBeUpdated);
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
    public List<OutgoingStream> getOutgoingStreamsForRanges(RangesAtEndpoint replicas, Collection<ColumnFamilyStore> stores, UUID pendingRepair, PreviewKind previewKind)
    {
        List<OutgoingStream> streams = new ArrayList<>();
        try
        {
            for (ColumnFamilyStore cfs: stores)
            {
                streams.addAll(cfs.getStreamManager().createOutgoingStreams(this, replicas, pendingRepair, previewKind));
            }
        }
        catch (Throwable t)
        {
            streams.forEach(OutgoingStream::finish);
            throw t;
        }
        return streams;
    }

    synchronized void addTransferStreams(Collection<OutgoingStream> streams)
    {
        failIfFinished();
        for (OutgoingStream stream: streams)
        {
            TableId tableId = stream.getTableId();
            StreamTransferTask task = transfers.get(tableId);
            if (task == null)
            {
                //guarantee atomicity
                StreamTransferTask newTask = new StreamTransferTask(this, tableId);
                task = transfers.putIfAbsent(tableId, newTask);
                if (task == null)
                    task = newTask;
            }
            task.addTransferStream(stream);
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
            case STREAM:
                receive((IncomingStreamMessage) message);
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
                         peer.getHostAddress(true),
                         peer.equals(preferredPeerInetAddressAndPort) ? "" : " through " + preferredPeerInetAddressAndPort.getHostAddress(true),
                         2 * DatabaseDescriptor.getStreamingKeepAlivePeriod(),
                         e);
        }
        else
        {
            logger.error("[Stream #{}] Streaming error occurred on session with peer {}{}", planId(),
                         peer.getHostAddress(true),
                         peer.equals(preferredPeerInetAddressAndPort) ? "" : " through " + preferredPeerInetAddressAndPort.getHostAddress(true),
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
            addTransferRanges(request.keyspace, RangesAtEndpoint.concat(request.full, request.transientReplicas), request.columnFamilies, true); // always flush on stream request
        for (StreamSummary summary : summaries)
            prepareReceiving(summary);

        PrepareSynAckMessage prepareSynAck = new PrepareSynAckMessage();
        if (!peer.equals(FBUtilities.getBroadcastAddressAndPort()))
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
     * Call back after sending StreamMessageHeader.
     *
     * @param message sent stream message
     */
    public void streamSent(OutgoingStreamMessage message)
    {
        long headerSize = message.stream.getSize();
        StreamingMetrics.totalOutgoingBytes.inc(headerSize);
        metrics.outgoingBytes.inc(headerSize);
        // schedule timeout for receiving ACK
        StreamTransferTask task = transfers.get(message.header.tableId);
        if (task != null)
        {
            task.scheduleTimeout(message.header.sequenceNumber, 12, TimeUnit.HOURS);
        }
    }

    /**
     * Call back after receiving a stream.
     *
     * @param message received stream
     */
    public void receive(IncomingStreamMessage message)
    {
        if (isPreview())
        {
            throw new RuntimeException("Cannot receive files for preview session");
        }

        long headerSize = message.stream.getSize();
        StreamingMetrics.totalIncomingBytes.inc(headerSize);
        metrics.incomingBytes.inc(headerSize);
        // send back file received message
        messageSender.sendMessage(new ReceivedMessage(message.header.tableId, message.header.sequenceNumber));
        StreamHook.instance.reportIncomingStream(message.header.tableId, message.stream, this, message.header.sequenceNumber);
        receivers.get(message.header.tableId).received(message.stream);
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
        logger.error("[Stream #{}] Remote peer {} failed stream session.", planId(), peer.toString());
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
        return new SessionInfo(peer, index, preferredPeerInetAddressAndPort, receivingSummaries, transferSummaries, state);
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

    public void onJoin(InetAddressAndPort endpoint, EndpointState epState) {}
    public void beforeChange(InetAddressAndPort endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {}
    public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value) {}
    public void onAlive(InetAddressAndPort endpoint, EndpointState state) {}
    public void onDead(InetAddressAndPort endpoint, EndpointState state) {}

    public void onRemove(InetAddressAndPort endpoint)
    {
        logger.error("[Stream #{}] Session failed because remote peer {} has left.", planId(), peer.toString());
        closeSession(State.FAILED);
    }

    public void onRestart(InetAddressAndPort endpoint, EndpointState epState)
    {
        logger.error("[Stream #{}] Session failed because remote peer {} was restarted.", planId(), peer.toString());
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
            // aborting the tasks here needs to be the last thing we do so that we accurately report
            // expected streaming, but don't leak any resources held by the task
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

    @VisibleForTesting
    public synchronized void prepareReceiving(StreamSummary summary)
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
            Collection<OutgoingStreamMessage> messages = task.getFileMessages();
            if (!messages.isEmpty())
            {
                for (OutgoingStreamMessage ofm : messages)
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

    @VisibleForTesting
    public int getNumRequests()
    {
        return requests.size();
    }

    @VisibleForTesting
    public int getNumTransfers()
    {
        return transferredRangesPerKeyspace.size();
    }
}
