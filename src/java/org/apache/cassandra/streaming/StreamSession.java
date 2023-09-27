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

import java.io.EOFException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.FileStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Future; //checkstyle: permit this import

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionStrategyManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.async.StreamingMultiplexedChannel;
import org.apache.cassandra.streaming.messages.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

import static com.google.common.collect.Iterables.all;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_STREAMING_DEBUG_STACKTRACE_LIMIT;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.locator.InetAddressAndPort.hostAddressAndPort;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;

/**
 * Handles the streaming a one or more streams to and from a specific remote node.
 *<p/>
 * Both this node and the remote one will create a similar symmetrical {@link StreamSession}. A streaming
 * session has the following life-cycle:
 *<pre>
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
 *       This will send, sequentially on each outbound streaming connection (see {@link StreamingMultiplexedChannel}),
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
 *   (a) When the initiator finishes streaming, it enters the {@link StreamSession.State#WAIT_COMPLETE} state, and waits
 *       for the follower to send a {@link CompleteMessage} once it finishes streaming too. Once the {@link CompleteMessage}
 *       is received, initiator sets its own state to {@link StreamSession.State#COMPLETE} and closes all channels attached
 *       to this session.
 *
 * </pre>
 *
 * In brief, the message passing looks like this (I for initiator, F for follwer):
 * <pre>
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
 * F: CompleteMessage
 *</pre>
 *
 * All messages which derive from {@link StreamMessage} are sent by the standard internode messaging
 * (via {@link org.apache.cassandra.net.MessagingService}, while the actual files themselves are sent by a special
 * "streaming" connection type. See {@link StreamingMultiplexedChannel} for details. Because of the asynchronous
 */
public class StreamSession
{
    private static final Logger logger = LoggerFactory.getLogger(StreamSession.class);
    private static final int DEBUG_STACKTRACE_LIMIT = CASSANDRA_STREAMING_DEBUG_STACKTRACE_LIMIT.getInt();

    public enum PrepareDirection { SEND, ACK }

    // for test purpose to record received message and state transition
    public volatile static MessageStateSink sink = MessageStateSink.NONE;

    private final StreamOperation streamOperation;

    /**
     * Streaming endpoint.
     *
     * Each {@code StreamSession} is identified by this InetAddressAndPort which is broadcast address of the node streaming.
     */
    public final InetAddressAndPort peer;

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

    private final boolean isFollower;
    private final StreamingMultiplexedChannel channel;
    // contains both inbound and outbound channels
    private final ConcurrentMap<Object, StreamingChannel> inbound = new ConcurrentHashMap<>();
    private final ConcurrentMap<Object, StreamingChannel> outbound = new ConcurrentHashMap<>();

    // "maybeCompleted()" should be executed at most once. Because it can be executed asynchronously by IO
    // threads(serialization/deserialization) and stream messaging processing thread, causing connection closed before
    // receiving peer's CompleteMessage.
    private boolean maybeCompleted = false;
    private Future<?> closeFuture;
    private final Object closeFutureLock = new Object();

    private final TimeUUID pendingRepair;
    private final PreviewKind previewKind;

    public String failureReason;

/**
 * State Transition:
 *
 * <pre>
 *  +------------------+-----> FAILED | ABORTED <---------------+
 *  |                  |              ^                         |
 *  |                  |              |       initiator         |
 *  INITIALIZED --> PREPARING --> STREAMING ------------> WAIT_COMPLETE ----> COMPLETED
 *  |                  |              |                         ^                 ^
 *  |                  |              |       follower          |                 |
 *  |                  |              +-------------------------)-----------------+
 *  |                  |                                        |                 |
 *  |                  |         if preview                     |                 |
 *  |                  +----------------------------------------+                 |
 *  |               nothing to request or to transfer                             |
 *  +-----------------------------------------------------------------------------+
 *                  nothing to request or to transfer
 *
 *  </pre>
 */
    public enum State
    {
        INITIALIZED(false),
        PREPARING(false),
        STREAMING(false),
        WAIT_COMPLETE(false),
        COMPLETE(true),
        FAILED(true),
        ABORTED(true);

        private final boolean finalState;

        State(boolean finalState)
        {
            this.finalState = finalState;
        }

        /**
         * @return true if current state is final, either COMPLETE, FAILED, or ABORTED.
         */
        public boolean isFinalState()
        {
             return finalState;
        }
    }

    private volatile State state = State.INITIALIZED;

    /**
     * Create new streaming session with the peer.
     */
    public StreamSession(StreamOperation streamOperation, InetAddressAndPort peer, StreamingChannel.Factory factory, @Nullable StreamingChannel controlChannel, int messagingVersion,
                         boolean isFollower, int index, TimeUUID pendingRepair, PreviewKind previewKind)
    {
        this.streamOperation = streamOperation;
        this.peer = peer;
        this.isFollower = isFollower;
        this.index = index;

        this.channel = new StreamingMultiplexedChannel(this, factory, peer, controlChannel, messagingVersion);
        this.metrics = StreamingMetrics.get(peer);
        this.pendingRepair = pendingRepair;
        this.previewKind = previewKind;
    }

    public boolean isFollower()
    {
        return isFollower;
    }

    public TimeUUID planId()
    {
        return streamResult == null ? null : streamResult.planId;
    }

    public int sessionIndex()
    {
        return index;
    }

    public StreamOperation streamOperation()
    {
        if (streamResult == null)
        {
            logger.warn("StreamResultFuture not initialized {} {}", channel.connectedTo(), isFollower ? "follower" : "initiator");
            return null;
        }
        else
        {
            return streamResult.streamOperation;
        }
    }

    public StreamOperation getStreamOperation()
    {
        return streamOperation;
    }

    public TimeUUID getPendingRepair()
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

    /**
     * Attach a channel to this session upon receiving the first inbound message.
     *
     * @param channel The channel to attach.
     * @return False if the channel was already attached, true otherwise.
     */
    public synchronized boolean attachInbound(StreamingChannel channel)
    {
        failIfFinished();

        boolean attached = inbound.putIfAbsent(channel.id(), channel) == null;
        if (attached)
            channel.onClose(() -> {
                if (null != inbound.remove(channel.id()) && inbound.isEmpty())
                    this.channel.close();
            });
        return attached;
    }

    /**
     * Attach a channel to this session upon sending the first outbound message.
     *
     * @param channel The channel to attach.
     * @return False if the channel was already attached, true otherwise.
     */
    public synchronized boolean attachOutbound(StreamingChannel channel)
    {
        failIfFinished();

        boolean attached = outbound.putIfAbsent(channel.id(), channel) == null;
        if (attached)
            channel.onClose(() -> outbound.remove(channel.id()));
        return attached;
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
                        hostAddressAndPort(channel.peer()),
                        channel.connectedTo().equals(channel.peer()) ? "" : " through " + hostAddressAndPort(channel.connectedTo()));

            StreamInitMessage message = new StreamInitMessage(getBroadcastAddressAndPort(),
                                                              sessionIndex(),
                                                              planId(),
                                                              streamOperation(),
                                                              getPendingRepair(),
                                                              getPreviewKind());

            channel.sendControlMessage(message).sync();
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
        if (state().isFinalState())
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
    public List<OutgoingStream> getOutgoingStreamsForRanges(RangesAtEndpoint replicas, Collection<ColumnFamilyStore> stores, TimeUUID pendingRepair, PreviewKind previewKind)
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

    private Future<?> closeSession(State finalState)
    {
        return closeSession(finalState, null);
    }

    private Future<?> closeSession(State finalState, String failureReason)
    {
        // Keep a separate lock on the closeFuture so that we create it once and only once.
        // Cannot use the StreamSession monitor here as StreamDeserializingTask/StreamSession.messageReceived
        // holds it while calling syncUninterruptibly on sendMessage which can trigger a closeSession in
        // the Netty event loop on error and cause a deadlock.
        synchronized (closeFutureLock)
        {
            if (closeFuture != null)
                return closeFuture;

            closeFuture = ScheduledExecutors.nonPeriodicTasks.submit(() -> {
                synchronized (this) {
                    state(finalState);
                    //this refers to StreamInfo
                    this.failureReason = failureReason;

                    sink.onClose(peer);
                    // closed before init?
                    if (streamResult != null)
                        streamResult.handleSessionComplete(this);
                }}).flatMap(ignore -> {
                    List<Future<?>> futures = new ArrayList<>();
                    // ensure aborting the tasks do not happen on the network IO thread (read: netty event loop)
                    // as we don't want any blocking disk IO to stop the network thread
                    if (finalState == State.FAILED || finalState == State.ABORTED)
                        futures.add(ScheduledExecutors.nonPeriodicTasks.submit(this::abortTasks));

                    // Channels should only be closed by the initiator; but, if this session closed
                    // due to failure, channels should be always closed regardless, even if this is not the initator.
                    if (!isFollower || state != State.COMPLETE)
                    {
                        logger.debug("[Stream #{}] Will close attached inbound {} and outbound {} channels", planId(), inbound, outbound);
                        inbound.values().forEach(channel -> futures.add(channel.close()));
                        outbound.values().forEach(channel -> futures.add(channel.close()));
                    }
                    return FutureCombiner.allOf(futures);
                });

            return closeFuture;
        }
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
            logger.warn("[Stream #{}] failed to abort some streaming tasks", planId(), e);
        }
    }

    /**
     * Set current state to {@code newState}.
     *
     * @param newState new state to set
     */
    public void state(State newState)
    {
        if (logger.isDebugEnabled())
            logger.debug("[Stream #{}] Changing session state from {} to {}", planId(), state, newState);

        sink.recordState(peer, newState);
        state = newState;
    }

    /**
     * @return current state
     */
    public State state()
    {
        return state;
    }

    public StreamingMultiplexedChannel getChannel()
    {
        return channel;
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

    public synchronized void messageReceived(StreamMessage message)
    {
        if (message.type != StreamMessage.Type.KEEP_ALIVE)
            failIfFinished();

        sink.recordMessage(peer, message.type);

        switch (message.type)
        {
            case STREAM_INIT:
                // at follower, nop
                break;
            case PREPARE_SYN:
                // at follower
                PrepareSynMessage msg = (PrepareSynMessage) message;
                prepare(msg.requests, msg.summaries);
                break;
            case PREPARE_SYNACK:
                // at initiator
                prepareSynAck((PrepareSynAckMessage) message);
                break;
            case PREPARE_ACK:
                // at follower
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
                // at initiator
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
        {
            prepare.summaries.add(task.getSummary());
        }

        channel.sendControlMessage(prepare).syncUninterruptibly();
    }

    /**
     * Signal an error to this stream session: if it's an EOF exception, it tries to understand if the socket was closed
     * after completion or because the peer was down, otherwise sends a {@link SessionFailedMessage} and closes
     * the session as {@link State#FAILED}.
     */
    public Future<?> onError(Throwable e)
    {
        boolean isEofException = e instanceof EOFException || e instanceof ClosedChannelException;
        if (isEofException)
        {
            State state = this.state;
            if (state.finalState)
            {
                logger.debug("[Stream #{}] Socket closed after session completed with state {}", planId(), state);
                return null;
            }
            else
            {
                logger.error("[Stream #{}] Socket closed before session completion, peer {} is probably down.",
                             planId(),
                             peer.getHostAddressAndPort(),
                             e);
                return closeSession(State.FAILED, "Failed because there was an " + e.getClass().getCanonicalName() + " with state=" + state.name());
            }
        }

        logError(e);

        if (channel.connected())
        {
            state(State.FAILED); // make sure subsequent error handling sees the session in a final state 
            channel.sendControlMessage(new SessionFailedMessage()).awaitUninterruptibly();
        }
        StringBuilder failureReason = new StringBuilder("Failed because of an unknown exception\n");
        boundStackTrace(e, DEBUG_STACKTRACE_LIMIT, failureReason);
        return closeSession(State.FAILED, failureReason.toString());
    }

    private void logError(Throwable e)
    {
        if (e instanceof SocketTimeoutException)
        {
            logger.error("[Stream #{}] Timeout from peer {}{}. Is peer down? " +
                         "If not, and earlier failure detection is required enable (or lower) streaming_keep_alive_period.",
                         planId(),
                         hostAddressAndPort(channel.peer()),
                         channel.peer().equals(channel.connectedTo()) ? "" : " through " + hostAddressAndPort(channel.connectedTo()),
                         e);
        }
        else
        {
            logger.error("[Stream #{}] Streaming error occurred on session with peer {}{}", planId(),
                         hostAddressAndPort(channel.peer()),
                         channel.peer().equals(channel.connectedTo()) ? "" : " through " + hostAddressAndPort(channel.connectedTo()),
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
        ScheduledExecutors.nonPeriodicTasks.execute(() -> {
            try
            {
                prepareAsync(requests, summaries);
            }
            catch (Exception e)
            {
                onError(e);
            }
        });
    }

    public void countStreamedIn(boolean isEntireSSTable)
    {
        metrics.countStreamedIn(isEntireSSTable);
    }

    /**
     * Finish preparing the session. This method is blocking (memtables are flushed in {@link #addTransferRanges}),
     * so the logic should not execute on the main IO thread (read: netty event loop).
     */
    private void prepareAsync(Collection<StreamRequest> requests, Collection<StreamSummary> summaries)
    {
        if (StreamOperation.REPAIR == streamOperation())
            checkAvailableDiskSpaceAndCompactions(summaries);
        for (StreamRequest request : requests)
            addTransferRanges(request.keyspace, RangesAtEndpoint.concat(request.full, request.transientReplicas), request.columnFamilies, true); // always flush on stream request
        for (StreamSummary summary : summaries)
            prepareReceiving(summary);

        PrepareSynAckMessage prepareSynAck = new PrepareSynAckMessage();
        if (!peer.equals(FBUtilities.getBroadcastAddressAndPort()))
            for (StreamTransferTask task : transfers.values())
                prepareSynAck.summaries.add(task.getSummary());

        streamResult.handleSessionPrepared(this, PrepareDirection.SEND);
        // After sending the message the initiator can close the channel which will cause a ClosedChannelException
        // in buffer logic, this then gets sent to onError which validates the state isFinalState, if not fails
        // the session.  To avoid a race condition between sending and setting state, make sure to update the state
        // before sending the message (without closing the channel)
        // see CASSANDRA-17116
        if (isPreview())
            state(State.COMPLETE);
        channel.sendControlMessage(prepareSynAck).syncUninterruptibly();

        if (isPreview())
            completePreview();
        else
            maybeCompleted();
    }

    private void prepareSynAck(PrepareSynAckMessage msg)
    {
        if (StreamOperation.REPAIR == streamOperation())
            checkAvailableDiskSpaceAndCompactions(msg.summaries);
        if (!msg.summaries.isEmpty())
        {
            for (StreamSummary summary : msg.summaries)
                prepareReceiving(summary);

            // only send the (final) ACK if we are expecting the peer to send this node (the initiator) some files
            if (!isPreview())
                channel.sendControlMessage(new PrepareAckMessage()).syncUninterruptibly();
        }

        if (isPreview())
            completePreview();
        else
            startStreamingFiles(PrepareDirection.ACK);
    }

    private void prepareAck(PrepareAckMessage msg)
    {
        if (isPreview())
            throw new RuntimeException(String.format("[Stream #%s] Cannot receive PrepareAckMessage for preview session", planId()));
        startStreamingFiles(PrepareDirection.ACK);
    }

    /**
     * In the case where we have an error checking disk space we allow the Operation to continue.
     * In the case where we do _not_ have available space, this method raises a RTE.
     * TODO: Consider revising this to returning a boolean and allowing callers upstream to handle that.
     */
    private void checkAvailableDiskSpaceAndCompactions(Collection<StreamSummary> summaries)
    {
        if (DatabaseDescriptor.getSkipStreamDiskSpaceCheck())
            return;

        boolean hasAvailableSpace = true;

        try
        {
            hasAvailableSpace = checkAvailableDiskSpaceAndCompactions(summaries, planId(), peer.getHostAddress(true), pendingRepair != null);
        }
        catch (Exception e)
        {
            logger.error("[Stream #{}] Could not check available disk space and compactions for {}, summaries = {}", planId(), this, summaries, e);
        }
        if (!hasAvailableSpace)
            throw new RuntimeException(String.format("Not enough disk space for stream %s), summaries=%s", this, summaries));
    }

    /**
     * Makes sure that we expect to have enough disk space available for the new streams, taking into consideration
     * the ongoing compactions and streams.
     */
    @VisibleForTesting
    public static boolean checkAvailableDiskSpaceAndCompactions(Collection<StreamSummary> summaries,
                                                                @Nullable TimeUUID planId,
                                                                @Nullable String remoteAddress,
                                                                boolean isForIncremental)
    {
        Map<TableId, Long> perTableIdIncomingBytes = new HashMap<>();
        Map<TableId, Integer> perTableIdIncomingFiles = new HashMap<>();
        long newStreamTotal = 0;
        for (StreamSummary summary : summaries)
        {
            perTableIdIncomingFiles.merge(summary.tableId, summary.files, Integer::sum);
            perTableIdIncomingBytes.merge(summary.tableId, summary.totalSize, Long::sum);
            newStreamTotal += summary.totalSize;
        }
        if (perTableIdIncomingBytes.isEmpty() || newStreamTotal == 0)
            return true;

        return checkDiskSpace(perTableIdIncomingBytes, planId, Directories::getFileStore) &&
               checkPendingCompactions(perTableIdIncomingBytes, perTableIdIncomingFiles, planId, remoteAddress, isForIncremental, newStreamTotal);
    }

    @VisibleForTesting
    static boolean checkDiskSpace(Map<TableId, Long> perTableIdIncomingBytes,
                                  TimeUUID planId,
                                  Function<File, FileStore> fileStoreMapper)
    {
        Map<FileStore, Long> newStreamBytesToWritePerFileStore = new HashMap<>();
        Set<FileStore> allFileStores = new HashSet<>();
        // Sum up the incoming bytes per file store - we assume that the stream is evenly distributed over the writable
        // file stores for the table.
        for (Map.Entry<TableId, Long> entry : perTableIdIncomingBytes.entrySet())
        {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(entry.getKey());
            if (cfs == null || perTableIdIncomingBytes.get(entry.getKey()) == 0)
                continue;

            Set<FileStore> allWriteableFileStores = cfs.getDirectories().allFileStores(fileStoreMapper);
            if (allWriteableFileStores.isEmpty())
            {
                logger.error("[Stream #{}] Could not get any writeable FileStores for {}.{}", planId, cfs.getKeyspaceName(), cfs.getTableName());
                continue;
            }
            allFileStores.addAll(allWriteableFileStores);
            long totalBytesInPerFileStore = entry.getValue() / allWriteableFileStores.size();
            for (FileStore fs : allWriteableFileStores)
                newStreamBytesToWritePerFileStore.merge(fs, totalBytesInPerFileStore, Long::sum);
        }
        Map<FileStore, Long> totalCompactionWriteRemaining = Directories.perFileStore(CompactionManager.instance.active.estimatedRemainingWriteBytes(),
                                                                                      fileStoreMapper);
        long totalStreamRemaining = StreamManager.instance.getTotalRemainingOngoingBytes();
        long totalBytesStreamRemainingPerFileStore = totalStreamRemaining / Math.max(1, allFileStores.size());
        Map<FileStore, Long> allWriteData = new HashMap<>();
        for (Map.Entry<FileStore, Long> fsBytes : newStreamBytesToWritePerFileStore.entrySet())
            allWriteData.put(fsBytes.getKey(), fsBytes.getValue() +
                                               totalBytesStreamRemainingPerFileStore +
                                               totalCompactionWriteRemaining.getOrDefault(fsBytes.getKey(), 0L));

        if (!Directories.hasDiskSpaceForCompactionsAndStreams(allWriteData))
        {
            logger.error("[Stream #{}] Not enough disk space to stream {} to {} (stream ongoing remaining={}, compaction ongoing remaining={}, all ongoing writes={})",
                         planId,
                         newStreamBytesToWritePerFileStore,
                         perTableIdIncomingBytes.keySet().stream()
                                                .map(ColumnFamilyStore::getIfExists).filter(Objects::nonNull)
                                                .map(cfs -> cfs.getKeyspaceName() + '.' + cfs.name)
                                                .collect(Collectors.joining(",")),
                         totalStreamRemaining,
                         totalCompactionWriteRemaining,
                         allWriteData);
            return false;
        }
        return true;
    }

    @VisibleForTesting
    static boolean checkPendingCompactions(Map<TableId, Long> perTableIdIncomingBytes,
                                           Map<TableId, Integer> perTableIdIncomingFiles,
                                           TimeUUID planId, String remoteAddress,
                                           boolean isForIncremental,
                                           long newStreamTotal)
    {

        int pendingCompactionsBeforeStreaming = 0;
        int pendingCompactionsAfterStreaming = 0;
        List<String> tables = new ArrayList<>(perTableIdIncomingFiles.size());
        for (Keyspace ks : Keyspace.all())
        {
            Map<ColumnFamilyStore, TableId> cfStreamed = perTableIdIncomingBytes.keySet().stream()
                                                                                .filter(ks::hasColumnFamilyStore)
                                                                                .collect(Collectors.toMap(ks::getColumnFamilyStore, Function.identity()));
            for (ColumnFamilyStore cfs : ks.getColumnFamilyStores())
            {
                CompactionStrategyManager csm = cfs.getCompactionStrategyManager();
                int tasksOther = csm.getEstimatedRemainingTasks();
                int tasksStreamed = tasksOther;
                if (cfStreamed.containsKey(cfs))
                {
                    TableId tableId = cfStreamed.get(cfs);
                    tasksStreamed = csm.getEstimatedRemainingTasks(perTableIdIncomingFiles.get(tableId),
                                                                   perTableIdIncomingBytes.get(tableId),
                                                                   isForIncremental);
                    tables.add(String.format("%s.%s", cfs.getKeyspaceName(), cfs.name));
                }
                pendingCompactionsBeforeStreaming += tasksOther;
                pendingCompactionsAfterStreaming += tasksStreamed;
            }
        }
        Collections.sort(tables);
        int pendingThreshold = ActiveRepairService.instance().getRepairPendingCompactionRejectThreshold();
        if (pendingCompactionsAfterStreaming > pendingThreshold)
        {
            logger.error("[Stream #{}] Rejecting incoming files based on pending compactions calculation " +
                         "pendingCompactionsBeforeStreaming={} pendingCompactionsAfterStreaming={} pendingThreshold={} remoteAddress={}",
                         planId, pendingCompactionsBeforeStreaming, pendingCompactionsAfterStreaming, pendingThreshold, remoteAddress);
            return false;
        }

        long newStreamFiles = perTableIdIncomingFiles.values().stream().mapToInt(i -> i).sum();

        logger.info("[Stream #{}] Accepting incoming files newStreamTotalSSTables={} newStreamTotalBytes={} " +
                    "pendingCompactionsBeforeStreaming={} pendingCompactionsAfterStreaming={} pendingThreshold={} remoteAddress={} " +
                    "streamedTables=\"{}\"",
                    planId, newStreamFiles, newStreamTotal,
                    pendingCompactionsBeforeStreaming, pendingCompactionsAfterStreaming, pendingThreshold, remoteAddress,
                    String.join(",", tables));
        return true;
    }

    /**
     * Call back after sending StreamMessageHeader.
     *
     * @param message sent stream message
     */
    public void streamSent(OutgoingStreamMessage message)
    {
        long headerSize = message.stream.getEstimatedSize();
        StreamingMetrics.totalOutgoingBytes.inc(headerSize);
        metrics.outgoingBytes.inc(headerSize);

        if(StreamOperation.REPAIR == getStreamOperation())
        {
            StreamingMetrics.totalOutgoingRepairBytes.inc(headerSize);
            StreamingMetrics.totalOutgoingRepairSSTables.inc(message.stream.getNumFiles());
        }

        // schedule timeout for receiving ACK
        StreamTransferTask task = transfers.get(message.header.tableId);
        if (task != null)
        {
            task.scheduleTimeout(message.header.sequenceNumber, DatabaseDescriptor.getStreamTransferTaskTimeout().toMilliseconds(), TimeUnit.MILLISECONDS);
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
            throw new RuntimeException(String.format("[Stream #%s] Cannot receive files for preview session", planId()));
        }

        long headerSize = message.stream.getSize();
        StreamingMetrics.totalIncomingBytes.inc(headerSize);
        metrics.incomingBytes.inc(headerSize);
        // send back file received message
        channel.sendControlMessage(new ReceivedMessage(message.header.tableId, message.header.sequenceNumber)).syncUninterruptibly();
        StreamHook.instance.reportIncomingStream(message.header.tableId, message.stream, this, message.header.sequenceNumber);
        long receivedStartNanos = nanoTime();
        try
        {
            receivers.get(message.header.tableId).received(message.stream);
        }
        finally
        {
            long latencyNanos = nanoTime() - receivedStartNanos;
            metrics.incomingProcessTime.update(latencyNanos, TimeUnit.NANOSECONDS);
            long latencyMs = TimeUnit.NANOSECONDS.toMillis(latencyNanos);
            int timeout = DatabaseDescriptor.getInternodeStreamingTcpUserTimeoutInMS();
            if (timeout > 0 && latencyMs > timeout)
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN,
                                 1, TimeUnit.MINUTES,
                                 "The time taken ({} ms) for processing the incoming stream message ({})" +
                                 " exceeded internode streaming TCP user timeout ({} ms).\n" +
                                 "The streaming connection might be closed due to tcp user timeout.\n" +
                                 "Try to increase the internode_streaming_tcp_user_timeout" +
                                 " or set it to 0 to use system defaults.",
                                 latencyMs, message, timeout);
        }
    }

    public void progress(String filename, ProgressInfo.Direction direction, long bytes, long delta, long total)
    {
        if (delta < 0)
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES,
                             "[id={}, key={{}, {}, {})] Stream event reported a negative delta ({})",
                             planId(), peer, filename, direction, delta);
        ProgressInfo progress = new ProgressInfo(peer, index, filename, direction, bytes, delta, total);
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
        logger.debug("[Stream #{}] handling Complete message, state = {}", planId(), state);

        if (!isFollower) // initiator
        {
            initiatorCompleteOrWait();
        }
        else // follower
        {
            // pre-4.0 nodes should not be connected via streaming, see {@link MessagingService#accept_streaming}
            throw new IllegalStateException(String.format("[Stream #%s] Complete message can be only received by the initiator!", planId()));
        }
    }

    /**
     * Synchronize both {@link #complete()} and {@link #maybeCompleted()} to avoid racing
     */
    private synchronized boolean maybeCompleted()
    {
        if (!(receivers.isEmpty() && transfers.isEmpty()))
            return false;

        // if already executed once, skip it
        if (maybeCompleted)
            return true;

        maybeCompleted = true;
        if (!isFollower) // initiator
        {
            initiatorCompleteOrWait();
        }
        else // follower
        {
            // After sending the message the initiator can close the channel which will cause a ClosedChannelException
            // in buffer logic, this then gets sent to onError which validates the state isFinalState, if not fails
            // the session.  To avoid a race condition between sending and setting state, make sure to update the state
            // before sending the message (without closing the channel)
            // see CASSANDRA-17116
            state(State.COMPLETE);
            channel.sendControlMessage(new CompleteMessage()).syncUninterruptibly();
            closeSession(State.COMPLETE);
        }

        return true;
    }

    private void initiatorCompleteOrWait()
    {
        // This is called when coordination completes AND when COMPLETE message is seen; it is possible that the
        // COMPLETE method is seen first!
        if (state == State.WAIT_COMPLETE)
            closeSession(State.COMPLETE);
        else
            state(State.WAIT_COMPLETE);
    }

    /**
     * Call back on receiving {@code StreamMessage.Type.SESSION_FAILED} message.
     */
    public synchronized void sessionFailed()
    {
        logger.error("[Stream #{}] Remote peer {} failed stream session.", planId(), peer.toString());
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Remote peer ").append(peer).append(" failed stream session");
        closeSession(State.FAILED, stringBuilder.toString());
    }

    /**
     * Call back on receiving {@code StreamMessage.Type.SESSION_FAILED} message.
     */
    public synchronized void sessionTimeout()
    {
        logger.error("[Stream #{}] timeout with {}.", planId(), peer.toString());
        closeSession(State.FAILED, "Session timed out");
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
        return new SessionInfo(channel.peer(), index, channel.connectedTo(), receivingSummaries, transferSummaries, state, failureReason);
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

    private void completePreview()
    {
        try
        {
            if (state != State.COMPLETE) // mark as waiting to complete while closeSession futures run.
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

    /**
     * Flushes matching column families from the given keyspace, or all columnFamilies
     * if the cf list is empty.
     */
    private void flushSSTables(Iterable<ColumnFamilyStore> stores)
    {
        List<Future<?>> flushes = new ArrayList<>();
        for (ColumnFamilyStore cfs : stores)
            flushes.add(cfs.forceFlush(ColumnFamilyStore.FlushReason.STREAMING));
        FBUtilities.waitOnFutures(flushes);
    }

    @VisibleForTesting
    public synchronized void prepareReceiving(StreamSummary summary)
    {
        failIfFinished();
        if (summary.files > 0)
            receivers.put(summary.tableId, new StreamReceiveTask(this, summary.tableId, summary.files, summary.totalSize));
    }

    private void startStreamingFiles(@Nullable PrepareDirection prepareDirection)
    {
        if (prepareDirection != null)
            streamResult.handleSessionPrepared(this, prepareDirection);

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
                    // do not sync here as this does disk access
                    channel.sendControlMessage(ofm);
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

    @VisibleForTesting
    public static interface MessageStateSink
    {
        static final MessageStateSink NONE = new MessageStateSink() {
            @Override
            public void recordState(InetAddressAndPort from, State state)
            {
            }

            @Override
            public void recordMessage(InetAddressAndPort from, StreamMessage.Type message)
            {
            }

            @Override
            public void onClose(InetAddressAndPort from)
            {
            }
        };

        /**
         * @param from peer that is connected in the stream session
         * @param state new state to change to
         */
        public void recordState(InetAddressAndPort from, StreamSession.State state);

        /**
         * @param from peer that sends the given message
         * @param message stream message sent by peer
         */
        public void recordMessage(InetAddressAndPort from, StreamMessage.Type message);

        /**
         *
         * @param from peer that is being disconnected
         */
        public void onClose(InetAddressAndPort from);
    }

    public static String createLogTag(StreamSession session)
    {
        return createLogTag(session, (Object) null);
    }

    public static String createLogTag(StreamSession session, StreamingChannel channel)
    {
        return createLogTag(session, channel == null ? null : channel.id());
    }

    public static String createLogTag(StreamSession session, Channel channel)
    {
        return createLogTag(session, channel == null ? null : channel.id());
    }

    public static String createLogTag(StreamSession session, Object channelId)
    {
        StringBuilder sb = new StringBuilder(64);
        sb.append("[Stream");

        if (session != null)
            sb.append(" #").append(session.planId());

        if (channelId != null)
            sb.append(" channel: ").append(channelId);

        sb.append(']');
        return sb.toString();
    }

    public synchronized void abort()
    {
        if (state.isFinalState())
        {
            logger.debug("[Stream #{}] Stream session with peer {} is already in a final state on abort.", planId(), peer);
            return;
        }

        logger.info("[Stream #{}] Aborting stream session with peer {}...", planId(), peer);

        if (channel.connected())
            channel.sendControlMessage(new SessionFailedMessage());

        try
        {
            closeSession(State.ABORTED);
        }
        catch (Exception e)
        {
            logger.error("[Stream #{}] Error aborting stream session with peer {}", planId(), peer);
        }
    }

    @Override
    public String toString()
    {
        return "StreamSession{" +
               "streamOperation=" + streamOperation +
               ", peer=" + peer +
               ", channel=" + channel +
               ", requests=" + requests +
               ", transfers=" + transfers +
               ", isFollower=" + isFollower +
               ", pendingRepair=" + pendingRepair +
               ", previewKind=" + previewKind +
               ", state=" + state +
               '}';
    }

    public static StringBuilder boundStackTrace(Throwable e, int limit, StringBuilder out)
    {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        return boundStackTrace(e, limit, limit, visited, out);
    }

    public static StringBuilder boundStackTrace(Throwable e, int limit, int counter, Set<Throwable> visited, StringBuilder out)
    {
        if (e == null)
            return out;

        if (!visited.add(e))
            return out.append("[CIRCULAR REFERENCE: ").append(e.getClass().getName()).append(": ").append(e.getMessage()).append("]").append('\n');
        visited.add(e);

        StackTraceElement[] stackTrace = e.getStackTrace();
        out.append(e.getClass().getName() + ": " + e.getMessage()).append('\n');

        // When dealing with the leaf, ignore how many stack traces were already written, and allow the max.
        // This is here as the leaf tends to show where the issue started, so tends to be impactful for debugging
        if (e.getCause() == null)
            counter = limit;

        for (int i = 0, size = Math.min(e.getStackTrace().length, limit); i < size && counter > 0; i++)
        {
            out.append('\t').append(stackTrace[i]).append('\n');
            counter--;
        }

        boundStackTrace(e.getCause(), limit, counter, visited, out);
        return out;
    }
}
