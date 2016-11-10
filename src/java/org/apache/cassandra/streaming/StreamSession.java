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

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.*;

import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.messages.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.Refs;

/**
 * Handles the streaming a one or more section of one of more sstables to and from a specific
 * remote node.
 *
 * Both this node and the remote one will create a similar symmetrical StreamSession. A streaming
 * session has the following life-cycle:
 *
 * 1. Connections Initialization
 *
 *   (a) A node (the initiator in the following) create a new StreamSession, initialize it (init())
 *       and then start it (start()). Start will create a {@link ConnectionHandler} that will create
 *       two connections to the remote node (the follower in the following) with whom to stream and send
 *       a StreamInit message. The first connection will be the incoming connection for the
 *       initiator, and the second connection will be the outgoing.
 *   (b) Upon reception of that StreamInit message, the follower creates its own StreamSession,
 *       initialize it if it still does not exist, and attach connecting socket to its ConnectionHandler
 *       according to StreamInit message's isForOutgoing flag.
 *   (d) When the both incoming and outgoing connections are established, StreamSession calls
 *       StreamSession#onInitializationComplete method to start the streaming prepare phase
 *       (StreamResultFuture.startStreaming()).
 *
 * 2. Streaming preparation phase
 *
 *   (a) This phase is started when the initiator onInitializationComplete() method is called. This method sends a
 *       PrepareMessage that includes what files/sections this node will stream to the follower
 *       (stored in a StreamTransferTask, each column family has it's own transfer task) and what
 *       the follower needs to stream back (StreamReceiveTask, same as above). If the initiator has
 *       nothing to receive from the follower, it goes directly to its Streaming phase. Otherwise,
 *       it waits for the follower PrepareMessage.
 *   (b) Upon reception of the PrepareMessage, the follower records which files/sections it will receive
 *       and send back its own PrepareMessage with a summary of the files/sections that will be sent to
 *       the initiator (prepare()). After having sent that message, the follower goes to its Streamning
 *       phase.
 *   (c) When the initiator receives the follower PrepareMessage, it records which files/sections it will
 *       receive and then goes to his own Streaming phase.
 *
 * 3. Streaming phase
 *
 *   (a) The streaming phase is started by each node (the sender in the follower, but note that each side
 *       of the StreamSession may be sender for some of the files) involved by calling startStreamingFiles().
 *       This will sequentially send a FileMessage for each file of each SteamTransferTask. Each FileMessage
 *       consists of a FileMessageHeader that indicates which file is coming and then start streaming the
 *       content for that file (StreamWriter in FileMessage.serialize()). When a file is fully sent, the
 *       fileSent() method is called for that file. If all the files for a StreamTransferTask are sent
 *       (StreamTransferTask.complete()), the task is marked complete (taskCompleted()).
 *   (b) On the receiving side, a SSTable will be written for the incoming file (StreamReader in
 *       FileMessage.deserialize()) and once the FileMessage is fully received, the file will be marked as
 *       complete (received()). When all files for the StreamReceiveTask have been received, the sstables
 *       are added to the CFS (and 2ndary index are built, StreamReceiveTask.complete()) and the task
 *       is marked complete (taskCompleted())
 *   (b) If during the streaming of a particular file an error occurs on the receiving end of a stream
 *       (FileMessage.deserialize), the node will send a SessionFailedMessage to the sender and close the stream session.
 *   (c) When all transfer and receive tasks for a session are complete, the move to the Completion phase
 *       (maybeCompleted()).
 *
 * 4. Completion phase
 *
 *   (a) When a node has finished all transfer and receive task, it enter the completion phase (maybeCompleted()).
 *       If it had already received a CompleteMessage from the other side (it is in the WAIT_COMPLETE state), that
 *       session is done is is closed (closeSession()). Otherwise, the node switch to the WAIT_COMPLETE state and
 *       send a CompleteMessage to the other side.
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
    protected final ConcurrentHashMap<UUID, StreamTransferTask> transfers = new ConcurrentHashMap<>();
    // data receivers, filled after receiving prepare message
    private final Map<UUID, StreamReceiveTask> receivers = new ConcurrentHashMap<>();
    private final StreamingMetrics metrics;
    /* can be null when session is created in remote */
    private final StreamConnectionFactory factory;

    public final ConnectionHandler handler;

    private AtomicBoolean isAborted = new AtomicBoolean(false);
    private final boolean keepSSTableLevel;
    private final boolean isIncremental;

    public static enum State
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
     *
     * @param peer Address of streaming peer
     * @param connecting Actual connecting address
     * @param factory is used for establishing connection
     */
    public StreamSession(InetAddress peer, InetAddress connecting, StreamConnectionFactory factory, int index, boolean keepSSTableLevel, boolean isIncremental)
    {
        this.peer = peer;
        this.connecting = connecting;
        this.index = index;
        this.factory = factory;
        this.handler = new ConnectionHandler(this);
        this.metrics = StreamingMetrics.get(connecting);
        this.keepSSTableLevel = keepSSTableLevel;
        this.isIncremental = isIncremental;
    }

    public UUID planId()
    {
        return streamResult == null ? null : streamResult.planId;
    }

    public int sessionIndex()
    {
        return index;
    }

    public String description()
    {
        return streamResult == null ? null : streamResult.description;
    }

    public boolean keepSSTableLevel()
    {
        return keepSSTableLevel;
    }

    public boolean isIncremental()
    {
        return isIncremental;
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
    }

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
            handler.initiate();
            onInitializationComplete();
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            onError(e);
        }
    }

    public Socket createConnection() throws IOException
    {
        assert factory != null;
        return factory.createConnection(connecting);
    }

    /**
     * Request data fetch task to this session.
     *
     * @param keyspace Requesting keyspace
     * @param ranges Ranges to retrieve data
     * @param columnFamilies ColumnFamily names. Can be empty if requesting all CF under the keyspace.
     */
    public void addStreamRequest(String keyspace, Collection<Range<Token>> ranges, Collection<String> columnFamilies, long repairedAt)
    {
        requests.add(new StreamRequest(keyspace, ranges, columnFamilies, repairedAt));
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
     * @param repairedAt the time the repair started.
     */
    public void addTransferRanges(String keyspace, Collection<Range<Token>> ranges, Collection<String> columnFamilies, boolean flushTables, long repairedAt)
    {
        Collection<ColumnFamilyStore> stores = getColumnFamilyStores(keyspace, columnFamilies);
        if (flushTables)
            flushSSTables(stores);

        List<Range<Token>> normalizedRanges = Range.normalize(ranges);
        List<SSTableStreamingSections> sections = getSSTableSectionsForRanges(normalizedRanges, stores, repairedAt, isIncremental);
        try
        {
            addTransferFiles(sections);
        }
        finally
        {
            for (SSTableStreamingSections release : sections)
                release.ref.release();
        }
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
    public static List<SSTableStreamingSections> getSSTableSectionsForRanges(Collection<Range<Token>> ranges, Collection<ColumnFamilyStore> stores, long overriddenRepairedAt, final boolean isIncremental)
    {
        Refs<SSTableReader> refs = new Refs<>();
        try
        {
            for (ColumnFamilyStore cfStore : stores)
            {
                final List<Range<RowPosition>> keyRanges = new ArrayList<>(ranges.size());
                for (Range<Token> range : ranges)
                    keyRanges.add(Range.makeRowRange(range));
                refs.addAll(cfStore.selectAndReference(new Function<View, List<SSTableReader>>()
                {
                    public List<SSTableReader> apply(View view)
                    {
                        SSTableIntervalTree intervalTree = SSTableIntervalTree.build(ColumnFamilyStore.CANONICAL_SSTABLES.apply(view));
                        Set<SSTableReader> sstables = Sets.newHashSet();
                        for (Range<RowPosition> keyRange : keyRanges)
                        {
                            // keyRange excludes its start, while sstableInBounds is inclusive (of both start and end).
                            // This is fine however, because keyRange has been created from a token range through Range.makeRowRange (see above).
                            // And that later method uses the Token.maxKeyBound() method to creates the range, which return a "fake" key that
                            // sort after all keys having the token. That "fake" key cannot however be equal to any real key, so that even
                            // including keyRange.left will still exclude any key having the token of the original token range, and so we're
                            // still actually selecting what we wanted.
                            for (SSTableReader sstable : View.sstablesInBounds(keyRange.left, keyRange.right, intervalTree))
                            {
                                if (!isIncremental || !sstable.isRepaired())
                                    sstables.add(sstable);
                            }
                        }

                        logger.debug("ViewFilter for {}/{} sstables", sstables.size(), view.sstables.size());
                        return ImmutableList.copyOf(sstables);
                    }
                }).refs);
            }

            List<SSTableStreamingSections> sections = new ArrayList<>(refs.size());
            for (SSTableReader sstable : refs)
            {
                long repairedAt = overriddenRepairedAt;
                if (overriddenRepairedAt == ActiveRepairService.UNREPAIRED_SSTABLE)
                    repairedAt = sstable.getSSTableMetadata().repairedAt;
                sections.add(new SSTableStreamingSections(refs.get(sstable),
                                                          sstable.getPositionsForRanges(ranges),
                                                          sstable.estimatedKeysForRanges(ranges),
                                                          repairedAt));
            }
            return sections;
        }
        catch (Throwable t)
        {
            refs.release();
            throw t;
        }
    }

    public void addTransferFiles(Collection<SSTableStreamingSections> sstableDetails)
    {
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

            UUID cfId = details.ref.get().metadata.cfId;
            StreamTransferTask task = transfers.get(cfId);
            if (task == null)
            {
                //guarantee atomicity
                StreamTransferTask newTask = new StreamTransferTask(this, cfId);
                task = transfers.putIfAbsent(cfId, newTask);
                if (task == null)
                    task = newTask;
            }
            task.addTransferFile(details.ref, details.estimatedKeys, details.sections, details.repairedAt);
            iter.remove();
        }
    }

    public static class SSTableStreamingSections
    {
        public final Ref<SSTableReader> ref;
        public final List<Pair<Long, Long>> sections;
        public final long estimatedKeys;
        public final long repairedAt;

        public SSTableStreamingSections(Ref<SSTableReader> ref, List<Pair<Long, Long>> sections, long estimatedKeys, long repairedAt)
        {
            this.ref = ref;
            this.sections = sections;
            this.estimatedKeys = estimatedKeys;
            this.repairedAt = repairedAt;
        }
    }

    private synchronized void closeSession(State finalState)
    {
        if (isAborted.compareAndSet(false, true))
        {
            state(finalState);

            if (finalState == State.FAILED)
            {
                for (StreamTask task : Iterables.concat(receivers.values(), transfers.values()))
                    task.abort();
            }

            // Note that we shouldn't block on this close because this method is called on the handler
            // incoming thread (so we would deadlock).
            handler.close();

            streamResult.handleSessionComplete(this);
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
            case PREPARE:
                PrepareMessage msg = (PrepareMessage) message;
                prepare(msg.requests, msg.summaries);
                break;

            case FILE:
                receive((IncomingFileMessage) message);
                break;

            case RECEIVED:
                ReceivedMessage received = (ReceivedMessage) message;
                received(received.cfId, received.sequenceNumber);
                break;

            case COMPLETE:
                complete();
                break;

            case SESSION_FAILED:
                sessionFailed();
                break;
        }
    }

    /**
     * Call back when connection initialization is complete to start the prepare phase.
     */
    public void onInitializationComplete()
    {
        // send prepare message
        state(State.PREPARING);
        PrepareMessage prepare = new PrepareMessage();
        prepare.requests.addAll(requests);
        for (StreamTransferTask task : transfers.values())
            prepare.summaries.add(task.getSummary());
        handler.sendMessage(prepare);

        // if we don't need to prepare for receiving stream, start sending files immediately
        if (requests.isEmpty())
            startStreamingFiles();
    }

    /**l
     * Call back for handling exception during streaming.
     *
     * @param e thrown exception
     */
    public void onError(Throwable e)
    {
        if (e instanceof SocketTimeoutException)
        {
            logger.error("[Stream #{}] Streaming socket timed out. This means the session peer stopped responding or " +
                         "is still processing received data. If there is no sign of failure in the other end or a very " +
                         "dense table is being transferred you may want to increase streaming_socket_timeout_in_ms " +
                         "property. Current value is {}ms.", planId(), DatabaseDescriptor.getStreamingSocketTimeout(), e);
        }
        else
        {
            logger.error("[Stream #{}] Streaming error occurred", planId(), e);
        }
        // send session failure message
        if (handler.isOutgoingConnected())
            handler.sendMessage(new SessionFailedMessage());
        // fail session
        closeSession(State.FAILED);
    }

    /**
     * Prepare this session for sending/receiving files.
     */
    public void prepare(Collection<StreamRequest> requests, Collection<StreamSummary> summaries)
    {
        // prepare tasks
        state(State.PREPARING);
        for (StreamRequest request : requests)
            addTransferRanges(request.keyspace, request.ranges, request.columnFamilies, true, request.repairedAt); // always flush on stream request
        for (StreamSummary summary : summaries)
            prepareReceiving(summary);

        // send back prepare message if prepare message contains stream request
        if (!requests.isEmpty())
        {
            PrepareMessage prepare = new PrepareMessage();
            for (StreamTransferTask task : transfers.values())
                prepare.summaries.add(task.getSummary());
            handler.sendMessage(prepare);
        }

        // if there are files to stream
        if (!maybeCompleted())
            startStreamingFiles();
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
        StreamTransferTask task = transfers.get(header.cfId);
        if (task != null)
        {
            task.scheduleTimeout(header.sequenceNumber, 12, TimeUnit.HOURS);
        }
    }

    /**
     * Call back after receiving FileMessageHeader.
     *
     * @param message received file
     */
    public void receive(IncomingFileMessage message)
    {
        long headerSize = message.header.size();
        StreamingMetrics.totalIncomingBytes.inc(headerSize);
        metrics.incomingBytes.inc(headerSize);
        // send back file received message
        handler.sendMessage(new ReceivedMessage(message.header.cfId, message.header.sequenceNumber));
        receivers.get(message.header.cfId).received(message.sstable);
    }

    public void progress(Descriptor desc, ProgressInfo.Direction direction, long bytes, long total)
    {
        ProgressInfo progress = new ProgressInfo(peer, index, desc.filenameFor(Component.DATA), direction, bytes, total);
        streamResult.handleProgress(progress);
    }

    public void received(UUID cfId, int sequenceNumber)
    {
        transfers.get(cfId).complete(sequenceNumber);
    }

    /**
     * Check if session is completed on receiving {@code StreamMessage.Type.COMPLETE} message.
     */
    public synchronized void complete()
    {
        if (state == State.WAIT_COMPLETE)
        {
            if (!completeSent)
            {
                handler.sendMessage(new CompleteMessage());
                completeSent = true;
            }
            closeSession(State.COMPLETE);
        }
        else
        {
            state(State.WAIT_COMPLETE);
            handler.closeIncoming();
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
        receivers.remove(completedTask.cfId);
        maybeCompleted();
    }

    public synchronized void taskCompleted(StreamTransferTask completedTask)
    {
        transfers.remove(completedTask.cfId);
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

    private boolean maybeCompleted()
    {
        boolean completed = receivers.isEmpty() && transfers.isEmpty();
        if (completed)
        {
            if (state == State.WAIT_COMPLETE)
            {
                if (!completeSent)
                {
                    handler.sendMessage(new CompleteMessage());
                    completeSent = true;
                }
                closeSession(State.COMPLETE);
            }
            else
            {
                // notify peer that this session is completed
                handler.sendMessage(new CompleteMessage());
                completeSent = true;
                state(State.WAIT_COMPLETE);
                handler.closeOutgoing();
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

    private void prepareReceiving(StreamSummary summary)
    {
        if (summary.files > 0)
            receivers.put(summary.cfId, new StreamReceiveTask(this, summary.cfId, summary.files, summary.totalSize));
    }

    private void startStreamingFiles()
    {
        streamResult.handleSessionPrepared(this);

        state(State.STREAMING);
        for (StreamTransferTask task : transfers.values())
        {
            Collection<OutgoingFileMessage> messages = task.getFileMessages();
            if (messages.size() > 0)
                handler.sendMessages(messages);
            else
                taskCompleted(task); // there is no file to send
        }
    }
}
