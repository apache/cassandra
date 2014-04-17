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
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.streaming.messages.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

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
 *       (stored in a StreamTranferTask, each column family has it's own transfer task) and what
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
 *   (b) If during the streaming of a particular file an I/O error occurs on the receiving end of a stream
 *       (FileMessage.deserialize), the node will retry the file (up to DatabaseDescriptor.getMaxStreamingRetries())
 *       by sending a RetryMessage to the sender. On receiving a RetryMessage, the sender simply issue a new
 *       FileMessage for that file.
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
public class StreamSession implements IEndpointStateChangeSubscriber, IFailureDetectionEventListener
{
    private static final Logger logger = LoggerFactory.getLogger(StreamSession.class);

    // Executor that establish the streaming connection. Once we're connected to the other end, the rest of the streaming
    // is directly handled by the ConnectionHandler incoming and outgoing threads.
    private static final DebuggableThreadPoolExecutor streamExecutor = DebuggableThreadPoolExecutor.createWithFixedPoolSize("StreamConnectionEstablisher",
                                                                                                                            FBUtilities.getAvailableProcessors());
    public final InetAddress peer;

    // should not be null when session is started
    private StreamResultFuture streamResult;

    // stream requests to send to the peer
    private final List<StreamRequest> requests = new ArrayList<>();
    // streaming tasks are created and managed per ColumnFamily ID
    private final Map<UUID, StreamTransferTask> transfers = new HashMap<>();
    // data receivers, filled after receiving prepare message
    private final Map<UUID, StreamReceiveTask> receivers = new HashMap<>();
    private final StreamingMetrics metrics;

    public final ConnectionHandler handler;

    private int retries;

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
     */
    public StreamSession(InetAddress peer)
    {
        this.peer = peer;
        this.handler = new ConnectionHandler(this);
        this.metrics = StreamingMetrics.get(peer);
    }

    public UUID planId()
    {
        return streamResult == null ? null : streamResult.planId;
    }

    public String description()
    {
        return streamResult == null ? null : streamResult.description;
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

        // register to gossiper/FD to fail on node failure
        Gossiper.instance.register(this);
        FailureDetector.instance.registerFailureDetectionEventListener(this);
    }

    public void start()
    {
        if (requests.isEmpty() && transfers.isEmpty())
        {
            logger.info("[Stream #{}] Session does not have any tasks.", planId());
            closeSession(State.COMPLETE);
            return;
        }

        streamExecutor.execute(new Runnable()
        {
            public void run()
            {
                try
                {
                    handler.initiate();
                    onInitializationComplete();
                }
                catch (IOException e)
                {
                    onError(e);
                }
            }
        });
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
     * @param keyspace Transfer keyspace
     * @param ranges Transfer ranges
     * @param columnFamilies Transfer ColumnFamilies
     */
    public void addTransferRanges(String keyspace, Collection<Range<Token>> ranges, Collection<String> columnFamilies, boolean flushTables)
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

        if (flushTables)
            flushSSTables(stores);

        List<Range<Token>> normalizedRanges = Range.normalize(ranges);
        List<SSTableReader> sstables = Lists.newLinkedList();
        for (ColumnFamilyStore cfStore : stores)
        {
            List<AbstractBounds<RowPosition>> rowBoundsList = Lists.newLinkedList();
            for (Range<Token> range : normalizedRanges)
                rowBoundsList.add(range.toRowBounds());
            ColumnFamilyStore.ViewFragment view = cfStore.markReferenced(rowBoundsList);
            sstables.addAll(view.sstables);
        }
        addTransferFiles(normalizedRanges, sstables);
    }

    /**
     * Set up transfer of the specific SSTables.
     * {@code sstables} must be marked as referenced so that not get deleted until transfer completes.
     *
     * @param ranges Transfer ranges
     * @param sstables Transfer files
     */
    public void addTransferFiles(Collection<Range<Token>> ranges, Collection<SSTableReader> sstables)
    {
        List<SSTableStreamingSections> sstableDetails = new ArrayList<>(sstables.size());
        for (SSTableReader sstable : sstables)
            sstableDetails.add(new SSTableStreamingSections(sstable, sstable.getPositionsForRanges(ranges), sstable.estimatedKeysForRanges(ranges)));

        addTransferFiles(sstableDetails);
    }

    public void addTransferFiles(Collection<SSTableStreamingSections> sstableDetails)
    {
        for (SSTableStreamingSections details : sstableDetails)
        {
            if (details.sections.isEmpty())
            {
                // A reference was acquired on the sstable and we won't stream it
                details.sstable.releaseReference();
                continue;
            }

            UUID cfId = details.sstable.metadata.cfId;
            StreamTransferTask task = transfers.get(cfId);
            if (task == null)
            {
                task = new StreamTransferTask(this, cfId);
                transfers.put(cfId, task);
            }
            task.addTransferFile(details.sstable, details.estimatedKeys, details.sections);
        }
    }

    public static class SSTableStreamingSections
    {
        public final SSTableReader sstable;
        public final List<Pair<Long, Long>> sections;
        public final long estimatedKeys;

        public SSTableStreamingSections(SSTableReader sstable, List<Pair<Long, Long>> sections, long estimatedKeys)
        {
            this.sstable = sstable;
            this.sections = sections;
            this.estimatedKeys = estimatedKeys;
        }
    }

    private void closeSession(State finalState)
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

        Gossiper.instance.unregister(this);
        FailureDetector.instance.unregisterFailureDetectionEventListener(this);
        streamResult.handleSessionComplete(this);
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

            case RETRY:
                RetryMessage retry = (RetryMessage) message;
                retry(retry.cfId, retry.sequenceNumber);
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

    /**
     * Call back for handling exception during streaming.
     *
     * @param e thrown exception
     */
    public void onError(Throwable e)
    {
        logger.error("[Stream #" + planId() + "] Streaming error occurred", e);
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
            addTransferRanges(request.keyspace, request.ranges, request.columnFamilies, true); // always flush on stream request
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
        ProgressInfo progress = new ProgressInfo(peer, desc.filenameFor(Component.DATA), direction, bytes, total);
        streamResult.handleProgress(progress);
    }

    public void received(UUID cfId, int sequenceNumber)
    {
        transfers.get(cfId).complete(sequenceNumber);
    }

    /**
     * Call back on receiving {@code StreamMessage.Type.RETRY} message.
     *
     * @param cfId ColumnFamily ID
     * @param sequenceNumber Sequence number to indicate which file to stream again
     */
    public void retry(UUID cfId, int sequenceNumber)
    {
        OutgoingFileMessage message = transfers.get(cfId).createMessageForRetry(sequenceNumber);
        handler.sendMessage(message);
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
        }
    }

    /**
     * Call back on receiving {@code StreamMessage.Type.SESSION_FAILED} message.
     */
    public synchronized void sessionFailed()
    {
        closeSession(State.FAILED);
    }

    public void doRetry(FileMessageHeader header, Throwable e)
    {
        logger.warn("[Stream #" + planId() + "] Retrying for following error", e);
        // retry
        retries++;
        if (retries > DatabaseDescriptor.getMaxStreamingRetries())
            onError(new IOException("Too many retries for " + header, e));
        else
            handler.sendMessage(new RetryMessage(header.cfId, header.sequenceNumber));
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
        return new SessionInfo(peer, receivingSummaries, transferSummaries, state);
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
        convict(endpoint, Double.MAX_VALUE);
    }

    public void onRestart(InetAddress endpoint, EndpointState epState)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void convict(InetAddress endpoint, double phi)
    {
        if (!endpoint.equals(peer))
            return;

        // We want a higher confidence in the failure detection than usual because failing a streaming wrongly has a high cost.
        if (phi < 2 * DatabaseDescriptor.getPhiConvictThreshold())
            return;

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
