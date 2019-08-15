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
package org.apache.cassandra.net;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Checksum;

import javax.net.ssl.SSLHandshakeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocalThread;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.Coalescable;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;
import org.apache.cassandra.utils.UUIDGen;
import org.xerial.snappy.SnappyOutputStream;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;

public class OutboundTcpConnection extends FastThreadLocalThread
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundTcpConnection.class);

    private static final String PREFIX = Config.PROPERTY_PREFIX;

    /*
     * Enabled/disable TCP_NODELAY for intradc connections. Defaults to enabled.
     */
    private static final String INTRADC_TCP_NODELAY_PROPERTY = PREFIX + "otc_intradc_tcp_nodelay";
    private static final boolean INTRADC_TCP_NODELAY = Boolean.parseBoolean(System.getProperty(INTRADC_TCP_NODELAY_PROPERTY, "true"));

    /*
     * Size of buffer in output stream
     */
    private static final String BUFFER_SIZE_PROPERTY = PREFIX + "otc_buffer_size";
    private static final int BUFFER_SIZE = Integer.getInteger(BUFFER_SIZE_PROPERTY, 1024 * 64);

    public static final int MAX_COALESCED_MESSAGES = 128;

    private static CoalescingStrategy newCoalescingStrategy(String displayName)
    {
        return CoalescingStrategies.newCoalescingStrategy(DatabaseDescriptor.getOtcCoalescingStrategy(),
                                                          DatabaseDescriptor.getOtcCoalescingWindow(),
                                                          logger,
                                                          displayName);
    }

    static
    {
        String strategy = DatabaseDescriptor.getOtcCoalescingStrategy();
        switch (strategy)
        {
        case "TIMEHORIZON":
            break;
        case "MOVINGAVERAGE":
        case "FIXED":
        case "DISABLED":
            logger.info("OutboundTcpConnection using coalescing strategy {}", strategy);
            break;
            default:
                //Check that it can be loaded
                newCoalescingStrategy("dummy");
        }

        int coalescingWindow = DatabaseDescriptor.getOtcCoalescingWindow();
        if (coalescingWindow != Config.otc_coalescing_window_us_default)
            logger.info("OutboundTcpConnection coalescing window set to {}μs", coalescingWindow);

        if (coalescingWindow < 0)
            throw new ExceptionInInitializerError(
                    "Value provided for coalescing window must be greater than 0: " + coalescingWindow);

        int otc_backlog_expiration_interval_in_ms = DatabaseDescriptor.getOtcBacklogExpirationInterval();
        if (otc_backlog_expiration_interval_in_ms != Config.otc_backlog_expiration_interval_ms_default)
            logger.info("OutboundTcpConnection backlog expiration interval set to to {}ms", otc_backlog_expiration_interval_in_ms);
    }

    private static final MessageOut<?> CLOSE_SENTINEL = new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE);
    private volatile boolean isStopped = false;

    private static final int OPEN_RETRY_DELAY = 100; // ms between retries
    public static final int WAIT_FOR_VERSION_MAX_TIME = 5000;
    private static final int NO_VERSION = Integer.MIN_VALUE;

    static final int LZ4_HASH_SEED = 0x9747b28c;

    private final BlockingQueue<QueuedMessage> backlog = new LinkedBlockingQueue<>();
    private static final String BACKLOG_PURGE_SIZE_PROPERTY = PREFIX + "otc_backlog_purge_size";
    @VisibleForTesting
    static final int BACKLOG_PURGE_SIZE = Integer.getInteger(BACKLOG_PURGE_SIZE_PROPERTY, 1024);
    private final AtomicBoolean backlogExpirationActive = new AtomicBoolean(false);
    private volatile long backlogNextExpirationTime;

    private final OutboundTcpConnectionPool poolReference;

    private final CoalescingStrategy cs;
    private DataOutputStreamPlus out;
    private Socket socket;
    private volatile long completed;
    private final AtomicLong dropped = new AtomicLong();
    private volatile int currentMsgBufferCount = 0;
    private volatile int targetVersion;

    public OutboundTcpConnection(OutboundTcpConnectionPool pool, String name)
    {
        super("MessagingService-Outgoing-" + pool.endPoint() + "-" + name);
        this.poolReference = pool;
        cs = newCoalescingStrategy(pool.endPoint().getHostAddress());

        // We want to use the most precise version we know because while there is version detection on connect(),
        // the target version might be accessed by the pool (in getConnection()) before we actually connect (as we
        // connect when the first message is submitted). Note however that the only case where we'll connect
        // without knowing the true version of a node is if that node is a seed (otherwise, we can't know a node
        // unless it has been gossiped to us or it has connected to us and in both case this sets the version) and
        // in that case we won't rely on that targetVersion before we're actually connected and so the version
        // detection in connect() will do its job.
        targetVersion = MessagingService.instance().getVersion(pool.endPoint());
    }

    private static boolean isLocalDC(InetAddress targetHost)
    {
        String remoteDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(targetHost);
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
        return remoteDC.equals(localDC);
    }

    public void enqueue(MessageOut<?> message, int id)
    {
        long nanoTime = System.nanoTime();
        expireMessages(nanoTime);
        try
        {
            backlog.put(new QueuedMessage(message, id, nanoTime));
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    /**
     * This is a helper method for unit testing. Disclaimer: Do not use this method outside unit tests, as
     * this method is iterating the queue which can be an expensive operation (CPU time, queue locking).
     * 
     * @return true, if the queue contains at least one expired element
     */
    @VisibleForTesting // (otherwise = VisibleForTesting.NONE)
    boolean backlogContainsExpiredMessages(long nowNanos)
    {
        return backlog.stream().anyMatch(entry -> entry.isTimedOut(nowNanos));
    }

    void closeSocket(boolean destroyThread)
    {
        logger.debug("Enqueuing socket close for {}", poolReference.endPoint());
        isStopped = destroyThread; // Exit loop to stop the thread
        backlog.clear();
        // in the "destroyThread = true" case, enqueuing the sentinel is important mostly to unblock the backlog.take()
        // (via the CoalescingStrategy) in case there's a data race between this method enqueuing the sentinel
        // and run() clearing the backlog on connection failure.
        enqueue(CLOSE_SENTINEL, -1);
    }

    void softCloseSocket()
    {
        enqueue(CLOSE_SENTINEL, -1);
    }

    public int getTargetVersion()
    {
        return targetVersion;
    }

    public void run()
    {
        final int drainedMessageSize = MAX_COALESCED_MESSAGES;
        // keeping list (batch) size small for now; that way we don't have an unbounded array (that we never resize)
        final List<QueuedMessage> drainedMessages = new ArrayList<>(drainedMessageSize);

        outer:
        while (!isStopped)
        {
            try
            {
                cs.coalesce(backlog, drainedMessages, drainedMessageSize);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }

            int count = currentMsgBufferCount = drainedMessages.size();

            //The timestamp of the first message has already been provided to the coalescing strategy
            //so skip logging it.
            inner:
            for (QueuedMessage qm : drainedMessages)
            {
                try
                {
                    MessageOut<?> m = qm.message;
                    if (m == CLOSE_SENTINEL)
                    {
                        disconnect();
                        if (isStopped)
                            break outer;
                        continue;
                    }

                    if (qm.isTimedOut(System.nanoTime()))
                        dropped.incrementAndGet();
                    else if (socket != null || connect())
                        writeConnected(qm, count == 1 && backlog.isEmpty());
                    else
                    {
                        // Not connected! Clear out the queue, else gossip messages back up. Update dropped
                        // statistics accordingly. Hint: The statistics may be slightly too low, if messages
                        // are added between the calls of backlog.size() and backlog.clear()
                        dropped.addAndGet(backlog.size());
                        backlog.clear();
                        currentMsgBufferCount = 0;
                        break inner;
                    }
                }
                catch (Exception e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    // really shouldn't get here, as exception handling in writeConnected() is reasonably robust
                    // but we want to catch anything bad we don't drop the messages in the current batch
                    logger.error("error processing a message intended for {}", poolReference.endPoint(), e);
                }
                currentMsgBufferCount = --count;
            }
            // Update dropped statistics by the number of unprocessed drainedMessages
            dropped.addAndGet(currentMsgBufferCount);
            drainedMessages.clear();
        }
    }

    public int getPendingMessages()
    {
        return backlog.size() + currentMsgBufferCount;
    }

    public long getCompletedMesssages()
    {
        return completed;
    }

    public long getDroppedMessages()
    {
        return dropped.get();
    }

    private boolean shouldCompressConnection()
    {
        // assumes version >= 1.2
        return DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.all
               || (DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.dc && !isLocalDC(poolReference.endPoint()));
    }

    private void writeConnected(QueuedMessage qm, boolean flush)
    {
        try
        {
            byte[] sessionBytes = qm.message.parameters.get(Tracing.TRACE_HEADER);
            if (sessionBytes != null)
            {
                UUID sessionId = UUIDGen.getUUID(ByteBuffer.wrap(sessionBytes));
                TraceState state = Tracing.instance.get(sessionId);
                String message = String.format("Sending %s message to %s", qm.message.verb, poolReference.endPoint());
                // session may have already finished; see CASSANDRA-5668
                if (state == null)
                {
                    byte[] traceTypeBytes = qm.message.parameters.get(Tracing.TRACE_TYPE);
                    Tracing.TraceType traceType = traceTypeBytes == null ? Tracing.TraceType.QUERY : Tracing.TraceType.deserialize(traceTypeBytes[0]);
                    Tracing.instance.trace(ByteBuffer.wrap(sessionBytes), message, traceType.getTTL());
                }
                else
                {
                    state.trace(message);
                    if (qm.message.verb == MessagingService.Verb.REQUEST_RESPONSE)
                        Tracing.instance.doneWithNonLocalSession(state);
                }
            }

            long timestampMillis = NanoTimeToCurrentTimeMillis.convert(qm.timestampNanos);
            writeInternal(qm.message, qm.id, timestampMillis);

            completed++;
            if (flush)
                out.flush();
        }
        catch (Throwable e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            disconnect();
            if (e instanceof IOException || e.getCause() instanceof IOException)
            {
                logger.debug("Error writing to {}", poolReference.endPoint(), e);

                // If we haven't retried this message yet, put it back on the queue to retry after re-connecting.
                // See CASSANDRA-5393 and CASSANDRA-12192.
                if (qm.shouldRetry())
                {
                    try
                    {
                        backlog.put(new RetriedQueuedMessage(qm));
                    }
                    catch (InterruptedException e1)
                    {
                        throw new AssertionError(e1);
                    }
                }
            }
            else
            {
                // Non IO exceptions are likely a programming error so let's not silence them
                logger.error("error writing to {}", poolReference.endPoint(), e);
            }
        }
    }

    private void writeInternal(MessageOut<?> message, int id, long timestamp) throws IOException
    {
        out.writeInt(MessagingService.PROTOCOL_MAGIC);

        if (targetVersion < MessagingService.VERSION_20)
            out.writeUTF(String.valueOf(id));
        else
            out.writeInt(id);

        // int cast cuts off the high-order half of the timestamp, which we can assume remains
        // the same between now and when the recipient reconstructs it.
        out.writeInt((int) timestamp);
        message.serialize(out, targetVersion);
    }

    private static void writeHeader(DataOutput out, int version, boolean compressionEnabled) throws IOException
    {
        // 2 bits: unused.  used to be "serializer type," which was always Binary
        // 1 bit: compression
        // 1 bit: streaming mode
        // 3 bits: unused
        // 8 bits: version
        // 15 bits: unused
        int header = 0;
        if (compressionEnabled)
            header |= 4;
        header |= (version << 8);
        out.writeInt(header);
    }

    private void disconnect()
    {
        if (socket != null)
        {
            try
            {
                socket.close();
                logger.debug("Socket to {} closed", poolReference.endPoint());
            }
            catch (IOException e)
            {
                logger.debug("Exception closing connection to {}", poolReference.endPoint(), e);
            }
            out = null;
            socket = null;
        }
    }

    @SuppressWarnings("resource")
    private boolean connect()
    {
        logger.debug("Attempting to connect to {}", poolReference.endPoint());

        long start = System.nanoTime();
        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getRpcTimeout());
        while (System.nanoTime() - start < timeout && !isStopped)
        {
            targetVersion = MessagingService.instance().getVersion(poolReference.endPoint());
            try
            {
                socket = poolReference.newSocket();
                socket.setKeepAlive(true);
                if (isLocalDC(poolReference.endPoint()))
                {
                    socket.setTcpNoDelay(INTRADC_TCP_NODELAY);
                }
                else
                {
                    socket.setTcpNoDelay(DatabaseDescriptor.getInterDCTcpNoDelay());
                }
                if (DatabaseDescriptor.getInternodeSendBufferSize() > 0)
                {
                    try
                    {
                        socket.setSendBufferSize(DatabaseDescriptor.getInternodeSendBufferSize());
                    }
                    catch (SocketException se)
                    {
                        logger.warn("Failed to set send buffer size on internode socket.", se);
                    }
                }

                // SocketChannel may be null when using SSL
                WritableByteChannel ch = socket.getChannel();
                out = new BufferedDataOutputStreamPlus(ch != null ? ch : Channels.newChannel(socket.getOutputStream()), BUFFER_SIZE);

                out.writeInt(MessagingService.PROTOCOL_MAGIC);
                writeHeader(out, targetVersion, shouldCompressConnection());
                out.flush();

                DataInputStream in = new DataInputStream(socket.getInputStream());
                int maxTargetVersion = handshakeVersion(in);
                if (maxTargetVersion == NO_VERSION)
                {
                    // no version is returned, so disconnect an try again: we will either get
                    // a different target version (targetVersion < MessagingService.VERSION_12)
                    // or if the same version the handshake will finally succeed
                    logger.trace("Target max version is {}; no version information yet, will retry", maxTargetVersion);
                    disconnect();
                    continue;
                }
                else
                {
                    MessagingService.instance().setVersion(poolReference.endPoint(), maxTargetVersion);
                }

                if (targetVersion > maxTargetVersion)
                {
                    logger.trace("Target max version is {}; will reconnect with that version", maxTargetVersion);
                    try
                    {
                        if (DatabaseDescriptor.getSeeds().contains(poolReference.endPoint()))
                            logger.warn("Seed gossip version is {}; will not connect with that version", maxTargetVersion);
                    }
                    catch (Throwable e)
                    {
                        // If invalid yaml has been added to the config since startup, getSeeds() will throw an AssertionError
                        // Additionally, third party seed providers may throw exceptions if network is flakey
                        // Regardless of what's thrown, we must catch it, disconnect, and try again
                        JVMStabilityInspector.inspectThrowable(e);
                        logger.warn("Configuration error prevented outbound connection: {}", e.getLocalizedMessage());
                    }
                    finally
                    {
                        disconnect();
                        return false;
                    }
                }

                if (targetVersion < maxTargetVersion && targetVersion < MessagingService.current_version)
                {
                    logger.trace("Detected higher max version {} (using {}); will reconnect when queued messages are done",
                                 maxTargetVersion, targetVersion);
                    softCloseSocket();
                }

                out.writeInt(MessagingService.current_version);
                CompactEndpointSerializationHelper.serialize(FBUtilities.getBroadcastAddress(), out);
                if (shouldCompressConnection())
                {
                    out.flush();
                    logger.trace("Upgrading OutputStream to {} to be compressed", poolReference.endPoint());
                    if (targetVersion < MessagingService.VERSION_21)
                    {
                        // Snappy is buffered, so no need for extra buffering output stream
                        out = new WrappedDataOutputStreamPlus(new SnappyOutputStream(socket.getOutputStream()));
                    }
                    else
                    {
                        // TODO: custom LZ4 OS that supports BB write methods
                        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
                        Checksum checksum = XXHashFactory.fastestInstance().newStreamingHash32(LZ4_HASH_SEED).asChecksum();
                        out = new WrappedDataOutputStreamPlus(new LZ4BlockOutputStream(socket.getOutputStream(),
                                                                            1 << 14,  // 16k block size
                                                                            compressor,
                                                                            checksum,
                                                                            true)); // no async flushing
                    }
                }
                logger.debug("Done connecting to {}", poolReference.endPoint());
                return true;
            }
            catch (SSLHandshakeException e)
            {
                logger.error("SSL handshake error for outbound connection to " + socket, e);
                disconnect();
                // SSL errors won't be recoverable within timeout period so we'll just abort
                return false;
            }
            catch (IOException e)
            {
                disconnect();
                logger.debug("Unable to connect to {}", poolReference.endPoint(), e);
                Uninterruptibles.sleepUninterruptibly(OPEN_RETRY_DELAY, TimeUnit.MILLISECONDS);
            }
        }
        return false;
    }

    private int handshakeVersion(final DataInputStream inputStream)
    {
        final AtomicInteger version = new AtomicInteger(NO_VERSION);
        final CountDownLatch versionLatch = new CountDownLatch(1);
        NamedThreadFactory.createThread(() ->
        {
            try
            {
                logger.info("Handshaking version with {}", poolReference.endPoint());
                version.set(inputStream.readInt());
            }
            catch (IOException ex)
            {
                final String msg = "Cannot handshake version with " + poolReference.endPoint();
                if (logger.isTraceEnabled())
                    logger.trace(msg, ex);
                else
                    logger.info(msg);
            }
            finally
            {
                //unblock the waiting thread on either success or fail
                versionLatch.countDown();
            }
        }, "HANDSHAKE-" + poolReference.endPoint()).start();

        try
        {
            versionLatch.await(WAIT_FOR_VERSION_MAX_TIME, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }
        return version.get();
    }

    /**
     * Expire elements from the queue if the queue is pretty full and expiration is not already in progress.
     * This method will only remove droppable expired entries. If no such element exists, nothing is removed from the queue.
     * 
     * @param timestampNanos The current time as from System.nanoTime()
     */
    @VisibleForTesting
    void expireMessages(long timestampNanos)
    {
        if (backlog.size() <= BACKLOG_PURGE_SIZE)
            return; // Plenty of space

        if (backlogNextExpirationTime - timestampNanos > 0)
            return; // Expiration is not due.

        /**
         * Expiration is an expensive process. Iterating the queue locks the queue for both writes and
         * reads during iter.next() and iter.remove(). Thus letting only a single Thread do expiration.
         */
        if (backlogExpirationActive.compareAndSet(false, true))
        {
            try
            {
                Iterator<QueuedMessage> iter = backlog.iterator();
                while (iter.hasNext())
                {
                    QueuedMessage qm = iter.next();
                    if (!qm.droppable)
                        continue;
                    if (!qm.isTimedOut(timestampNanos))
                        continue;
                    iter.remove();
                    dropped.incrementAndGet();
                }

                if (logger.isTraceEnabled())
                {
                    long duration = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - timestampNanos);
                    logger.trace("Expiration of {} took {}μs", getName(), duration);
                }
            }
            finally
            {
                long backlogExpirationIntervalNanos = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getOtcBacklogExpirationInterval());
                backlogNextExpirationTime = timestampNanos + backlogExpirationIntervalNanos;
                backlogExpirationActive.set(false);
            }
        }
    }

    /** messages that have not been retried yet */
    private static class QueuedMessage implements Coalescable
    {
        final MessageOut<?> message;
        final int id;
        final long timestampNanos;
        final boolean droppable;

        QueuedMessage(MessageOut<?> message, int id, long timestampNanos)
        {
            this.message = message;
            this.id = id;
            this.timestampNanos = timestampNanos;
            this.droppable = MessagingService.DROPPABLE_VERBS.contains(message.verb);
        }

        /** don't drop a non-droppable message just because it's timestamp is expired */
        boolean isTimedOut(long nowNanos)
        {
            long messageTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(message.getTimeout());
            return droppable && nowNanos - timestampNanos  > messageTimeoutNanos;
        }

        boolean shouldRetry()
        {
            // retry all messages once
            return true;
        }

        public long timestampNanos()
        {
            return timestampNanos;
        }
    }

    private static class RetriedQueuedMessage extends QueuedMessage
    {
        RetriedQueuedMessage(QueuedMessage msg)
        {
            super(msg.message, msg.id, msg.timestampNanos);
        }

        boolean shouldRetry()
        {
            return false;
        }
    }
}
