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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.xerial.snappy.SnappyOutputStream;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

public class OutboundTcpConnection extends Thread
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundTcpConnection.class);

    private static final MessageOut CLOSE_SENTINEL = new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE);
    private volatile boolean isStopped = false;

    private static final int OPEN_RETRY_DELAY = 100; // ms between retries

    // sending thread reads from "active" (one of queue1, queue2) until it is empty.
    // then it swaps it with "backlog."
    private volatile BlockingQueue<QueuedMessage> backlog = new LinkedBlockingQueue<QueuedMessage>();
    private volatile BlockingQueue<QueuedMessage> active = new LinkedBlockingQueue<QueuedMessage>();

    private final OutboundTcpConnectionPool poolReference;

    private DataOutputStream out;
    private Socket socket;
    private volatile long completed;
    private final AtomicLong dropped = new AtomicLong();
    private int targetVersion;

    public OutboundTcpConnection(OutboundTcpConnectionPool pool)
    {
        super("WRITE-" + pool.endPoint());
        this.poolReference = pool;
    }

    private static boolean isLocalDC(InetAddress targetHost)
    {
        String remoteDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(targetHost);
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
        return remoteDC.equals(localDC);
    }

    public void enqueue(MessageOut<?> message, String id)
    {
        expireMessages();
        try
        {
            backlog.put(new QueuedMessage(message, id, System.currentTimeMillis()));
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    void closeSocket(boolean destroyThread)
    {
        active.clear();
        backlog.clear();
        isStopped = destroyThread; // Exit loop to stop the thread
        enqueue(CLOSE_SENTINEL, null);
    }

    void softCloseSocket()
    {
        enqueue(CLOSE_SENTINEL, null);
    }

    public int getTargetVersion()
    {
        return targetVersion;
    }

    public void run()
    {
        while (true)
        {
            QueuedMessage qm = active.poll();
            if (qm == null)
            {
                // exhausted the active queue.  switch to backlog, once there's something to process there
                try
                {
                    qm = backlog.take();
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }

                BlockingQueue<QueuedMessage> tmp = backlog;
                backlog = active;
                active = tmp;
            }

            MessageOut<?> m = qm.message;
            if (m == CLOSE_SENTINEL)
            {
                disconnect();
                if (isStopped)
                    break;
                continue;
            }
            if (qm.timestamp < System.currentTimeMillis() - m.getTimeout())
                dropped.incrementAndGet();
            else if (socket != null || connect())
                writeConnected(qm);
            else
                // clear out the queue, else gossip messages back up.
                active.clear();
        }
    }

    public int getPendingMessages()
    {
        return active.size() + backlog.size();
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

    private void writeConnected(QueuedMessage qm)
    {
        try
        {
            byte[] sessionBytes = qm.message.parameters.get(Tracing.TRACE_HEADER);
            if (sessionBytes != null)
            {
                UUID sessionId = UUIDGen.getUUID(ByteBuffer.wrap(sessionBytes));
                TraceState state = Tracing.instance().get(sessionId);
                state.trace("Sending message to {}", poolReference.endPoint());
                Tracing.instance().stopIfNonLocal(state);
            }

            write(qm.message, qm.id, qm.timestamp, out, targetVersion);
            completed++;
            if (active.peek() == null)
            {
                out.flush();
            }
        }
        catch (Exception e)
        {
            // Non IO exceptions is likely a programming error so let's not silence it
            if (!(e instanceof IOException))
                logger.error("error writing to " + poolReference.endPoint(), e);
            else if (logger.isDebugEnabled())
                logger.debug("error writing to " + poolReference.endPoint(), e);
            disconnect();
        }
    }

    public static void write(MessageOut message, String id, long timestamp, DataOutputStream out, int version) throws IOException
    {
        out.writeInt(MessagingService.PROTOCOL_MAGIC);
        if (version < MessagingService.VERSION_12)
        {
            writeHeader(out, version, false);
            // 0.8 included a total message size int.  1.0 doesn't need it but expects it to be there.
            out.writeInt(-1);
        }

        out.writeUTF(id);
        if (version >= MessagingService.VERSION_12)
        {
            // int cast cuts off the high-order half of the timestamp, which we can assume remains
            // the same between now and when the recipient reconstructs it.
            out.writeInt((int) timestamp);
        }
        message.serialize(out, version);
    }

    private static void writeHeader(DataOutputStream out, int version, boolean compressionEnabled) throws IOException
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
            }
            catch (IOException e)
            {
                if (logger.isTraceEnabled())
                    logger.trace("exception closing connection to " + poolReference.endPoint(), e);
            }
            out = null;
            socket = null;
        }
    }

    private boolean connect()
    {
        if (logger.isDebugEnabled())
            logger.debug("attempting to connect to " + poolReference.endPoint());

        targetVersion = MessagingService.instance().getVersion(poolReference.endPoint());

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() < start + DatabaseDescriptor.getRpcTimeout())
        {
            try
            {
                socket = poolReference.newSocket();
                socket.setKeepAlive(true);
                if (isLocalDC(poolReference.endPoint()))
                {
                    socket.setTcpNoDelay(true);
                }
                else
                {
                    socket.setTcpNoDelay(DatabaseDescriptor.getInterDCTcpNoDelay());
                }
                if (DatabaseDescriptor.getInternodeSendBufferSize() != null)
                {
                    try
                    {
                        socket.setSendBufferSize(DatabaseDescriptor.getInternodeSendBufferSize().intValue());
                    }
                    catch (SocketException se)
                    {
                        logger.warn("Failed to set send buffer size on internode socket.", se);
                    }
                }
                out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 4096));

                if (targetVersion >= MessagingService.VERSION_12)
                {
                    out.writeInt(MessagingService.PROTOCOL_MAGIC);
                    writeHeader(out, targetVersion, shouldCompressConnection());
                    out.flush();

                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    int maxTargetVersion = in.readInt();
                    if (targetVersion > maxTargetVersion)
                    {
                        logger.debug("Target max version is {}; will reconnect with that version", maxTargetVersion);
                        MessagingService.instance().setVersion(poolReference.endPoint(), maxTargetVersion);
                        disconnect();
                        return false;
                    }

                    if (targetVersion < maxTargetVersion && targetVersion < MessagingService.current_version)
                    {
                        logger.trace("Detected higher max version {} (using {}); will reconnect when queued messages are done",
                                     maxTargetVersion, targetVersion);
                        MessagingService.instance().setVersion(poolReference.endPoint(), Math.min(MessagingService.current_version, maxTargetVersion));
                        softCloseSocket();
                    }

                    out.writeInt(MessagingService.current_version);
                    CompactEndpointSerializationHelper.serialize(FBUtilities.getBroadcastAddress(), out);
                    if (shouldCompressConnection())
                    {
                        out.flush();
                        logger.trace("Upgrading OutputStream to be compressed");
                        out = new DataOutputStream(new SnappyOutputStream(new BufferedOutputStream(socket.getOutputStream())));
                    }
                }

                return true;
            }
            catch (IOException e)
            {
                socket = null;
                if (logger.isTraceEnabled())
                    logger.trace("unable to connect to " + poolReference.endPoint(), e);
                try
                {
                    Thread.sleep(OPEN_RETRY_DELAY);
                }
                catch (InterruptedException e1)
                {
                    throw new AssertionError(e1);
                }
            }
        }
        return false;
    }

    private void expireMessages()
    {
        while (true)
        {
            QueuedMessage qm = backlog.peek();
            if (qm == null || qm.timestamp >= System.currentTimeMillis() - qm.message.getTimeout())
                break;

            QueuedMessage qm2 = backlog.poll();
            if (qm2 != qm)
            {
                // sending thread switched queues.  add this entry (from the "new" backlog)
                // at the end of the active queue, which keeps it in the same position relative to the other entries
                // without having to contend with other clients for the head-of-backlog lock.
                if (qm2 != null)
                    active.add(qm2);
                break;
            }

            dropped.incrementAndGet();
        }
    }

    private static class QueuedMessage
    {
        final MessageOut<?> message;
        final String id;
        final long timestamp;

        QueuedMessage(MessageOut<?> message, String id, long timestamp)
        {
            this.message = message;
            this.id = id;
            this.timestamp = timestamp;
        }
    }
}
