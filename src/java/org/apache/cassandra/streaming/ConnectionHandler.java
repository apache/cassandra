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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.net.IncomingStreamingConnection;
import org.apache.cassandra.streaming.messages.StreamInitMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * ConnectionHandler manages incoming/outgoing message exchange for the {@link StreamSession}.
 *
 * <p>
 * Internally, ConnectionHandler manages thread to receive incoming {@link StreamMessage} and thread to
 * send outgoing message. Messages are encoded/decoded on those thread and handed to
 * {@link StreamSession#messageReceived(org.apache.cassandra.streaming.messages.StreamMessage)}.
 */
public class ConnectionHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectionHandler.class);

    private final StreamSession session;

    private IncomingMessageHandler incoming;
    private OutgoingMessageHandler outgoing;

    ConnectionHandler(StreamSession session, int incomingSocketTimeout)
    {
        this.session = session;
        this.incoming = new IncomingMessageHandler(session, incomingSocketTimeout);
        this.outgoing = new OutgoingMessageHandler(session);
    }

    /**
     * Set up incoming message handler and initiate streaming.
     *
     * This method is called once on initiator.
     *
     * @throws IOException
     */
    @SuppressWarnings("resource")
    public void initiate() throws IOException
    {
        logger.debug("[Stream #{}] Sending stream init for incoming stream", session.planId());
        Socket incomingSocket = session.createConnection();
        incoming.start(incomingSocket, StreamMessage.CURRENT_VERSION, true);

        logger.debug("[Stream #{}] Sending stream init for outgoing stream", session.planId());
        Socket outgoingSocket = session.createConnection();
        outgoing.start(outgoingSocket, StreamMessage.CURRENT_VERSION, true);
    }

    /**
     * Set up outgoing message handler on receiving side.
     *
     * @param connection Incoming connection to use for {@link OutgoingMessageHandler}.
     * @param version Streaming message version
     * @throws IOException
     */
    public void initiateOnReceivingSide(IncomingStreamingConnection connection, boolean isForOutgoing, int version) throws IOException
    {
        if (isForOutgoing)
            outgoing.start(connection, version);
        else
            incoming.start(connection, version);
    }

    public ListenableFuture<?> close()
    {
        logger.debug("[Stream #{}] Closing stream connection handler on {}", session.planId(), session.peer);

        ListenableFuture<?> inClosed = closeIncoming();
        ListenableFuture<?> outClosed = closeOutgoing();

        return Futures.allAsList(inClosed, outClosed);
    }

    public ListenableFuture<?> closeOutgoing()
    {
        return outgoing == null ? Futures.immediateFuture(null) : outgoing.close();
    }

    public ListenableFuture<?> closeIncoming()
    {
        return incoming == null ? Futures.immediateFuture(null) : incoming.close();
    }

    /**
     * Enqueue messages to be sent.
     *
     * @param messages messages to send
     */
    public void sendMessages(Collection<? extends StreamMessage> messages)
    {
        for (StreamMessage message : messages)
            sendMessage(message);
    }

    public void sendMessage(StreamMessage message)
    {
        if (outgoing.isClosed())
            throw new RuntimeException("Outgoing stream handler has been closed");

        outgoing.enqueue(message);
    }

    /**
     * @return true if outgoing connection is opened and ready to send messages
     */
    public boolean isOutgoingConnected()
    {
        return outgoing != null && !outgoing.isClosed();
    }

    abstract static class MessageHandler implements Runnable
    {
        protected final StreamSession session;

        protected int protocolVersion;
        private final boolean isOutgoingHandler;
        protected Socket socket;

        private final AtomicReference<SettableFuture<?>> closeFuture = new AtomicReference<>();
        private IncomingStreamingConnection incomingConnection;

        protected MessageHandler(StreamSession session, boolean isOutgoingHandler)
        {
            this.session = session;
            this.isOutgoingHandler = isOutgoingHandler;
        }

        protected abstract String name();

        @SuppressWarnings("resource")
        protected static DataOutputStreamPlus getWriteChannel(Socket socket) throws IOException
        {
            WritableByteChannel out = socket.getChannel();
            // socket channel is null when encrypted(SSL)
            if (out == null)
                return new WrappedDataOutputStreamPlus(new BufferedOutputStream(socket.getOutputStream()));
            return new BufferedDataOutputStreamPlus(out);
        }

        protected static ReadableByteChannel getReadChannel(Socket socket) throws IOException
        {
            //we do this instead of socket.getChannel() so socketSoTimeout is respected
            return Channels.newChannel(socket.getInputStream());
        }

        @SuppressWarnings("resource")
        private void sendInitMessage() throws IOException
        {
            StreamInitMessage message = new StreamInitMessage(
                    FBUtilities.getBroadcastAddress(),
                    session.sessionIndex(),
                    session.planId(),
                    session.description(),
                    !isOutgoingHandler,
                    session.keepSSTableLevel(),
                    session.isIncremental());
            ByteBuffer messageBuf = message.createMessage(false, protocolVersion);
            DataOutputStreamPlus out = getWriteChannel(socket);
            out.write(messageBuf);
            out.flush();
        }

        public void start(IncomingStreamingConnection connection, int protocolVersion) throws IOException
        {
            this.incomingConnection = connection;
            start(connection.socket, protocolVersion, false);
        }

        public void start(Socket socket, int protocolVersion, boolean initiator) throws IOException
        {
            this.socket = socket;
            this.protocolVersion = protocolVersion;
            if (initiator)
                sendInitMessage();

            new FastThreadLocalThread(this, name() + "-" + socket.getRemoteSocketAddress()).start();
        }

        public ListenableFuture<?> close()
        {
            // Assume it wasn't closed. Not a huge deal if we create a future on a race
            SettableFuture<?> future = SettableFuture.create();
            return closeFuture.compareAndSet(null, future)
                 ? future
                 : closeFuture.get();
        }

        public boolean isClosed()
        {
            return closeFuture.get() != null;
        }

        protected void signalCloseDone()
        {
            if (!isClosed())
                close();

            closeFuture.get().set(null);

            // We can now close the socket
            if (incomingConnection != null)
            {
                //this will close the underlying socket and remove it
                //from active MessagingService connections (CASSANDRA-11854)
                incomingConnection.close();
            }
            else
            {
                //this is an outgoing connection not registered in the MessagingService
                //so we can close the socket directly
                try
                {
                    socket.close();
                }
                catch (IOException e)
                {
                    // Erroring out while closing shouldn't happen but is not really a big deal, so just log
                    // it at DEBUG and ignore otherwise.
                    logger.debug("Unexpected error while closing streaming connection", e);
                }
            }
        }
    }

    /**
     * Incoming streaming message handler
     */
    static class IncomingMessageHandler extends MessageHandler
    {
        private final int socketTimeout;

        IncomingMessageHandler(StreamSession session, int socketTimeout)
        {
            super(session, false);
            this.socketTimeout = socketTimeout;
        }

        @Override
        public void start(Socket socket, int version, boolean initiator) throws IOException
        {
            try
            {
                socket.setSoTimeout(socketTimeout);
            }
            catch (SocketException e)
            {
                logger.warn("Could not set incoming socket timeout to {}", socketTimeout, e);
            }
            super.start(socket, version, initiator);
        }

        protected String name()
        {
            return "STREAM-IN";
        }

        @SuppressWarnings("resource")
        public void run()
        {
            try
            {
                ReadableByteChannel in = getReadChannel(socket);
                while (!isClosed())
                {
                    // receive message
                    StreamMessage message = StreamMessage.deserialize(in, protocolVersion, session);
                    logger.debug("[Stream #{}] Received {}", session.planId(), message);
                    // Might be null if there is an error during streaming (see FileMessage.deserialize). It's ok
                    // to ignore here since we'll have asked for a retry.
                    if (message != null)
                    {
                        session.messageReceived(message);
                    }
                }
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                session.onError(t);
            }
            finally
            {
                signalCloseDone();
            }
        }
    }

    /**
     * Outgoing file transfer thread
     */
    static class OutgoingMessageHandler extends MessageHandler
    {
        /*
         * All out going messages are queued up into messageQueue.
         * The size will grow when received streaming request.
         *
         * Queue is also PriorityQueue so that prior messages can go out fast.
         */
        private final PriorityBlockingQueue<StreamMessage> messageQueue = new PriorityBlockingQueue<>(64, new Comparator<StreamMessage>()
        {
            public int compare(StreamMessage o1, StreamMessage o2)
            {
                return o2.getPriority() - o1.getPriority();
            }
        });

        OutgoingMessageHandler(StreamSession session)
        {
            super(session, true);
        }

        protected String name()
        {
            return "STREAM-OUT";
        }

        public void enqueue(StreamMessage message)
        {
            messageQueue.put(message);
        }

        @SuppressWarnings("resource")
        public void run()
        {
            try
            {
                DataOutputStreamPlus out = getWriteChannel(socket);

                StreamMessage next;
                while (!isClosed())
                {
                    if ((next = messageQueue.poll(1, TimeUnit.SECONDS)) != null)
                    {
                        logger.debug("[Stream #{}] Sending {}", session.planId(), next);
                        sendMessage(out, next);
                        if (next.type == StreamMessage.Type.SESSION_FAILED)
                            close();
                    }
                }

                // Sends the last messages on the queue
                while ((next = messageQueue.poll()) != null)
                    sendMessage(out, next);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            catch (Throwable e)
            {
                session.onError(e);
            }
            finally
            {
                signalCloseDone();
            }
        }

        private void sendMessage(DataOutputStreamPlus out, StreamMessage message)
        {
            try
            {
                StreamMessage.serialize(message, out, protocolVersion, session);
                out.flush();
                message.sent();
            }
            catch (SocketException e)
            {
                session.onError(e);
                close();
            }
            catch (IOException e)
            {
                session.onError(e);
            }
        }
    }
}
