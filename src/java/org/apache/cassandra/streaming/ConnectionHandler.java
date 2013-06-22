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
import java.net.SocketException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.messages.StreamInitMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.FBUtilities;

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

    private static final int MAX_CONNECT_ATTEMPTS = 3;

    private final StreamSession session;

    private IncomingMessageHandler incoming;
    private OutgoingMessageHandler outgoing;

    ConnectionHandler(StreamSession session)
    {
        this.session = session;
    }

    public ConnectionHandler initiate() throws IOException
    {
        // Connect to other side and use that as the outgoing socket. Once the receiving
        // peer send back his init message, we'll have our incoming handling.
        outgoing = new OutgoingMessageHandler(session, connect(session.peer), StreamMessage.CURRENT_VERSION);

        logger.debug("Sending stream init... for {}", session.planId());
        outgoing.sendInitMessage(true);
        outgoing.start();

        return this;
    }

    public ConnectionHandler initiateOnReceivingSide(Socket incomingSocket, int version) throws IOException
    {
        // Create and start the incoming handler
        incoming = new IncomingMessageHandler(session, incomingSocket, version);
        incoming.start();

        // Connect back to the other side, and use that new socket for the outgoing handler
        outgoing = new OutgoingMessageHandler(session, connect(session.peer), version);

        logger.debug("Sending stream init back to initiator...");
        outgoing.sendInitMessage(false);
        outgoing.start();
        return this;
    }

    public void attachIncomingSocket(Socket incomingSocket, int version) throws IOException
    {
        incoming = new IncomingMessageHandler(session, incomingSocket, version);
        incoming.start();
    }

    /**
     * Connect to peer and start exchanging message.
     * When connect attempt fails, this retries for maximum of MAX_CONNECT_ATTEMPTS times.
     *
     * @param peer the peer to connect to.
     * @return the created socket.
     *
     * @throws IOException when connection failed.
     */
    private static Socket connect(InetAddress peer) throws IOException
    {
        int attempts = 0;
        while (true)
        {
            try
            {
                logger.info("Connecting to {} for streaming", peer);
                Socket socket = MessagingService.instance().getConnectionPool(peer).newSocket();
                socket.setSoTimeout(DatabaseDescriptor.getStreamingSocketTimeout());
                return socket;
            }
            catch (IOException e)
            {
                if (++attempts >= MAX_CONNECT_ATTEMPTS)
                    throw e;

                long waitms = DatabaseDescriptor.getRpcTimeout() * (long)Math.pow(2, attempts);
                logger.warn("Failed attempt " + attempts + " to connect to " + peer + ". Retrying in " + waitms + " ms. (" + e + ")");
                try
                {
                    Thread.sleep(waitms);
                }
                catch (InterruptedException wtf)
                {
                    throw new IOException("interrupted", wtf);
                }
            }
        }
    }

    public ListenableFuture<?> close()
    {
        logger.debug("Closing stream connection handler on {}", session.peer);

        ListenableFuture<?> inClosed = incoming == null ? Futures.immediateFuture(null) : incoming.close();
        ListenableFuture<?> outClosed = outgoing == null ? Futures.immediateFuture(null) : outgoing.close();

        return Futures.allAsList(inClosed, outClosed);
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

    abstract static class MessageHandler implements Runnable
    {
        protected final StreamSession session;

        protected final Socket socket;
        protected final int protocolVersion;

        private final AtomicReference<SettableFuture<?>> closeFuture = new AtomicReference<>();

        protected MessageHandler(StreamSession session, Socket socket, int protocolVersion)
        {
            this.session = session;
            this.socket = socket;
            this.protocolVersion = protocolVersion;
        }

        protected abstract String name();

        protected WritableByteChannel getWriteChannel() throws IOException
        {
            WritableByteChannel out = socket.getChannel();
            // socket channel is null when encrypted(SSL)
            return out == null
                 ? Channels.newChannel(socket.getOutputStream())
                 : out;
        }

        protected ReadableByteChannel getReadChannel() throws IOException
        {
            ReadableByteChannel in = socket.getChannel();
            // socket channel is null when encrypted(SSL)
            return in == null
                 ? Channels.newChannel(socket.getInputStream())
                 : in;
        }

        public void sendInitMessage(boolean sentByInitiator) throws IOException
        {
            StreamInitMessage message = new StreamInitMessage(FBUtilities.getBroadcastAddress(), session.planId(), session.description(), sentByInitiator);
            getWriteChannel().write(message.createMessage(false, protocolVersion));
        }

        public void start()
        {
            new Thread(this, name() + "-" + session.peer).start();
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
            closeFuture.get().set(null);

            // We can now close the socket
            try
            {
                socket.close();
            }
            catch (IOException ignore) {}
        }
    }

    /**
     * Incoming streaming message handler
     */
    static class IncomingMessageHandler extends MessageHandler
    {
        private final ReadableByteChannel in;

        IncomingMessageHandler(StreamSession session, Socket socket, int protocolVersion) throws IOException
        {
            super(session, socket, protocolVersion);
            this.in = getReadChannel();
        }

        protected String name()
        {
            return "STREAM-IN";
        }

        public void run()
        {
            while (!isClosed())
            {
                try
                {
                    // receive message
                    StreamMessage message = StreamMessage.deserialize(in, protocolVersion, session);
                    // Might be null if there is an error during streaming (see FileMessage.deserialize). It's ok
                    // to ignore here since we'll have asked for a retry.
                    if (message != null)
                    {
                        logger.debug("Received {}", message);
                        session.messageReceived(message);
                    }
                }
                catch (SocketException e)
                {
                    // socket is closed
                    close();
                }
                catch (Throwable e)
                {
                    session.onError(e);
                }
            }
            signalCloseDone();
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

        private final WritableByteChannel out;

        OutgoingMessageHandler(StreamSession session, Socket socket, int protocolVersion) throws IOException
        {
            super(session, socket, protocolVersion);
            this.out = getWriteChannel();
        }

        protected String name()
        {
            return "STREAM-OUT";
        }

        public void enqueue(StreamMessage message)
        {
            messageQueue.put(message);
        }

        public void run()
        {
            StreamMessage next;
            while (!isClosed())
            {
                try
                {
                    if ((next = messageQueue.poll(1, TimeUnit.SECONDS)) != null)
                    {
                        logger.debug("Sending {}", next);
                        sendMessage(next);
                        if (next.type == StreamMessage.Type.SESSION_FAILED)
                            close();
                    }
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
            }

            try
            {
                // Sends the last messages on the queue
                while ((next = messageQueue.poll()) != null)
                    sendMessage(next);
            }
            finally
            {
                signalCloseDone();
            }
        }

        private void sendMessage(StreamMessage message)
        {
            try
            {
                StreamMessage.serialize(message, out, protocolVersion, session);
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
