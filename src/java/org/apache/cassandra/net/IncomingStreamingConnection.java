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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.StreamInitMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;

/**
 * Thread to consume stream init messages.
 */
public class IncomingStreamingConnection extends Thread
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingStreamingConnection.class);

    private final int version;
    private final Socket socket;

    public IncomingStreamingConnection(int version, Socket socket)
    {
        super("stream-init " + socket.getRemoteSocketAddress());
        this.version = version;
        this.socket = socket;
    }

    @Override
    public void run()
    {
        try
        {
            // streaming connections are per-session and have a fixed version.  we can't do anything with a wrong-version stream connection, so drop it.
            if (version != StreamMessage.CURRENT_VERSION)
                throw new IOException(String.format("Received stream using protocol version %d (my version %d). Terminating connection", version, MessagingService.current_version));

            DataInput input = new DataInputStream(socket.getInputStream());
            StreamInitMessage init = StreamInitMessage.serializer.deserialize(input, version);

            // We will use the current socket to incoming stream. So if the other side is the
            // stream initiator, we must first create an outgoing stream, after which real streaming
            // will start. If we were the initiator however, this socket will just be our incoming
            // stream, everything is setup and we can initiate real streaming by sending the prepare message.
            // Note: we cannot use the same socket for incoming and outgoing streams because we want to
            // parallelize said streams and the socket is blocking, so we might deadlock.
            if (init.sentByInitiator)
            {
                StreamResultFuture.initReceivingSide(init.planId, init.description, init.from, socket, version);
            }
            else
            {
                StreamResultFuture stream = StreamManager.instance.getStream(init.planId);
                if (stream == null)
                {
                    // This should not happen. All we can do is close the socket to inform the other side, but that's a bug.
                    logger.error("Got StreamInit message for a stream we are supposed to be the initiator of, but stream not found.");
                    socket.close();
                    return;
                }
                // We're fully setup for this session, start the actual streaming
                stream.startStreaming(init.from, socket, version);
            }
        }
        catch (IOException e)
        {
            logger.debug("IOException reading from socket; closing", e);
            try
            {
                socket.close();
            }
            catch (IOException e2)
            {
                logger.debug("error closing socket", e2);
            }
        }
    }
}
