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

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.messages.StreamInitMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;

/**
 * Thread to consume stream init messages.
 */
public class IncomingStreamingConnection extends Thread implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingStreamingConnection.class);

    private final int version;
    private final Socket socket;
    private final Set<Closeable> group;

    public IncomingStreamingConnection(int version, Socket socket, Set<Closeable> group)
    {
        super("STREAM-INIT-" + socket.getRemoteSocketAddress());
        this.version = version;
        this.socket = socket;
        this.group = group;
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

            // The initiator makes two connections, one for incoming and one for outgoing.
            // The receiving side distinguish two connections by looking at StreamInitMessage#isForOutgoing.
            // Note: we cannot use the same socket for incoming and outgoing streams because we want to
            // parallelize said streams and the socket is blocking, so we might deadlock.
            StreamResultFuture.initReceivingSide(init.sessionIndex, init.planId, init.description, init.from, socket, init.isForOutgoing, version);
        }
        catch (IOException e)
        {
            logger.debug("IOException reading from socket; closing", e);
            close();
        }
    }
    
    @Override
    public void close()
    {
        try
        {
            if (!socket.isClosed())
            {
                socket.close();
            }
        }
        catch (IOException e)
        {
            logger.debug("Error closing socket", e);
        }
        finally
        {
            group.remove(this);
        }
    }
}
