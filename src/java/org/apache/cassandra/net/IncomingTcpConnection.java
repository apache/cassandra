package org.apache.cassandra.net;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.*;
import java.net.InetAddress;
import java.net.Socket;

import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.streaming.IncomingStreamReader;
import org.apache.cassandra.streaming.StreamHeader;

public class IncomingTcpConnection extends Thread
{
    private static Logger logger = LoggerFactory.getLogger(IncomingTcpConnection.class);

    private static final int CHUNK_SIZE = 1024 * 1024;

    private Socket socket;
    public InetAddress from;

    public IncomingTcpConnection(Socket socket)
    {
        assert socket != null;
        this.socket = socket;
    }

    /**
     * A new connection will either stream or message for its entire lifetime: because streaming
     * bypasses the InputStream implementations to use sendFile, we cannot begin buffering until
     * we've determined the type of the connection.
     */
    @Override
    public void run()
    {
        DataInputStream input;
        boolean isStream;
        int version;
        try
        {
            // determine the connection type to decide whether to buffer
            input = new DataInputStream(socket.getInputStream());
            MessagingService.validateMagic(input.readInt());
            int header = input.readInt();
            isStream = MessagingService.getBits(header, 3, 1) == 1;
            version = MessagingService.getBits(header, 15, 8);
            logger.debug("Version for {} is {}", from, version);
            if (isStream)
            {
                if (version == MessagingService.version_)
                {
                    int size = input.readInt();
                    byte[] headerBytes = new byte[size];
                    input.readFully(headerBytes);
                    stream(StreamHeader.serializer().deserialize(new DataInputStream(new FastByteArrayInputStream(headerBytes)), version), input);
                }
                else
                {
                    // streaming connections are per-session and have a fixed version.  we can't do anything with a wrong-version stream connection, so drop it.
                    logger.error("Received stream using protocol version {} (my version {}). Terminating connection",
                                 version, MessagingService.version_);
                }
                // We are done with this connection....
                return;
            }

            // we should buffer
            input = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 4096));
            // Receive the first message to set the version.
            from = receiveMessage(input, version); // why? see => CASSANDRA-4099
            if (version > MessagingService.version_)
            {
                // save the endpoint so gossip will reconnect to it
                Gossiper.instance.addSavedEndpoint(from);
                logger.info("Received " + (isStream ? "streaming " : "") + "connection from newer protocol version. Ignoring");
                return;
            }
            Gossiper.instance.setVersion(from, version);
            logger.debug("set version for {} to {}", from, version);

            // loop to get the next message.
            while (true)
            {
                // prepare to read the next message
                MessagingService.validateMagic(input.readInt());
                header = input.readInt();
                assert isStream == (MessagingService.getBits(header, 3, 1) == 1) : "Connections cannot change type: " + isStream;
                version = MessagingService.getBits(header, 15, 8);
                logger.trace("Version is now {}", version);
                receiveMessage(input, version);
            }
        }
        catch (EOFException e)
        {
            logger.trace("eof reading from socket; closing", e);
            // connection will be reset so no need to throw an exception.
        }
        catch (IOException e)
        {
            logger.debug("IOError reading from socket; closing", e);
        }
        finally
        {
            close();
        }
    }

    private InetAddress receiveMessage(DataInputStream input, int version) throws IOException
    {
        int totalSize = input.readInt();
        String id = input.readUTF();
        Header header = Header.serializer().deserialize(input, version);

        int bodySize = input.readInt();
        byte[] body = new byte[bodySize];
        // readFully allocates a direct buffer the size of the chunk it is asked to read,
        // so we cap that at CHUNK_SIZE.  See https://issues.apache.org/jira/browse/CASSANDRA-2654
        int remainder = bodySize % CHUNK_SIZE;
        for (int offset = 0; offset < bodySize - remainder; offset += CHUNK_SIZE)
            input.readFully(body, offset, CHUNK_SIZE);
        input.readFully(body, bodySize - remainder, remainder);
        // earlier versions would send unnecessary bytes left over at the end of a buffer, too
        long remaining = totalSize - OutboundTcpConnection.messageLength(header, id, body);
        while (remaining > 0)
            remaining -= input.skip(remaining);

        // for non-streaming connections, continue to read the messages (and ignore them) until sender
        // starts sending correct-version messages (which it can do without reconnecting -- version is per-Message)
        if (version <= MessagingService.version_)
        {
            Message message = new Message(header, body, version);
            MessagingService.instance().receive(message, id);
        }
        else
        {
            logger.debug("Received connection from newer protocol version {}. Ignoring message", version);
        }
        return header.getFrom();
    }

    private void close()
    {
        // reset version here, since we set when starting an incoming socket
        if (from != null)
            Gossiper.instance.resetVersion(from);
        try
        {
            socket.close();
        }
        catch (IOException e)
        {
            if (logger.isDebugEnabled())
                logger.debug("error closing socket", e);
        }
    }

    private void stream(StreamHeader streamHeader, DataInputStream input) throws IOException
    {
        new IncomingStreamReader(streamHeader, socket).read();
    }
}
