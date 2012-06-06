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

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.xerial.snappy.SnappyInputStream;

import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.streaming.IncomingStreamReader;
import org.apache.cassandra.streaming.StreamHeader;

public class IncomingTcpConnection extends Thread
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingTcpConnection.class);

    private final Socket socket;
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
        try
        {
            // determine the connection type to decide whether to buffer
            DataInputStream in = new DataInputStream(socket.getInputStream());
            MessagingService.validateMagic(in.readInt());
            int header = in.readInt();
            boolean isStream = MessagingService.getBits(header, 3, 1) == 1;
            int version = MessagingService.getBits(header, 15, 8);
            logger.debug("Connection version {} from {}", version, socket.getInetAddress());

            if (isStream)
                handleStream(in, version);
            else if (version < MessagingService.VERSION_12)
                handleLegacyVersion(version);
            else
                handleModernVersion(version, header);
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

    private void handleModernVersion(int version, int header) throws IOException
    {
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeInt(MessagingService.current_version);
        out.flush();

        DataInputStream in = new DataInputStream(socket.getInputStream());
        int maxVersion = in.readInt();
        from = CompactEndpointSerializationHelper.deserialize(in);
        boolean compressed = MessagingService.getBits(header, 2, 1) == 1;

        if (compressed)
        {
            logger.debug("Upgrading incoming connection to be compressed");
            in = new DataInputStream(new SnappyInputStream(socket.getInputStream()));
        }
        else
        {
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 4096));
        }

        logger.debug("Max version for {} is {}", from, maxVersion);
        if (version > MessagingService.current_version)
        {
            // save the endpoint so gossip will reconnect to it
            Gossiper.instance.addSavedEndpoint(from);
            logger.info("Received messages from newer protocol version {}. Ignoring", version);
            return;
        }
        MessagingService.instance().setVersion(from, Math.min(MessagingService.current_version, maxVersion));
        logger.debug("set version for {} to {}", from, Math.min(MessagingService.current_version, maxVersion));
        // outbound side will reconnect if necessary to upgrade version

        while (true)
        {
            MessagingService.validateMagic(in.readInt());
            receiveMessage(in, version);
        }
    }

    private void handleLegacyVersion(int version) throws IOException
    {
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 4096));

        from = receiveMessage(in, version); // why? see => CASSANDRA-4099
        logger.debug("Version for {} is {}", from, version);
        if (version > MessagingService.current_version)
        {
            // save the endpoint so gossip will reconnect to it
            Gossiper.instance.addSavedEndpoint(from);
            logger.info("Received messages from newer protocol version. Ignoring");
            return;
        }
        int lastVersion = MessagingService.instance().setVersion(from, version);
        logger.debug("set version for {} to {}", from, version);
        if (lastVersion < version)
        {
            logger.debug("breaking outbound connections to force version upgrade");
            MessagingService.instance().getConnectionPool(from).resetToNewerVersion(version);
        }

        while (true)
        {
            MessagingService.validateMagic(in.readInt());
            int header = in.readInt(); // legacy protocol re-sends header for each message
            assert !(MessagingService.getBits(header, 3, 1) == 1) : "Non-stream connection cannot change to stream";
            version = MessagingService.getBits(header, 15, 8);
            logger.trace("Version is now {}", version);
            receiveMessage(in, version);
        }
    }

    private void handleStream(DataInputStream input, int version) throws IOException
    {
        if (version == MessagingService.current_version)
        {
            int size = input.readInt();
            byte[] headerBytes = new byte[size];
            input.readFully(headerBytes);
            stream(StreamHeader.serializer.deserialize(new DataInputStream(new FastByteArrayInputStream(headerBytes)), version), input);
        }
        else
        {
            // streaming connections are per-session and have a fixed version.  we can't do anything with a wrong-version stream connection, so drop it.
            logger.error("Received stream using protocol version {} (my version {}). Terminating connection",
                         version, MessagingService.current_version);
        }
    }

    private InetAddress receiveMessage(DataInputStream input, int version) throws IOException
    {
        if (version <= MessagingService.VERSION_11)
            input.readInt(); // size of entire message. in 1.0+ this is just a placeholder

        String id = input.readUTF();
        MessageIn message = MessageIn.read(input, version, id);
        if (message == null)
        {
            // callback expired; nothing to do
            return null;
        }
        if (version <= MessagingService.current_version)
        {
            MessagingService.instance().receive(message, id);
        }
        else
        {
            logger.debug("Received connection from newer protocol version {}. Ignoring message", version);
        }
        return message.from;
    }

    private void close()
    {
        // reset version here, since we set when starting an incoming socket
        if (from != null)
            MessagingService.instance().resetVersion(from);
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
