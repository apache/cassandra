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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            if (isStream)
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
                // We are done with this connection....
                return;
            }

            // we should buffer
            input = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 4096));
            // Receive the first message to set the version.
            from = receiveMessage(input, version); // why? see => CASSANDRA-4099
            logger.debug("Version for {} is {}", from, version);
            if (version > MessagingService.current_version)
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
