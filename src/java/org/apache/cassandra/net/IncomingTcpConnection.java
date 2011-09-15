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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.security.streaming.SSLIncomingStreamReader;
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
        from = socket.getInetAddress(); // maximize chance of this not being nulled by disconnect
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
            if (!isStream)
                // we should buffer
                input = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 4096));
            version = MessagingService.getBits(header, 15, 8);
            logger.debug("Version for {} is {}", from, version);
        }
        catch (IOException e)
        {
            logger.debug("Incoming IOException", e);
            close();
            return;
        }

        if (version > MessagingService.version_)
        {
            // save the endpoint so gossip will reconnect to it
            Gossiper.instance.addSavedEndpoint(from);
            logger.info("Received " + (isStream ? "streaming " : "") + "connection from newer protocol version. Ignorning");

            // streaming connections are per-session and have a fixed version.  we can't do anything with a new-version
            // stream connection, so drop it.
            if (isStream)
            {
                close();
                return;
            }
            // for non-streaming connections, continue to read the messages (and ignore them) until sender
            // starts sending correct-version messages (which it can do without reconnecting -- version is per-Message)
        }
        else
        {
            // only set version when <= to us, otherwise it's the responsibility of the other end to mimic us
            Gossiper.instance.setVersion(from, version);
            logger.debug("set version for {} to {}", from, version);
        }

        while (true)
        {
            try
            {
                if (isStream)
                {
                    int size = input.readInt();
                    byte[] headerBytes = new byte[size];
                    input.readFully(headerBytes);
                    stream(StreamHeader.serializer().deserialize(new DataInputStream(new ByteArrayInputStream(headerBytes)), version), input);
                    break;
                }
                else
                {
                    int size = input.readInt();
                    byte[] contentBytes = new byte[size];
                    // readFully allocates a direct buffer the size of the chunk it is asked to read,
                    // so we cap that at CHUNK_SIZE.  See https://issues.apache.org/jira/browse/CASSANDRA-2654
                    int remainder = size % CHUNK_SIZE;
                    for (int offset = 0; offset < size - remainder; offset += CHUNK_SIZE)
                        input.readFully(contentBytes, offset, CHUNK_SIZE);
                    input.readFully(contentBytes, size - remainder, remainder);

                    if (version <= MessagingService.version_)
                    {
                        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes));
                        String id = dis.readUTF();
                        Message message = Message.serializer().deserialize(dis, version);
                        MessagingService.instance().receive(message, id);
                    }
                    else
                    {
                        logger.debug("Ignoring message version {}", version);
                    }
                }
                // prepare to read the next message
                MessagingService.validateMagic(input.readInt());
                int header = input.readInt();
                assert isStream == (MessagingService.getBits(header, 3, 1) == 1) : "Connections cannot change type: " + isStream;
                version = MessagingService.getBits(header, 15, 8);
                logger.debug("Version is now {}", version);
            }
            catch (EOFException e)
            {
                if (logger.isTraceEnabled())
                    logger.trace("eof reading from socket; closing", e);
                break;
            }
            catch (IOException e) 
            {
                if (logger.isDebugEnabled())
                    logger.debug("error reading from socket; closing", e);
                break;
            }
        }

        close();
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
        EncryptionOptions options = DatabaseDescriptor.getEncryptionOptions();
        if (options != null && options.internode_encryption == EncryptionOptions.InternodeEncryption.all)
            new SSLIncomingStreamReader(streamHeader, socket, input).read();
        else
            new IncomingStreamReader(streamHeader, socket).read();
    }
}
