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

    private Socket socket;

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
            if (!isStream)
                // we should buffer
                input = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 4096));
            version = MessagingService.getBits(header, 15, 8);
            Gossiper.instance.setVersion(socket.getInetAddress(), version);
        }
        catch (IOException e)
        {
            close();
            throw new IOError(e);
        }
        while (true)
        {
            try
            {
                if (isStream)
                {
                    if (version > MessagingService.version_)
                    {
                        logger.error("Received untranslated stream from newer protcol version. Terminating connection!");
                        close();
                        return;
                    }
                    int size = input.readInt();
                    byte[] headerBytes = new byte[size];
                    input.readFully(headerBytes);
                    // todo: need to be aware of message version.
                    stream(StreamHeader.serializer().deserialize(new DataInputStream(new ByteArrayInputStream(headerBytes))), input);
                    break;
                }
                else
                {
                    int size = input.readInt();
                    byte[] contentBytes = new byte[size];
                    input.readFully(contentBytes);
                    
                    if (version > MessagingService.version_)
                        logger.info("Received connection from newer protocol version. Ignorning message.");
                    else
                    {
                        // todo: need to be aware of message version.
                        Message message = Message.serializer().deserialize(new DataInputStream(new ByteArrayInputStream(contentBytes)));
                        MessagingService.instance().receive(message);
                    }
                }
                // prepare to read the next message
                MessagingService.validateMagic(input.readInt());
                int header = input.readInt();
                version = MessagingService.getBits(header, 15, 8);
                assert isStream == (MessagingService.getBits(header, 3, 1) == 1) : "Connections cannot change type: " + isStream;
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
        if (DatabaseDescriptor.getEncryptionOptions().internode_encryption == EncryptionOptions.InternodeEncryption.all)
            new SSLIncomingStreamReader(streamHeader, socket, input).read();
        else
            new IncomingStreamReader(streamHeader, socket).read();
    }
}
