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

import org.apache.log4j.Logger;

import org.apache.cassandra.streaming.IncomingStreamReader;

public class IncomingTcpConnection extends Thread
{
    private static Logger logger = Logger.getLogger(IncomingTcpConnection.class);

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
                    new IncomingStreamReader(socket.getChannel()).read();
                    break;
                }
                else
                {
                    int size = input.readInt();
                    byte[] contentBytes = new byte[size];
                    input.readFully(contentBytes);
                    
                    Message message = Message.serializer().deserialize(new DataInputStream(new ByteArrayInputStream(contentBytes)));
                    MessagingService.receive(message);
                }
                // prepare to read the next message
                MessagingService.validateMagic(input.readInt());
                int header = input.readInt();
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
}
