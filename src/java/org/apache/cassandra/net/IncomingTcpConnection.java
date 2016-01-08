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
import java.net.SocketException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.zip.Checksum;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

import org.apache.cassandra.config.Config;
import org.xerial.snappy.SnappyInputStream;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.UnknownColumnFamilyException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.NIODataInputStream;

public class IncomingTcpConnection extends Thread implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingTcpConnection.class);

    private static final int BUFFER_SIZE = Integer.getInteger(Config.PROPERTY_PREFIX + ".itc_buffer_size", 1024 * 4);

    private final int version;
    private final boolean compressed;
    private final Socket socket;
    private final Set<Closeable> group;
    public InetAddress from;

    public IncomingTcpConnection(int version, boolean compressed, Socket socket, Set<Closeable> group)
    {
        super("MessagingService-Incoming-" + socket.getInetAddress());
        this.version = version;
        this.compressed = compressed;
        this.socket = socket;
        this.group = group;
        if (DatabaseDescriptor.getInternodeRecvBufferSize() != null)
        {
            try
            {
                this.socket.setReceiveBufferSize(DatabaseDescriptor.getInternodeRecvBufferSize());
            }
            catch (SocketException se)
            {
                logger.warn("Failed to set receive buffer size on internode socket.", se);
            }
        }
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
            if (version < MessagingService.VERSION_20)
                throw new UnsupportedOperationException(String.format("Unable to read obsolete message version %s; "
                                                                      + "The earliest version supported is 2.0.0",
                                                                      version));

            receiveMessages();
        }
        catch (EOFException e)
        {
            logger.trace("eof reading from socket; closing", e);
            // connection will be reset so no need to throw an exception.
        }
        catch (UnknownColumnFamilyException e)
        {
            logger.warn("UnknownColumnFamilyException reading from socket; closing", e);
        }
        catch (IOException e)
        {
            logger.trace("IOException reading from socket; closing", e);
        }
        finally
        {
            close();
        }
    }

    @Override
    public void close()
    {
        try
        {
            if (logger.isTraceEnabled())
                logger.trace("Closing socket {} - isclosed: {}", socket, socket.isClosed());
            if (!socket.isClosed())
            {
                socket.close();
            }
        }
        catch (IOException e)
        {
            logger.trace("Error closing socket", e);
        }
        finally
        {
            group.remove(this);
        }
    }

    @SuppressWarnings("resource") // Not closing constructed DataInputPlus's as the stream needs to remain open.
    private void receiveMessages() throws IOException
    {
        // handshake (true) endpoint versions
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        // if this version is < the MS version the other node is trying
        // to connect with, the other node will disconnect
        out.writeInt(MessagingService.current_version);
        out.flush();
        DataInputPlus in = new DataInputStreamPlus(socket.getInputStream());
        int maxVersion = in.readInt();
        // outbound side will reconnect if necessary to upgrade version
        assert version <= MessagingService.current_version;
        from = CompactEndpointSerializationHelper.deserialize(in);
        // record the (true) version of the endpoint
        MessagingService.instance().setVersion(from, maxVersion);
        logger.trace("Set version for {} to {} (will use {})", from, maxVersion, MessagingService.instance().getVersion(from));

        if (compressed)
        {
            logger.trace("Upgrading incoming connection to be compressed");
            if (version < MessagingService.VERSION_21)
            {
                in = new DataInputStreamPlus(new SnappyInputStream(socket.getInputStream()));
            }
            else
            {
                LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();
                Checksum checksum = XXHashFactory.fastestInstance().newStreamingHash32(OutboundTcpConnection.LZ4_HASH_SEED).asChecksum();
                in = new DataInputStreamPlus(new LZ4BlockInputStream(socket.getInputStream(),
                                                                 decompressor,
                                                                 checksum));
            }
        }
        else
        {
            ReadableByteChannel channel = socket.getChannel();
            in = new NIODataInputStream(channel != null ? channel : Channels.newChannel(socket.getInputStream()), BUFFER_SIZE);
        }

        while (true)
        {
            MessagingService.validateMagic(in.readInt());
            receiveMessage(in, version);
        }
    }

    private InetAddress receiveMessage(DataInputPlus input, int version) throws IOException
    {
        int id;
        if (version < MessagingService.VERSION_20)
            id = Integer.parseInt(input.readUTF());
        else
            id = input.readInt();

        MessageIn message = MessageIn.read(input, version, id, MessageIn.readTimestamp(input));
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
            logger.trace("Received connection from newer protocol version {}. Ignoring message", version);
        }
        return message.from;
    }
}
