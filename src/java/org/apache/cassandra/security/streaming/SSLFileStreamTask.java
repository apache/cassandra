/**
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

package org.apache.cassandra.security.streaming;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.streaming.FileStreamTask;
import org.apache.cassandra.streaming.StreamHeader;
import org.apache.cassandra.utils.FBUtilities;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.Pair;

/**
 * This class uses a DataOutputStream to write data as opposed to a FileChannel.transferTo
 * used by FileStreamTask because the underlying SSLSocket doesn't support
 * encrypting over NIO SocketChannel.
 */
public class SSLFileStreamTask extends FileStreamTask
{
    private DataOutputStream output;
    private Socket socket;
    
    public SSLFileStreamTask(StreamHeader header, InetAddress to)
    {
        super(header, to);
    }

    @Override
    protected long write(FileChannel fc, Pair<Long, Long> section, long length, long bytesTransferred) throws IOException
    {
        int toTransfer = (int)Math.min(CHUNK_SIZE, length - bytesTransferred);
        fc.position(section.left + bytesTransferred);
        ByteBuffer buf = ByteBuffer.allocate(toTransfer);
        fc.read(buf);
        buf.flip();
        output.write(buf.array(), 0, buf.limit());
        output.flush();
        return buf.limit();
    }

    @Override
    protected void writeHeader(ByteBuffer buffer) throws IOException
    {
        output.write(buffer.array(), 0, buffer.limit());
        output.flush();
    }

    @Override
    protected void bind() throws IOException
    {
        socket = SSLFactory.getSocket(DatabaseDescriptor.getEncryptionOptions());
        socket.bind(new InetSocketAddress(FBUtilities.getLocalAddress(), 0));
    }

    @Override
    protected void connect() throws IOException
    {
        socket.connect(new InetSocketAddress(to, DatabaseDescriptor.getStoragePort()));
        output = new DataOutputStream(socket.getOutputStream());
    }

    @Override
    protected void close() throws IOException
    {
        socket.close();
    }
}
