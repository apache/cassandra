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

import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.IOException;
import java.io.DataInputStream;

import org.apache.cassandra.streaming.FileStreamTask;
import org.apache.cassandra.streaming.IncomingStreamReader;
import org.apache.cassandra.streaming.StreamHeader;

/**
 * This class uses a DataInputStream to read data as opposed to a FileChannel.transferFrom
 * used by IncomingStreamReader because the underlying SSLServerSocket doesn't support
 * encrypting over NIO SocketChannel.
 */
public class SSLIncomingStreamReader extends IncomingStreamReader
{
    private final DataInputStream input;

    public SSLIncomingStreamReader(StreamHeader header, Socket socket, DataInputStream input) throws IOException
    {
        super(header, socket);
        this.input = input;
    }

    @Override
    protected long readnwrite(long length, long bytesRead, long offset, FileChannel fc) throws IOException
    {
        int toRead = (int)Math.min(FileStreamTask.CHUNK_SIZE, length - bytesRead);
        ByteBuffer buf = ByteBuffer.allocate(toRead);
        input.readFully(buf.array());
        fc.write(buf);
        bytesRead += buf.limit();
        remoteFile.progress += buf.limit();
        return bytesRead;
    }
}
