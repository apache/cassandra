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
package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Optional;

import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;

import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.streaming.StreamReader;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.compress.CompressedStreamReader;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.utils.Throwables.extractIOExceptionCause;

/**
 * IncomingFileMessage is used to receive the part(or whole) of a SSTable data file.
 */
public class IncomingFileMessage extends StreamMessage
{
    public static Serializer<IncomingFileMessage> serializer = new Serializer<IncomingFileMessage>()
    {
        @SuppressWarnings("resource")
        public IncomingFileMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            DataInputPlus input = new DataInputStreamPlus(Channels.newInputStream(in));
            FileMessageHeader header = FileMessageHeader.serializer.deserialize(input, version);
            StreamReader reader = !header.isCompressed() ? new StreamReader(header, session)
                    : new CompressedStreamReader(header, session);

            try
            {
                return new IncomingFileMessage(reader.read(in), header);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                throw t;
            }
        }

        public void serialize(IncomingFileMessage message, DataOutputStreamPlus out, int version, StreamSession session)
        {
            throw new UnsupportedOperationException("Not allowed to call serialize on an incoming file");
        }
    };

    public FileMessageHeader header;
    public SSTableMultiWriter sstable;

    public IncomingFileMessage(SSTableMultiWriter sstable, FileMessageHeader header)
    {
        super(Type.FILE);
        this.header = header;
        this.sstable = sstable;
    }

    @Override
    public String toString()
    {
        return "File (" + header + ", file: " + sstable.getFilename() + ")";
    }
}

