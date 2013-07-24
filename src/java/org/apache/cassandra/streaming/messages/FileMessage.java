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

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.streaming.StreamReader;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamWriter;
import org.apache.cassandra.streaming.compress.CompressedStreamReader;
import org.apache.cassandra.streaming.compress.CompressedStreamWriter;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.utils.Pair;

/**
 * FileMessage is used to transfer/receive the part(or whole) of a SSTable data file.
 */
public class FileMessage extends StreamMessage
{
    public static Serializer<FileMessage> serializer = new Serializer<FileMessage>()
    {
        public FileMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            DataInputStream input = new DataInputStream(Channels.newInputStream(in));
            FileMessageHeader header = FileMessageHeader.serializer.deserialize(input, version);
            StreamReader reader = header.compressionInfo == null ? new StreamReader(header, session)
                                          : new CompressedStreamReader(header, session);

            try
            {
                return new FileMessage(reader.read(in), header);
            }
            catch (Throwable e)
            {
                session.doRetry(header, e);
                return null;
            }
        }

        public void serialize(FileMessage message, WritableByteChannel out, int version, StreamSession session) throws IOException
        {
            DataOutput output = new DataOutputStream(Channels.newOutputStream(out));
            FileMessageHeader.serializer.serialize(message.header, output, version);
            StreamWriter writer = message.header.compressionInfo == null ?
                                          new StreamWriter(message.sstable, message.header.sections, session) :
                                          new CompressedStreamWriter(message.sstable,
                                                                     message.header.sections,
                                                                     message.header.compressionInfo, session);
            writer.write(out);
            session.fileSent(message.header);
        }
    };

    public final FileMessageHeader header;
    public final SSTableReader sstable;

    public FileMessage(SSTableReader sstable, FileMessageHeader header)
    {
        super(Type.FILE);
        this.header = header;
        this.sstable = sstable;
    }

    public FileMessage(SSTableReader sstable, int sequenceNumber, long estimatedKeys, List<Pair<Long, Long>> sections)
    {
        super(Type.FILE);
        this.sstable = sstable;

        CompressionInfo compressionInfo = null;
        if (sstable.compression)
        {
            CompressionMetadata meta = sstable.getCompressionMetadata();
            compressionInfo = new CompressionInfo(meta.getChunksForSections(sections), meta.parameters);
        }
        this.header = new FileMessageHeader(sstable.metadata.cfId,
                                            sequenceNumber,
                                            sstable.descriptor.version.toString(),
                                            estimatedKeys,
                                            sections,
                                            compressionInfo);
    }

    @Override
    public String toString()
    {
        return "File (" + header + ", file: " + sstable.getFilename() + ")";
    }
}
