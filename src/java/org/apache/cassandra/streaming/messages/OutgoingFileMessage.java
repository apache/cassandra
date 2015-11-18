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
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamWriter;
import org.apache.cassandra.streaming.compress.CompressedStreamWriter;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * OutgoingFileMessage is used to transfer the part(or whole) of a SSTable data file.
 */
public class OutgoingFileMessage extends StreamMessage
{
    public static Serializer<OutgoingFileMessage> serializer = new Serializer<OutgoingFileMessage>()
    {
        public OutgoingFileMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            throw new UnsupportedOperationException("Not allowed to call deserialize on an outgoing file");
        }

        public void serialize(OutgoingFileMessage message, DataOutputStreamPlus out, int version, StreamSession session) throws IOException
        {
            message.serialize(out, version, session);
            session.fileSent(message.header);
        }
    };

    public final FileMessageHeader header;
    private final Ref<SSTableReader> ref;
    private final String filename;
    private boolean completed = false;

    public OutgoingFileMessage(Ref<SSTableReader> ref, int sequenceNumber, long estimatedKeys, List<Pair<Long, Long>> sections, long repairedAt, boolean keepSSTableLevel)
    {
        super(Type.FILE);
        this.ref = ref;

        SSTableReader sstable = ref.get();
        filename = sstable.getFilename();
        this.header = new FileMessageHeader(sstable.metadata.cfId,
                                            sequenceNumber,
                                            sstable.descriptor.version,
                                            sstable.descriptor.formatType,
                                            estimatedKeys,
                                            sections,
                                            sstable.compression ? sstable.getCompressionMetadata() : null,
                                            repairedAt,
                                            keepSSTableLevel ? sstable.getSSTableLevel() : 0,
                                            sstable.header == null ? null : sstable.header.toComponent());
    }

    public synchronized void serialize(DataOutputStreamPlus out, int version, StreamSession session) throws IOException
    {
        if (completed)
        {
            return;
        }

        CompressionInfo compressionInfo = FileMessageHeader.serializer.serialize(header, out, version);

        final SSTableReader reader = ref.get();
        StreamWriter writer = compressionInfo == null ?
                                      new StreamWriter(reader, header.sections, session) :
                                      new CompressedStreamWriter(reader, header.sections,
                                                                 compressionInfo, session);
        writer.write(out);
    }

    public synchronized void complete()
    {
        if (!completed)
        {
            completed = true;
            ref.release();
        }
    }

    @Override
    public String toString()
    {
        return "File (" + header + ", file: " + filename + ")";
    }
}

