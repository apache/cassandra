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

package org.apache.cassandra.db.streaming;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.streaming.OutgoingStreamData;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;

public class CassandraOutgoingFile implements OutgoingStreamData
{
    private final Ref<SSTableReader> ref;
    private final long estimatedKeys;
    private final List<Pair<Long, Long>> sections;
    private final String filename;

    public CassandraOutgoingFile(Ref<SSTableReader> ref, long estimatedKeys, List<Pair<Long, Long>> sections)
    {
        this.ref = ref;
        this.estimatedKeys = estimatedKeys;
        this.sections = sections;

        SSTableReader sstable = ref.get();
        filename = sstable.getFilename();
    }

    @Override
    public String getName()
    {
        return filename;
    }

    @Override
    public void write(StreamSession session, DataOutputStreamPlus out, int version) throws IOException
    {
        SSTableReader sstable = ref.get();
        StreamOperation operation = session.getStreamOperation();
        boolean keepSSTableLevel = operation == StreamOperation.BOOTSTRAP || operation == StreamOperation.REBUILD;
        CassandraStreamHeader header = new CassandraStreamHeader(sstable.descriptor.version,
                                                                 sstable.descriptor.formatType,
                                                                 estimatedKeys,
                                                                 sections,
                                                                 CompressionInfo.fromCompressionMetadata(sstable.getCompressionMetadata(), sections),
                                                                 keepSSTableLevel ? sstable.getSSTableLevel() : 0,
                                                                 sstable.header.toComponent());
        CassandraStreamHeader.serializer.serialize(header, out, version);
        out.flush();

        CassandraStreamWriter writer = header.compressionInfo == null ?
                                       new CassandraStreamWriter(sstable, header.sections, session) :
                                       new CompressedCassandraStreamWriter(sstable, header.sections,
                                                                           header.compressionInfo, session);
        writer.write(out);
    }

    @Override
    public void finish()
    {
        ref.release();
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CassandraOutgoingFile that = (CassandraOutgoingFile) o;
        return estimatedKeys == that.estimatedKeys &&
               Objects.equals(ref, that.ref) &&
               Objects.equals(sections, that.sections);
    }

    public int hashCode()
    {
        return Objects.hash(ref, estimatedKeys, sections);
    }

    @Override
    public String toString()
    {
        return "CassandraOutgoingFile{" +
               "ref=" + ref +
               ", estimatedKeys=" + estimatedKeys +
               ", sections=" + sections +
               '}';
    }
}
