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

import java.util.Objects;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.IncomingStream;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.StreamMessageHeader;

/**
 * used to receive the part(or whole) of a SSTable data file.
 *
 * This class deserializes the data stream into partitions and rows, and writes that out as an sstable
 */
public class CassandraIncomingFile implements IncomingStream
{
    private final ColumnFamilyStore cfs;
    private final StreamSession session;
    private final StreamMessageHeader header;

    private volatile SSTableMultiWriter sstable;
    private volatile long size = -1;
    private volatile int numFiles = 1;

    private volatile boolean isEntireSSTable = false;

    private static final Logger logger = LoggerFactory.getLogger(CassandraIncomingFile.class);

    public CassandraIncomingFile(ColumnFamilyStore cfs, StreamSession session, StreamMessageHeader header)
    {
        this.cfs = cfs;
        this.session = session;
        this.header = header;
    }

    @Override
    public StreamSession session()
    {
        return session;
    }

    @Override
    public synchronized void read(DataInputPlus in, int version) throws Throwable
    {
        CassandraStreamHeader streamHeader = CassandraStreamHeader.serializer.deserialize(in, version);
        logger.debug("Incoming stream entireSSTable={} components={}", streamHeader.isEntireSSTable, streamHeader.componentManifest);
        session.countStreamedIn(streamHeader.isEntireSSTable);

        IStreamReader reader;
        if (streamHeader.isEntireSSTable)
        {
            isEntireSSTable = true;
            reader = new CassandraEntireSSTableStreamReader(header, streamHeader, session);
            numFiles = streamHeader.componentManifest.components().size();
        }
        else if (streamHeader.isCompressed())
            reader = new CassandraCompressedStreamReader(header, streamHeader, session);
        else
            reader = new CassandraStreamReader(header, streamHeader, session);

        size = streamHeader.size();
        sstable = reader.read(in);
    }

    @Override
    public synchronized String getName()
    {
        return sstable == null ? "null" : sstable.getFilename();
    }

    @Override
    public synchronized long getSize()
    {
        Preconditions.checkState(size != -1, "Stream hasn't been read yet");
        return size;
    }

    @Override
    public int getNumFiles()
    {
        return numFiles;
    }

    public boolean isEntireSSTable()
    {
        return isEntireSSTable;
    }
    
    @Override
    public TableId getTableId()
    {
        Preconditions.checkState(sstable != null, "Stream hasn't been read yet");
        return sstable.getTableId();
    }

    @Override
    public String toString()
    {
        SSTableMultiWriter sst = sstable;
        return "CassandraIncomingFile{" +
               "sstable=" + (sst == null ? "null" : sst.getFilename()) +
               '}';
    }

    public SSTableMultiWriter getSSTable()
    {
        Preconditions.checkState(sstable != null, "Stream hasn't been read yet");
        return sstable;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CassandraIncomingFile that = (CassandraIncomingFile) o;
        return Objects.equals(cfs, that.cfs) &&
               Objects.equals(session, that.session) &&
               Objects.equals(header, that.header) &&
               Objects.equals(sstable, that.sstable);
    }

    public int hashCode()
    {

        return Objects.hash(cfs, session, header, sstable);
    }
}
