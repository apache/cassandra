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

package org.apache.cassandra.io.sstable.format;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableBuilder;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.JVMStabilityInspector;

public abstract class SSTableReaderBuilder<R extends SSTableReader, B extends SSTableReaderBuilder<R, B>> extends SSTableBuilder<R, B>
{
    private long maxDataAge;
    private StatsMetadata statsMetadata;
    private SSTableReader.OpenReason openReason;
    private SerializationHeader serializationHeader;
    private FileHandle dataFile;
    private IFilter filter;
    private DecoratedKey first;
    private DecoratedKey last;
    private boolean suspected;

    public SSTableReaderBuilder(Descriptor descriptor)
    {
        super(descriptor);
    }

    public B setMaxDataAge(long maxDataAge)
    {
        Preconditions.checkArgument(maxDataAge >= 0);
        this.maxDataAge = maxDataAge;
        return (B) this;
    }

    public B setStatsMetadata(StatsMetadata statsMetadata)
    {
        Preconditions.checkNotNull(statsMetadata);
        this.statsMetadata = statsMetadata;
        return (B) this;
    }

    public B setOpenReason(SSTableReader.OpenReason openReason)
    {
        Preconditions.checkNotNull(openReason);
        this.openReason = openReason;
        return (B) this;
    }

    public B setSerializationHeader(SerializationHeader serializationHeader)
    {
        this.serializationHeader = serializationHeader;
        return (B) this;
    }

    public B setDataFile(FileHandle dataFile)
    {
        this.dataFile = dataFile;
        return (B) this;
    }

    public B setFilter(IFilter filter)
    {
        this.filter = filter;
        return (B) this;
    }

    public B setFirst(DecoratedKey first)
    {
        this.first = first != null ? SSTable.getMinimalKey(first) : null;
        return (B) this;
    }

    public B setLast(DecoratedKey last)
    {
        this.last = last != null ? SSTable.getMinimalKey(last) : null;
        return (B) this;
    }

    public B setSuspected(boolean suspected)
    {
        this.suspected = suspected;
        return (B) this;
    }

    public long getMaxDataAge()
    {
        return maxDataAge;
    }

    public StatsMetadata getStatsMetadata()
    {
        return statsMetadata;
    }

    public SSTableReader.OpenReason getOpenReason()
    {
        return openReason;
    }

    public SerializationHeader getSerializationHeader()
    {
        return serializationHeader;
    }

    public FileHandle getDataFile()
    {
        return dataFile;
    }

    public IFilter getFilter()
    {
        return filter;
    }

    public DecoratedKey getFirst()
    {
        return first;
    }

    public DecoratedKey getLast()
    {
        return last;
    }

    public boolean isSuspected()
    {
        return suspected;
    }

    protected abstract R buildInternal();

    public R build(boolean validate, boolean online)
    {
        R reader = buildInternal();

        try
        {
            if (isSuspected())
                reader.markSuspect();

            reader.setup(online);

            if (validate)
                reader.validate();
        }
        catch (RuntimeException | Error ex)
        {
            JVMStabilityInspector.inspectThrowable(ex);
            reader.selfRef().release();
            throw ex;
        }

        return reader;
    }
}