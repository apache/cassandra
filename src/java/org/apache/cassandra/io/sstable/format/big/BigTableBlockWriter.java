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

package org.apache.cassandra.io.sstable.format.big;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.net.async.RebufferingByteBufDataInputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadataRef;

public class BigTableBlockWriter extends SSTable implements SSTableMultiWriter
{
    private final TableMetadataRef metadata;
    private final LifecycleTransaction txn;
    private volatile SSTableReader finalReader;
    private final Map<Component.Type, SequentialWriter> componentWriters;

    private final Logger logger = LoggerFactory.getLogger(BigTableBlockWriter.class);

    private final SequentialWriterOption writerOption = SequentialWriterOption.newBuilder()
                                                                              .trickleFsync(false)
                                                                              .bufferSize(2 * 1024 * 1024)
                                                                              .bufferType(BufferType.OFF_HEAP)
                                                                              .build();
    public static final ImmutableSet<Component> supportedComponents = ImmutableSet.of(Component.DATA, Component.PRIMARY_INDEX, Component.STATS,
                                                                               Component.COMPRESSION_INFO, Component.FILTER, Component.SUMMARY,
                                                                               Component.DIGEST, Component.CRC);

    public BigTableBlockWriter(Descriptor descriptor,
                               TableMetadataRef metadata,
                               LifecycleTransaction txn,
                               final Set<Component> components)
    {
        super(descriptor, ImmutableSet.copyOf(components), metadata,
              DatabaseDescriptor.getDiskOptimizationStrategy());
        txn.trackNew(this);
        this.metadata = metadata;
        this.txn = txn;
        this.componentWriters = new HashMap<>(components.size());

        assert supportedComponents.containsAll(components) : String.format("Unsupported streaming component detected %s",
                                                                           new HashSet(components).removeAll(supportedComponents));

        for (Component c : components)
            componentWriters.put(c.type, makeWriter(descriptor, c, writerOption));
    }

    private static SequentialWriter makeWriter(Descriptor descriptor, Component component, SequentialWriterOption writerOption)
    {
        return new SequentialWriter(new File(descriptor.filenameFor(component)), writerOption, false);
    }

    private void write(DataInputPlus in, long size, SequentialWriter out) throws FSWriteError
    {
        final int BUFFER_SIZE = 1 * 1024 * 1024;
        long bytesRead = 0;
        byte[] buff = new byte[BUFFER_SIZE];
        try
        {
            while (bytesRead < size)
            {
                int toRead = (int) Math.min(size - bytesRead, BUFFER_SIZE);
                in.readFully(buff, 0, toRead);
                int count = Math.min(toRead, BUFFER_SIZE);
                out.write(buff, 0, count);
                bytesRead += count;
            }
            out.sync();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, out.getPath());
        }
    }

    @Override
    public boolean append(UnfilteredRowIterator partition)
    {
        throw new UnsupportedOperationException("Operation not supported by BigTableBlockWriter");
    }

    @Override
    public Collection<SSTableReader> finish(long repairedAt, long maxDataAge, boolean openResult)
    {
        return finish(openResult);
    }

    @Override
    public Collection<SSTableReader> finish(boolean openResult)
    {
        setOpenResult(openResult);
        return finished();
    }

    @Override
    public Collection<SSTableReader> finished()
    {
        if (finalReader == null)
            finalReader = SSTableReader.open(descriptor,
                                             components,
                                             metadata);

        return ImmutableList.of(finalReader);
    }

    @Override
    public SSTableMultiWriter setOpenResult(boolean openResult)
    {
        return null;
    }

    @Override
    public long getFilePointer()
    {
        return 0;
    }

    @Override
    public TableId getTableId()
    {
        return metadata.id;
    }

    @Override
    public Throwable commit(Throwable accumulate)
    {
        for (SequentialWriter writer : componentWriters.values())
            writer.commit(accumulate);

        return accumulate;
    }

    @Override
    public Throwable abort(Throwable accumulate)
    {
        for (SequentialWriter writer : componentWriters.values())
            writer.abort(accumulate);

        return accumulate;
    }

    @Override
    public void prepareToCommit()
    {
        for (SequentialWriter writer : componentWriters.values())
            writer.prepareToCommit();
    }

    @Override
    public void close()
    {
        for (SequentialWriter writer : componentWriters.values())
            writer.close();
    }

    public void writeComponent(Component.Type type, DataInputPlus in, long size)
    {
        logger.info("Writing component {} to {} length {}", type, componentWriters.get(type).getPath(), size);
        if (in instanceof RebufferingByteBufDataInputPlus)
            write((RebufferingByteBufDataInputPlus) in, size, componentWriters.get(type));
        else
            write(in, size, componentWriters.get(type));
    }

    private void write(RebufferingByteBufDataInputPlus in, long size, SequentialWriter writer)
    {
        logger.info("Block Writing component to {} length {}", writer.getPath(), size);

        try
        {
            long bytesWritten = in.consumeUntil(writer, size);

            if (bytesWritten != size)
                throw new IOException(String.format("Failed to read correct number of bytes from Channel {}", writer));
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, writer.getPath());
        }
    }
}
