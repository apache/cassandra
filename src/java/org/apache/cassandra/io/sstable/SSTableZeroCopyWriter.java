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
package org.apache.cassandra.io.sstable;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.net.AsyncStreamingInputPlus;
import org.apache.cassandra.schema.TableId;

import static java.lang.String.format;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;

public class SSTableZeroCopyWriter extends SSTable implements SSTableMultiWriter
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableZeroCopyWriter.class);

    private volatile SSTableReader finalReader;
    private final Map<String, SequentialWriter> componentWriters; // indexed by component name

    public SSTableZeroCopyWriter(Builder<?, ?> builder,
                                 LifecycleNewTracker lifecycleNewTracker,
                                 SSTable.Owner owner)
    {
        super(builder, owner);

        lifecycleNewTracker.trackNew(this);
        this.componentWriters = new HashMap<>();

        Set<Component> unsupported = components.stream()
                                               .filter(c -> !c.type.streamable)
                                               .collect(Collectors.toSet());
        if (!unsupported.isEmpty())
            throw new AssertionError(format("Unsupported streaming components detected: %s", unsupported));

        for (Component c : components)
            componentWriters.put(c.name, makeWriter(descriptor, c));
    }

    @Override
    public DecoratedKey getFirst()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public DecoratedKey getLast()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AbstractBounds<Token> getBounds()
    {
        throw new UnsupportedOperationException();
    }

    private SequentialWriter makeWriter(Descriptor descriptor, Component component)
    {
        return new SequentialWriter(descriptor.fileFor(component), ioOptions.writerOptions, false);
    }

    private void write(DataInputPlus in, long size, SequentialWriter out) throws FSWriteError
    {
        final int BUFFER_SIZE = 1 << 20;
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
            out.sync(); // finish will also call sync(). Leaving here to get stuff flushed as early as possible
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, out.getFile());
        }
    }

    @Override
    public void append(UnfilteredRowIterator partition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<SSTableReader> finish(boolean openResult)
    {
        setOpenResult(openResult);

        for (SequentialWriter writer : componentWriters.values())
            writer.finish();

        return finished();
    }

    @Override
    public Collection<SSTableReader> finished()
    {
        if (finalReader == null)
            finalReader = SSTableReader.open(owner().orElse(null), descriptor, components, metadata);

        return ImmutableList.of(finalReader);
    }

    @Override
    public SSTableMultiWriter setOpenResult(boolean openResult)
    {
        return null;
    }

    @Override
    public long getBytesWritten()
    {
        return 0;
    }

    @Override
    public long getOnDiskBytesWritten()
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
            accumulate = writer.commit(accumulate);
        return accumulate;
    }

    @Override
    public Throwable abort(Throwable accumulate)
    {
        for (SequentialWriter writer : componentWriters.values())
            accumulate = writer.abort(accumulate);
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

    public void writeComponent(Component component, DataInputPlus in, long size) throws ClosedChannelException
    {
        SequentialWriter writer = componentWriters.get(component.name);
        logger.info("Writing component {} to {} length {}", component, writer.getPath(), prettyPrintMemory(size));

        if (in instanceof AsyncStreamingInputPlus)
            write((AsyncStreamingInputPlus) in, size, writer);
        else
            write(in, size, writer);
    }

    private void write(AsyncStreamingInputPlus in, long size, SequentialWriter writer) throws ClosedChannelException
    {
        logger.info("Block Writing component to {} length {}", writer.getPath(), prettyPrintMemory(size));

        try
        {
            in.consume(writer::writeDirectlyToChannel, size);
            writer.sync();
        }
        catch (EOFException e)
        {
            in.close();
        }
        catch (ClosedChannelException e)
        {
            // FSWriteError triggers disk failure policy, but if we get a connection issue we do not want to do that
            // so rethrow so the error handling logic higher up is able to deal with this
            // see CASSANDRA-17116
            throw e;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, writer.getPath());
        }
    }
}
