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
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.net.AsyncStreamingInputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.lang.String.format;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;

public class SSTableZeroCopyWriter extends SSTable implements SSTableMultiWriter
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableZeroCopyWriter.class);

    private volatile SSTableReader finalReader;
    private final Map<String, ZeroCopySequentialWriter> componentWriters; // indexed by component name

    /**
     * CRC32 of every byte written to Data.db, kept only when the sender did not send a Digest.crc32 of its own.
     * <p>
     * A partial zero-copy stream cannot produce one: its Data.db is byte ranges of a larger file that reach the
     * socket by {@code sendfile} without ever entering the sending process, so a digest there would cost a second
     * full read of the data. Computing it here costs nothing -- the bytes are already in hand on their way to the
     * file -- and produces exactly the same value, because it is a checksum of the same bytes. Null when the
     * manifest named DIGEST, in which case the sender's file is authoritative and is written verbatim like any
     * other component.
     */
    private final CRC32 dataDigest;

    public SSTableZeroCopyWriter(Builder<?, ?> builder,
                                 ILifecycleTransaction txn,
                                 SSTable.Owner owner)
    {
        super(builder, owner);
        txn.trackNew(this);
        this.componentWriters = new HashMap<>();

        Set<Component> unsupported = components.stream()
                                               .filter(c -> !c.type.streamable)
                                               .collect(Collectors.toSet());
        if (!unsupported.isEmpty())
            throw new AssertionError(format("Unsupported streaming components detected: %s", unsupported));

        for (Component c : components)
            componentWriters.put(c.name, makeWriter(descriptor, c));

        this.dataDigest = components.contains(SSTableFormat.Components.DIGEST) ? null : new CRC32();
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

    private ZeroCopySequentialWriter makeWriter(Descriptor descriptor, Component component)
    {
        return new ZeroCopySequentialWriter(descriptor.fileFor(component), ioOptions.writerOptions, false);
    }

    private void write(DataInputPlus in, long size, ZeroCopySequentialWriter out, CRC32 digest) throws FSWriteError
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
                if (digest != null)
                    digest.update(buff, 0, count);
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

        for (ZeroCopySequentialWriter writer : componentWriters.values())
            writer.finish();

        return finished();
    }

    @Override
    public Collection<SSTableReader> finished()
    {
        if (finalReader == null)
        {
            // The sender could not produce a Digest.crc32 for what it sent -- a partial zero-copy stream never has
            // the bytes in process -- so this is where the component comes from. It is a CRC of exactly the file
            // just written, which is what the verifier compares against, and writing it here rather than leaving
            // it out is what keeps `nodetool verify` on a received sstable a whole-file CRC instead of an extended
            // row-by-row verification.
            if (dataDigest != null && components.contains(SSTableFormat.Components.DATA))
                writeDigest();

            finalReader = SSTableReader.open(owner().orElse(null), descriptor, components, metadata);
        }

        return ImmutableList.of(finalReader);
    }

    /**
     * Digest.crc32 the way {@link org.apache.cassandra.io.util.ChecksumWriter#writeFullChecksum} writes it: the
     * plain decimal ASCII of the CRC32, no newline and no prefix, fsynced. Failure to write it is logged rather
     * than thrown: the sstable is complete and correct without the component, and the only consequence is a
     * slower verification.
     */
    private void writeDigest()
    {
        File file = descriptor.fileFor(SSTableFormat.Components.DIGEST);
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(file))
        {
            out.write(String.valueOf(dataDigest.getValue()).getBytes(StandardCharsets.UTF_8));
            out.flush();
            out.sync();
            components.add(SSTableFormat.Components.DIGEST);
        }
        catch (IOException e)
        {
            logger.warn("Failed writing {} for a received sstable; it will verify by extended verification instead",
                        file, e);
            file.deleteIfExists();
        }
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
    public long getTotalRows()
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
        for (ZeroCopySequentialWriter writer : componentWriters.values())
            accumulate = writer.commit(accumulate);
        return accumulate;
    }

    @Override
    public Throwable abort(Throwable accumulate)
    {
        for (ZeroCopySequentialWriter writer : componentWriters.values())
            accumulate = writer.abort(accumulate);
        return accumulate;
    }

    @Override
    public void prepareToCommit()
    {
        for (ZeroCopySequentialWriter writer : componentWriters.values())
            writer.prepareToCommit();
    }

    @Override
    public void close()
    {
        for (ZeroCopySequentialWriter writer : componentWriters.values())
            writer.close();
    }

    public void writeComponent(Component component, DataInputPlus in, long size) throws ClosedChannelException
    {
        ZeroCopySequentialWriter writer = componentWriters.get(component.name);
        logger.info("Writing component {} to {} length {}", component, writer.getPath(), prettyPrintMemory(size));

        // Only Data.db is digested, and only when the sender sent no digest of its own.
        CRC32 digest = component.equals(SSTableFormat.Components.DATA) ? dataDigest : null;

        if (in instanceof AsyncStreamingInputPlus)
            write((AsyncStreamingInputPlus) in, size, writer, digest);
        else
            // this code path is not valid for production and only exists to simplify unit tests
            write(in, size, writer, digest);
    }

    private void write(AsyncStreamingInputPlus in, long size, ZeroCopySequentialWriter writer, CRC32 digest) throws ClosedChannelException
    {
        logger.info("Block Writing component to {} length {}", writer.getPath(), prettyPrintMemory(size));

        try
        {
            in.consume(buffer -> {
                // duplicate(): update() consumes the buffer it is given, and the channel still needs it.
                if (digest != null)
                    digest.update(buffer.duplicate());
                return writer.writeDirectlyToChannel(buffer);
            }, size);
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

    private static class ZeroCopySequentialWriter extends SequentialWriter
    {
        private ZeroCopySequentialWriter(File file, SequentialWriterOption option, boolean strictFlushing)
        {
            super(file, ByteBufferUtil.EMPTY_BYTE_BUFFER, option, strictFlushing);
        }

        /**
         * In production, we do not expect this method to be called, as only writeDirectlyToChannel should be invoked for zero-copy.
         * <p>
         * This method only exists for tests.
         */
        @Override
        public void write(byte[] b, int off, int len) throws IOException
        {
            if (this.buffer == ByteBufferUtil.EMPTY_BYTE_BUFFER)
                this.buffer = option.allocateBuffer();
            super.write(b, off, len);
        }
    }
}
