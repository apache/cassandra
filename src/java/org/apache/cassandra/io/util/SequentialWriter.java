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
package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.function.LongConsumer;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.concurrent.Transactional;

import static org.apache.cassandra.utils.Throwables.merge;

/**
 * Adds buffering, mark, and fsyncing to OutputStream.  We always fsync on close; we may also
 * fsync incrementally if Config.trickle_fsync is enabled.
 */
public class SequentialWriter extends BufferedDataOutputStreamPlus implements Transactional
{
    // absolute path to the given file
    private final String filePath;
    private final File file;

    // Offset for start of buffer relative to underlying file
    protected long bufferOffset;

    protected final FileChannel fchannel;

    //Allow derived classes to specify writing to the channel
    //directly shouldn't happen because they intercept via doFlush for things
    //like compression or checksumming
    //Another hack for this value is that it also indicates that flushing early
    //should not occur, flushes aligned with buffer size are desired
    //Unless... it's the last flush. Compression and checksum formats
    //expect block (same as buffer size) alignment for everything except the last block
    private final boolean strictFlushing;

    // whether to do trickling fsync() to avoid sudden bursts of dirty buffer flushing by kernel causing read
    // latency spikes
    private final SequentialWriterOption option;
    private int bytesSinceTrickleFsync = 0;

    protected long lastFlushOffset;

    protected LongConsumer runPostFlush;

    private final TransactionalProxy txnProxy = txnProxy();

    // due to lack of multiple-inheritance, we proxy our transactional implementation
    protected class TransactionalProxy extends AbstractTransactional
    {
        @Override
        protected Throwable doPreCleanup(Throwable accumulate)
        {
            // close is idempotent
            try { channel.close(); }
            catch (Throwable t) { accumulate = merge(accumulate, t); }

            if (buffer != null)
            {
                try { FileUtils.clean(buffer); }
                catch (Throwable t) { accumulate = merge(accumulate, t); }
                buffer = null;
            }

            return accumulate;
        }

        @Override
        protected void doPrepare()
        {
            syncInternal();
        }

        @Override
        protected Throwable doCommit(Throwable accumulate)
        {
            return accumulate;
        }

        @Override
        protected Throwable doAbort(Throwable accumulate)
        {
            return accumulate;
        }
    }

    // TODO: we should specify as a parameter if we permit an existing file or not
    private static FileChannel openChannel(File file)
    {
        try
        {
            if (file.exists())
            {
                return FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
            }
            else
            {
                FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
                try
                {
                    SyncUtil.trySyncDir(file.parent());
                }
                catch (Throwable t)
                {
                    try { channel.close(); }
                    catch (Throwable t2) { t.addSuppressed(t2); }
                }
                return channel;
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create heap-based, non-compressed SequenialWriter with default buffer size(64k).
     *
     * @param file File to write
     */
    public SequentialWriter(File file)
    {
       this(file, SequentialWriterOption.DEFAULT);
    }

    /**
     * Create SequentialWriter for given file with specific writer option.
     *
     * @param file File to write
     * @param option Writer option
     */
    public SequentialWriter(File file, SequentialWriterOption option)
    {
        this(file, option, true);
    }

    /**
     * Create SequentialWriter for given file with specific writer option.
     * @param file
     * @param option
     * @param strictFlushing
     */
    public SequentialWriter(File file, SequentialWriterOption option, boolean strictFlushing)
    {
        super(openChannel(file), option.allocateBuffer());
        this.strictFlushing = strictFlushing;
        this.fchannel = (FileChannel)channel;

        this.file = file;
        this.filePath = file.absolutePath();

        this.option = option;
    }

    public void skipBytes(long numBytes) throws IOException
    {
        flush();
        fchannel.position(fchannel.position() + numBytes);
        bufferOffset = fchannel.position();
    }

    /**
     * Synchronize file contents with disk.
     */
    public void sync()
    {
        syncInternal();
    }

    protected void syncDataOnlyInternal()
    {
        try
        {
            SyncUtil.force(fchannel, false);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    /*
     * This is only safe to call before truncation or close for CompressedSequentialWriter
     * Otherwise it will leave a non-uniform size compressed block in the middle of the file
     * and the compressed format can't handle that.
     */
    protected void syncInternal()
    {
        doFlush(0);
        syncDataOnlyInternal();
    }

    @Override
    protected void doFlush(int count)
    {
        flushData();

        if (option.trickleFsync())
        {
            bytesSinceTrickleFsync += buffer.position();
            if (bytesSinceTrickleFsync >= option.trickleFsyncByteInterval())
            {
                syncDataOnlyInternal();
                bytesSinceTrickleFsync = 0;
            }
        }

        // Remember that we wrote, so we don't write it again on next flush().
        resetBuffer();
    }

    public void setPostFlushListener(LongConsumer runPostFlush)
    {
        assert this.runPostFlush == null;
        this.runPostFlush = runPostFlush;
    }

    /**
     * Override this method instead of overriding flush()
     * @throws FSWriteError on any I/O error.
     */
    protected void flushData()
    {
        try
        {
            buffer.flip();
            channel.write(buffer);
            lastFlushOffset += buffer.position();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
        if (runPostFlush != null)
            runPostFlush.accept(getLastFlushOffset());
    }

    @Override
    public boolean hasPosition()
    {
        return true;
    }

    @Override
    public long position()
    {
        return current();
    }

    @Override
    public int maxBytesInPage()
    {
        return PageAware.PAGE_SIZE;
    }

    @Override
    public void padToPageBoundary() throws IOException
    {
        PageAware.pad(this);
    }

    @Override
    public int bytesLeftInPage()
    {
        return PageAware.bytesLeftInPage(position());
    }

    @Override
    public long paddedPosition()
    {
        return PageAware.padded(position());
    }

    /**
     * Returns the current file pointer of the underlying on-disk file.
     * Note that since write works by buffering data, the value of this will increase by buffer
     * size and not every write to the writer will modify this value.
     * Furthermore, for compressed files, this value refers to compressed data, while the
     * writer getFilePointer() refers to uncompressedFile
     *
     * @return the current file pointer
     */
    public long getOnDiskFilePointer()
    {
        return position();
    }

    public long getEstimatedOnDiskBytesWritten()
    {
        return getOnDiskFilePointer();
    }

    public long length()
    {
        try
        {
            return Math.max(current(), fchannel.size());
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }
    }

    public String getPath()
    {
        return filePath;
    }

    public File getFile()
    {
        return file;
    }

    protected void resetBuffer()
    {
        bufferOffset = current();
        buffer.clear();
    }

    protected long current()
    {
        return bufferOffset + (buffer == null ? 0 : buffer.position());
    }

    public DataPosition mark()
    {
        return new BufferedFileWriterMark(current());
    }

    /**
     * Drops all buffered data that's past the limits of our new file mark + buffer capacity, or syncs and truncates
     * the underlying file to the marked position
     */
    public void resetAndTruncate(DataPosition mark)
    {
        assert mark instanceof BufferedFileWriterMark;

        long previous = current();
        long truncateTarget = ((BufferedFileWriterMark) mark).pointer;

        // If we're resetting to a point within our buffered data, just adjust our buffered position to drop bytes to
        // the right of the desired mark.
        if (previous - truncateTarget <= buffer.position())
        {
            buffer.position(buffer.position() - ((int) (previous - truncateTarget)));
            return;
        }

        // synchronize current buffer with disk - we don't want any data loss
        syncInternal();

        // truncate file to given position
        truncate(truncateTarget);

        try
        {
            fchannel.position(truncateTarget);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }

        bufferOffset = truncateTarget;
        resetBuffer();
    }

    public long getLastFlushOffset()
    {
        return lastFlushOffset;
    }

    public void truncate(long toSize)
    {
        try
        {
            fchannel.truncate(toSize);
            lastFlushOffset = toSize;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    public boolean isOpen()
    {
        return channel.isOpen();
    }

    @Override
    public final void prepareToCommit()
    {
        txnProxy.prepareToCommit();
    }

    @Override
    public final Throwable commit(Throwable accumulate)
    {
        return txnProxy.commit(accumulate);
    }

    @Override
    public final Throwable abort(Throwable accumulate)
    {
        return txnProxy.abort(accumulate);
    }

    @Override
    public final void close()
    {
        if (option.finishOnClose())
            txnProxy.finish();
        else
            txnProxy.close();
    }

    public int writeDirectlyToChannel(ByteBuffer buf) throws IOException
    {
        if (strictFlushing)
            throw new UnsupportedOperationException();
        // Don't allow writes to the underlying channel while data is buffered
        flush();
        return channel.write(buf);
    }

    public final void finish()
    {
        txnProxy.finish();
    }

    protected TransactionalProxy txnProxy()
    {
        return new TransactionalProxy();
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class BufferedFileWriterMark implements DataPosition
    {
        final long pointer;

        public BufferedFileWriterMark(long pointer)
        {
            this.pointer = pointer;
        }
    }
}