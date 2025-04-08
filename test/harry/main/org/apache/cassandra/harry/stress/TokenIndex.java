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

package org.apache.cassandra.harry.stress;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Indexed reader over a merged token data file and its companion {@code .idx} file,
 * both produced by {@link TokenIndexGenerator#merge}.
 *
 * <p>Data file format (sorted by token, then pd): {@code [token:8][pd:8][count:4][lts:8 * count]} repeated.
 * <p>Index file format (sorted by token): {@code [token:8][offset:8]} repeated, fixed 16-byte entries.
 *
 * <p>The index is binary-searchable because entries are fixed-size and token-sorted.
 * After locating the start of a range in the index, entries are read sequentially from the data file.
 */
public class TokenIndex implements Closeable
{
    private final FileInputStream dataFis;
    private final FileInputStream idxFis;
    private final FileChannel dataChannel;
    private final FileChannel idxChannel;
    private final long entryCount;
    private final ByteBuffer idxBuf = ByteBuffer.allocate(16); // token(8) + offset(8)

    public TokenIndex(File dataFile, File idxFile)
    {
        try
        {
            this.dataFis = new FileInputStream(dataFile);
            this.idxFis = new FileInputStream(idxFile);
            this.dataChannel = dataFis.getChannel();
            this.idxChannel = idxFis.getChannel();
            this.entryCount = idxChannel.size() / 16;
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    /** Convenience overload accepting Cassandra's {@link org.apache.cassandra.io.util.File}. */
    public TokenIndex(org.apache.cassandra.io.util.File dataFile, org.apache.cassandra.io.util.File idxFile)
    {
        this(dataFile.toJavaIOFile(), idxFile.toJavaIOFile());
    }

    public long entryCount()
    {
        return entryCount;
    }

    public long lookup(long token)
    {
        long idx = lowerBound(token);
        if (idx >= entryCount)
            return -1;
        readIdxEntry(idx);
        long foundToken = idxBuf.getLong();
        if (foundToken != token)
            return -1;
        return idxBuf.getLong();
    }

    public EntryIterator range(long minToken, long maxToken)
    {
        long startIdx = lowerBound(minToken);
        if (startIdx >= entryCount)
            return new EntryIterator(dataChannel, 0, maxToken, false);
        readIdxEntry(startIdx);
        idxBuf.getLong(); // skip token
        long dataOffset = idxBuf.getLong();
        return new EntryIterator(dataChannel, dataOffset, maxToken, true);
    }

    private long lowerBound(long target)
    {
        long lo = 0, hi = entryCount;
        while (lo < hi)
        {
            long mid = lo + (hi - lo) / 2;
            if (readIdxToken(mid) < target)
                lo = mid + 1;
            else
                hi = mid;
        }
        return lo;
    }

    private long readIdxToken(long index)
    {
        readIdxEntry(index);
        return idxBuf.getLong();
    }

    private void readIdxEntry(long index)
    {
        idxBuf.clear();
        long pos = index * 16;
        while (idxBuf.hasRemaining())
        {
            try
            {
                int n = idxChannel.read(idxBuf, pos + idxBuf.position());
                if (n < 0)
                    throw new IllegalStateException("Unexpected EOF in index at entry " + index);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        idxBuf.flip();
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            dataChannel.close();
        }
        finally
        {
            try
            {
                dataFis.close();
            }
            finally
            {
                try
                {
                    idxChannel.close();
                }
                finally
                {
                    idxFis.close();
                }
            }
        }
    }

    public static class EntryIterator
    {
        private final FileChannel channel;
        private final long maxToken;
        private final ByteBuffer headerBuf = ByteBuffer.allocate(20); // token(8) + pd(8) + count(4)
        private long currentToken;
        private long currentPd;
        private int currentLtsCount;
        private long ltsDataOffset;
        private boolean hasMore;

        EntryIterator(FileChannel channel, long startOffset, long maxToken, boolean hasData)
        {
            this.channel = channel;
            this.maxToken = maxToken;
            if (hasData)
            {
                try
                {
                    channel.position(startOffset);
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
                readHeader();
            }
            else
            {
                this.hasMore = false;
            }
        }

        public boolean hasNext()
        {
            return hasMore;
        }

        public long token()
        {
            return currentToken;
        }

        public long pd()
        {
            return currentPd;
        }

        public int ltsCount()
        {
            return currentLtsCount;
        }

        public long[] readLts()
        {
            try
            {
                ByteBuffer buf = ByteBuffer.allocate(currentLtsCount * Long.BYTES);
                long filePos = ltsDataOffset;
                while (buf.hasRemaining())
                {
                    int n = channel.read(buf, filePos);
                    if (n < 0)
                        throw new IOException("Unexpected EOF reading LTS data");
                    filePos += n;
                }
                buf.flip();
                long[] lts = new long[currentLtsCount];
                for (int i = 0; i < currentLtsCount; i++)
                    lts[i] = buf.getLong();
                return lts;
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        public void advance()
        {
            try
            {
                channel.position(ltsDataOffset + (long) currentLtsCount * Long.BYTES);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
            readHeader();
        }

        private void readHeader()
        {
            headerBuf.clear();
            try
            {
                while (headerBuf.hasRemaining())
                {
                    int read = channel.read(headerBuf);
                    if (read < 0)
                    {
                        hasMore = false;
                        return;
                    }
                }
                headerBuf.flip();
                currentToken = headerBuf.getLong();
                currentPd = headerBuf.getLong();
                currentLtsCount = headerBuf.getInt();
                ltsDataOffset = channel.position();
                hasMore = currentToken <= maxToken;
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }
    }
}