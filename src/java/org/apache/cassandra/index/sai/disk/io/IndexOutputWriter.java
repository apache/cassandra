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
package org.apache.cassandra.index.sai.disk.io;

import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.lucene.store.IndexOutput;

/**
 * This is a wrapper over a Cassandra {@link SequentialWriter} that provides a Lucene {@link IndexOutput}
 * interface for the Lucene index writers.
 */
@NotThreadSafe
public class IndexOutputWriter extends IndexOutput
{
    private static final Logger logger = LoggerFactory.getLogger(IndexOutputWriter.class);

    /**
     * the byte order of `out`'s native writeX operations doesn't matter,
     * because we only use `write(byte[])` and `writeByte` methods. IndexOutput calls these
     * (via DataOutput) with methods that enforce LittleEndian-ness.
    */
    private final SequentialWriter out;
    private boolean closed;

    public IndexOutputWriter(SequentialWriter out)
    {
        super(out.getPath(), out.getPath());
        this.out = out;
    }

    public void skipBytes(long length) throws IOException
    {
        this.out.skipBytes(length);
    }

    public File getFile()
    {
        return new File(out.getPath());
    }

    @Override
    public long getChecksum() throws IOException
    {
        return ((IndexFileUtils.ChecksummingWriter)out).getChecksum();
    }

    @Override
    public long getFilePointer()
    {
        return out.position();
    }

    @Override
    public void writeBytes(byte[] bytes, int offset, int len) throws IOException
    {
        out.write(bytes, offset, len);
    }

    @Override
    public void writeByte(byte b) throws IOException
    {
        out.writeByte(b);
    }

    @Override
    public void close()
    {
        // IndexOutput#close contract allows any output to be closed multiple times,
        // and Lucene does it in few places. SequentialWriter can be closed once.
        if (!closed)
        {
            if (logger.isTraceEnabled())
            {
                logger.trace("Closing index output: {}", this);
            }

            // The writer should sync its contents to disk before closing...
            out.close();
            closed = true;
        }
    }

    @Override
    public String toString()
    {
        String checksum;
        try {
            checksum = String.valueOf(getChecksum());
        } catch (IOException e) {
            checksum = "unknown due to I/O error: " + e;
        }
        return MoreObjects.toStringHelper(this)
                          .add("path", out.getPath())
                          .add("bytesWritten", getFilePointer())
                          .add("crc", checksum)
                          .toString();
    }

    /**
     * Returns {@link SequentialWriter} associated with this writer. Convenient when interacting with Cassandra codebase to
     * write files to disk. Note that all bytes written to the returned writer will still contribute to the checksum.
     *
     * @return {@link SequentialWriter} associated with this writer
     */
    public SequentialWriter asSequentialWriter()
    {
        return out;
    }
}
