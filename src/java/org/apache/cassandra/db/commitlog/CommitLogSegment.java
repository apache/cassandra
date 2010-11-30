/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.DataOutputBuffer;

public class CommitLogSegment
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogSegment.class);

    private final BufferedRandomAccessFile logWriter;
    private final CommitLogHeader header;

    public CommitLogSegment()
    {
        this.header = new CommitLogHeader();
        String logFile = DatabaseDescriptor.getCommitLogLocation() + File.separator + "CommitLog-" + System.currentTimeMillis() + ".log";
        logger.info("Creating new commitlog segment " + logFile);

        try
        {
            logWriter = createWriter(logFile);
            writeHeader();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public static boolean possibleCommitLogFile(String filename)
    {
        return filename.matches("CommitLog-\\d+.log");
    }

    public void writeHeader() throws IOException
    {
        CommitLogHeader.writeCommitLogHeader(header, getHeaderPath());
    }

    private static BufferedRandomAccessFile createWriter(String file) throws IOException
    {
        return new BufferedRandomAccessFile(file, "rw", 128 * 1024);
    }

    public CommitLogSegment.CommitLogContext write(RowMutation rowMutation, Object serializedRow) throws IOException
    {
        long currentPosition = -1L;
        try
        {
            currentPosition = logWriter.getFilePointer();
            CommitLogSegment.CommitLogContext cLogCtx = new CommitLogSegment.CommitLogContext(currentPosition);

            // update header
            for (ColumnFamily columnFamily : rowMutation.getColumnFamilies())
            {
                // we can ignore the serialized map in the header (and avoid deserializing it) since we know we are
                // writing the cfs as they exist now.  check for null cfm in case a cl write goes through after the cf is 
                // defined but before a new segment is created.
                CFMetaData cfm = DatabaseDescriptor.getCFMetaData(columnFamily.id());
                if (cfm == null)
                {
                    logger.error("Attempted to write commit log entry for unrecognized column family: " + columnFamily.id());
                }
                else
                {
                    Integer id = cfm.cfId;
                    if (!header.isDirty(id))
                    {
                        header.turnOn(id, logWriter.getFilePointer());
                        writeHeader();
                    }
                }
            }

            // write mutation, w/ checksum on the size and data
            byte[] bytes;
            Checksum checksum = new CRC32();
            if (serializedRow instanceof DataOutputBuffer)
            {
                bytes = ((DataOutputBuffer) serializedRow).getData();
            }
            else
            {
                assert serializedRow instanceof byte[];
                bytes = (byte[]) serializedRow;
            }

            checksum.update(bytes.length);
            logWriter.writeInt(bytes.length);
            logWriter.writeLong(checksum.getValue());
            logWriter.write(bytes);
            checksum.update(bytes, 0, bytes.length);
            logWriter.writeLong(checksum.getValue());

            return cLogCtx;
        }
        catch (IOException e)
        {
            if (currentPosition != -1)
                logWriter.seek(currentPosition);
            throw e;
        }
    }

    public void sync() throws IOException
    {
        logWriter.sync();
    }

    public void flush() throws IOException
    {
        logWriter.flush();
    }

    public CommitLogContext getContext()
    {
        return new CommitLogContext(logWriter.getFilePointer());
    }

    public CommitLogHeader getHeader()
    {
        return header;
    }

    public String getPath()
    {
        return logWriter.getPath();
    }

    public String getHeaderPath()
    {
        return CommitLogHeader.getHeaderPathFromSegment(this);
    }

    public long length()
    {
        try
        {
            return logWriter.length();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public void close()
    {
        try
        {
            logWriter.close();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    @Override
    public String toString()
    {
        return "CommitLogSegment(" + logWriter.getPath() + ')';
    }

    public class CommitLogContext
    {
        public final long position;

        public CommitLogContext(long position)
        {
            assert position >= 0;
            this.position = position;
        }

        public CommitLogSegment getSegment()
        {
            return CommitLogSegment.this;
        }

        @Override
        public String toString()
        {
            return "CommitLogContext(" +
                   "file='" + logWriter.getPath() + '\'' +
                   ", position=" + position +
                   ')';
        }
    }
}
