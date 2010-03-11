package org.apache.cassandra.db.commitlog;
/*
 * 
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


import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.cassandra.config.CFMetaData;
import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.DataOutputBuffer;

public class CommitLogSegment
{
    private static final Logger logger = Logger.getLogger(CommitLogSegment.class);

    private final BufferedRandomAccessFile logWriter;
    private final CommitLogHeader header;

    public CommitLogSegment()
    {
        this.header = new CommitLogHeader();
        String logFile = DatabaseDescriptor.getLogFileLocation() + File.separator + "CommitLog-" + System.currentTimeMillis() + ".log";
        logger.info("Creating new commitlog segment " + logFile);

        try
        {
            logWriter = createWriter(logFile);
            writeCommitLogHeader(header.toByteArray());
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public void writeHeader() throws IOException
    {
        seekAndWriteCommitLogHeader(header.toByteArray());
    }

    /** writes header at the beginning of the file, then seeks back to current position */
    void seekAndWriteCommitLogHeader(byte[] bytes) throws IOException
    {
        long currentPos = logWriter.getFilePointer();
        logWriter.seek(0);

        writeCommitLogHeader(bytes);

        logWriter.seek(currentPos);
    }

    private void writeCommitLogHeader(byte[] bytes) throws IOException
    {
        logWriter.writeInt(bytes.length);
        logWriter.write(bytes);
        logWriter.sync();
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
            Table table = Table.open(rowMutation.getTable());

            // update header
            for (ColumnFamily columnFamily : rowMutation.getColumnFamilies())
            {
                // we can ignore the serialized map in the header (and avoid deserializing it) since we know we are
                // writing the cfs as they exist now.  check for null cfm in case a cl write goes through after the cf is 
                // defined but before a new segment is created.
                CFMetaData cfm = DatabaseDescriptor.getTableDefinition(table.name).cfMetaData().get(columnFamily.name());
                if (cfm == null)
                {
                    logger.error("Attempted to write commit log entry for unrecognized column family: " + columnFamily.name());
                }
                else
                {
                    int id = cfm.cfId;
                    if (!header.isDirty(id))
                    {
                        header.turnOn(id, logWriter.getFilePointer());
                        seekAndWriteCommitLogHeader(header.toByteArray());
                    }
                }
            }

            // write mutation, w/ checksum
            Checksum checkum = new CRC32();
            if (serializedRow instanceof DataOutputBuffer)
            {
                DataOutputBuffer buffer = (DataOutputBuffer) serializedRow;
                logWriter.writeLong(buffer.getLength());
                logWriter.write(buffer.getData(), 0, buffer.getLength());
                checkum.update(buffer.getData(), 0, buffer.getLength());
            }
            else
            {
                assert serializedRow instanceof byte[];
                byte[] bytes = (byte[]) serializedRow;
                logWriter.writeLong(bytes.length);
                logWriter.write(bytes);
                checkum.update(bytes, 0, bytes.length);
            }
            logWriter.writeLong(checkum.getValue());

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
