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
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.net.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.ColumnFamily;

public class CommitLogSegment
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogSegment.class);
    private static Pattern COMMIT_LOG_FILE_PATTERN = Pattern.compile("CommitLog-(\\d+).log");

    public final long id;
    private final SequentialWriter logWriter;
    private long finalSize = -1;

    // cache which cf is dirty in this segment to avoid having to lookup all ReplayPositions to decide if we could delete this segment
    public final Map<Integer, Integer> cfLastWrite = new HashMap<Integer, Integer>();

    public CommitLogSegment()
    {
        id = System.currentTimeMillis();
        String logFile = DatabaseDescriptor.getCommitLogLocation() + File.separator + "CommitLog-" + id + ".log";
        logger.info("Creating new commitlog segment " + logFile);

        try
        {
            logWriter = createWriter(logFile);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    // assume filename is a 'possibleCommitLogFile()'
    public static long idFromFilename(String filename)
    {
        Matcher matcher = COMMIT_LOG_FILE_PATTERN.matcher(filename);
        try
        {
            if (matcher.matches())
                return Long.valueOf(matcher.group(1));
            else
                return -1L;
        }
        catch (NumberFormatException e)
        {
            return -1L;
        }
    }

    public static boolean possibleCommitLogFile(String filename)
    {
        return COMMIT_LOG_FILE_PATTERN.matcher(filename).matches();
    }

    private static SequentialWriter createWriter(String file) throws IOException
    {
        return SequentialWriter.open(new File(file), true);
    }

    public ReplayPosition write(RowMutation rowMutation) throws IOException
    {
        ReplayPosition cLogCtx = getContext();

        try
        {
            for (ColumnFamily columnFamily : rowMutation.getColumnFamilies())
            {
                // check for null cfm in case a cl write goes through after the cf is
                // defined but before a new segment is created.
                CFMetaData cfm = Schema.instance.getCFMetaData(columnFamily.id());
                if (cfm == null)
                {
                    logger.error("Attempted to write commit log entry for unrecognized column family: " + columnFamily.id());
                }
                else
                {
                    turnOn(cfm.cfId, cLogCtx.position);
                }
            }

            // write mutation, w/ checksum on the size and data
            Checksum checksum = new CRC32();
            byte[] serializedRow = rowMutation.getSerializedBuffer(MessagingService.version_);
            checksum.update(serializedRow.length);
            logWriter.stream.writeInt(serializedRow.length);
            logWriter.stream.writeLong(checksum.getValue());
            logWriter.write(serializedRow);
            checksum.update(serializedRow, 0, serializedRow.length);
            logWriter.stream.writeLong(checksum.getValue());

            return cLogCtx;
        }
        catch (IOException e)
        {
            logWriter.truncate(cLogCtx.position);
            throw e;
        }
    }

    public void sync() throws IOException
    {
        logWriter.sync();
    }

    public ReplayPosition getContext()
    {
        long position = logWriter.getFilePointer();
        assert position <= Integer.MAX_VALUE;
        return new ReplayPosition(id, (int) position);
    }

    public String getPath()
    {
        return logWriter.getPath();
    }

    public String getName()
    {
        return logWriter.getPath().substring(logWriter.getPath().lastIndexOf(File.separator) + 1);
    }

    public long length()
    {
        if (finalSize >= 0)
            return finalSize;
        
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
        if (finalSize >= 0)
            return;

        try
        {
            finalSize = logWriter.length();
            logWriter.close();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    void turnOn(Integer cfId, Integer position)
    {
        cfLastWrite.put(cfId, position);
    }

    /**
     * Turn the dirty bit off only if there has been no write since the flush
     * position was grabbed.
     */
    void turnOffIfNotWritten(Integer cfId, Integer flushPosition)
    {
        Integer lastWritten = cfLastWrite.get(cfId);
        if (lastWritten == null || lastWritten < flushPosition)
            cfLastWrite.remove(cfId);
    }

    void turnOff(Integer cfId)
    {
        cfLastWrite.remove(cfId);
    }

    // For debugging, not fast
    String dirtyString()
    {
        StringBuilder sb = new StringBuilder();
        for (Integer cfId : cfLastWrite.keySet())
        {
            CFMetaData m = Schema.instance.getCFMetaData(cfId);
            sb.append(m == null ? "<deleted>" : m.cfName).append(" (").append(cfId).append("), ");
        }
        return sb.toString();
    }

    boolean isSafeToDelete()
    {
        return cfLastWrite.isEmpty();
    }

    @Override
    public String toString()
    {
        return "CommitLogSegment(" + logWriter.getPath() + ')';
    }
}
