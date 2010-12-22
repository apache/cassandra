/**
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

package org.apache.cassandra.db.commitlog;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.io.util.FileUtils;

public class CommitLogHeader
{
    public static String getHeaderPathFromSegment(CommitLogSegment segment)
    {
        return getHeaderPathFromSegmentPath(segment.getPath());
    }

    public static String getHeaderPathFromSegmentPath(String segmentPath)
    {
        return segmentPath + ".header";
    }

    public static CommitLogHeaderSerializer serializer = new CommitLogHeaderSerializer();

    private Map<Integer, Integer> cfDirtiedAt; // position at which each CF was last flushed

    CommitLogHeader()
    {
        this(new HashMap<Integer, Integer>());
    }
    
    /*
     * This ctor is used while deserializing. This ctor
     * also builds an index of position to column family
     * Id.
    */
    private CommitLogHeader(Map<Integer, Integer> cfDirtiedAt)
    {
        this.cfDirtiedAt = cfDirtiedAt;
    }
        
    boolean isDirty(Integer cfId)
    {
        return cfDirtiedAt.containsKey(cfId);
    } 
    
    int getPosition(Integer cfId)
    {
        Integer x = cfDirtiedAt.get(cfId);
        return x == null ? 0 : x;
    }
    
    void turnOn(Integer cfId, long position)
    {
        assert position >= 0 && position <= Integer.MAX_VALUE;
        cfDirtiedAt.put(cfId, (int)position);
    }

    void turnOff(Integer cfId)
    {
        cfDirtiedAt.remove(cfId);
    }

    boolean isSafeToDelete() throws IOException
    {
        return cfDirtiedAt.isEmpty();
    }
    
    // we use cf ids. getting the cf names would be pretty pretty expensive.
    public String toString()
    {
        StringBuilder sb = new StringBuilder("");
        sb.append("CLH(dirty+flushed={");
        for (Map.Entry<Integer, Integer> entry : cfDirtiedAt.entrySet())
        {       
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
        }
        sb.append("})");
        return sb.toString();
    }

    public String dirtyString()
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, Integer> entry : cfDirtiedAt.entrySet())
            sb.append(entry.getKey()).append(", ");
        return sb.toString();
    }

    static void writeCommitLogHeader(CommitLogHeader header, String headerFile) throws IOException
    {
        DataOutputStream out = null;
        try
        {
            /*
             * FileOutputStream doesn't sync on flush/close.
             * As headers are "optional" now there is no reason to sync it.
             * This provides nearly double the performance of BRAF, more under heavey load.
             */
            out = new DataOutputStream(new FileOutputStream(headerFile));
            serializer.serialize(header, out);
        }
        finally
        {
            if (out != null)
                out.close();
        }
    }

    static CommitLogHeader readCommitLogHeader(String headerFile) throws IOException
    {
        DataInputStream reader = null;
        try
        {
            reader = new DataInputStream(new BufferedInputStream(new FileInputStream(headerFile)));
            return serializer.deserialize(reader);
        }
        finally
        {
            FileUtils.closeQuietly(reader);
        }
    }

    int getReplayPosition()
    {
        return cfDirtiedAt.isEmpty() ? -1 : Collections.min(cfDirtiedAt.values());
    }

    static class CommitLogHeaderSerializer implements ICompactSerializer2<CommitLogHeader>
    {
        public void serialize(CommitLogHeader clHeader, DataOutput dos) throws IOException
        {
            Checksum checksum = new CRC32();

            // write the first checksum after the fixed-size part, so we won't read garbage lastFlushedAt data.
            dos.writeInt(clHeader.cfDirtiedAt.size()); // 4
            checksum.update(clHeader.cfDirtiedAt.size());
            dos.writeLong(checksum.getValue());

            // write the 2nd checksum after the lastflushedat map
            for (Map.Entry<Integer, Integer> entry : clHeader.cfDirtiedAt.entrySet())
            {
                dos.writeInt(entry.getKey()); // 4
                checksum.update(entry.getKey());
                dos.writeInt(entry.getValue()); // 4
                checksum.update(entry.getValue());
            }
            dos.writeLong(checksum.getValue());
        }

        public CommitLogHeader deserialize(DataInput dis) throws IOException
        {
            Checksum checksum = new CRC32();

            int lastFlushedAtSize = dis.readInt();
            checksum.update(lastFlushedAtSize);
            if (checksum.getValue() != dis.readLong())
            {
                throw new IOException("Invalid or corrupt commitlog header");
            }
            Map<Integer, Integer> lastFlushedAt = new HashMap<Integer, Integer>();
            for (int i = 0; i < lastFlushedAtSize; i++)
            {
                int key = dis.readInt();
                checksum.update(key);
                int value = dis.readInt();
                checksum.update(value);
                lastFlushedAt.put(key, value);
            }
            if (checksum.getValue() != dis.readLong())
            {
                throw new IOException("Invalid or corrupt commitlog header");
            }

            return new CommitLogHeader(lastFlushedAt);
        }
    }
}
