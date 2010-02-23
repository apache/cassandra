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
import java.util.BitSet;
import java.util.Arrays;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.utils.BitSetSerializer;

class CommitLogHeader
{
    private static CommitLogHeaderSerializer serializer = new CommitLogHeaderSerializer();

    static CommitLogHeaderSerializer serializer()
    {
        return serializer;
    }
        
    static int getLowestPosition(CommitLogHeader clHeader)
    {
        int minPosition = Integer.MAX_VALUE;
        for ( int position : clHeader.lastFlushedAt)
        {
            if ( position < minPosition && position > 0)
            {
                minPosition = position;
            }
        }
        
        if(minPosition == Integer.MAX_VALUE)
            minPosition = 0;
        return minPosition;
    }

    private BitSet dirty; // columnfamilies with un-flushed data in this CommitLog
    private int[] lastFlushedAt; // position at which each CF was last flushed
    
    CommitLogHeader(int size)
    {
        dirty = new BitSet(size);
        lastFlushedAt = new int[size];
    }
    
    /*
     * This ctor is used while deserializing. This ctor
     * also builds an index of position to column family
     * Id.
    */
    CommitLogHeader(BitSet dirty, int[] lastFlushedAt)
    {
        this.dirty = dirty;
        this.lastFlushedAt = lastFlushedAt;
    }
        
    boolean isDirty(int index)
    {
        return dirty.get(index);
    } 
    
    int getPosition(int index)
    {
        return lastFlushedAt[index];
    }
    
    void turnOn(int index, long position)
    {
        dirty.set(index);
        lastFlushedAt[index] = (int) position;
    }

    void turnOff(int index)
    {
        dirty.set(index, false);
        lastFlushedAt[index] = 0;
    }

    boolean isSafeToDelete() throws IOException
    {
        return dirty.isEmpty();
    }

    byte[] toByteArray() throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);        
        CommitLogHeader.serializer().serialize(this, dos);
        return bos.toByteArray();
    }
    
    public String toString()
    {
        StringBuilder sb = new StringBuilder("");
        sb.append("CLH(dirty={");
        for ( int i = 0; i < dirty.size(); ++i )
        {
            if (dirty.get(i))
            {
                sb.append(Table.TableMetadata.getColumnFamilyName(i)).append(", ");
            }
        }
        sb.append("}, flushed={");
        for (int i = 0; i < lastFlushedAt.length; i++)
        {
            sb.append(Table.TableMetadata.getColumnFamilyName(i)).append(": ").append(lastFlushedAt[i]).append(", ");
        }
        sb.append("})");
        return sb.toString();
    }

    public String dirtyString()
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < dirty.length(); i++)
        {
            if (dirty.get(i))
            {
                sb.append(i).append(", ");
            }
        }
        return sb.toString();
    }

    static CommitLogHeader readCommitLogHeader(BufferedRandomAccessFile logReader) throws IOException
    {
        int size = (int)logReader.readLong();
        byte[] bytes = new byte[size];
        logReader.read(bytes);
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        return serializer().deserialize(new DataInputStream(byteStream));
    }

    public int getColumnFamilyCount()
    {
        return lastFlushedAt.length;
    }

    static class CommitLogHeaderSerializer implements ICompactSerializer<CommitLogHeader>
    {
        public void serialize(CommitLogHeader clHeader, DataOutputStream dos) throws IOException
        {
            BitSetSerializer.serialize(clHeader.dirty, dos);
            dos.writeInt(clHeader.lastFlushedAt.length);
            for (int position : clHeader.lastFlushedAt)
            {
                dos.writeInt(position);
            }
        }

        public CommitLogHeader deserialize(DataInputStream dis) throws IOException
        {
            BitSet bitFlags = BitSetSerializer.deserialize(dis);
            int[] position = new int[dis.readInt()];
            for (int i = 0; i < position.length; ++i)
            {
                position[i] = dis.readInt();
            }
            return new CommitLogHeader(bitFlags, position);
        }
    }
}
