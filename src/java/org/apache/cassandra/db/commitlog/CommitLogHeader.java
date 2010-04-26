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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.utils.Pair;

class CommitLogHeader
{    
    private static CommitLogHeaderSerializer serializer = new CommitLogHeaderSerializer();

    static int getLowestPosition(CommitLogHeader clheader)
    {
        return clheader.lastFlushedAt.size() == 0 ? 0 : Collections.min(clheader.lastFlushedAt.values(), new Comparator<Integer>(){
            public int compare(Integer o1, Integer o2)
            {
                if (o1 == 0)
                    return 1;
                else if (o2 == 0)
                    return -1;
                else
                    return o1 - o2;
            }
        });
    }

    private Map<Integer, Integer> lastFlushedAt; // position at which each CF was last flushed
    private final byte[] serializedCfMap; // serialized. only needed during commit log recovery.
    private final int cfCount; // we keep this in case cfcount changes in the interim (size of lastFlushedAt is not a good indication).
    
    private transient final int maxSerializedSize;
    private transient Map<Pair<String, String>, Integer> cfIdMap; // only needed during recovery. created from this.serializedCfMap.
    
    CommitLogHeader()
    {
        this(new HashMap<Integer, Integer>(), serializeCfIdMap(CFMetaData.getCfIdMap()), CFMetaData.getCfIdMap().size());
    }
    
    /*
     * This ctor is used while deserializing. This ctor
     * also builds an index of position to column family
     * Id.
    */
    private CommitLogHeader(Map<Integer, Integer> lastFlushedAt, byte[] serializedCfMap, int cfCount)
    {
        this.cfCount = cfCount;
        this.lastFlushedAt = lastFlushedAt;
        this.serializedCfMap = serializedCfMap;
        assert lastFlushedAt.size() <= cfCount;
        // (size of lastFlushedAt) + (size of map buf) + (size of cfCount int)
        maxSerializedSize = (8 * cfCount + 4) + (serializedCfMap.length + 4) + (4);
    }
        
    boolean isDirty(int cfId)
    {
        return lastFlushedAt.containsKey(cfId);
    } 
    
    int getPosition(int index)
    {
        Integer x = lastFlushedAt.get(index);
        return x == null ? 0 : x;
    }
    
    void turnOn(int cfId, long position)
    {
        lastFlushedAt.put(cfId, (int)position);
    }

    void turnOff(int cfId)
    {
        lastFlushedAt.remove(cfId);
    }

    boolean isSafeToDelete() throws IOException
    {
        return lastFlushedAt.isEmpty();
    }
    
    synchronized Map<Pair<String, String>, Integer> getCfIdMap()
    {
        if (cfIdMap != null)
            return cfIdMap;
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(serializedCfMap));
        cfIdMap = new HashMap<Pair<String, String>, Integer>();
        try
        {
            int sz = in.readInt();
            for (int i = 0; i < sz; i++)
            {
                Pair<String, String> key = new Pair<String, String>(in.readUTF(), in.readUTF());
                cfIdMap.put(key, in.readInt());
            }
        }
        catch (IOException ex)
        {
            throw new IOError(ex);
        }
        return cfIdMap;
    }
    
    private static byte[] serializeCfIdMap(Map<Pair<String, String>, Integer> map)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        
        try
        {
            dos.writeInt(map.size());
            for (Map.Entry<Pair<String, String>, Integer> entry : map.entrySet())
            {
                Pair<String, String> p = entry.getKey();
                dos.writeUTF(p.left);
                dos.writeUTF(p.right);
                dos.writeInt(entry.getValue());
            }
            dos.close();
        }
        catch (IOException ex)
        {
            throw new IOError(ex);
        }
            
        return out.toByteArray();
    }

    byte[] toByteArray() throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(maxSerializedSize);
        DataOutputStream dos = new DataOutputStream(bos);        
        serializer.serialize(this, dos);
        byte[] src = bos.toByteArray();
        assert src.length <= maxSerializedSize;
        byte[] dst = new byte[maxSerializedSize];
        System.arraycopy(src, 0, dst, 0, src.length);
        return dst;
    }
    
    // we use cf ids. getting the cf names would be pretty pretty expensive.
    public String toString()
    {
        StringBuilder sb = new StringBuilder("");
        sb.append("CLH(dirty+flushed={");
        for (Map.Entry<Integer, Integer> entry : lastFlushedAt.entrySet())
        {       
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
        }
        sb.append("})");
        return sb.toString();
    }

    public String dirtyString()
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, Integer> entry : lastFlushedAt.entrySet())
            sb.append(entry.getKey()).append(", ");
        return sb.toString();
    }

    static CommitLogHeader readCommitLogHeader(BufferedRandomAccessFile logReader) throws IOException
    {
        int statedSize = logReader.readInt();
        byte[] bytes = new byte[statedSize];
        logReader.readFully(bytes);
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        return serializer.deserialize(new DataInputStream(byteStream));
    }

    static class CommitLogHeaderSerializer implements ICompactSerializer<CommitLogHeader>
    {
        public void serialize(CommitLogHeader clHeader, DataOutputStream dos) throws IOException
        {
            assert clHeader.lastFlushedAt.size() <= clHeader.cfCount;
            Checksum checksum = new CRC32();

            // write the first checksum after the fixed-size part, so we won't OOM allocating a bogus cfmap buffer
            dos.writeInt(clHeader.cfCount); // 4
            dos.writeInt(clHeader.serializedCfMap.length); // 4
            dos.writeInt(clHeader.lastFlushedAt.size()); // 4
            checksum.update(clHeader.cfCount);
            checksum.update(clHeader.serializedCfMap.length);
            checksum.update(clHeader.lastFlushedAt.size());
            dos.writeLong(checksum.getValue());

            // write the 2nd checksum after the cfmap and lastflushedat map
            dos.write(clHeader.serializedCfMap); // colMap.length
            checksum.update(clHeader.serializedCfMap, 0, clHeader.serializedCfMap.length);
            for (Map.Entry<Integer, Integer> entry : clHeader.lastFlushedAt.entrySet())
            {
                dos.writeInt(entry.getKey()); // 4
                checksum.update(entry.getKey());
                dos.writeInt(entry.getValue()); // 4
                checksum.update(entry.getValue());
            }
            dos.writeLong(checksum.getValue());
        }

        public CommitLogHeader deserialize(DataInputStream dis) throws IOException
        {
            Checksum checksum = new CRC32();

            int cfCount = dis.readInt();
            checksum.update(cfCount);
            int cfMapLength = dis.readInt();
            checksum.update(cfMapLength);
            int lastFlushedAtSize = dis.readInt();
            checksum.update(lastFlushedAtSize);
            if (checksum.getValue() != dis.readLong())
            {
                throw new IOException("Invalid or corrupt commitlog header");
            }

            byte[] cfMap = new byte[cfMapLength];
            dis.readFully(cfMap);
            checksum.update(cfMap, 0, cfMap.length);
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

            return new CommitLogHeader(lastFlushedAt, cfMap, cfCount);
        }
    }
}
