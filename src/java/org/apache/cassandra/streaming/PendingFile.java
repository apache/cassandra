package org.apache.cassandra.streaming;
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


import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

/**
 * Represents portions of a file to be streamed between nodes.
 */
public class PendingFile
{
    private static PendingFileSerializer serializer_ = new PendingFileSerializer();

    public static PendingFileSerializer serializer()
    {
        return serializer_;
    }

    // NB: this reference is used to be able to release the acquired reference upon completion
    public final SSTableReader sstable;

    public final Descriptor desc;
    public final String component;
    public final List<Pair<Long,Long>> sections;
    public final OperationType type;
    public final long size;
    public final long estimatedKeys;
    public long progress;

    public PendingFile(Descriptor desc, PendingFile pf)
    {
        this(null, desc, pf.component, pf.sections, pf.type, pf.estimatedKeys);
    }

    public PendingFile(SSTableReader sstable, Descriptor desc, String component, List<Pair<Long,Long>> sections, OperationType type)
    {
        this(sstable, desc, component, sections, type, 0);
    }
    
    public PendingFile(SSTableReader sstable, Descriptor desc, String component, List<Pair<Long,Long>> sections, OperationType type, long estimatedKeys)
    {
        this.sstable = sstable;
        this.desc = desc;
        this.component = component;
        this.sections = sections;
        this.type = type;

        long tempSize = 0;
        for(Pair<Long,Long> section : sections)
        {
            tempSize += section.right - section.left;
        }
        size = tempSize;

        this.estimatedKeys = estimatedKeys;
    }

    public String getFilename()
    {
        return desc.filenameFor(component);
    }
    
    public boolean equals(Object o)
    {
        if ( !(o instanceof PendingFile) )
            return false;

        PendingFile rhs = (PendingFile)o;
        return getFilename().equals(rhs.getFilename());
    }

    public int hashCode()
    {
        return getFilename().hashCode();
    }

    public String toString()
    {
        return getFilename() + " sections=" + sections.size() + " progress=" + progress + "/" + size + " - " + progress*100/size + "%";
    }

    public static class PendingFileSerializer implements IVersionedSerializer<PendingFile>
    {
        public void serialize(PendingFile sc, DataOutput dos, int version) throws IOException
        {
            if (sc == null)
            {
                dos.writeUTF("");
                return;
            }

            dos.writeUTF(sc.desc.filenameFor(sc.component));
            dos.writeUTF(sc.component);
            dos.writeInt(sc.sections.size());
            for (Pair<Long,Long> section : sc.sections)
            {
                dos.writeLong(section.left); dos.writeLong(section.right);
            }
            if (version > MessagingService.VERSION_07)
                dos.writeUTF(sc.type.name());
            if (version > MessagingService.VERSION_080)
                dos.writeLong(sc.estimatedKeys);
        }

        public PendingFile deserialize(DataInput dis, int version) throws IOException
        {
            String filename = dis.readUTF();
            if (filename.isEmpty())
                return null;
            
            Descriptor desc = Descriptor.fromFilename(filename);
            String component = dis.readUTF();
            int count = dis.readInt();
            List<Pair<Long,Long>> sections = new ArrayList<Pair<Long,Long>>(count);
            for (int i = 0; i < count; i++)
                sections.add(new Pair<Long,Long>(Long.valueOf(dis.readLong()), Long.valueOf(dis.readLong())));
            // this controls the way indexes are rebuilt when streaming in.  
            OperationType type = OperationType.RESTORE_REPLICA_COUNT;
            if (version > MessagingService.VERSION_07)
                type = OperationType.valueOf(dis.readUTF());
            long estimatedKeys = 0;
            if (version > MessagingService.VERSION_080)
                estimatedKeys = dis.readLong();
            return new PendingFile(null, desc, component, sections, type, estimatedKeys);
        }

        public long serializedSize(PendingFile pendingFile, int version)
        {
            throw new UnsupportedOperationException();
        }
    }
}
