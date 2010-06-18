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


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.Pair;

/**
 * Represents portions of a file to be streamed between nodes.
 */
public class PendingFile
{
    private static ICompactSerializer<PendingFile> serializer_;

    static
    {
        serializer_ = new PendingFileSerializer();
    }

    public static ICompactSerializer<PendingFile> serializer()
    {
        return serializer_;
    }

    private final Descriptor desc;
    private final String component;
    private final List<Pair<Long,Long>> sections;
    private long ptr;

    public PendingFile(Descriptor desc, PendingFile pf)
    {
        this(desc, pf.component, pf.sections);
    }

    public PendingFile(Descriptor desc, String component, List<Pair<Long,Long>> sections)
    {
        this.desc = desc;
        this.component = component;
        this.sections = sections;
        ptr = 0;
    }

    public void update(long ptr)
    {
        this.ptr = ptr;
    }

    /**
     * @return The current section of the file, as an (offset,end) pair, or null if nothing left to stream.
     */
    public Pair<Long,Long> currentSection()
    {
        // linear search for the first appropriate section
        for (Pair<Long,Long> section : sections)
            if (ptr < section.right)
                return new Pair<Long,Long>(Long.valueOf(Math.max(ptr, section.left)), section.right);
        return null;
    }

    public String getComponent()
    {
        return component;
    }

    public Descriptor getDescriptor()
    {
        return desc;
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
        return getFilename() + ":" + ptr + "/" + sections;
    }

    private static class PendingFileSerializer implements ICompactSerializer<PendingFile>
    {
        public void serialize(PendingFile sc, DataOutputStream dos) throws IOException
        {
            dos.writeUTF(sc.desc.filenameFor(sc.component));
            dos.writeUTF(sc.component);
            dos.writeInt(sc.sections.size());
            for (Pair<Long,Long> section : sc.sections)
            {
                dos.writeLong(section.left); dos.writeLong(section.right);
            }
        }

        public PendingFile deserialize(DataInputStream dis) throws IOException
        {
            Descriptor desc = Descriptor.fromFilename(dis.readUTF());
            String component = dis.readUTF();
            int count = dis.readInt();
            List<Pair<Long,Long>> sections = new ArrayList<Pair<Long,Long>>(count);
            for (int i = 0; i < count; i++)
                sections.add(new Pair<Long,Long>(Long.valueOf(dis.readLong()), Long.valueOf(dis.readLong())));
            return new PendingFile(desc, component, sections);
        }
    }
}
