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

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.SSTable;

class PendingFile
{
    private static ICompactSerializer<PendingFile> serializer_;

    static
    {
        serializer_ = new InitiatedFileSerializer();
    }

    public static ICompactSerializer<PendingFile> serializer()
    {
        return serializer_;
    }

    private SSTable.Descriptor desc;        
    private String component;
    private long expectedBytes;                     
    private long ptr;

    public PendingFile(SSTable.Descriptor desc, String component, long expectedBytes)
    {
        this.desc = desc;
        this.component = component;
        this.expectedBytes = expectedBytes;         
        ptr = 0;
    }

    public void update(long ptr)
    {
        this.ptr = ptr;
    }

    public long getPtr()
    {
        return ptr;
    }

    public String getComponent()
    {
        return component;
    }

    public SSTable.Descriptor getDescriptor()
    {
        return desc;
    }
    
    public String getFilename()
    {
        return desc.filenameFor(component);
    }
    
    public long getExpectedBytes()
    {
        return expectedBytes;
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
        return toString().hashCode();
    }

    public String toString()
    {
        return getFilename() + ":" + expectedBytes;
    }

    private static class InitiatedFileSerializer implements ICompactSerializer<PendingFile>
    {
        public void serialize(PendingFile sc, DataOutputStream dos) throws IOException
        {
            dos.writeUTF(sc.desc.filenameFor(sc.component));
            dos.writeUTF(sc.component);
            dos.writeLong(sc.expectedBytes);            
        }

        public PendingFile deserialize(DataInputStream dis) throws IOException
        {
            SSTable.Descriptor desc = SSTable.Descriptor.fromFilename(dis.readUTF());
            String component = dis.readUTF();
            long expectedBytes = dis.readLong();           
            return new PendingFile(desc, component, expectedBytes);
        }
    }
}
