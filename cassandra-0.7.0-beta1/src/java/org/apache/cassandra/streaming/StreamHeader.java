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

public class StreamHeader
{
    private static ICompactSerializer<StreamHeader> serializer;

    static
    {
        serializer = new StreamHeaderSerializer();
    }

    public static ICompactSerializer<StreamHeader> serializer()
    {
        return serializer;
    }

    private PendingFile file;
    private long sessionId;
    
    // indicates an initiated transfer as opposed to a request
    protected final boolean initiatedTransfer;
    
    // this list will only be non-null when the first of a batch of files are being sent. it avoids having to have
    // a separate message indicating which files to expect.
    private final List<PendingFile> pending;

    public StreamHeader(long sessionId, PendingFile file, boolean initiatedTransfer)
    {
        this.sessionId = sessionId;
        this.file = file;
        this.initiatedTransfer = initiatedTransfer;
        pending = null;
    }

    public StreamHeader(long sessionId, PendingFile file, List<PendingFile> pending, boolean initiatedTransfer)
    {
        this.sessionId  = sessionId;
        this.file = file;
        this.initiatedTransfer = initiatedTransfer;
        this.pending = pending;
    }

    public List<PendingFile> getPendingFiles()
    {
        return pending;
    }

    public PendingFile getStreamFile()
    {
        return file;
    }

    public long getSessionId()
    {
        return sessionId;
    }

    private static class StreamHeaderSerializer implements ICompactSerializer<StreamHeader>
    {
        public void serialize(StreamHeader sh, DataOutputStream dos) throws IOException
        {
            dos.writeLong(sh.getSessionId());
            PendingFile.serializer().serialize(sh.getStreamFile(), dos);
            dos.writeBoolean(sh.initiatedTransfer);
            if (sh.pending != null)
            {
                dos.writeInt(sh.getPendingFiles().size());
                for(PendingFile file : sh.getPendingFiles())
                {
                    PendingFile.serializer().serialize(file, dos);
                }
            }
            else
                dos.writeInt(0);
        }

        public StreamHeader deserialize(DataInputStream dis) throws IOException
        {
           long sessionId = dis.readLong();
           PendingFile file = PendingFile.serializer().deserialize(dis);
           boolean initiatedTransfer = dis.readBoolean();
           int size = dis.readInt();
           StreamHeader header;

           if (size > 0)
           {
               List<PendingFile> pendingFiles = new ArrayList<PendingFile>(size);
               for (int i=0; i<size; i++)
               {
                   pendingFiles.add(PendingFile.serializer().deserialize(dis));
               }
               header = new StreamHeader(sessionId, file, pendingFiles, initiatedTransfer);
           }
           else
           {
               header = new StreamHeader(sessionId, file, initiatedTransfer);
           }

           return header;
        }
    }
}
