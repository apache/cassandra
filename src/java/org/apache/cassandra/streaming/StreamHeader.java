/*
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
package org.apache.cassandra.streaming;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDSerializer;

public class StreamHeader
{
    public static final IVersionedSerializer<StreamHeader> serializer = new StreamHeaderSerializer();

    public final String table;

    /** file being sent on initial stream */
    public final PendingFile file;

    /** session ID */
    public final UUID sessionId;

    /** files to add to the session */
    public final Collection<PendingFile> pendingFiles;

    public StreamHeader(String table, UUID sessionId, PendingFile file)
    {
        this(table, sessionId, file, Collections.<PendingFile>emptyList());
    }

    public StreamHeader(String table, UUID sessionId, PendingFile first, Collection<PendingFile> pendingFiles)
    {
        this.table = table;
        this.sessionId  = sessionId;
        this.file = first;
        this.pendingFiles = pendingFiles;
    }

    private static class StreamHeaderSerializer implements IVersionedSerializer<StreamHeader>
    {
        public void serialize(StreamHeader sh, DataOutput out, int version) throws IOException
        {
            out.writeUTF(sh.table);
            UUIDSerializer.serializer.serialize(sh.sessionId, out, MessagingService.current_version);
            PendingFile.serializer.serialize(sh.file, out, version);
            out.writeInt(sh.pendingFiles.size());
            for (PendingFile file : sh.pendingFiles)
                PendingFile.serializer.serialize(file, out, version);
        }

        public StreamHeader deserialize(DataInput in, int version) throws IOException
        {
            String table = in.readUTF();
            UUID sessionId = UUIDSerializer.serializer.deserialize(in, MessagingService.current_version);
            PendingFile file = PendingFile.serializer.deserialize(in, version);
            int size = in.readInt();

            List<PendingFile> pendingFiles = new ArrayList<PendingFile>(size);
            for (int i = 0; i < size; i++)
                pendingFiles.add(PendingFile.serializer.deserialize(in, version));
            return new StreamHeader(table, sessionId, file, pendingFiles);
        }

        public long serializedSize(StreamHeader sh, int version)
        {
            long size = TypeSizes.NATIVE.sizeof(sh.table);
            size += TypeSizes.NATIVE.sizeof(sh.sessionId);
            size += PendingFile.serializer.serializedSize(sh.file, version);
            size += TypeSizes.NATIVE.sizeof(sh.pendingFiles.size());
            for (PendingFile file : sh.pendingFiles)
                size += PendingFile.serializer.serializedSize(file, version);
            return size;
       }
    }
}