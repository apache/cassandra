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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

public class StreamHeader
{
    private static IVersionedSerializer<StreamHeader> serializer;

    // Streaming sessionId flags, used to avoid duplicate session id's between nodes.
    // See StreamInSession and StreamOutSession
    public static final int STREAM_IN_SOURCE_FLAG = 0;
    public static final int STREAM_OUT_SOURCE_FLAG = 1;

    static
    {
        serializer = new StreamHeaderSerializer();
    }

    public static IVersionedSerializer<StreamHeader> serializer()
    {
        return serializer;
    }

    public final String table;

    /** file being sent on initial stream */
    public final PendingFile file;

    /** session is tuple of (host, sessionid) */
    public final long sessionId;

    /** files to add to the session */
    public final Collection<PendingFile> pendingFiles;

    /** Address of the sender **/
    public final InetAddress broadcastAddress;

    public StreamHeader(String table, long sessionId, PendingFile file)
    {
        this(table, sessionId, file, Collections.<PendingFile>emptyList());
    }

    public StreamHeader(String table, long sessionId, PendingFile first, Collection<PendingFile> pendingFiles)
    {
        this(table, sessionId, first, pendingFiles, FBUtilities.getBroadcastAddress());
    }

    public StreamHeader(String table, long sessionId, PendingFile first, Collection<PendingFile> pendingFiles, InetAddress broadcastAddress)
    {
        this.table = table;
        this.sessionId  = sessionId;
        this.file = first;
        this.pendingFiles = pendingFiles;
        this.broadcastAddress = broadcastAddress;
    }

    private static class StreamHeaderSerializer implements IVersionedSerializer<StreamHeader>
    {
        public void serialize(StreamHeader sh, DataOutput dos, int version) throws IOException
        {
            dos.writeUTF(sh.table);
            dos.writeLong(sh.sessionId);
            PendingFile.serializer().serialize(sh.file, dos, version);
            dos.writeInt(sh.pendingFiles.size());
            for(PendingFile file : sh.pendingFiles)
            {
                PendingFile.serializer().serialize(file, dos, version);
            }
            CompactEndpointSerializationHelper.serialize(sh.broadcastAddress, dos);
        }

        public StreamHeader deserialize(DataInput dis, int version) throws IOException
        {
            String table = dis.readUTF();
            long sessionId = dis.readLong();
            PendingFile file = PendingFile.serializer().deserialize(dis, version);
            int size = dis.readInt();

            List<PendingFile> pendingFiles = new ArrayList<PendingFile>(size);
            for (int i = 0; i < size; i++)
            {
                pendingFiles.add(PendingFile.serializer().deserialize(dis, version));
            }
            InetAddress bca = null;
            if (version > MessagingService.VERSION_10)
                bca = CompactEndpointSerializationHelper.deserialize(dis);
            return new StreamHeader(table, sessionId, file, pendingFiles, bca);
        }

        public long serializedSize(StreamHeader streamHeader, int version)
        {
            throw new UnsupportedOperationException();
        }
    }
}