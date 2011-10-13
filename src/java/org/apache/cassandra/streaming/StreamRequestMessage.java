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
import java.util.*;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.FastByteArrayOutputStream;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/**
* This class encapsulates the message that needs to be sent to nodes
* that handoff data. The message contains information about ranges
* that need to be transferred and the target node.
* 
* If a file is specified, ranges and table will not. vice-versa should hold as well.
*/
class StreamRequestMessage implements MessageProducer
{
    private static IVersionedSerializer<StreamRequestMessage> serializer_;
    static
    {
        serializer_ = new StreamRequestMessageSerializer();
    }

    protected static IVersionedSerializer<StreamRequestMessage> serializer()
    {
        return serializer_;
    }

    protected final long sessionId;
    protected final InetAddress target;
    
    // if this is specified, ranges and table should not be.
    protected final PendingFile file;
    
    // if these are specified, file shoud not be.
    protected final Collection<Range> ranges;
    protected final String table;
    protected final Iterable<ColumnFamilyStore> columnFamilies;
    protected final OperationType type;

    StreamRequestMessage(InetAddress target, Collection<Range> ranges, String table, Iterable<ColumnFamilyStore> columnFamilies, long sessionId, OperationType type)
    {
        this.target = target;
        this.ranges = ranges;
        this.table = table;
        this.columnFamilies = columnFamilies;
        this.sessionId = sessionId;
        this.type = type;
        file = null;
    }

    StreamRequestMessage(InetAddress target, PendingFile file, long sessionId)
    {
        this.target = target;
        this.file = file;
        this.sessionId = sessionId;
        this.type = file.type;
        ranges = null;
        table = null;
        columnFamilies = null;
    }
    
    public Message getMessage(Integer version)
    {
    	FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        try
        {
            StreamRequestMessage.serializer().serialize(this, dos, version);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        return new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.STREAM_REQUEST, bos.toByteArray(), version);
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder("");
        if (file == null)
        {
            sb.append(table);
            sb.append("@");
            sb.append(columnFamilies.toString());
            sb.append("@");
            sb.append(target);
            sb.append("------->");
            for ( Range range : ranges )
            {
                sb.append(range);
                sb.append(" ");
            }
            sb.append(type);
        }
        else
        {
            sb.append(file.toString());
        }
        return sb.toString();
    }

    private static class StreamRequestMessageSerializer implements IVersionedSerializer<StreamRequestMessage>
    {
        public void serialize(StreamRequestMessage srm, DataOutput dos, int version) throws IOException
        {
            dos.writeLong(srm.sessionId);
            CompactEndpointSerializationHelper.serialize(srm.target, dos);
            if (srm.file != null)
            {
                dos.writeBoolean(true);
                PendingFile.serializer().serialize(srm.file, dos, version);
            }
            else
            {
                dos.writeBoolean(false);
                dos.writeUTF(srm.table);
                dos.writeInt(srm.ranges.size());
                for (Range range : srm.ranges)
                {
                    AbstractBounds.serializer().serialize(range, dos);
                }

                if (version > MessagingService.VERSION_07)
                    dos.writeUTF(srm.type.name());

                if (version > MessagingService.VERSION_080)
                {
                    dos.writeInt(Iterables.size(srm.columnFamilies));
                    for (ColumnFamilyStore cfs : srm.columnFamilies)
                        dos.writeInt(cfs.metadata.cfId);
                }
            }
        }

        public StreamRequestMessage deserialize(DataInput dis, int version) throws IOException
        {
            long sessionId = dis.readLong();
            InetAddress target = CompactEndpointSerializationHelper.deserialize(dis);
            boolean singleFile = dis.readBoolean();
            if (singleFile)
            {
                PendingFile file = PendingFile.serializer().deserialize(dis, version);
                return new StreamRequestMessage(target, file, sessionId);
            }
            else
            {
                String table = dis.readUTF();
                int size = dis.readInt();
                List<Range> ranges = (size == 0) ? null : new ArrayList<Range>();
                for( int i = 0; i < size; ++i )
                {
                    ranges.add((Range) AbstractBounds.serializer().deserialize(dis));
                }
                OperationType type = OperationType.RESTORE_REPLICA_COUNT;
                if (version > MessagingService.VERSION_07)
                    type = OperationType.valueOf(dis.readUTF());

                List<ColumnFamilyStore> stores = new ArrayList<ColumnFamilyStore>();
                if (version > MessagingService.VERSION_080)
                {
                    int cfsSize = dis.readInt();
                    for (int i = 0; i < cfsSize; ++i)
                        stores.add(Table.open(table).getColumnFamilyStore(dis.readInt()));
                }

                return new StreamRequestMessage(target, ranges, table, stores, sessionId, type);
            }
        }

        public long serializedSize(StreamRequestMessage streamRequestMessage, int version)
        {
            throw new UnsupportedOperationException();
        }
    }
}
