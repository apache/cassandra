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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

/**
* This class encapsulates the message that needs to be sent to nodes
* that handoff data. The message contains information about ranges
* that need to be transferred and the target node.
*
* If a file is specified, ranges and table will not. vice-versa should hold as well.
*/
public class StreamRequest
{
    public static final IVersionedSerializer<StreamRequest> serializer = new StreamRequestSerializer();

    protected final long sessionId;
    protected final InetAddress target;

    // if this is specified, ranges and table should not be.
    protected final PendingFile file;

    // if these are specified, file shoud not be.
    protected final Collection<Range<Token>> ranges;
    protected final String table;
    protected final Iterable<ColumnFamilyStore> columnFamilies;
    protected final OperationType type;

    StreamRequest(InetAddress target, Collection<Range<Token>> ranges, String table, Iterable<ColumnFamilyStore> columnFamilies, long sessionId, OperationType type)
    {
        this.target = target;
        this.ranges = ranges;
        this.table = table;
        this.columnFamilies = columnFamilies;
        this.sessionId = sessionId;
        this.type = type;
        file = null;
    }

    StreamRequest(InetAddress target, PendingFile file, long sessionId)
    {
        this.target = target;
        this.file = file;
        this.sessionId = sessionId;
        this.type = file.type;
        ranges = null;
        table = null;
        columnFamilies = null;
    }

    public MessageOut<StreamRequest> createMessage()
    {
        return new MessageOut<StreamRequest>(MessagingService.Verb.STREAM_REQUEST, this, serializer);
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
            for ( Range<Token> range : ranges )
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

    private static class StreamRequestSerializer implements IVersionedSerializer<StreamRequest>
    {
        public void serialize(StreamRequest srm, DataOutput dos, int version) throws IOException
        {
            dos.writeLong(srm.sessionId);
            CompactEndpointSerializationHelper.serialize(srm.target, dos);
            if (srm.file != null)
            {
                dos.writeBoolean(true);
                PendingFile.serializer.serialize(srm.file, dos, version);
            }
            else
            {
                dos.writeBoolean(false);
                dos.writeUTF(srm.table);
                dos.writeInt(srm.ranges.size());
                for (Range<Token> range : srm.ranges)
                    AbstractBounds.serializer.serialize(range, dos, version);

                dos.writeUTF(srm.type.name());

                dos.writeInt(Iterables.size(srm.columnFamilies));
                for (ColumnFamilyStore cfs : srm.columnFamilies)
                    ColumnFamily.serializer.serializeCfId(cfs.metadata.cfId, dos, version);
            }
        }

        public StreamRequest deserialize(DataInput dis, int version) throws IOException
        {
            long sessionId = dis.readLong();
            InetAddress target = CompactEndpointSerializationHelper.deserialize(dis);
            boolean singleFile = dis.readBoolean();
            if (singleFile)
            {
                PendingFile file = PendingFile.serializer.deserialize(dis, version);
                return new StreamRequest(target, file, sessionId);
            }
            else
            {
                String table = dis.readUTF();
                int size = dis.readInt();
                List<Range<Token>> ranges = (size == 0) ? null : new ArrayList<Range<Token>>(size);
                for( int i = 0; i < size; ++i )
                    ranges.add((Range<Token>) AbstractBounds.serializer.deserialize(dis, version).toTokenBounds());
                OperationType type = OperationType.RESTORE_REPLICA_COUNT;
                type = OperationType.valueOf(dis.readUTF());

                List<ColumnFamilyStore> stores = new ArrayList<ColumnFamilyStore>();
                int cfsSize = dis.readInt();
                for (int i = 0; i < cfsSize; ++i)
                    stores.add(Table.open(table).getColumnFamilyStore(ColumnFamily.serializer.deserializeCfId(dis, version)));

                return new StreamRequest(target, ranges, table, stores, sessionId, type);
            }
        }

        public long serializedSize(StreamRequest sr, int version)
        {
            long size = TypeSizes.NATIVE.sizeof(sr.sessionId);
            size += CompactEndpointSerializationHelper.serializedSize(sr.target);
            size += TypeSizes.NATIVE.sizeof(true);
            if (sr.file != null)
                return size + PendingFile.serializer.serializedSize(sr.file, version);

            size += TypeSizes.NATIVE.sizeof(sr.table);
            size += TypeSizes.NATIVE.sizeof(sr.ranges.size());
            for (Range<Token> range : sr.ranges)
                size += AbstractBounds.serializer.serializedSize(range, version);
            size += TypeSizes.NATIVE.sizeof(sr.type.name());
            size += TypeSizes.NATIVE.sizeof(Iterables.size(sr.columnFamilies));
            for (ColumnFamilyStore cfs : sr.columnFamilies)
                size += ColumnFamily.serializer.cfIdSerializedSize(cfs.metadata.cfId, TypeSizes.NATIVE, version);
            return size;
        }
    }
}
