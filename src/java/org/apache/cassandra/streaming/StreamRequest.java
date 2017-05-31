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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

public class StreamRequest
{
    public static final IVersionedSerializer<StreamRequest> serializer = new StreamRequestSerializer();

    public final String keyspace;
    public final Collection<Range<Token>> ranges;
    public final Collection<String> columnFamilies = new HashSet<>();
    public StreamRequest(String keyspace, Collection<Range<Token>> ranges, Collection<String> columnFamilies)
    {
        this.keyspace = keyspace;
        this.ranges = ranges;
        this.columnFamilies.addAll(columnFamilies);
    }

    public static class StreamRequestSerializer implements IVersionedSerializer<StreamRequest>
    {
        public void serialize(StreamRequest request, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(request.keyspace);
            out.writeInt(request.ranges.size());
            for (Range<Token> range : request.ranges)
            {
                MessagingService.validatePartitioner(range);
                Token.serializer.serialize(range.left, out, version);
                Token.serializer.serialize(range.right, out, version);
            }
            out.writeInt(request.columnFamilies.size());
            for (String cf : request.columnFamilies)
                out.writeUTF(cf);
        }

        public StreamRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            String keyspace = in.readUTF();
            int rangeCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++)
            {
                Token left = Token.serializer.deserialize(in, MessagingService.globalPartitioner(), version);
                Token right = Token.serializer.deserialize(in, MessagingService.globalPartitioner(), version);
                ranges.add(new Range<>(left, right));
            }
            int cfCount = in.readInt();
            List<String> columnFamilies = new ArrayList<>(cfCount);
            for (int i = 0; i < cfCount; i++)
                columnFamilies.add(in.readUTF());
            return new StreamRequest(keyspace, ranges, columnFamilies);
        }

        public long serializedSize(StreamRequest request, int version)
        {
            int size = TypeSizes.sizeof(request.keyspace);
            size += TypeSizes.sizeof(request.ranges.size());
            for (Range<Token> range : request.ranges)
            {
                size += Token.serializer.serializedSize(range.left, version);
                size += Token.serializer.serializedSize(range.right, version);
            }
            size += TypeSizes.sizeof(request.columnFamilies.size());
            for (String cf : request.columnFamilies)
                size += TypeSizes.sizeof(cf);
            return size;
        }
    }
}
