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

package org.apache.cassandra.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class RangesSerializer implements IVersionedSerializer<Collection<Range<Token>>>
{
    public static final RangesSerializer serializer = new RangesSerializer();

    @Override
    public void serialize(Collection<Range<Token>> ranges, DataOutputPlus out, int version) throws IOException
    {
        out.writeInt(ranges.size());
        for (Range<Token> r : ranges)
        {
            Token.serializer.serialize(r.left, out, version);
            Token.serializer.serialize(r.right, out, version);
        }
    }

    @Override
    public Collection<Range<Token>> deserialize(DataInputPlus in, int version) throws IOException
    {
        int count = in.readInt();
        List<Range<Token>> ranges = new ArrayList<>(count);
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        for (int i = 0; i < count; i++)
        {
            Token start = Token.serializer.deserialize(in, partitioner, version);
            Token end = Token.serializer.deserialize(in, partitioner, version);
            ranges.add(new Range<>(start, end));
        }
        return ranges;
    }

    @Override
    public long serializedSize(Collection<Range<Token>> ranges, int version)
    {
        int size = TypeSizes.sizeof(ranges.size());
        if (ranges.size() > 0)
            size += ranges.size() * 2 * Token.serializer.serializedSize(ranges.iterator().next().left, version);
        return size;
    }
}
