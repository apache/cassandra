/**
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

package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.FBUtilities;

public class RangeSliceReply
{
    public final List<Row> rows;

    public RangeSliceReply(List<Row> rows)
    {
        this.rows = rows;
    }

    public Message getReply(Message originalMessage) throws IOException
    {
        int size = DBConstants.intSize;
        for (Row row : rows)
            size += Row.serializer().serializedSize(row, originalMessage.getVersion());

        DataOutputBuffer buffer = new DataOutputBuffer(size);
        buffer.writeInt(rows.size());
        for (Row row : rows)
            Row.serializer().serialize(row, buffer, originalMessage.getVersion());
        assert buffer.getLength() == buffer.getData().length;
        return originalMessage.getReply(FBUtilities.getBroadcastAddress(), buffer.getData(), originalMessage.getVersion());
    }

    @Override
    public String toString()
    {
        return "RangeSliceReply{" +
               "rows=" + StringUtils.join(rows, ",") +
               '}';
    }

    public static RangeSliceReply read(byte[] body, int version) throws IOException
    {
        FastByteArrayInputStream bufIn = new FastByteArrayInputStream(body);
        DataInputStream dis = new DataInputStream(bufIn);
        int rowCount = dis.readInt();
        List<Row> rows = new ArrayList<Row>(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            rows.add(Row.serializer().deserialize(dis, version));
        }
        return new RangeSliceReply(rows);
    }
}
