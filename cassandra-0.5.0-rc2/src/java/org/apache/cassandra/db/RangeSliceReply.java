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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.*;

public class RangeSliceReply
{
    public final List<Row> rows;
    public final boolean rangeCompletedLocally;

    public RangeSliceReply(List<Row> rows, boolean rangeCompletedLocally)
    {
        this.rows = rows;
        this.rangeCompletedLocally = rangeCompletedLocally;
    }

    public Message getReply(Message originalMessage) throws IOException
    {
        DataOutputBuffer dob = new DataOutputBuffer();
        dob.writeBoolean(rangeCompletedLocally);
        dob.writeInt(rows.size());
        for (Row row : rows)
        {
            Row.serializer().serialize(row, dob);
        }
        byte[] data = Arrays.copyOf(dob.getData(), dob.getLength());
        return originalMessage.getReply(FBUtilities.getLocalAddress(), data);
    }

    @Override
    public String toString()
    {
        return "RangeSliceReply{" +
               "rows=" + StringUtils.join(rows, ",") +
               ", rangeCompletedLocally=" + rangeCompletedLocally +
               '}';
    }

    public static RangeSliceReply read(byte[] body) throws IOException
    {
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(body, body.length);
        boolean completed = bufIn.readBoolean();
        int rowCount = bufIn.readInt();
        List<Row> rows = new ArrayList<Row>(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            rows.add(Row.serializer().deserialize(bufIn));
        }
        return new RangeSliceReply(rows, completed);
    }
}
