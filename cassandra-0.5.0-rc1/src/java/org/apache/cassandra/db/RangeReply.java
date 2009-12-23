/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.util.*;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.FBUtilities;

public class RangeReply
{
    public final List<String> keys;
    public final boolean rangeCompletedLocally;

    public RangeReply(List<String> keys, boolean rangeCompletedLocally)
    {
        this.keys = keys;
        this.rangeCompletedLocally = rangeCompletedLocally;
    }

    public Message getReply(Message originalMessage) throws IOException
    {
        DataOutputBuffer dob = new DataOutputBuffer();
        dob.writeBoolean(rangeCompletedLocally);

        for (String key : keys)
        {
            dob.writeUTF(key);
        }
        byte[] data = Arrays.copyOf(dob.getData(), dob.getLength());
        return originalMessage.getReply(FBUtilities.getLocalAddress(), data);
    }

    @Override
    public String toString()
    {
        return "RangeReply(" +
               "keys=[" + StringUtils.join(keys, ", ") +
               "], completed=" + rangeCompletedLocally +
               ')';
    }

    public static RangeReply read(byte[] body) throws IOException
    {
        DataInputBuffer bufIn = new DataInputBuffer();
        boolean rangeCompletedLocally;        
        bufIn.reset(body, body.length);
        rangeCompletedLocally = bufIn.readBoolean();

        List<String> keys = new ArrayList<String>();
        while (bufIn.getPosition() < body.length)
        {
            keys.add(bufIn.readUTF());
        }
        
        return new RangeReply(keys, rangeCompletedLocally);
    }
}
