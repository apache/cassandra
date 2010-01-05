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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInputStream;
import java.util.Arrays;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.concurrent.StageManager;

public class RangeCommand
{
    private static RangeCommandSerializer serializer = new RangeCommandSerializer();

    public final String table;
    public final String columnFamily;
    public final String startWith;
    public final String stopAt;
    public final int maxResults;

    public RangeCommand(String table, String columnFamily, String startWith, String stopAt, int maxResults)
    {
        this.table = table;
        this.columnFamily = columnFamily;
        this.startWith = startWith;
        this.stopAt = stopAt;
        this.maxResults = maxResults;
    }

    public Message getMessage() throws IOException
    {
        DataOutputBuffer dob = new DataOutputBuffer();
        serializer.serialize(this, dob);
        return new Message(FBUtilities.getLocalAddress(),
                           StageManager.readStage_,
                           StorageService.rangeVerbHandler_,
                           Arrays.copyOf(dob.getData(), dob.getLength()));
    }

    public static RangeCommand read(Message message) throws IOException
    {
        byte[] bytes = message.getMessageBody();
        DataInputBuffer dib = new DataInputBuffer();
        dib.reset(bytes, bytes.length);
        return serializer.deserialize(new DataInputStream(dib));
    }

    public String toString()
    {
        return "RangeCommand(" +
               "table='" + table + '\'' +
               ", columnFamily=" + columnFamily +
               ", startWith='" + startWith + '\'' +
               ", stopAt='" + stopAt + '\'' +
               ", maxResults=" + maxResults +
               ')';
    }
}

class RangeCommandSerializer implements ICompactSerializer<RangeCommand>
{
    public void serialize(RangeCommand command, DataOutputStream dos) throws IOException
    {
        dos.writeUTF(command.table);
        dos.writeUTF(command.columnFamily);
        dos.writeUTF(command.startWith);
        dos.writeUTF(command.stopAt);
        dos.writeInt(command.maxResults);
    }

    public RangeCommand deserialize(DataInputStream dis) throws IOException
    {
        return new RangeCommand(dis.readUTF(), dis.readUTF(), dis.readUTF(), dis.readUTF(), dis.readInt());
    }
}
