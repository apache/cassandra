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

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.ColumnParent;
import org.apache.cassandra.service.SlicePredicate;
import org.apache.cassandra.service.SliceRange;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RangeSliceCommand
{
    private static final SliceCommandSerializer serializer = new SliceCommandSerializer();
    
    public final String keyspace;

    public final String column_family;
    public final byte[] super_column;

    public final SlicePredicate predicate;

    public final String start_key;
    public final String finish_key;
    public final int max_keys;

    public RangeSliceCommand(String keyspace, ColumnParent column_parent, SlicePredicate predicate, String start_key, String finish_key, int max_keys)
    {
        this.keyspace = keyspace;
        column_family = column_parent.getColumn_family();
        super_column = column_parent.getSuper_column();
        this.predicate = predicate;
        this.start_key = start_key;
        this.finish_key = finish_key;
        this.max_keys = max_keys;
    }

    public RangeSliceCommand(RangeSliceCommand cmd, int max_keys)
    {
        this(cmd.keyspace,
             new ColumnParent(cmd.column_family, cmd.super_column),
             new SlicePredicate(cmd.predicate),
             cmd.start_key,
             cmd.finish_key,
             max_keys);

    }

    public Message getMessage() throws IOException
    {
        DataOutputBuffer dob = new DataOutputBuffer();
        serializer.serialize(this, dob);
        return new Message(FBUtilities.getLocalAddress(),
                           StageManager.readStage_,
                           StorageService.rangeSliceVerbHandler_,
                           Arrays.copyOf(dob.getData(), dob.getLength()));
    }

    public static RangeSliceCommand read(Message message) throws IOException
    {
        byte[] bytes = message.getMessageBody();
        DataInputBuffer dib = new DataInputBuffer();
        dib.reset(bytes, bytes.length);
        return serializer.deserialize(new DataInputStream(dib));
    }
}

class SliceCommandSerializer implements ICompactSerializer<RangeSliceCommand>
{
    public void serialize(RangeSliceCommand sliceCommand, DataOutputStream dos) throws IOException
    {
        dos.writeUTF(sliceCommand.keyspace);
        dos.writeUTF(sliceCommand.column_family);
        dos.writeInt(sliceCommand.super_column == null ? 0 : sliceCommand.super_column.length);
        if (sliceCommand.super_column != null)
            dos.write(sliceCommand.super_column);

        TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
        try
        {
            byte[] serPred = ser.serialize(sliceCommand.predicate);
            dos.writeInt(serPred.length);
            dos.write(serPred);
        }
        catch (TException ex)
        {
            throw new IOException(ex);
        }

        dos.writeUTF(sliceCommand.start_key);
        dos.writeUTF(sliceCommand.finish_key);
        dos.writeInt(sliceCommand.max_keys);
    }

    public RangeSliceCommand deserialize(DataInputStream dis) throws IOException
    {
        String keyspace = dis.readUTF();
        String column_family = dis.readUTF();

        int scLength = dis.readInt();
        byte[] super_column = null;
        if (scLength > 0)
            super_column = readBuf(scLength, dis);

        byte[] predBytes = new byte[dis.readInt()];
        dis.readFully(predBytes);
        TDeserializer dser = new TDeserializer(new TBinaryProtocol.Factory());
        SlicePredicate pred =  new SlicePredicate();
        try
        {
            dser.deserialize(pred, predBytes);
        }
        catch (TException ex)
        {
            throw new IOException(ex);
        }

        String start_key = dis.readUTF();
        String finish_key = dis.readUTF();
        int max_keys = dis.readInt();
        return new RangeSliceCommand(keyspace,
                                     new ColumnParent(column_family, super_column),
                                     pred,
                                     start_key,
                                     finish_key,
                                     max_keys);

    }

    static byte[] readBuf(int len, DataInputStream dis) throws IOException
    {
        byte[] buf = new byte[len];
        int read = 0;
        while (read < len)
            read = dis.read(buf, read, len - read);
        return buf;
    }
}
