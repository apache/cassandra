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
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.ColumnParent;
import org.apache.cassandra.service.SlicePredicate;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class RangeSliceCommand
{
    private static final SliceCommandSerializer serializer = new SliceCommandSerializer();
    
    public final String keyspace;

    public final String column_family;
    public final byte[] super_column;

    public final SlicePredicate predicate;

    public final DecoratedKey startKey;
    public final DecoratedKey finishKey;
    public final int max_keys;

    public RangeSliceCommand(String keyspace, ColumnParent column_parent, SlicePredicate predicate, DecoratedKey startKey, DecoratedKey finishKey, int max_keys)
    {
        this.keyspace = keyspace;
        column_family = column_parent.getColumn_family();
        super_column = column_parent.getSuper_column();
        this.predicate = predicate;
        this.startKey = startKey;
        this.finishKey = finishKey;
        this.max_keys = max_keys;
    }

    public RangeSliceCommand(String keyspace, String column_family, byte[] super_column, SlicePredicate predicate, DecoratedKey startKey, DecoratedKey finishKey, int max_keys)
    {
        this.keyspace = keyspace;
        this.column_family = column_family;
        this.super_column = super_column;
        this.predicate = predicate;
        this.startKey = startKey;
        this.finishKey = finishKey;
        this.max_keys = max_keys;
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

        DecoratedKey.serializer().serialize(sliceCommand.startKey, dos);
        DecoratedKey.serializer().serialize(sliceCommand.finishKey, dos);
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

        DecoratedKey startKey = DecoratedKey.serializer().deserialize(dis);
        DecoratedKey finishKey = DecoratedKey.serializer().deserialize(dis);
        int max_keys = dis.readInt();
        return new RangeSliceCommand(keyspace,
                                     new ColumnParent(column_family, super_column),
                                     pred,
                                     startKey,
                                     finishKey,
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
