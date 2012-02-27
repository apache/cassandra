package org.apache.cassandra.db;
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


import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class SnapshotCommand implements MessageProducer
{
    private static final SnapshotCommandSerializer serializer = new SnapshotCommandSerializer();

    public final String keyspace;
    public final String column_family;
    public final String snapshot_name;
    public final boolean clear_snapshot;

    public SnapshotCommand(String keyspace, String columnFamily, String snapshotName, boolean clearSnapshot)
    {
        this.keyspace = keyspace;
        this.column_family = columnFamily;
        this.snapshot_name = snapshotName;
        this.clear_snapshot = clearSnapshot;
    }

    public Message getMessage(Integer version) throws IOException
    {
        DataOutputBuffer dob = new DataOutputBuffer();
        serializer.serialize(this, dob, version);
        return new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.SNAPSHOT, Arrays.copyOf(dob.getData(), dob.getLength()), version);
    }

    public static SnapshotCommand read(Message message) throws IOException
    {
        byte[] bytes = message.getMessageBody();
        FastByteArrayInputStream bis = new FastByteArrayInputStream(bytes);
        return serializer.deserialize(new DataInputStream(bis), message.getVersion());
    }

    @Override
    public String toString()
    {
        return "SnapshotCommand{" + "keyspace='" + keyspace + '\'' +
                                  ", column_family='" + column_family + '\'' +
                                  ", snapshot_name=" + snapshot_name +
                                  ", clear_snapshot=" + clear_snapshot + '}';
    }
}

class SnapshotCommandSerializer implements IVersionedSerializer<SnapshotCommand>
{
    public void serialize(SnapshotCommand snapshot_command, DataOutput dos, int version) throws IOException
    {
        dos.writeUTF(snapshot_command.keyspace);
        dos.writeUTF(snapshot_command.column_family);
        dos.writeUTF(snapshot_command.snapshot_name);
        dos.writeBoolean(snapshot_command.clear_snapshot);
    }

    public SnapshotCommand deserialize(DataInput dis, int version) throws IOException
    {
        String keyspace = dis.readUTF();
        String column_family = dis.readUTF();
        String snapshot_name = dis.readUTF();
        boolean clear_snapshot = dis.readBoolean();
        return new SnapshotCommand(keyspace, column_family, snapshot_name, clear_snapshot);
    }

    public long serializedSize(SnapshotCommand snapshot_command, int version)
    {
        throw new UnsupportedOperationException();
    }
}
