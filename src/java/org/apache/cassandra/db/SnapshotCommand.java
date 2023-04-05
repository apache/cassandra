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
package org.apache.cassandra.db;

import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class SnapshotCommand
{
    public static final SnapshotCommandSerializer serializer = new SnapshotCommandSerializer();

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
    public void serialize(SnapshotCommand snapshot_command, DataOutputPlus out, int version) throws IOException
    {
        out.writeUTF(snapshot_command.keyspace);
        out.writeUTF(snapshot_command.column_family);
        out.writeUTF(snapshot_command.snapshot_name);
        out.writeBoolean(snapshot_command.clear_snapshot);
    }

    public SnapshotCommand deserialize(DataInputPlus in, int version) throws IOException
    {
        String keyspace = in.readUTF();
        String column_family = in.readUTF();
        String snapshot_name = in.readUTF();
        boolean clear_snapshot = in.readBoolean();
        return new SnapshotCommand(keyspace, column_family, snapshot_name, clear_snapshot);
    }

    public long serializedSize(SnapshotCommand sc, int version)
    {
        return TypeSizes.sizeof(sc.keyspace)
             + TypeSizes.sizeof(sc.column_family)
             + TypeSizes.sizeof(sc.snapshot_name)
             + TypeSizes.sizeof(sc.clear_snapshot);
    }
}
