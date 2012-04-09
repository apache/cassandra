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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.IReadCommand;
import org.apache.cassandra.service.RepairCallback;


public abstract class ReadCommand implements IReadCommand
{
    public static final byte CMD_TYPE_GET_SLICE_BY_NAMES = 1;
    public static final byte CMD_TYPE_GET_SLICE = 2;

    public static final ReadCommandSerializer serializer = new ReadCommandSerializer();

    public MessageOut<ReadCommand> createMessage()
    {
        return new MessageOut<ReadCommand>(MessagingService.Verb.READ, this, serializer);
    }

    public final QueryPath queryPath;
    public final String table;
    public final ByteBuffer key;
    private boolean isDigestQuery = false;
    protected final byte commandType;

    protected ReadCommand(String table, ByteBuffer key, QueryPath queryPath, byte cmdType)
    {
        this.table = table;
        this.key = key;
        this.queryPath = queryPath;
        this.commandType = cmdType;
    }

    public boolean isDigestQuery()
    {
        return isDigestQuery;
    }

    public void setDigestQuery(boolean isDigestQuery)
    {
        this.isDigestQuery = isDigestQuery;
    }

    public String getColumnFamilyName()
    {
        return queryPath.columnFamilyName;
    }

    public abstract ReadCommand copy();

    public abstract Row getRow(Table table) throws IOException;

    protected AbstractType<?> getComparator()
    {
        return ColumnFamily.getComparatorFor(table, getColumnFamilyName(), queryPath.superColumnName);
    }

    public String getKeyspace()
    {
        return table;
    }

    // maybeGenerateRetryCommand is used to generate a retry for short reads
    public ReadCommand maybeGenerateRetryCommand(RepairCallback handler, Row row)
    {
        return null;
    }

    // maybeTrim removes columns from a response that is too long
    public void maybeTrim(Row row)
    {
        // noop
    }
}

class ReadCommandSerializer implements IVersionedSerializer<ReadCommand>
{
    private static final Map<Byte, IVersionedSerializer<ReadCommand>> CMD_SERIALIZER_MAP = new HashMap<Byte, IVersionedSerializer<ReadCommand>>();
    static
    {
        CMD_SERIALIZER_MAP.put(ReadCommand.CMD_TYPE_GET_SLICE_BY_NAMES, new SliceByNamesReadCommandSerializer());
        CMD_SERIALIZER_MAP.put(ReadCommand.CMD_TYPE_GET_SLICE, new SliceFromReadCommandSerializer());
    }


    public void serialize(ReadCommand command, DataOutput dos, int version) throws IOException
    {
        dos.writeByte(command.commandType);
        CMD_SERIALIZER_MAP.get(command.commandType).serialize(command, dos, version);
    }

    public ReadCommand deserialize(DataInput dis, int version) throws IOException
    {
        byte msgType = dis.readByte();
        return CMD_SERIALIZER_MAP.get(msgType).deserialize(dis, version);
    }

    public long serializedSize(ReadCommand command, int version)
    {
        return 1 + CMD_SERIALIZER_MAP.get(command.commandType).serializedSize(command, version);
    }
}
