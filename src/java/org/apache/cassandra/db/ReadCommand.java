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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.IReadCommand;
import org.apache.cassandra.service.RowDataResolver;
import org.apache.cassandra.utils.IFilter;


public abstract class ReadCommand implements IReadCommand
{
    public enum Type {
        GET_BY_NAMES((byte)1),
        GET_SLICES((byte)2);

        public final byte serializedValue;

        private Type(byte b)
        {
            this.serializedValue = b;
        }

        public static Type fromSerializedValue(byte b)
        {
            return b == 1 ? GET_BY_NAMES : GET_SLICES;
        }
    }

    public static final ReadCommandSerializer serializer = new ReadCommandSerializer();

    public MessageOut<ReadCommand> createMessage()
    {
        return new MessageOut<ReadCommand>(MessagingService.Verb.READ, this, serializer);
    }

    public final String table;
    public final String cfName;
    public final ByteBuffer key;
    private boolean isDigestQuery = false;
    protected final Type commandType;

    protected ReadCommand(String table, ByteBuffer key, String cfName, Type cmdType)
    {
        this.table = table;
        this.key = key;
        this.cfName = cfName;
        this.commandType = cmdType;
    }

    public static ReadCommand create(String table, ByteBuffer key, String cfName, IDiskAtomFilter filter)
    {
        if (filter instanceof SliceQueryFilter)
            return new SliceFromReadCommand(table, key, cfName, (SliceQueryFilter)filter);
        else
            return new SliceByNamesReadCommand(table, key, cfName, (NamesQueryFilter)filter);
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
        return cfName;
    }

    public abstract ReadCommand copy();

    public abstract Row getRow(Table table) throws IOException;

    public abstract IDiskAtomFilter filter();

    public String getKeyspace()
    {
        return table;
    }

    // maybeGenerateRetryCommand is used to generate a retry for short reads
    public ReadCommand maybeGenerateRetryCommand(RowDataResolver resolver, Row row)
    {
        return null;
    }

    // maybeTrim removes columns from a response that is too long
    public void maybeTrim(Row row)
    {
        // noop
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getReadRpcTimeout();
    }
}

class ReadCommandSerializer implements IVersionedSerializer<ReadCommand>
{
    public void serialize(ReadCommand command, DataOutput out, int version) throws IOException
    {
        // For super columns, when talking to an older node, we need to translate the filter used.
        // That translation can change the filter type (names -> slice), and so change the command type.
        // Hence we need to detect that early on, before we've written the command type.
        ReadCommand newCommand = command;
        ByteBuffer superColumn = null;
        if (version < MessagingService.VERSION_20)
        {
            CFMetaData metadata = Schema.instance.getCFMetaData(command.table, command.cfName);
            if (metadata.cfType == ColumnFamilyType.Super)
            {
                SuperColumns.SCFilter scFilter = SuperColumns.filterToSC((CompositeType)metadata.comparator, command.filter());
                newCommand = ReadCommand.create(command.table, command.key, command.cfName, scFilter.updatedFilter);
                newCommand.setDigestQuery(command.isDigestQuery());
                superColumn = scFilter.scName;
            }
        }

        out.writeByte(newCommand.commandType.serializedValue);
        switch (command.commandType)
        {
            case GET_BY_NAMES:
                SliceByNamesReadCommand.serializer.serialize(newCommand, superColumn, out, version);
                break;
            case GET_SLICES:
                SliceFromReadCommand.serializer.serialize(newCommand, superColumn, out, version);
                break;
            default:
                throw new AssertionError();
        }
    }

    public ReadCommand deserialize(DataInput in, int version) throws IOException
    {
        ReadCommand.Type msgType = ReadCommand.Type.fromSerializedValue(in.readByte());
        switch (msgType)
        {
            case GET_BY_NAMES:
                return SliceByNamesReadCommand.serializer.deserialize(in, version);
            case GET_SLICES:
                return SliceFromReadCommand.serializer.deserialize(in, version);
            default:
                throw new AssertionError();
        }
    }

    public long serializedSize(ReadCommand command, int version)
    {
        ReadCommand newCommand = command;
        ByteBuffer superColumn = null;
        if (version < MessagingService.VERSION_20)
        {
            CFMetaData metadata = Schema.instance.getCFMetaData(command.table, command.cfName);
            if (metadata.cfType == ColumnFamilyType.Super)
            {
                SuperColumns.SCFilter scFilter = SuperColumns.filterToSC((CompositeType)metadata.comparator, command.filter());
                newCommand = ReadCommand.create(command.table, command.key, command.cfName, scFilter.updatedFilter);
                newCommand.setDigestQuery(command.isDigestQuery());
                superColumn = scFilter.scName;
            }
        }

        switch (command.commandType)
        {
            case GET_BY_NAMES:
                return 1 + SliceByNamesReadCommand.serializer.serializedSize(newCommand, superColumn, version);
            case GET_SLICES:
                return 1 + SliceFromReadCommand.serializer.serializedSize(newCommand, superColumn, version);
            default:
                throw new AssertionError();
        }
    }
}
