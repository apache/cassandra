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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.service.IReadCommand;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;


public abstract class ReadCommand implements MessageProducer, IReadCommand
{
    public static final byte CMD_TYPE_GET_SLICE_BY_NAMES = 1;
    public static final byte CMD_TYPE_GET_SLICE = 2;

    private static ReadCommandSerializer serializer = new ReadCommandSerializer();

    public static ReadCommandSerializer serializer()
    {
        return serializer;
    }

    public Message getMessage(Integer version) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        ReadCommand.serializer().serialize(this, dos, version);
        return new Message(FBUtilities.getLocalAddress(), StorageService.Verb.READ, bos.toByteArray(), version);
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

    protected AbstractType getComparator()
    {
        return ColumnFamily.getComparatorFor(table, getColumnFamilyName(), queryPath.superColumnName);
    }

    public String getKeyspace()
    {
        return table;
    }
}

class ReadCommandSerializer implements ICompactSerializer<ReadCommand>
{
    private static final Map<Byte, ReadCommandSerializer> CMD_SERIALIZER_MAP = new HashMap<Byte, ReadCommandSerializer>(); 
    static 
    {
        CMD_SERIALIZER_MAP.put(ReadCommand.CMD_TYPE_GET_SLICE_BY_NAMES, new SliceByNamesReadCommandSerializer());
        CMD_SERIALIZER_MAP.put(ReadCommand.CMD_TYPE_GET_SLICE, new SliceFromReadCommandSerializer());
    }


    public void serialize(ReadCommand rm, DataOutputStream dos, int version) throws IOException
    {
        dos.writeByte(rm.commandType);
        ReadCommandSerializer ser = CMD_SERIALIZER_MAP.get(rm.commandType);
        ser.serialize(rm, dos, version);
    }

    public ReadCommand deserialize(DataInputStream dis, int version) throws IOException
    {
        byte msgType = dis.readByte();
        return CMD_SERIALIZER_MAP.get(msgType).deserialize(dis, version);
    }
        
}
