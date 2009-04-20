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
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class ReadCommand
{
    public static final String DO_REPAIR = "READ-REPAIR";

    private static ReadCommandSerializer serializer = new ReadCommandSerializer();

    public static ReadCommandSerializer serializer()
    {
        return serializer;
    }

    private static List<String> EMPTY_COLUMNS = Arrays.asList(new String[0]);

    public Message makeReadMessage() throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        ReadCommand.serializer().serialize(this, dos);
        return new Message(StorageService.getLocalStorageEndPoint(), StorageService.readStage_, StorageService.readVerbHandler_, bos.toByteArray());
    }

    public final String table;

    public final String key;

    public final String columnFamilyColumn;

    public final int start;

    public final int count;

    public final long sinceTimestamp;

    public final List<String> columnNames;

    private boolean isDigestQuery = false;

    public ReadCommand(String table, String key, String columnFamilyColumn, int start, int count, long sinceTimestamp, List<String> columnNames)
    {
        this.table = table;
        this.key = key;
        this.columnFamilyColumn = columnFamilyColumn;
        this.start = start;
        this.count = count;
        this.sinceTimestamp = sinceTimestamp;
        this.columnNames = Collections.unmodifiableList(columnNames);
    }

    public ReadCommand(String table, String key)
    {
        this(table, key, null, -1, -1, -1, EMPTY_COLUMNS);
    }

    public ReadCommand(String table, String key, String columnFamilyColumn)
    {
        this(table, key, columnFamilyColumn, -1, -1, -1, EMPTY_COLUMNS);
    }

    public ReadCommand(String table, String key, String columnFamilyColumn, List<String> columnNames)
    {
        this(table, key, columnFamilyColumn, -1, -1, -1, columnNames);
    }

    public ReadCommand(String table, String key, String columnFamilyColumn, int start, int count)
    {
        this(table, key, columnFamilyColumn, start, count, -1, EMPTY_COLUMNS);
    }

    public ReadCommand(String table, String key, String columnFamilyColumn, long sinceTimestamp)
    {
        this(table, key, columnFamilyColumn, -1, -1, sinceTimestamp, EMPTY_COLUMNS);
    }

    public boolean isDigestQuery()
    {
        return isDigestQuery;
    }

    public void setDigestQuery(boolean isDigestQuery)
    {
        this.isDigestQuery = isDigestQuery;
    }

    public ReadCommand copy()
    {
        return new ReadCommand(table, key, columnFamilyColumn, start, count, sinceTimestamp, columnNames);
    }

    public String toString()
    {
        return "ReadMessage(" +
               "table='" + table + '\'' +
               ", key='" + key + '\'' +
               ", columnFamilyColumn='" + columnFamilyColumn + '\'' +
               ", start=" + start +
               ", count=" + count +
               ", sinceTimestamp=" + sinceTimestamp +
               ", columns=[" + StringUtils.join(columnNames, ", ") + "]" +
               ')';
    }
}

class ReadCommandSerializer implements ICompactSerializer<ReadCommand>
{
    public void serialize(ReadCommand rm, DataOutputStream dos) throws IOException
    {
        dos.writeUTF(rm.table);
        dos.writeUTF(rm.key);
        dos.writeUTF(rm.columnFamilyColumn);
        dos.writeInt(rm.start);
        dos.writeInt(rm.count);
        dos.writeLong(rm.sinceTimestamp);
        dos.writeBoolean(rm.isDigestQuery());
        dos.writeInt(rm.columnNames.size());
        if (rm.columnNames.size() > 0)
        {
            for (String cName : rm.columnNames)
            {
                dos.writeInt(cName.getBytes().length);
                dos.write(cName.getBytes());
            }
        }
    }

    public ReadCommand deserialize(DataInputStream dis) throws IOException
    {
        String table = dis.readUTF();
        String key = dis.readUTF();
        String columnFamily_column = dis.readUTF();
        int start = dis.readInt();
        int count = dis.readInt();
        long sinceTimestamp = dis.readLong();
        boolean isDigest = dis.readBoolean();

        int size = dis.readInt();
        List<String> columns = new ArrayList<String>();
        for (int i = 0; i < size; ++i)
        {
            byte[] bytes = new byte[dis.readInt()];
            dis.readFully(bytes);
            columns.add(new String(bytes));
        }
        ReadCommand rm = null;
        if (columns.size() > 0)
        {
            rm = new ReadCommand(table, key, columnFamily_column, columns);
        }
        else if (sinceTimestamp > 0)
        {
            rm = new ReadCommand(table, key, columnFamily_column, sinceTimestamp);
        }
        else
        {
            rm = new ReadCommand(table, key, columnFamily_column, start, count);
        }
        rm.setDigestQuery(isDigest);
        return rm;
    }
}
