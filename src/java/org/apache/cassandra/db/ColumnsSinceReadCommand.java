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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.cassandra.service.ColumnParent;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.TimeQueryFilter;

public class ColumnsSinceReadCommand extends ReadCommand
{
    public final QueryPath columnParent;
    public final long sinceTimestamp;

    public ColumnsSinceReadCommand(String table, String key, ColumnParent column_parent, long sinceTimestamp)
    {
        this(table, key, new QueryPath(column_parent), sinceTimestamp);
    }

    public ColumnsSinceReadCommand(String table, String key, QueryPath columnParent, long sinceTimestamp)
    {
        super(table, key, CMD_TYPE_GET_COLUMNS_SINCE);
        this.columnParent = columnParent;
        this.sinceTimestamp = sinceTimestamp;
    }

    @Override
    public String getColumnFamilyName()
    {
        return columnParent.columnFamilyName;
    }

    @Override
    public ReadCommand copy()
    {
        ReadCommand readCommand = new ColumnsSinceReadCommand(table, key, columnParent, sinceTimestamp);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }

    @Override
    public Row getRow(Table table) throws IOException
    {        
        return table.getRow(new TimeQueryFilter(key, columnParent, sinceTimestamp));
    }

    @Override
    public String toString()
    {
        return "ColumnsSinceReadCommand(" +
               "table='" + table + '\'' +
               ", key='" + key + '\'' +
               ", columnParent='" + columnParent + '\'' +
               ", sinceTimestamp='" + sinceTimestamp + '\'' +
               ')';
    }
}

class ColumnsSinceReadCommandSerializer extends ReadCommandSerializer
{
    @Override
    public void serialize(ReadCommand rm, DataOutputStream dos) throws IOException
    {
        ColumnsSinceReadCommand realRM = (ColumnsSinceReadCommand)rm;
        dos.writeBoolean(realRM.isDigestQuery());
        dos.writeUTF(realRM.table);
        dos.writeUTF(realRM.key);
        realRM.columnParent.serialize(dos);
        dos.writeLong(realRM.sinceTimestamp);
    }

    @Override
    public ReadCommand deserialize(DataInputStream dis) throws IOException
    {
        boolean isDigest = dis.readBoolean();
        String table = dis.readUTF();
        String key = dis.readUTF();
        QueryPath columnParent = QueryPath.deserialize(dis);
        long sinceTimestamp = dis.readLong();

        ColumnsSinceReadCommand rm = new ColumnsSinceReadCommand(table, key, columnParent, sinceTimestamp);
        rm.setDigestQuery(isDigest);
        return rm;
    }
}
