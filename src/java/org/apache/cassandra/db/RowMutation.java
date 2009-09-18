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
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.nio.ByteBuffer;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.config.DatabaseDescriptor;

public class RowMutation implements Serializable
{
    private static ICompactSerializer<RowMutation> serializer_;
    public static final String HINT = "HINT";

    static
    {
        serializer_ = new RowMutationSerializer();
    }   

    static ICompactSerializer<RowMutation> serializer()
    {
        return serializer_;
    }

    private String table_;
    private String key_;
    protected Map<String, ColumnFamily> modifications_ = new HashMap<String, ColumnFamily>();

    public RowMutation(String table, String key)
    {
        table_ = table;
        key_ = key;
    }

    public RowMutation(String table, Row row)
    {
        table_ = table;
        key_ = row.key();
        for (ColumnFamily cf : row.getColumnFamilies())
        {
            add(cf);
        }
    }

    protected RowMutation(String table, String key, Map<String, ColumnFamily> modifications)
    {
        table_ = table;
        key_ = key;
        modifications_ = modifications;
    }

    public String table()
    {
        return table_;
    }

    public String key()
    {
        return key_;
    }

    public Set<String> columnFamilyNames()
    {
        return modifications_.keySet();
    }

    void addHints(String key, String host) throws IOException
    {
        QueryPath path = new QueryPath(HintedHandOffManager.HINTS_CF, key.getBytes("UTF-8"), host.getBytes("UTF-8"));
        add(path, ArrayUtils.EMPTY_BYTE_ARRAY, System.currentTimeMillis());
    }

    /*
     * Specify a column family name and the corresponding column
     * family object.
     * param @ cf - column family name
     * param @ columnFamily - the column family.
    */
    public void add(ColumnFamily columnFamily)
    {
        if (modifications_.containsKey(columnFamily.name()))
        {
            throw new IllegalArgumentException("ColumnFamily " + columnFamily.name() + " is already being modified");
        }
        modifications_.put(columnFamily.name(), columnFamily);
    }

    /*
     * Specify a column name and a corresponding value for
     * the column. Column name is specified as <column family>:column.
     * This will result in a ColumnFamily associated with
     * <column family> as name and a Column with <column>
     * as name. The column can be further broken up
     * as super column name : columnname  in case of super columns
     *
     * param @ cf - column name as <column family>:<column>
     * param @ value - value associated with the column
     * param @ timestamp - timestamp associated with this data.
    */
    public void add(QueryPath path, byte[] value, long timestamp)
    {
        ColumnFamily columnFamily = modifications_.get(path.columnFamilyName);
        if (columnFamily == null)
        {
            columnFamily = ColumnFamily.create(table_, path.columnFamilyName);
        }
        columnFamily.addColumn(path, value, timestamp);
        modifications_.put(path.columnFamilyName, columnFamily);
    }

    public void delete(QueryPath path, long timestamp)
    {
        assert path.columnFamilyName != null;
        String cfName = path.columnFamilyName;

        if (modifications_.containsKey(cfName))
        {
            throw new IllegalArgumentException("ColumnFamily " + cfName + " is already being modified");
        }

        int localDeleteTime = (int) (System.currentTimeMillis() / 1000);

        ColumnFamily columnFamily = modifications_.get(cfName);
        if (columnFamily == null)
            columnFamily = ColumnFamily.create(table_, cfName);

        if (path.superColumnName == null && path.columnName == null)
        {
            columnFamily.delete(localDeleteTime, timestamp);
        }
        else if (path.columnName == null)
        {
            SuperColumn sc = new SuperColumn(path.superColumnName, DatabaseDescriptor.getSubComparator(table_, cfName));
            sc.markForDeleteAt(localDeleteTime, timestamp);
            columnFamily.addColumn(sc);
        }
        else
        {
            ByteBuffer bytes = ByteBuffer.allocate(4);
            bytes.putInt(localDeleteTime);
            columnFamily.addColumn(path, bytes.array(), timestamp, true);
        }

        modifications_.put(cfName, columnFamily);
    }

    /*
     * This is equivalent to calling commit. Applies the changes to
     * to the table that is obtained by calling Table.open().
    */
    public void apply() throws IOException
    {
        Row row = createRow();
        Table.open(table_).apply(row, row.getSerializedBuffer());
    }

    private Row createRow()
    {
        Row row = new Row(table_, key_);
        for (String cfName : modifications_.keySet())
        {
            row.addColumnFamily(modifications_.get(cfName));
        }
        return row;
    }

    /*
     * This is equivalent to calling commit. Applies the changes to
     * to the table that is obtained by calling Table.open().
    */
    void applyBinary() throws IOException, ExecutionException, InterruptedException
    {
        Table.open(table_).load(createRow());
    }

    public Message makeRowMutationMessage() throws IOException
    {
        return makeRowMutationMessage(StorageService.mutationVerbHandler_);
    }

    public Message makeRowMutationMessage(String verbHandlerName) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        serializer().serialize(this, dos);
        EndPoint local = StorageService.getLocalStorageEndPoint();
        EndPoint from = (local != null) ? local : new EndPoint(FBUtilities.getHostAddress(), 7000);
        return new Message(from, StorageService.mutationStage_, verbHandlerName, bos.toByteArray());
    }

    public static RowMutation getRowMutation(String table, String key, Map<String, List<ColumnOrSuperColumn>> cfmap)
    {
        RowMutation rm = new RowMutation(table, key.trim());
        for (Map.Entry<String, List<ColumnOrSuperColumn>> entry : cfmap.entrySet())
        {
            String cfName = entry.getKey();
            for (ColumnOrSuperColumn cosc : entry.getValue())
            {
                if (cosc.column == null)
                {
                    assert cosc.super_column != null;
                    for (org.apache.cassandra.service.Column column : cosc.super_column.columns)
                    {
                        rm.add(new QueryPath(cfName, cosc.super_column.name, column.name), column.value, column.timestamp);
                    }
                }
                else
                {
                    assert cosc.super_column == null;
                    rm.add(new QueryPath(cfName, null, cosc.column.name), cosc.column.value, cosc.column.timestamp);
                }
            }
        }
        return rm;
    }

    public String toString()
    {
        return "RowMutation(" +
               "table='" + table_ + '\'' +
               ", key='" + key_ + '\'' +
               ", modifications=[" + StringUtils.join(modifications_.values(), ", ") + "]" +
               ')';
    }
}

class RowMutationSerializer implements ICompactSerializer<RowMutation>
{
    private void freezeTheMaps(Map<String, ColumnFamily> map, DataOutputStream dos) throws IOException
    {
        int size = map.size();
        dos.writeInt(size);
        if (size > 0)
        {
            Set<String> keys = map.keySet();
            for (String key : keys)
            {
                dos.writeUTF(key);
                ColumnFamily cf = map.get(key);
                if (cf != null)
                {
                    ColumnFamily.serializer().serialize(cf, dos);
                }
            }
        }
    }

    public void serialize(RowMutation rm, DataOutputStream dos) throws IOException
    {
        dos.writeUTF(rm.table());
        dos.writeUTF(rm.key());

        /* serialize the modifications_ in the mutation */
        freezeTheMaps(rm.modifications_, dos);
    }

    private Map<String, ColumnFamily> defreezeTheMaps(DataInputStream dis) throws IOException
    {
        Map<String, ColumnFamily> map = new HashMap<String, ColumnFamily>();
        int size = dis.readInt();
        for (int i = 0; i < size; ++i)
        {
            String key = dis.readUTF();
            ColumnFamily cf = ColumnFamily.serializer().deserialize(dis);
            map.put(key, cf);
        }
        return map;
    }

    public RowMutation deserialize(DataInputStream dis) throws IOException
    {
        String table = dis.readUTF();
        String key = dis.readUTF();
        Map<String, ColumnFamily> modifications = defreezeTheMaps(dis);
        return new RowMutation(table, key, modifications);
    }
}
