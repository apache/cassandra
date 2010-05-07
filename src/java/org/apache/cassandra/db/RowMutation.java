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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.nio.ByteBuffer;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.*;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.concurrent.StageManager;

public class RowMutation
{
    private static ICompactSerializer<RowMutation> serializer_;
    public static final String HINT = "HINT";

    static
    {
        serializer_ = new RowMutationSerializer();
    }   

    public static ICompactSerializer<RowMutation> serializer()
    {
        return serializer_;
    }

    private String table_;
    private byte[] key_;
    protected Map<String, ColumnFamily> modifications_ = new HashMap<String, ColumnFamily>();

    public RowMutation(String table, byte[] key)
    {
        table_ = table;
        key_ = key;
    }

    public RowMutation(String table, Row row)
    {
        table_ = table;
        key_ = row.key.key;
        add(row.cf);
    }

    protected RowMutation(String table, byte[] key, Map<String, ColumnFamily> modifications)
    {
        table_ = table;
        key_ = key;
        modifications_ = modifications;
    }

    public String getTable()
    {
        return table_;
    }

    public byte[] key()
    {
        return key_;
    }

    public Set<String> columnFamilyNames()
    {
        return modifications_.keySet();
    }
    
    public Collection<ColumnFamily> getColumnFamilies()
    {
        return modifications_.values();
    }

    void addHints(byte[] key, byte[] host) throws IOException
    {
        QueryPath path = new QueryPath(HintedHandOffManager.HINTS_CF, key, host);
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
        assert columnFamily != null;
        if (modifications_.containsKey(columnFamily.name()))
        {
            throw new IllegalArgumentException("ColumnFamily " + columnFamily.name() + " is already being modified");
        }
        modifications_.put(columnFamily.name(), columnFamily);
    }

    /** should only be called by commitlog replay code */
    public void removeColumnFamily(String cfName)
    {
        modifications_.remove(cfName);
    }
    
    public boolean isEmpty()
    {
        return modifications_.isEmpty();
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
     * param @ timeToLive - ttl for the column, 0 for standard (non expiring) columns
    */
    public void add(QueryPath path, byte[] value, long timestamp, int timeToLive)
    {
        ColumnFamily columnFamily = modifications_.get(path.columnFamilyName);
        if (columnFamily == null)
        {
            columnFamily = ColumnFamily.create(table_, path.columnFamilyName);
        }
        columnFamily.addColumn(path, value, timestamp, timeToLive);
        modifications_.put(path.columnFamilyName, columnFamily);
    }

    public void add(QueryPath path, byte[] value, long timestamp)
    {
        add(path, value, timestamp, 0);
    }

    public void delete(QueryPath path, long timestamp)
    {
        assert path.columnFamilyName != null;
        String cfName = path.columnFamilyName;

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
            columnFamily.deleteColumn(path, localDeleteTime, timestamp);
        }

        modifications_.put(cfName, columnFamily);
    }

    /*
     * This is equivalent to calling commit. Applies the changes to
     * to the table that is obtained by calling Table.open().
    */
    public void apply() throws IOException
    {
        Table.open(table_).apply(this, getSerializedBuffer(), true);
    }

    /*
     * This is equivalent to calling commit. Applies the changes to
     * to the table that is obtained by calling Table.open().
    */
    void applyBinary() throws IOException, ExecutionException, InterruptedException
    {
        Table.open(table_).load(this);
    }

    public Message makeRowMutationMessage() throws IOException
    {
        return makeRowMutationMessage(StorageService.Verb.MUTATION);
    }

    public Message makeRowMutationMessage(StorageService.Verb verb) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        serializer().serialize(this, dos);
        return new Message(FBUtilities.getLocalAddress(), StageManager.MUTATION_STAGE, verb, bos.toByteArray());
    }

    public static RowMutation getRowMutationFromMutations(String keyspace, byte[] key, Map<String, List<Mutation>> cfmap)
    {
        RowMutation rm = new RowMutation(keyspace, key);
        for (Map.Entry<String, List<Mutation>> entry : cfmap.entrySet())
        {
            String cfName = entry.getKey();
            for (Mutation mutation : entry.getValue())
            {
                if (mutation.deletion != null)
                {
                    deleteColumnOrSuperColumnToRowMutation(rm, cfName, mutation.deletion);
                }
                else
                {
                    addColumnOrSuperColumnToRowMutation(rm, cfName, mutation.column_or_supercolumn);
                }
            }
        }
        return rm;
    }
    
    public static RowMutation getRowMutation(String table, byte[] key, Map<String, List<ColumnOrSuperColumn>> cfmap)
    {
        RowMutation rm = new RowMutation(table, key);
        for (Map.Entry<String, List<ColumnOrSuperColumn>> entry : cfmap.entrySet())
        {
            String cfName = entry.getKey();
            for (ColumnOrSuperColumn cosc : entry.getValue())
            {
                if (cosc.column == null)
                {
                    assert cosc.super_column != null;
                    for (org.apache.cassandra.thrift.Column column : cosc.super_column.columns)
                    {
                        rm.add(new QueryPath(cfName, cosc.super_column.name, column.name), column.value, column.timestamp, column.ttl);
                    }
                }
                else
                {
                    assert cosc.super_column == null;
                    rm.add(new QueryPath(cfName, null, cosc.column.name), cosc.column.value, cosc.column.timestamp, cosc.column.ttl);
                }
            }
        }
        return rm;
    }
    
    public DataOutputBuffer getSerializedBuffer() throws IOException
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        RowMutation.serializer().serialize(this, buffer);
        return buffer;
    }

    public String toString()
    {
        return "RowMutation(" +
               "table='" + table_ + '\'' +
               ", key='" + FBUtilities.bytesToHex(key_) + '\'' +
               ", modifications=[" + StringUtils.join(modifications_.values(), ", ") + "]" +
               ')';
    }

    private static void addColumnOrSuperColumnToRowMutation(RowMutation rm, String cfName, ColumnOrSuperColumn cosc)
    {
        if (cosc.column == null)
        {
            for (org.apache.cassandra.thrift.Column column : cosc.super_column.columns)
            {
                rm.add(new QueryPath(cfName, cosc.super_column.name, column.name), column.value, column.timestamp);
            }
        }
        else
        {
            rm.add(new QueryPath(cfName, null, cosc.column.name), cosc.column.value, cosc.column.timestamp);
        }
    }

    private static void deleteColumnOrSuperColumnToRowMutation(RowMutation rm, String cfName, Deletion del)
    {
        if (del.predicate != null && del.predicate.column_names != null)
        {
            for(byte[] c : del.predicate.column_names)
            {
                if (del.super_column == null && DatabaseDescriptor.getColumnFamilyType(rm.table_, cfName) == ColumnFamilyType.Super)
                    rm.delete(new QueryPath(cfName, c), del.timestamp);
                else
                    rm.delete(new QueryPath(cfName, del.super_column, c), del.timestamp);
            }
        }
        else
        {
            rm.delete(new QueryPath(cfName, del.super_column), del.timestamp);
        }
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
            for (Map.Entry<String,ColumnFamily> entry : map.entrySet())
            {
                dos.writeUTF(entry.getKey());
                ColumnFamily.serializer().serialize(entry.getValue(), dos);
            }
        }
    }

    public void serialize(RowMutation rm, DataOutputStream dos) throws IOException
    {
        dos.writeUTF(rm.getTable());
        FBUtilities.writeShortByteArray(rm.key(), dos);

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
        byte[] key = FBUtilities.readShortByteArray(dis);
        Map<String, ColumnFamily> modifications = defreezeTheMaps(dis);
        return new RowMutation(table, key, modifications);
    }
}
