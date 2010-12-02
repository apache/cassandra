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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang.StringUtils;

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
    private ByteBuffer key_;
    // map of column family id to mutations for that column family.
    protected Map<Integer, ColumnFamily> modifications_ = new HashMap<Integer, ColumnFamily>();
    
    private byte[] preserializedBuffer = null;

    public RowMutation(String table, ByteBuffer key)
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

    protected RowMutation(String table, ByteBuffer key, Map<Integer, ColumnFamily> modifications)
    {
        table_ = table;
        key_ = key;
        modifications_ = modifications;
    }

    public String getTable()
    {
        return table_;
    }

    public ByteBuffer key()
    {
        return key_;
    }

    public Collection<ColumnFamily> getColumnFamilies()
    {
        return modifications_.values();
    }

    void addHints(RowMutation rm) throws IOException
    {
        for (ColumnFamily cf : rm.getColumnFamilies())
        {
            ByteBuffer combined = HintedHandOffManager.makeCombinedName(rm.getTable(), cf.metadata().cfName);
            QueryPath path = new QueryPath(HintedHandOffManager.HINTS_CF, rm.key(), combined);
            add(path, FBUtilities.EMPTY_BYTE_BUFFER, System.currentTimeMillis(), cf.metadata().getGcGraceSeconds());
        }
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
        ColumnFamily prev = modifications_.put(columnFamily.id(), columnFamily);
        if (prev != null)
            // developer error
            throw new IllegalArgumentException("ColumnFamily " + columnFamily + " already has modifications in this mutation: " + prev);
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
    public void add(QueryPath path, ByteBuffer value, long timestamp, int timeToLive)
    {
        Integer id = CFMetaData.getId(table_, path.columnFamilyName);
        ColumnFamily columnFamily = modifications_.get(id);
        if (columnFamily == null)
        {
            columnFamily = ColumnFamily.create(table_, path.columnFamilyName);
            modifications_.put(id, columnFamily);
        }
        columnFamily.addColumn(path, value, timestamp, timeToLive);
    }

    public void add(QueryPath path, ByteBuffer value, long timestamp)
    {
        add(path, value, timestamp, 0);
    }

    public void delete(QueryPath path, long timestamp)
    {
        Integer id = CFMetaData.getId(table_, path.columnFamilyName);

        int localDeleteTime = (int) (System.currentTimeMillis() / 1000);

        ColumnFamily columnFamily = modifications_.get(id);
        if (columnFamily == null)
        {
            columnFamily = ColumnFamily.create(table_, path.columnFamilyName);
            modifications_.put(id, columnFamily);
        }

        if (path.superColumnName == null && path.columnName == null)
        {
            columnFamily.delete(localDeleteTime, timestamp);
        }
        else if (path.columnName == null)
        {
            SuperColumn sc = new SuperColumn(path.superColumnName, columnFamily.getSubComparator());
            sc.markForDeleteAt(localDeleteTime, timestamp);
            columnFamily.addColumn(sc);
        }
        else
        {
            columnFamily.addTombstone(path, localDeleteTime, timestamp);
        }
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
        return new Message(FBUtilities.getLocalAddress(), verb, getSerializedBuffer());
    }

    public static RowMutation getRowMutationFromMutations(String keyspace, ByteBuffer key, Map<String, List<Mutation>> cfmap)
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
    
    private synchronized byte[] getSerializedBuffer() throws IOException
    {
        if (preserializedBuffer == null)
        {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream dout = new DataOutputStream(bout);
            RowMutation.serializer().serialize(this, dout);
            dout.close();
            preserializedBuffer = bout.toByteArray();
        }
        return preserializedBuffer;
    }

    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean shallow)
    {
        StringBuilder buff = new StringBuilder("RowMutation(");
        buff.append("keyspace='").append(table_).append('\'');
        buff.append(", key='").append(FBUtilities.bytesToHex(key_)).append('\'');
        buff.append(", modifications=[");
        if (shallow)
        {
            List<String> cfnames = new ArrayList<String>();
            for (Integer cfid : modifications_.keySet())
            {
                CFMetaData cfm = DatabaseDescriptor.getCFMetaData(cfid);
                cfnames.add(cfm == null ? "-dropped-" : cfm.cfName);
            }
            buff.append(StringUtils.join(cfnames, ", "));
        }
        else
            buff.append(StringUtils.join(modifications_.values(), ", "));
        return buff.append("])").toString();
    }

    private static void addColumnOrSuperColumnToRowMutation(RowMutation rm, String cfName, ColumnOrSuperColumn cosc)
    {
        if (cosc.column == null)
        {
            for (org.apache.cassandra.thrift.Column column : cosc.super_column.columns)
            {
                rm.add(new QueryPath(cfName, cosc.super_column.name, column.name), column.value, column.timestamp, column.ttl);
            }
        }
        else
        {
            rm.add(new QueryPath(cfName, null, cosc.column.name), cosc.column.value, cosc.column.timestamp, cosc.column.ttl);
        }
    }

    private static void deleteColumnOrSuperColumnToRowMutation(RowMutation rm, String cfName, Deletion del)
    {
        if (del.predicate != null && del.predicate.column_names != null)
        {
            for(ByteBuffer c : del.predicate.column_names)
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
    private void freezeTheMaps(Map<Integer, ColumnFamily> map, DataOutputStream dos) throws IOException
    {
        int size = map.size();
        dos.writeInt(size);
        if (size > 0)
        {
            for (Map.Entry<Integer,ColumnFamily> entry : map.entrySet())
            {
                dos.writeInt(entry.getKey());
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

    private Map<Integer, ColumnFamily> defreezeTheMaps(DataInputStream dis) throws IOException
    {
        Map<Integer, ColumnFamily> map = new HashMap<Integer, ColumnFamily>();
        int size = dis.readInt();
        for (int i = 0; i < size; ++i)
        {
            Integer cfid = Integer.valueOf(dis.readInt());
            ColumnFamily cf = ColumnFamily.serializer().deserialize(dis);
            map.put(cfid, cf);
        }
        return map;
    }

    public RowMutation deserialize(DataInputStream dis) throws IOException
    {
        String table = dis.readUTF();
        ByteBuffer key = FBUtilities.readShortByteArray(dis);
        Map<Integer, ColumnFamily> modifications = defreezeTheMaps(dis);
        return new RowMutation(table, key, modifications);
    }
}
