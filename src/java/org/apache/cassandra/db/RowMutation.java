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
import java.util.Iterator;
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
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.BatchMutationSuper;
import org.apache.cassandra.service.BatchMutation;
import org.apache.cassandra.service.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.config.DatabaseDescriptor;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

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
        Row row = new Row(table_, key_);
        apply(row);
    }

    /*
     * Allows RowMutationVerbHandler to optimize by re-using a single Row object.
    */
    void apply(Row emptyRow) throws IOException
    {
        assert emptyRow.getColumnFamilies().size() == 0;
        Table table = Table.open(table_);
        for (String cfName : modifications_.keySet())
        {
            assert table.isValidColumnFamily(cfName);
            emptyRow.addColumnFamily(modifications_.get(cfName));
        }
        table.apply(emptyRow);
    }

    /*
     * This is equivalent to calling commit. Applies the changes to
     * to the table that is obtained by calling Table.open().
    */
    void applyBinary(Row emptyRow) throws IOException, ExecutionException, InterruptedException
    {
        assert emptyRow.getColumnFamilies().size() == 0;
        Table table = Table.open(table_);
        Set<String> cfNames = modifications_.keySet();
        for (String cfName : cfNames)
        {
            assert table.isValidColumnFamily(cfName);
            emptyRow.addColumnFamily(modifications_.get(cfName));
        }
        table.load(emptyRow);
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

    public static RowMutation getRowMutation(String table, BatchMutation batchMutation)
    {
        RowMutation rm = new RowMutation(table, batchMutation.key.trim());
        for (String cfname : batchMutation.cfmap.keySet())
        {
            List<org.apache.cassandra.service.Column> list = batchMutation.cfmap.get(cfname);
            for (org.apache.cassandra.service.Column column : list)
            {
                rm.add(new QueryPath(cfname, null, column.name), column.value, column.timestamp);
            }
        }
        return rm;
    }

    public static RowMutation getRowMutation(String table, BatchMutationSuper batchMutationSuper) throws InvalidRequestException
    {
        RowMutation rm = new RowMutation(table, batchMutationSuper.key.trim());
        for (String cfName : batchMutationSuper.cfmap.keySet())
        {
            for (org.apache.cassandra.service.SuperColumn super_column : batchMutationSuper.cfmap.get(cfName))
            {
                for (org.apache.cassandra.service.Column column : super_column.columns)
                {
                    try
                    {
                        rm.add(new QueryPath(cfName, super_column.name, column.name), column.value, column.timestamp);
                    }
                    catch (MarshalException e)
                    {
                        throw new InvalidRequestException(e.getMessage());
                    }
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
