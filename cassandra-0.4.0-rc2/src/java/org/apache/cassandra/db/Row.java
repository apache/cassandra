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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.utils.FBUtilities;

public class Row
{
    private static Logger logger_ = Logger.getLogger(Row.class);
    private String table_;
    private static RowSerializer serializer = new RowSerializer();

    static RowSerializer serializer()
    {
        return serializer;
    }

    public Row(String table, String key) {
        assert table != null;
        this.table_ = table;
        this.key_ = key;
    }

    // only for use by RMVH
    Row()
    {
    }

    public String getTable() {
        return table_;
    }

    private String key_;

    private Map<String, ColumnFamily> columnFamilies_ = new HashMap<String, ColumnFamily>();

    public String key()
    {
        return key_;
    }

    void setKey(String key)
    {
        key_ = key;
    }

    public void setTable(String table)
    {
        table_ = table;
    }

    public Set<String> getColumnFamilyNames()
    {
        return columnFamilies_.keySet();
    }

    public Collection<ColumnFamily> getColumnFamilies()
    {
        return columnFamilies_.values();
    }

    public ColumnFamily getColumnFamily(String cfName)
    {
        return columnFamilies_.get(cfName);
    }

    void addColumnFamily(ColumnFamily columnFamily)
    {
        columnFamilies_.put(columnFamily.name(), columnFamily);
    }

    void removeColumnFamily(ColumnFamily columnFamily)
    {
        columnFamilies_.remove(columnFamily.name());
    }

    public boolean isEmpty()
    {
        return (columnFamilies_.size() == 0);
    }

    /*
     * This function will repair the current row with the input row
     * what that means is that if there are any differences between the 2 rows then
     * this function will make the current row take the latest changes.
     */
    public void repair(Row rowOther)
    {
        for (ColumnFamily cfOld : rowOther.getColumnFamilies())
        {
            ColumnFamily cf = columnFamilies_.get(cfOld.name());
            if (cf == null)
            {
                addColumnFamily(cfOld);
            }
            else
            {
                columnFamilies_.remove(cf.name());
                addColumnFamily(ColumnFamily.resolve(Arrays.asList(cfOld, cf)));
            }
        }
    }

    /*
     * This function will calculate the difference between 2 rows
     * and return the resultant row. This assumes that the row that
     * is being submitted is a super set of the current row so
     * it only calculates additional
     * difference and does not take care of what needs to be removed from the current row to make
     * it same as the input row.
     */
    public Row diff(Row rowComposite)
    {
        Row rowDiff = new Row(table_, key_);

        for (ColumnFamily cfComposite : rowComposite.getColumnFamilies())
        {
            ColumnFamily cf = columnFamilies_.get(cfComposite.name());
            if (cf == null)
                rowDiff.addColumnFamily(cfComposite);
            else
            {
                ColumnFamily cfDiff = cf.diff(cfComposite);
                if (cfDiff != null)
                    rowDiff.addColumnFamily(cfDiff);
            }
        }
        if (rowDiff.getColumnFamilies().isEmpty())
            return null;
        else
            return rowDiff;
    }

    public Row cloneMe()
    {
        Row row = new Row(table_, key_);
        row.columnFamilies_ = new HashMap<String, ColumnFamily>(columnFamilies_);
        return row;
    }

    public byte[] digest()
    {
        Set<String> cfamilies = columnFamilies_.keySet();
        byte[] xorHash = ArrayUtils.EMPTY_BYTE_ARRAY;
        for (String cFamily : cfamilies)
        {
            if (xorHash.length == 0)
            {
                xorHash = columnFamilies_.get(cFamily).digest();
            }
            else
            {
                xorHash = FBUtilities.xor(xorHash, columnFamilies_.get(cFamily).digest());
            }
        }
        return xorHash;
    }

    void clear()
    {
        columnFamilies_.clear();
    }

    public String toString()
    {
        return "Row(" + key_ + " [" + StringUtils.join(columnFamilies_.values(), ", ") + ")]";
    }
}

class RowSerializer implements ICompactSerializer<Row>
{
    public void serialize(Row row, DataOutputStream dos) throws IOException
    {
        dos.writeUTF(row.getTable());
        dos.writeUTF(row.key());
        Collection<ColumnFamily> columnFamilies = row.getColumnFamilies();
        int size = columnFamilies.size();
        dos.writeInt(size);

        for (ColumnFamily cf : columnFamilies)
        {
            ColumnFamily.serializer().serialize(cf, dos);
        }
    }

    public Row deserialize(DataInputStream dis) throws IOException
    {
        String table = dis.readUTF();
        String key = dis.readUTF();
        Row row = new Row(table, key);
        int size = dis.readInt();

        for (int i = 0; i < size; ++i)
        {
            ColumnFamily cf = ColumnFamily.serializer().deserialize(dis);
            row.addColumnFamily(cf);
        }
        return row;
    }
}
