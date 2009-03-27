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

import java.util.*;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.ByteArrayOutputStream;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;


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
    protected Map<String, ColumnFamily> deletions_ = new HashMap<String, ColumnFamily>();

    /* Ctor for JAXB */
    private RowMutation()
    {
    }

    public RowMutation(String table, String key)
    {
        table_ = table;
        key_ = key;
    }

    public RowMutation(String table, Row row)
    {
        table_ = table;
        key_ = row.key();
        Map<String, ColumnFamily> cfSet = row.getColumnFamilyMap();
        Set<String> keyset = cfSet.keySet();
        for(String cfName : keyset)
        {
        	add(cfName, cfSet.get(cfName));
        }
    }

    protected RowMutation(String table, String key, Map<String, ColumnFamily> modifications, Map<String, ColumnFamily> deletions)
    {
    	table_ = table;
    	key_ = key;
    	modifications_ = modifications;
    	deletions_ = deletions;
    }

    public static String[] getColumnAndColumnFamily(String cf)
    {
        return cf.split(":");
    }

    String table()
    {
        return table_;
    }

    public String key()
    {
        return key_;
    }

    void addHints(String hint) throws IOException, ColumnFamilyNotDefinedException
    {
        String cfName = Table.hints_ + ":" + hint;
        add(cfName, new byte[0]);
    }

    /*
     * Specify a column family name and the corresponding column
     * family object.
     * param @ cf - column family name
     * param @ columnFamily - the column family.
    */
    public void add(String cf, ColumnFamily columnFamily)
    {
        modifications_.put(cf, columnFamily);
    }

    /*
     * Specify a column name and a corresponding value for
     * the column. Column name is specified as <column family>:column.
     * This will result in a ColumnFamily associated with
     * <column family> as name and a Column with <column>
     * as name.
     *
     * param @ cf - column name as <column family>:<column>
     * param @ value - value associated with the column
    */
    public void add(String cf, byte[] value) throws IOException, ColumnFamilyNotDefinedException
    {
        add(cf, value, 0);
    }

    /*
     * Specify a column name and a corresponding value for
     * the column. Column name is specified as <column family>:column.
     * This will result in a ColumnFamily associated with
     * <column family> as name and a Column with <column>
     * as name. The columan can be further broken up
     * as super column name : columnname  in case of super columns
     *
     * param @ cf - column name as <column family>:<column>
     * param @ value - value associated with the column
     * param @ timestamp - ts associated with this data.
    */
    public void add(String cf, byte[] value, long timestamp)
    {
        String[] values = RowMutation.getColumnAndColumnFamily(cf);

        if ( values.length == 0 || values.length == 1 || values.length > 3 )
            throw new IllegalArgumentException("Column Family " + cf + " in invalid format. Must be in <column family>:<column> format.");

        ColumnFamily columnFamily = modifications_.get(values[0]);
        if( values.length == 2 )
        {
            if ( columnFamily == null )
            {
            	columnFamily = new ColumnFamily(values[0], ColumnFamily.getColumnType("Standard"));
            }
        	columnFamily.addColumn(values[1], value, timestamp);
        }
        if( values.length == 3 )
        {
            if ( columnFamily == null )
            {
            	columnFamily = new ColumnFamily(values[0], ColumnFamily.getColumnType("Super"));
            }
        	columnFamily.addColumn(values[1]+ ":" + values[2], value, timestamp);
        }
        modifications_.put(values[0], columnFamily);
    }

    public void delete(String columnFamilyColumn, long timestamp)
    {
        String[] values = RowMutation.getColumnAndColumnFamily(columnFamilyColumn);
        String cfName = values[0];
        if (modifications_.containsKey(cfName))
        {
            throw new IllegalArgumentException("ColumnFamily " + cfName + " is already being modified");
        }

        if (values.length == 0 || values.length > 3)
            throw new IllegalArgumentException("Column Family " + columnFamilyColumn + " in invalid format. Must be in <column family>:<column> format.");

        ColumnFamily columnFamily = modifications_.get(cfName);
        if (columnFamily == null)
            columnFamily = new ColumnFamily(cfName);
        if (values.length == 2)
        {
            columnFamily.addColumn(values[1], ArrayUtils.EMPTY_BYTE_ARRAY, timestamp, true);
        }
        else if (values.length == 3)
        {
            columnFamily.addColumn(values[1] + ":" + values[2], ArrayUtils.EMPTY_BYTE_ARRAY, timestamp, true);
        }
        else
        {
            assert values.length == 1;
            columnFamily.delete(timestamp);
        }
        modifications_.put(cfName, columnFamily);
    }


    /*
     * This is equivalent to calling commit. Applies the changes to
     * to the table that is obtained by calling Table.open().
    */
    public void apply() throws IOException, ColumnFamilyNotDefinedException
    {
        Row row = new Row(key_);
        apply(row);
    }

    /*
     * Allows RowMutationVerbHandler to optimize by re-using a single Row object.
    */
    void apply(Row emptyRow) throws IOException, ColumnFamilyNotDefinedException
    {
        assert emptyRow.getColumnFamilyMap().size() == 0;
        Table table = Table.open(table_);
        for (String cfName : modifications_.keySet())
        {
            if (!table.isValidColumnFamily(cfName))
                throw new ColumnFamilyNotDefinedException("Column Family " + cfName + " has not been defined.");
            emptyRow.addColumnFamily(modifications_.get(cfName));
        }
        table.apply(emptyRow);
    }

    /*
     * This is equivalent to calling commit. Applies the changes to
     * to the table that is obtained by calling Table.open().
    */
    void load(Row row) throws IOException, ColumnFamilyNotDefinedException
    {
        Table table = Table.open(table_);
        Set<String> cfNames = modifications_.keySet();
        for (String cfName : cfNames )
        {
            if ( !table.isValidColumnFamily(cfName) )
                throw new ColumnFamilyNotDefinedException("Column Family " + cfName + " has not been defined.");
            row.addColumnFamily( modifications_.get(cfName) );
        }
        table.load(row);
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
        EndPoint from = (local != null) ? local : new EndPoint(FBUtilities.getHostName(), 7000);
        return new Message(from, StorageService.mutationStage_, verbHandlerName, bos.toByteArray());
    }
}

class RowMutationSerializer implements ICompactSerializer<RowMutation>
{
	private void freezeTheMaps(Map<String, ColumnFamily> map, DataOutputStream dos) throws IOException
	{
		int size = map.size();
        dos.writeInt(size);
        if ( size > 0 )
        {
            Set<String> keys = map.keySet();
            for( String key : keys )
            {
            	dos.writeUTF(key);
                ColumnFamily cf = map.get(key);
                if ( cf != null )
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

        /* serialize the deletions_ in the mutation */
        freezeTheMaps(rm.deletions_, dos);
	}

	private Map<String, ColumnFamily> defreezeTheMaps(DataInputStream dis) throws IOException
	{
		Map<String, ColumnFamily> map = new HashMap<String, ColumnFamily>();
        int size = dis.readInt();
        for ( int i = 0; i < size; ++i )
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

    	/* Defreeze the modifications_ map */
    	Map<String, ColumnFamily> modifications = defreezeTheMaps(dis);

    	/* Defreeze the deletions_ map */
    	Map<String, ColumnFamily> deletions = defreezeTheMaps(dis);

    	return new RowMutation(table, key, modifications, deletions);
    }
}
