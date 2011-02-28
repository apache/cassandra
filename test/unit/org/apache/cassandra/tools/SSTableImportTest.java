/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;

import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;

import static org.apache.cassandra.io.sstable.SSTableUtils.tempSSTableFile;
import static org.junit.Assert.assertEquals;

import org.apache.cassandra.Util;

import org.apache.cassandra.utils.FBUtilities;
import org.json.simple.parser.ParseException;
import org.junit.Test;

public class SSTableImportTest extends SchemaLoader
{   
    @Test
    public void testImportSimpleCf() throws IOException
    {
        // Import JSON to temp SSTable file
        String jsonUrl = getClass().getClassLoader().getResource("SimpleCF.json").getPath();
        File tempSS = tempSSTableFile("Keyspace1", "Standard1");
        SSTableImport.importJson(jsonUrl, "Keyspace1", "Standard1", tempSS.getPath());

        // Verify results
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(tempSS.getPath()));
        QueryFilter qf = QueryFilter.getIdentityFilter(Util.dk("rowA"), new QueryPath("Standard1"));
        IColumnIterator iter = qf.getSSTableColumnIterator(reader);
        ColumnFamily cf = iter.getColumnFamily();
        while (iter.hasNext()) cf.addColumn(iter.next());
        assert cf.getColumn(ByteBufferUtil.bytes("colAA")).value().equals(hexToBytes("76616c4141"));
        assert !(cf.getColumn(ByteBufferUtil.bytes("colAA")) instanceof DeletedColumn);
        IColumn expCol = cf.getColumn(ByteBufferUtil.bytes("colAC"));
        assert expCol.value().equals(hexToBytes("76616c4143"));
        assert expCol instanceof ExpiringColumn;
        assert ((ExpiringColumn)expCol).getTimeToLive() == 42 && expCol.getLocalDeletionTime() == 2000000000;
    }

    @Test
    public void testImportSimpleCfOldFormat() throws IOException
    {
        // Import JSON to temp SSTable file
        String jsonUrl = getClass().getClassLoader().getResource("SimpleCF.oldformat.json").getPath();
        File tempSS = tempSSTableFile("Keyspace1", "Standard1");
        SSTableImport.importJson(jsonUrl, "Keyspace1", "Standard1", tempSS.getPath());

        // Verify results
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(tempSS.getPath()));
        QueryFilter qf = QueryFilter.getIdentityFilter(Util.dk("rowA"), new QueryPath("Standard1"));
        IColumnIterator iter = qf.getSSTableColumnIterator(reader);
        ColumnFamily cf = iter.getColumnFamily();
        while (iter.hasNext()) cf.addColumn(iter.next());
        assert cf.getColumn(ByteBufferUtil.bytes("colAA")).value().equals(hexToBytes("76616c4141"));
        assert !(cf.getColumn(ByteBufferUtil.bytes("colAA")) instanceof DeletedColumn);
        IColumn expCol = cf.getColumn(ByteBufferUtil.bytes("colAC"));
        assert expCol.value().equals(hexToBytes("76616c4143"));
        assert expCol instanceof ExpiringColumn;
        assert ((ExpiringColumn)expCol).getTimeToLive() == 42 && expCol.getLocalDeletionTime() == 2000000000;
    }

    @Test
    public void testImportSuperCf() throws IOException, ParseException
    {
        String jsonUrl = getClass().getClassLoader().getResource("SuperCF.json").getPath();
        File tempSS = tempSSTableFile("Keyspace1", "Super4");
        SSTableImport.importJson(jsonUrl, "Keyspace1", "Super4", tempSS.getPath());
        
        // Verify results
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(tempSS.getPath()));
        QueryFilter qf = QueryFilter.getNamesFilter(Util.dk("rowA"), new QueryPath("Super4", null, null), ByteBufferUtil.bytes("superA"));
        ColumnFamily cf = qf.getSSTableColumnIterator(reader).getColumnFamily();
        IColumn superCol = cf.getColumn(ByteBufferUtil.bytes("superA"));
        assert superCol != null;
        assert superCol.getSubColumns().size() > 0;
        IColumn subColumn = superCol.getSubColumn(ByteBufferUtil.bytes("636f6c4141"));
        assert subColumn.value().equals(hexToBytes("76616c75654141"));
    }

    @Test
    public void testImportUnsortedMode() throws IOException
    {
        String jsonUrl = getClass().getClassLoader().getResource("UnsortedSuperCF.json").getPath();
        File tempSS = tempSSTableFile("Keyspace1", "Super4");

        ColumnFamily columnFamily = ColumnFamily.create("Keyspace1", "Super4");
        IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();

        SSTableImport.setKeyCountToImport(3);
        int result = SSTableImport.importSorted(jsonUrl, columnFamily, tempSS.getPath(), partitioner);
        assert result == -1;
    }

    @Test
    public void testImportCounterCf() throws IOException
    {
        // Import JSON to temp SSTable file
        String jsonUrl = getClass().getClassLoader().getResource("CounterCF.json").getPath();
        File tempSS = tempSSTableFile("Keyspace1", "Counter1");
        SSTableImport.importJson(jsonUrl, "Keyspace1", "Counter1", tempSS.getPath());

        // Verify results
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(tempSS.getPath()));
        QueryFilter qf = QueryFilter.getIdentityFilter(Util.dk("rowA"), new QueryPath("Counter1"));
        IColumnIterator iter = qf.getSSTableColumnIterator(reader);
        ColumnFamily cf = iter.getColumnFamily();
        while (iter.hasNext()) cf.addColumn(iter.next());
        IColumn c = cf.getColumn(ByteBufferUtil.bytes("colAA"));
        assert c instanceof CounterColumn: c;
        assert ((CounterColumn) c).total() == 42;
    }
}
