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

import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.io.sstable.SSTableUtils.tempSSTableFile;
import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SSTableImportTest extends SchemaLoader
{
    @Test
    public void testImportSimpleCf() throws IOException, URISyntaxException
    {
        // Import JSON to temp SSTable file
        String jsonUrl = resourcePath("SimpleCF.json");
        File tempSS = tempSSTableFile("Keyspace1", "Standard1");
        new SSTableImport(true).importJson(jsonUrl, "Keyspace1", "Standard1", tempSS.getPath());

        // Verify results
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(tempSS.getPath()));
        QueryFilter qf = QueryFilter.getIdentityFilter(Util.dk("rowA"), "Standard1", System.currentTimeMillis());
        OnDiskAtomIterator iter = qf.getSSTableColumnIterator(reader);
        ColumnFamily cf = cloneForAdditions(iter);
        while (iter.hasNext()) cf.addAtom(iter.next());
        assert cf.getColumn(Util.cellname("colAA")).value().equals(hexToBytes("76616c4141"));
        assert !(cf.getColumn(Util.cellname("colAA")) instanceof DeletedCell);
        Cell expCol = cf.getColumn(Util.cellname("colAC"));
        assert expCol.value().equals(hexToBytes("76616c4143"));
        assert expCol instanceof ExpiringCell;
        assert ((ExpiringCell)expCol).getTimeToLive() == 42 && expCol.getLocalDeletionTime() == 2000000000;
    }

    private ColumnFamily cloneForAdditions(OnDiskAtomIterator iter)
    {
        return iter.getColumnFamily().cloneMeShallow(ArrayBackedSortedColumns.factory, false);
    }

    private String resourcePath(String name) throws URISyntaxException
    {
        // Naive resource.getPath fails on Windows in many cases, for example if there are spaces in the path
        // which get encoded as %20 which Windows doesn't like. The trick is to create a URI first, which satisfies all platforms.
        return new URI(getClass().getClassLoader().getResource(name).toString()).getPath();
    }

    @Test
    public void testImportSimpleCfOldFormat() throws IOException, URISyntaxException
    {
        // Import JSON to temp SSTable file
        String jsonUrl = resourcePath("SimpleCF.oldformat.json");
        File tempSS = tempSSTableFile("Keyspace1", "Standard1");
        new SSTableImport(true).importJson(jsonUrl, "Keyspace1", "Standard1", tempSS.getPath());

        // Verify results
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(tempSS.getPath()));
        QueryFilter qf = QueryFilter.getIdentityFilter(Util.dk("rowA"), "Standard1", System.currentTimeMillis());
        OnDiskAtomIterator iter = qf.getSSTableColumnIterator(reader);
        ColumnFamily cf = cloneForAdditions(iter);
        while (iter.hasNext()) cf.addAtom(iter.next());
        assert cf.getColumn(Util.cellname("colAA")).value().equals(hexToBytes("76616c4141"));
        assert !(cf.getColumn(Util.cellname("colAA")) instanceof DeletedCell);
        Cell expCol = cf.getColumn(Util.cellname("colAC"));
        assert expCol.value().equals(hexToBytes("76616c4143"));
        assert expCol instanceof ExpiringCell;
        assert ((ExpiringCell)expCol).getTimeToLive() == 42 && expCol.getLocalDeletionTime() == 2000000000;
    }

    @Test
    public void testImportSuperCf() throws IOException, URISyntaxException
    {
        String jsonUrl = resourcePath("SuperCF.json");
        File tempSS = tempSSTableFile("Keyspace1", "Super4");
        new SSTableImport(true, true).importJson(jsonUrl, "Keyspace1", "Super4", tempSS.getPath());

        // Verify results
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(tempSS.getPath()));
        QueryFilter qf = QueryFilter.getIdentityFilter(Util.dk("rowA"), "Super4", System.currentTimeMillis());
        ColumnFamily cf = cloneForAdditions(qf.getSSTableColumnIterator(reader));
        qf.collateOnDiskAtom(cf, qf.getSSTableColumnIterator(reader), Integer.MIN_VALUE);

        DeletionTime delTime = cf.deletionInfo().deletionTimeFor(cf.getComparator().make(ByteBufferUtil.bytes("superA")));
        assertEquals("supercolumn deletion time did not match the expected time", new DeletionInfo(0, 0), new DeletionInfo(delTime));
        Cell subCell = cf.getColumn(Util.cellname("superA", "636f6c4141"));
        assert subCell.value().equals(hexToBytes("76616c75654141"));
    }

    @Test
    public void testImportUnsortedDataWithSortedOptionFails() throws IOException, URISyntaxException
    {
        String jsonUrl = resourcePath("UnsortedSuperCF.json");
        File tempSS = tempSSTableFile("Keyspace1", "Super4");

        int result = new SSTableImport(3,true, true).importJson(jsonUrl, "Keyspace1","Super4", tempSS.getPath());
        assert result == -1;
    }

    @Test
    public void testImportUnsortedMode() throws IOException, URISyntaxException
    {
        String jsonUrl = resourcePath("UnsortedCF.json");
        File tempSS = tempSSTableFile("Keyspace1", "Standard1");

        new SSTableImport().importJson(jsonUrl, "Keyspace1", "Standard1", tempSS.getPath());

        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(tempSS.getPath()));
        QueryFilter qf = QueryFilter.getIdentityFilter(Util.dk("rowA"), "Standard1", System.currentTimeMillis());
        OnDiskAtomIterator iter = qf.getSSTableColumnIterator(reader);
        ColumnFamily cf = cloneForAdditions(iter);
        while (iter.hasNext())
            cf.addAtom(iter.next());
        assert cf.getColumn(Util.cellname("colAA")).value().equals(hexToBytes("76616c4141"));
        assert !(cf.getColumn(Util.cellname("colAA")) instanceof DeletedCell);
        Cell expCol = cf.getColumn(Util.cellname("colAC"));
        assert expCol.value().equals(hexToBytes("76616c4143"));
        assert expCol instanceof ExpiringCell;
        assert ((ExpiringCell) expCol).getTimeToLive() == 42 && expCol.getLocalDeletionTime() == 2000000000;
    }

    @Test
    public void testImportWithDeletionInfoMetadata() throws IOException, URISyntaxException
    {
        // Import JSON to temp SSTable file
        String jsonUrl = resourcePath("SimpleCFWithDeletionInfo.json");
        File tempSS = tempSSTableFile("Keyspace1", "Standard1");
        new SSTableImport(true).importJson(jsonUrl, "Keyspace1", "Standard1", tempSS.getPath());

        // Verify results
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(tempSS.getPath()));
        QueryFilter qf = QueryFilter.getIdentityFilter(Util.dk("rowA"), "Standard1", System.currentTimeMillis());
        OnDiskAtomIterator iter = qf.getSSTableColumnIterator(reader);
        ColumnFamily cf = cloneForAdditions(iter);
        assertEquals(cf.deletionInfo(), new DeletionInfo(0, 0));
        while (iter.hasNext())
            cf.addAtom(iter.next());
        assert cf.getColumn(Util.cellname("colAA")).value().equals(hexToBytes("76616c4141"));
        assert !(cf.getColumn(Util.cellname("colAA")) instanceof DeletedCell);
        Cell expCol = cf.getColumn(Util.cellname("colAC"));
        assert expCol.value().equals(hexToBytes("76616c4143"));
        assert expCol instanceof ExpiringCell;
        assert ((ExpiringCell) expCol).getTimeToLive() == 42 && expCol.getLocalDeletionTime() == 2000000000;
    }

    @Test
    public void testImportCounterCf() throws IOException, URISyntaxException
    {
        // Import JSON to temp SSTable file
        String jsonUrl = resourcePath("CounterCF.json");
        File tempSS = tempSSTableFile("Keyspace1", "Counter1");
        new SSTableImport(true).importJson(jsonUrl, "Keyspace1", "Counter1", tempSS.getPath());

        // Verify results
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(tempSS.getPath()));
        QueryFilter qf = QueryFilter.getIdentityFilter(Util.dk("rowA"), "Counter1", System.currentTimeMillis());
        OnDiskAtomIterator iter = qf.getSSTableColumnIterator(reader);
        ColumnFamily cf = cloneForAdditions(iter);
        while (iter.hasNext()) cf.addAtom(iter.next());
        Cell c = cf.getColumn(Util.cellname("colAA"));
        assert c instanceof CounterCell : c;
        assert ((CounterCell) c).total() == 42;
    }
}
