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

import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;

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
        assert !(cf.getColumn(Util.cellname("colAA")) instanceof BufferDeletedCell);
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
        assert !(cf.getColumn(Util.cellname("colAA")) instanceof BufferDeletedCell);
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
        assert !(cf.getColumn(Util.cellname("colAA")) instanceof BufferDeletedCell);
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

    @Test
    public void testImportWithAsciiKeyValidator() throws IOException, URISyntaxException
    {
        // Import JSON to temp SSTable file
        String jsonUrl = resourcePath("SimpleCF.json");
        File tempSS = tempSSTableFile("Keyspace1", "AsciiKeys");
        new SSTableImport(true).importJson(jsonUrl, "Keyspace1", "AsciiKeys", tempSS.getPath());

        // Verify results
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(tempSS.getPath()));
        // check that keys are treated as ascii
        QueryFilter qf = QueryFilter.getIdentityFilter(Util.dk("726f7741", AsciiType.instance), "AsciiKeys", System.currentTimeMillis());
        OnDiskAtomIterator iter = qf.getSSTableColumnIterator(reader);
        assert iter.hasNext(); // "ascii" key exists
        QueryFilter qf2 = QueryFilter.getIdentityFilter(Util.dk("726f7741", BytesType.instance), "AsciiKeys", System.currentTimeMillis());
        OnDiskAtomIterator iter2 = qf2.getSSTableColumnIterator(reader);
        assert !iter2.hasNext(); // "bytes" key does not exist
    }

    @Test
    public void testBackwardCompatibilityOfImportWithAsciiKeyValidator() throws IOException, URISyntaxException
    {
        // Import JSON to temp SSTable file
        String jsonUrl = resourcePath("SimpleCF.json");
        File tempSS = tempSSTableFile("Keyspace1", "AsciiKeys");
        // To ignore current key validator
        System.setProperty("skip.key.validator", "true");
        new SSTableImport(true).importJson(jsonUrl, "Keyspace1", "AsciiKeys", tempSS.getPath());

        // Verify results
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(tempSS.getPath()));
        // check that keys are treated as bytes
        QueryFilter qf = QueryFilter.getIdentityFilter(Util.dk("rowA"), "AsciiKeys", System.currentTimeMillis());
        OnDiskAtomIterator iter = qf.getSSTableColumnIterator(reader);
        assert iter.hasNext(); // "bytes" key exists
    }
}
