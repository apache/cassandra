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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.thrift.TException;

public class SSTableImportTest
{
    public static final String KEYSPACE1 = "SSTableImportTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_COUNTER = "Counter1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException, IOException, TException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD),
                                    CFMetaData.denseCFMetaData(KEYSPACE1, CF_COUNTER, BytesType.instance).defaultValidator(CounterColumnType.instance));
    }

    @Test
    public void testImportSimpleCf() throws IOException, URISyntaxException
    {
        // Import JSON to temp SSTable file
        String jsonUrl = resourcePath("SimpleCF.json");
        File tempSS = tempSSTableFile(KEYSPACE1, "Standard1");
        new SSTableImport(true).importJson(jsonUrl, KEYSPACE1, "Standard1", tempSS.getPath());

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
        File tempSS = tempSSTableFile(KEYSPACE1, "Standard1");

        new SSTableImport().importJson(jsonUrl, KEYSPACE1, "Standard1", tempSS.getPath());

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
        File tempSS = tempSSTableFile(KEYSPACE1, "Standard1");
        new SSTableImport(true).importJson(jsonUrl, KEYSPACE1, "Standard1", tempSS.getPath());

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
        File tempSS = tempSSTableFile(KEYSPACE1, "Counter1");
        new SSTableImport(true).importJson(jsonUrl, KEYSPACE1, "Counter1", tempSS.getPath());

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
