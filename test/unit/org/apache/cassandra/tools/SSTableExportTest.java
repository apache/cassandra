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
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.db.BufferCounterCell;
import org.apache.cassandra.db.BufferExpiringCell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.thrift.TException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.io.sstable.SSTableUtils.tempSSTableFile;
import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SSTableExportTest
{
    public static final String KEYSPACE1 = "SSTableExportTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_COUNTER = "Counter1";
    public static final String CF_UUID = "UUIDKeys";
    public static final String CF_VALSWITHQUOTES = "ValuesWithQuotes";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD),
                                    CFMetaData.denseCFMetaData(KEYSPACE1, CF_COUNTER, BytesType.instance).defaultValidator(CounterColumnType.instance),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_UUID).keyValidator(UUIDType.instance),
                                    CFMetaData.denseCFMetaData(KEYSPACE1, CF_VALSWITHQUOTES, BytesType.instance).defaultValidator(UTF8Type.instance),
                                    SchemaLoader.standardCFMD(KEYSPACE1, "AsciiKeys").keyValidator(AsciiType.instance));
    }

    public String asHex(String str)
    {
        return bytesToHex(ByteBufferUtil.bytes(str));
    }
    
    @Test
    public void testEnumeratekeys() throws IOException
    {
        File tempSS = tempSSTableFile(KEYSPACE1, "Standard1");
        ColumnFamily cfamily = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "Standard1");
        SSTableWriter writer = SSTableWriter.create(tempSS.getPath(), 2, ActiveRepairService.UNREPAIRED_SSTABLE, 0);

        // Add rowA
        cfamily.addColumn(Util.cellname("colA"), ByteBufferUtil.bytes("valA"), System.currentTimeMillis());
        writer.append(Util.dk("rowA"), cfamily);
        cfamily.clear();

        // Add rowB
        cfamily.addColumn(Util.cellname("colB"), ByteBufferUtil.bytes("valB"), System.currentTimeMillis());
        writer.append(Util.dk("rowB"), cfamily);
        cfamily.clear();

        writer.closeAndOpenReader();

        // Enumerate and verify
        File temp = File.createTempFile("Standard1", ".txt");
        final Descriptor descriptor = Descriptor.fromFilename(writer.getFilename());
        SSTableExport.enumeratekeys(descriptor, new PrintStream(temp.getPath()),
                CFMetaData.sparseCFMetaData(descriptor.ksname, descriptor.cfname, BytesType.instance));


        try (FileReader file = new FileReader(temp))
        {
            char[] buf = new char[(int) temp.length()];
            file.read(buf);
            String output = new String(buf);
    
            String sep = System.getProperty("line.separator");
            assert output.equals(asHex("rowA") + sep + asHex("rowB") + sep) : output;
        }
    }

    @Test
    public void testExportSimpleCf() throws IOException, ParseException
    {
        File tempSS = tempSSTableFile(KEYSPACE1, "Standard1");
        ColumnFamily cfamily = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "Standard1");
        SSTableWriter writer = SSTableWriter.create(tempSS.getPath(), 2, ActiveRepairService.UNREPAIRED_SSTABLE, 0);

        int nowInSec = (int)(System.currentTimeMillis() / 1000) + 42; //live for 42 seconds
        // Add rowA
        cfamily.addColumn(Util.cellname("colA"), ByteBufferUtil.bytes("valA"), System.currentTimeMillis());
        cfamily.addColumn(new BufferExpiringCell(Util.cellname("colExp"), ByteBufferUtil.bytes("valExp"), System.currentTimeMillis(), 42, nowInSec));
        writer.append(Util.dk("rowA"), cfamily);
        cfamily.clear();

        // Add rowB
        cfamily.addColumn(Util.cellname("colB"), ByteBufferUtil.bytes("valB"), System.currentTimeMillis());
        writer.append(Util.dk("rowB"), cfamily);
        cfamily.clear();

        // Add rowExclude
        cfamily.addColumn(Util.cellname("colX"), ByteBufferUtil.bytes("valX"), System.currentTimeMillis());
        writer.append(Util.dk("rowExclude"), cfamily);
        cfamily.clear();

        SSTableReader reader = writer.closeAndOpenReader();

        // Export to JSON and verify
        File tempJson = File.createTempFile("Standard1", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()), new String[]{asHex("rowExclude")});

        JSONArray json = (JSONArray)JSONValue.parseWithException(new FileReader(tempJson));
        assertEquals("unexpected number of rows", 2, json.size());

        JSONObject rowA = (JSONObject)json.get(0);
        assertEquals("unexpected number of keys", 2, rowA.keySet().size());
        assertEquals("unexpected row key",asHex("rowA"),rowA.get("key"));

        JSONArray colsA = (JSONArray)rowA.get("cells");
        JSONArray colA = (JSONArray)colsA.get(0);
        assert hexToBytes((String)colA.get(1)).equals(ByteBufferUtil.bytes("valA"));

        JSONArray colExp = (JSONArray)colsA.get(1);
        assert ((Long)colExp.get(4)) == 42;
        assert ((Long)colExp.get(5)) == nowInSec;

        JSONObject rowB = (JSONObject)json.get(1);
        assertEquals("unexpected number of keys", 2, rowB.keySet().size());
        assertEquals("unexpected row key",asHex("rowB"),rowB.get("key"));

        JSONArray colsB = (JSONArray)rowB.get("cells");
        JSONArray colB = (JSONArray)colsB.get(0);
        assert colB.size() == 3;

    }

    @Test
    public void testRoundTripStandardCf() throws IOException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard1");
        File tempSS = tempSSTableFile(KEYSPACE1, "Standard1");
        ColumnFamily cfamily = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "Standard1");
        SSTableWriter writer = SSTableWriter.create(tempSS.getPath(), 2, ActiveRepairService.UNREPAIRED_SSTABLE, 0);

        // Add rowA
        cfamily.addColumn(Util.cellname("name"), ByteBufferUtil.bytes("val"), System.currentTimeMillis());
        writer.append(Util.dk("rowA"), cfamily);
        cfamily.clear();

        // Add rowExclude
        cfamily.addColumn(Util.cellname("name"), ByteBufferUtil.bytes("val"), System.currentTimeMillis());
        writer.append(Util.dk("rowExclude"), cfamily);
        cfamily.clear();

        SSTableReader reader = writer.closeAndOpenReader();

        // Export to JSON and verify
        File tempJson = File.createTempFile("Standard1", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()), new String[]{asHex("rowExclude")});

        // Import JSON to another SSTable file
        File tempSS2 = tempSSTableFile(KEYSPACE1, "Standard1");
        new SSTableImport().importJson(tempJson.getPath(), KEYSPACE1, "Standard1", tempSS2.getPath());

        reader = SSTableReader.open(Descriptor.fromFilename(tempSS2.getPath()));
        QueryFilter qf = Util.namesQueryFilter(cfs, Util.dk("rowA"), "name");
        ColumnFamily cf = qf.getSSTableColumnIterator(reader).getColumnFamily();
        qf.collateOnDiskAtom(cf, qf.getSSTableColumnIterator(reader), Integer.MIN_VALUE);
        assertNotNull(cf);
        assertEquals(hexToBytes("76616c"), cf.getColumn(Util.cellname("name")).value());

        qf = Util.namesQueryFilter(cfs, Util.dk("rowExclude"), "name");
        cf = qf.getSSTableColumnIterator(reader).getColumnFamily();
        assert cf == null;
        reader.selfRef().release();
    }

    @Test
    public void testExportCounterCf() throws IOException, ParseException
    {
        File tempSS = tempSSTableFile(KEYSPACE1, "Counter1");
        ColumnFamily cfamily = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "Counter1");
        SSTableWriter writer = SSTableWriter.create(tempSS.getPath(), 2, ActiveRepairService.UNREPAIRED_SSTABLE, 0);

        // Add rowA
        cfamily.addColumn(BufferCounterCell.createLocal(Util.cellname("colA"), 42, System.currentTimeMillis(), Long.MIN_VALUE));
        writer.append(Util.dk("rowA"), cfamily);
        cfamily.clear();

        SSTableReader reader = writer.closeAndOpenReader();

        // Export to JSON and verify
        File tempJson = File.createTempFile("Counter1", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()), new String[0]);
        JSONArray json = (JSONArray)JSONValue.parseWithException(new FileReader(tempJson));
        assertEquals("unexpected number of rows", 1, json.size());

        JSONObject row = (JSONObject)json.get(0);
        assertEquals("unexpected number of keys", 2, row.keySet().size());
        assertEquals("unexpected row key",asHex("rowA"),row.get("key"));

        JSONArray cols = (JSONArray)row.get("cells");
        JSONArray colA = (JSONArray)cols.get(0);
        assert hexToBytes((String)colA.get(0)).equals(ByteBufferUtil.bytes("colA"));
        assert ((String) colA.get(3)).equals("c");
        assert (Long) colA.get(4) == Long.MIN_VALUE;
    }

    @Test
    public void testEscapingDoubleQuotes() throws IOException, ParseException
    {
        File tempSS = tempSSTableFile(KEYSPACE1, "ValuesWithQuotes");
        ColumnFamily cfamily = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "ValuesWithQuotes");
        SSTableWriter writer = SSTableWriter.create(tempSS.getPath(), 2, ActiveRepairService.UNREPAIRED_SSTABLE);

        // Add rowA
        cfamily.addColumn(new BufferCell(Util.cellname("data"), UTF8Type.instance.fromString("{\"foo\":\"bar\"}")));
        writer.append(Util.dk("rowA"), cfamily);
        cfamily.clear();

        SSTableReader reader = writer.closeAndOpenReader();

        // Export to JSON and verify
        File tempJson = File.createTempFile("ValuesWithQuotes", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()), new String[0]);

        JSONArray json = (JSONArray)JSONValue.parseWithException(new FileReader(tempJson));
        assertEquals("unexpected number of rows", 1, json.size());

        JSONObject row = (JSONObject)json.get(0);
        assertEquals("unexpected number of keys", 2, row.keySet().size());
        assertEquals("unexpected row key",asHex("rowA"),row.get("key"));

        JSONArray cols = (JSONArray)row.get("cells");
        JSONArray colA = (JSONArray)cols.get(0);
        assert hexToBytes((String)colA.get(0)).equals(ByteBufferUtil.bytes("data"));
        assert colA.get(1).equals("{\"foo\":\"bar\"}");
    }

    @Test
    public void testExportColumnsWithMetadata() throws IOException, ParseException
    {
        File tempSS = tempSSTableFile(KEYSPACE1, "Standard1");
        ColumnFamily cfamily = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "Standard1");
        SSTableWriter writer = SSTableWriter.create(tempSS.getPath(), 2, ActiveRepairService.UNREPAIRED_SSTABLE);

        // Add rowA
        cfamily.addColumn(Util.cellname("colName"), ByteBufferUtil.bytes("val"), System.currentTimeMillis());
        cfamily.addColumn(Util.cellname("colName1"), ByteBufferUtil.bytes("val1"), System.currentTimeMillis());
        cfamily.delete(new DeletionInfo(0, 0));
        writer.append(Util.dk("rowA"), cfamily);

        SSTableReader reader = writer.closeAndOpenReader();
        // Export to JSON and verify
        File tempJson = File.createTempFile("CFWithDeletionInfo", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()), new String[0]);

        JSONArray json = (JSONArray)JSONValue.parseWithException(new FileReader(tempJson));
        assertEquals("unexpected number of rows", 1, json.size());

        JSONObject row = (JSONObject)json.get(0);
        assertEquals("unexpected number of keys", 3, row.keySet().size());
        assertEquals("unexpected row key",asHex("rowA"),row.get("key"));

        // check that the row key is there and present
        String rowKey = (String) row.get("key");
        assertNotNull("expecing key to be present", rowKey);
        assertEquals("key did not match", ByteBufferUtil.bytes("rowA"), hexToBytes(rowKey));

        // check that there is metadata and that it contains deletionInfo
        JSONObject meta = (JSONObject) row.get("metadata");
        assertNotNull("expecing metadata to be present", meta);

        assertEquals("unexpected number of metadata entries", 1, meta.keySet().size());

        JSONObject serializedDeletionInfo = (JSONObject) meta.get("deletionInfo");
        assertNotNull("expecing deletionInfo to be present", serializedDeletionInfo);

        assertEquals(
                "unexpected serialization format for topLevelDeletion",
                "{\"markedForDeleteAt\":0,\"localDeletionTime\":0}",
                serializedDeletionInfo.toJSONString());

        // check the colums are what we put in
        JSONArray cols = (JSONArray) row.get("cells");
        assertNotNull("expecing columns to be present", cols);
        assertEquals("expecting two columns", 2, cols.size());

        JSONArray col1 = (JSONArray) cols.get(0);
        assertEquals("column name did not match", ByteBufferUtil.bytes("colName"), hexToBytes((String) col1.get(0)));
        assertEquals("column value did not match", ByteBufferUtil.bytes("val"), hexToBytes((String) col1.get(1)));

        JSONArray col2 = (JSONArray) cols.get(1);
        assertEquals("column name did not match", ByteBufferUtil.bytes("colName1"), hexToBytes((String) col2.get(0)));
        assertEquals("column value did not match", ByteBufferUtil.bytes("val1"), hexToBytes((String) col2.get(1)));
    }

    /**
     * Tests CASSANDRA-6892 (key aliases being used improperly for validation)
     */
    @Test
    public void testColumnNameEqualToDefaultKeyAlias() throws IOException, ParseException
    {
        File tempSS = tempSSTableFile(KEYSPACE1, "UUIDKeys");
        ColumnFamily cfamily = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "UUIDKeys");
        SSTableWriter writer = SSTableWriter.create(tempSS.getPath(), 2, ActiveRepairService.UNREPAIRED_SSTABLE);

        // Add a row
        cfamily.addColumn(column(CFMetaData.DEFAULT_KEY_ALIAS, "not a uuid", 1L));
        writer.append(Util.dk(ByteBufferUtil.bytes(UUIDGen.getTimeUUID())), cfamily);

        SSTableReader reader = writer.closeAndOpenReader();
        // Export to JSON and verify
        File tempJson = File.createTempFile("CFWithColumnNameEqualToDefaultKeyAlias", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()), new String[0]);

        JSONArray json = (JSONArray)JSONValue.parseWithException(new FileReader(tempJson));
        assertEquals(1, json.size());

        JSONObject row = (JSONObject)json.get(0);
        JSONArray cols = (JSONArray) row.get("cells");
        assertEquals(1, cols.size());

        // check column name and value
        JSONArray col = (JSONArray) cols.get(0);
        assertEquals(CFMetaData.DEFAULT_KEY_ALIAS, ByteBufferUtil.string(hexToBytes((String) col.get(0))));
        assertEquals("not a uuid", ByteBufferUtil.string(hexToBytes((String) col.get(1))));
    }

    @Test
    public void testAsciiKeyValidator() throws IOException, ParseException
    {
        File tempSS = tempSSTableFile(KEYSPACE1, "AsciiKeys");
        ColumnFamily cfamily = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "AsciiKeys");
        SSTableWriter writer = SSTableWriter.create(tempSS.getPath(), 2, ActiveRepairService.UNREPAIRED_SSTABLE, 0);

        // Add a row
        cfamily.addColumn(column("column", "value", 1L));
        writer.append(Util.dk("key", AsciiType.instance), cfamily);

        SSTableReader reader = writer.closeAndOpenReader();
        // Export to JSON and verify
        File tempJson = File.createTempFile("CFWithAsciiKeys", ".json");
        SSTableExport.export(reader,
                             new PrintStream(tempJson.getPath()),
                             new String[0]);

        JSONArray json = (JSONArray)JSONValue.parseWithException(new FileReader(tempJson));
        assertEquals(1, json.size());

        JSONObject row = (JSONObject)json.get(0);
        // check row key
        assertEquals("key", row.get("key"));
    }
}
